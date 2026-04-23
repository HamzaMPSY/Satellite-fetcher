from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from nimbuschain_fetch.oci_auth import (
    default_oci_config_path,
    default_oci_profile,
    default_oci_region,
    resolve_oci_auth_mode,
)


class OCIStorageError(RuntimeError):
    """Raised when OCI-backed storage cannot be used."""


try:
    import oci  # type: ignore
    import ocifs  # type: ignore

    OCI_SUPPORT_AVAILABLE = True
except ImportError:
    oci = None
    ocifs = None
    OCI_SUPPORT_AVAILABLE = False


@dataclass(frozen=True)
class OCIPath:
    bucket: str
    namespace: str | None
    path: str


def is_oci_uri(uri: str) -> bool:
    return urlparse(str(uri or "")).scheme.lower() == "oci"


def parse_oci_uri(uri: str) -> OCIPath:
    parsed = urlparse(str(uri or "").strip())
    if parsed.scheme.lower() != "oci":
        raise OCIStorageError(f"Unsupported OCI URI: {uri}")

    netloc = parsed.netloc.strip()
    if not netloc:
        raise OCIStorageError(f"Missing OCI bucket in URI: {uri}")

    bucket, namespace = netloc, None
    if "@" in netloc:
        bucket, namespace = netloc.split("@", 1)
        bucket = bucket.strip()
        namespace = namespace.strip() or None

    if not bucket:
        raise OCIStorageError(f"Missing OCI bucket in URI: {uri}")

    object_path = parsed.path.lstrip("/")
    if not object_path:
        raise OCIStorageError(f"Missing OCI object path in URI: {uri}")

    return OCIPath(
        bucket=bucket,
        namespace=namespace or os.getenv("NIMBUS_OCI_NAMESPACE"),
        path=object_path,
    )


class OCIStore:
    """Thin OCI Object Storage adapter for remote Zarr and raw bundle access."""

    def __init__(
        self,
        *,
        bucket: str,
        namespace: str | None = None,
        compartment_id: str | None = None,
        config_path: str | None = None,
        profile: str | None = None,
        auth_mode: str | None = None,
    ) -> None:
        if not OCI_SUPPORT_AVAILABLE:
            raise OCIStorageError(
                "OCI support requires optional dependencies 'ocifs' and 'oci'."
            )

        self.bucket = bucket
        self._namespace = namespace
        self.compartment_id = compartment_id or os.getenv("NIMBUS_OCI_COMPARTMENT_ID")
        self.auth_mode = resolve_oci_auth_mode(auth_mode)
        self.config_path = default_oci_config_path(config_path)
        self.profile = default_oci_profile(profile)
        self.region = default_oci_region()
        self._fs = None
        self._signer = None
        self._oci_config = None

    @classmethod
    def from_uri(cls, uri: str) -> tuple["OCIStore", OCIPath]:
        parsed = parse_oci_uri(uri)
        store = cls(bucket=parsed.bucket, namespace=parsed.namespace)
        return store, parsed

    def _get_auth(self) -> tuple[dict[str, Any], Any]:
        if self._oci_config is None:
            if self.auth_mode == "instance_principal":
                self._signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
                effective_region = str(
                    self.region
                    or getattr(self._signer, "region", "")
                    or ""
                ).strip()
                self._oci_config = {"region": effective_region} if effective_region else {}
            else:
                self._oci_config = oci.config.from_file(self.config_path, self.profile)
                token_file = self._oci_config.get("security_token_file")
                if token_file:
                    with open(token_file, "r", encoding="utf-8") as handle:
                        token = handle.read()
                    private_key = oci.signer.load_private_key_from_file(self._oci_config["key_file"])
                    self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        return self._oci_config, self._signer

    @property
    def fs(self):  # type: ignore[no-untyped-def]
        if self._fs is None:
            config, signer = self._get_auth()
            kwargs: dict[str, Any] = {}
            if self._namespace:
                kwargs["namespace"] = self._namespace
            if signer is not None:
                self._fs = ocifs.OCIFileSystem(config=config, signer=signer, **kwargs)
            else:
                self._fs = ocifs.OCIFileSystem(
                    config=self.config_path,
                    profile=self.profile,
                    **kwargs,
                )
        return self._fs

    @property
    def namespace(self) -> str:
        if self._namespace is None:
            config, signer = self._get_auth()
            if signer is not None:
                client = oci.object_storage.ObjectStorageClient(config, signer=signer)
            else:
                client = oci.object_storage.ObjectStorageClient(config)
            if self.compartment_id:
                self._namespace = client.get_namespace(compartment_id=self.compartment_id).data
            else:
                self._namespace = client.get_namespace().data
        return self._namespace

    def _full_path(self, path: str) -> str:
        return f"{self.bucket}@{self.namespace}/{str(path).lstrip('/')}"

    def info(self, path: str) -> dict[str, Any] | None:
        try:
            return dict(self.fs.info(self._full_path(path)))
        except FileNotFoundError:
            return None

    def exists(self, path: str) -> bool:
        try:
            return bool(self.fs.exists(self._full_path(path)))
        except FileNotFoundError:
            return False

    def is_dir(self, path: str) -> bool:
        info = self.info(path)
        if info and str(info.get("type", "")).lower() in {"directory", "dir"}:
            return True
        try:
            listing = self.fs.ls(self._full_path(path), detail=True)
        except FileNotFoundError:
            return False
        return bool(listing)

    def is_file(self, path: str) -> bool:
        info = self.info(path)
        return bool(info and str(info.get("type", "")).lower() == "file")

    def delete(self, path: str, *, recursive: bool = True) -> None:
        full_path = self._full_path(path)
        if self.fs.exists(full_path):
            self.fs.rm(full_path, recursive=recursive)

    def get_mapper(self, path: str, *, create: bool = False):
        from fsspec.mapping import FSMap

        return FSMap(self._full_path(path), self.fs, check=False, create=create)

    def open(self, path: str, mode: str = "rb"):
        return self.fs.open(self._full_path(path), mode)

    def find(self, path: str) -> list[str]:
        full_path = self._full_path(path)
        try:
            items = list(self.fs.find(full_path))
        except FileNotFoundError:
            return []
        return [str(item) for item in items if not str(item).endswith("/")]

    def listdir(self, path: str, *, detail: bool = False) -> list[Any]:
        full_path = self._full_path(path)
        try:
            items = list(self.fs.ls(full_path, detail=detail))
        except FileNotFoundError:
            return []
        if detail:
            normalized: list[dict[str, Any]] = []
            for item in items:
                entry = dict(item)
                entry["name"] = str(entry.get("name") or "")
                normalized.append(entry)
            return normalized
        return [str(item) for item in items]

    def download_file(self, path: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self.open(path, "rb") as source, destination.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        return destination

    def download_tree(self, path: str, destination_root: Path) -> Path:
        destination_root.mkdir(parents=True, exist_ok=True)
        prefix = self._full_path(path).rstrip("/")
        files = self.find(path)
        if not files and self.is_file(path):
            return self.download_file(path, destination_root / Path(path).name)

        for remote_name in files:
            relative = remote_name[len(prefix):].lstrip("/") if remote_name.startswith(prefix) else Path(remote_name).name
            local_path = destination_root / relative
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with self.fs.open(remote_name, "rb") as source, local_path.open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
        return destination_root


def oci_support_status() -> dict[str, Any]:
    return {
        "available": OCI_SUPPORT_AVAILABLE,
        "auth_mode": resolve_oci_auth_mode(),
        "config_path": default_oci_config_path(),
        "profile": default_oci_profile(),
        "region": default_oci_region(),
        "namespace": os.getenv("NIMBUS_OCI_NAMESPACE"),
    }
