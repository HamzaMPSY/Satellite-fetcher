from __future__ import annotations

import mimetypes
import os
from pathlib import Path

from nimbuschain_fetch.oci_auth import (
    default_oci_config_path,
    default_oci_profile,
    default_oci_region,
    resolve_oci_auth_mode,
)


class OCIObjectStorageUploader:
    """Upload local files to OCI Object Storage using config/profile or instance principal auth."""

    def __init__(
        self,
        *,
        bucket: str,
        config_path: str | None = None,
        profile: str | None = None,
        namespace: str | None = None,
        verify_bucket_access: bool = True,
        auth_mode: str | None = None,
    ) -> None:
        if not bucket.strip():
            raise ValueError("OCI bucket is required.")

        self.bucket = bucket.strip()
        self.auth_mode = resolve_oci_auth_mode(auth_mode)
        self.config_path = default_oci_config_path(config_path)
        self.profile = default_oci_profile(profile)

        oci = self._load_oci_module()
        self._oci = oci
        if self.auth_mode == "instance_principal":
            self._config = {}
            self._signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            region = str(
                default_oci_region()
                or getattr(self._signer, "region", "")
                or ""
            ).strip()
            if region:
                self._config["region"] = region
        else:
            config = oci.config.from_file(
                file_location=self.config_path,
                profile_name=self.profile,
            )
            self._config = config
            self._signer = self._build_signer(oci, config)
        if self._signer is None:
            self._client = oci.object_storage.ObjectStorageClient(config=self._config)
        else:
            self._client = oci.object_storage.ObjectStorageClient(
                config=self._config,
                signer=self._signer,
            )

        configured_namespace = str(self._config.get("namespace", "") or "").strip()
        self.namespace = (namespace or configured_namespace or self._resolve_namespace()).strip()
        if verify_bucket_access:
            self._client.get_bucket(
                namespace_name=self.namespace,
                bucket_name=self.bucket,
            )

    @staticmethod
    def _load_oci_module():
        try:
            import oci  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "OCI support requires the 'oci' package. Install project dependencies again."
            ) from exc
        return oci

    @staticmethod
    def _build_signer(oci, config):
        token_path = str(config.get("security_token_file", "") or "").strip()
        if not token_path:
            return None

        token_path = os.path.expanduser(token_path)
        if not os.path.exists(token_path):
            raise RuntimeError(
                f"OCI security token file does not exist: {token_path}. "
                "Run 'oci session authenticate' again."
            )

        with open(token_path, "r", encoding="utf-8") as handle:
            token = handle.read()

        token_container = oci.auth.security_token_container.SecurityTokenContainer(None, token)
        if not token_container.valid():
            raise RuntimeError(
                "OCI session token has expired for the selected profile. "
                "Run 'oci session authenticate' again."
            )

        key_file = os.path.expanduser(str(config.get("key_file", "") or ""))
        if not key_file or not os.path.exists(key_file):
            raise RuntimeError(f"OCI key_file is missing for profile: {key_file}")

        private_key = oci.signer.load_private_key_from_file(key_file)
        return oci.auth.signers.SecurityTokenSigner(token, private_key)

    def _resolve_namespace(self) -> str:
        try:
            return str(self._client.get_namespace().data)
        except Exception:
            compartment_id = str(self._config.get("tenancy", "") or "").strip()
            if not compartment_id:
                raise
            return str(self._client.get_namespace(compartment_id=compartment_id).data)

    @staticmethod
    def _sanitize_object_key(value: str) -> str:
        parts = [part.strip() for part in value.replace("\\", "/").split("/") if part.strip()]
        return "/".join(parts)

    @classmethod
    def build_object_key(
        cls,
        *,
        prefix: str | None,
        local_path: str | Path,
        object_name: str | None = None,
    ) -> str:
        prefix_value = cls._sanitize_object_key(prefix or "")
        name_value = object_name or Path(local_path).name
        relative_value = cls._sanitize_object_key(str(name_value))
        if prefix_value and relative_value:
            return f"{prefix_value}/{relative_value}"
        return prefix_value or relative_value

    def uri_for(self, object_key: str) -> str:
        return f"oci://{self.bucket}@{self.namespace}/{self._sanitize_object_key(object_key)}"

    def upload_file(
        self,
        local_path: str | Path,
        *,
        prefix: str | None = None,
        object_name: str | None = None,
        content_type: str | None = None,
    ) -> str:
        local_file = Path(local_path)
        if not local_file.exists() or not local_file.is_file():
            raise FileNotFoundError(f"Cannot upload missing file: {local_file}")

        object_key = self.build_object_key(
            prefix=prefix,
            local_path=local_file,
            object_name=object_name,
        )
        guessed_type = content_type or mimetypes.guess_type(local_file.name)[0]
        extra_args = {"content_type": guessed_type} if guessed_type else {}

        with local_file.open("rb") as handle:
            self._client.put_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket,
                object_name=object_key,
                put_object_body=handle,
                **extra_args,
            )
        return self.uri_for(object_key)
