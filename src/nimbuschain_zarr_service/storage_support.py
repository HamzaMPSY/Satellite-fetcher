from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import unquote, urlparse
import shutil
import tarfile
import zipfile

from nimbuschain_shared.zarr import ConversionDependencyError, ConversionError
from nimbuschain_zarr_service.oci_storage import OCIStorageError, OCIStore, is_oci_uri


class CleanupBundle:
    """Composable cleanup handler for staged and extracted temporary sources."""

    def __init__(self, *entries: TemporaryDirectory[str]) -> None:
        self._entries: list[TemporaryDirectory[str]] = list(entries)

    def add(self, entry: TemporaryDirectory[str]) -> None:
        self._entries.append(entry)

    def cleanup(self) -> None:
        while self._entries:
            entry = self._entries.pop()
            try:
                entry.cleanup()
            except Exception:
                continue


@dataclass(frozen=True)
class PreparedSource:
    root: Path
    source_kind: str
    raw_path: Path
    cleanup: CleanupBundle | TemporaryDirectory[str] | None = None


@dataclass(frozen=True)
class TargetGrid:
    height: int
    width: int
    crs: str | None
    transform: list[float] | tuple[float, ...]
    pixel_size: list[float] | tuple[float, ...] | None = None
    reference_band: str | None = None


def resolve_local_path(raw_uri: str) -> Path:
    if raw_uri.startswith("file://"):
        parsed = urlparse(raw_uri)
        candidate = Path(unquote(parsed.path)).expanduser().resolve()
        if candidate.exists():
            return candidate
        mapped = fallback_mounted_data_path(candidate)
        if mapped is not None:
            return mapped
        return candidate
    parsed = urlparse(raw_uri)
    if parsed.scheme:
        raise ConversionError("Only local file paths are supported by the Zarr converter in v1.")
    candidate = Path(raw_uri).expanduser().resolve()
    if candidate.exists():
        return candidate
    mapped = fallback_mounted_data_path(candidate)
    if mapped is not None:
        return mapped
    return candidate


def prepare_source(raw_uri: str, *, label: str) -> PreparedSource:
    cleanup_bundle: CleanupBundle | None = None
    if is_remote_uri(raw_uri):
        raw_path, cleanup_bundle = stage_remote_source(raw_uri, label=label)
    else:
        raw_path = resolve_local_path(raw_uri)
    if not raw_path.exists():
        raise ConversionError(f"{label} source not found: {raw_path}")

    if raw_path.is_dir():
        return PreparedSource(
            root=raw_path,
            source_kind="directory",
            raw_path=raw_path,
            cleanup=cleanup_bundle,
        )

    if raw_path.is_file() and raw_path.suffix.lower() == ".nc":
        if cleanup_bundle is not None:
            return PreparedSource(
                root=raw_path.parent,
                source_kind="netcdf",
                raw_path=raw_path,
                cleanup=cleanup_bundle,
            )
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        copied = Path(tmp_dir.name) / raw_path.name
        shutil.copy2(raw_path, copied)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="netcdf",
            raw_path=raw_path,
            cleanup=CleanupBundle(tmp_dir),
        )

    if zipfile.is_zipfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with zipfile.ZipFile(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        cleanup = cleanup_bundle or CleanupBundle()
        cleanup.add(tmp_dir)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="zip",
            raw_path=raw_path,
            cleanup=cleanup,
        )

    if tarfile.is_tarfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with tarfile.open(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        cleanup = cleanup_bundle or CleanupBundle()
        cleanup.add(tmp_dir)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="tar",
            raw_path=raw_path,
            cleanup=cleanup,
        )

    raise ConversionError(f"Unsupported {label} source. Expected a directory, zip, or tar archive.")


def fallback_mounted_data_path(candidate: Path) -> Path | None:
    parts = list(candidate.parts)
    for idx in range(len(parts) - 1):
        if parts[idx] == "data" and parts[idx + 1] == "downloads":
            suffix = parts[idx + 2 :]
            mapped = Path("/data/downloads").joinpath(*suffix)
            if mapped.exists():
                return mapped
            return None
    return None


def fallback_mounted_data_output(candidate: Path) -> Path | None:
    parts = list(candidate.parts)
    for idx in range(len(parts)):
        if parts[idx] == "data":
            suffix = parts[idx + 1 :]
            if not suffix:
                return Path("/data")
            return Path("/data").joinpath(*suffix)
    return None


def resolve_output_path(output_uri: str) -> Path:
    if output_uri.startswith("file://"):
        parsed = urlparse(output_uri)
        candidate = Path(unquote(parsed.path)).expanduser().resolve()
        if candidate.exists() or candidate.parent.exists():
            return candidate
        mapped = fallback_mounted_data_output(candidate)
        return mapped if mapped is not None else candidate
    parsed = urlparse(output_uri)
    if parsed.scheme:
        raise ConversionError("Only local file paths can be resolved with resolve_output_path().")
    candidate = Path(output_uri).expanduser().resolve()
    if candidate.exists() or candidate.parent.exists():
        return candidate
    mapped = fallback_mounted_data_output(candidate)
    return mapped if mapped is not None else candidate


def is_remote_uri(uri: str) -> bool:
    parsed = urlparse(str(uri or "").strip())
    return bool(parsed.scheme and parsed.scheme.lower() not in {"", "file"})


def stage_remote_source(raw_uri: str, *, label: str) -> tuple[Path, CleanupBundle]:
    if is_oci_uri(raw_uri):
        return _stage_oci_source(raw_uri, label=label)
    raise ConversionError(f"Unsupported remote source URI: {raw_uri}")


def _stage_oci_source(raw_uri: str, *, label: str) -> tuple[Path, CleanupBundle]:
    try:
        store, parsed = OCIStore.from_uri(raw_uri)
    except OCIStorageError as exc:
        raise ConversionDependencyError(str(exc)) from exc

    staging_dir = TemporaryDirectory(prefix=f"nimbus_{label}_oci_")
    cleanup = CleanupBundle(staging_dir)
    staging_root = Path(staging_dir.name)

    if store.is_file(parsed.path):
        local_path = staging_root / Path(parsed.path).name
        store.download_file(parsed.path, local_path)
        return local_path, cleanup

    if store.is_dir(parsed.path):
        dest_name = Path(parsed.path.rstrip("/")).name or "source"
        local_root = staging_root / dest_name
        store.download_tree(parsed.path, local_root)
        return local_root, cleanup

    raise ConversionError(f"OCI source not found: {raw_uri}")


def prepare_output_store(output_uri: str) -> tuple[Any, str]:
    if is_oci_uri(output_uri):
        try:
            store, parsed = OCIStore.from_uri(output_uri)
        except OCIStorageError as exc:
            raise ConversionDependencyError(str(exc)) from exc
        store.delete(parsed.path, recursive=True)
        return store.get_mapper(parsed.path, create=True), output_uri

    output_path = resolve_output_path(output_uri)
    if output_path.exists() and output_path.is_file():
        raise ConversionError(f"Output path is a file, expected a directory: {output_path}")
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path, str(output_path)


def open_existing_output_store(output_uri: str) -> Any:
    if is_oci_uri(output_uri):
        try:
            store, parsed = OCIStore.from_uri(output_uri)
        except OCIStorageError as exc:
            raise ConversionDependencyError(str(exc)) from exc
        if not store.exists(parsed.path):
            raise ConversionError(f"Output store does not exist yet: {output_uri}")
        return store.get_mapper(parsed.path, create=False)

    output_path = resolve_output_path(output_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    return output_path
