from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import uuid

from nimbuschain_mask_service.path_resolution import local_path_for_uri
from nimbuschain_shared.zarr import ConversionError


def copy_source_zarr(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    source_path = local_path_for_uri(source_zarr_uri)
    output_path = local_path_for_uri(output_zarr_uri)
    if not source_path.exists():
        raise ConversionError(f"Source Zarr store not found: {source_zarr_uri}")
    if not source_path.is_dir():
        raise ConversionError(f"Source Zarr store must be a directory: {source_zarr_uri}")
    if source_path.resolve() == output_path.resolve():
        raise ConversionError("Masked Zarr output must differ from the source Zarr store.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        shutil.rmtree(output_path)
    copy_errors: list[str] = []
    for copier in (_copytree_via_cp, _copytree_via_rsync, _copytree_via_python):
        try:
            copier(source_path=source_path, output_path=output_path)
            return str(output_path)
        except Exception as exc:
            copy_errors.append(f"{copier.__name__}: {exc}")
            try:
                if output_path.exists():
                    shutil.rmtree(output_path)
            except OSError:
                pass
    raise ConversionError(
        "Failed to create derived masked Zarr copy. " + " | ".join(copy_errors)
    )


def cleanup_derived_zarr(output_zarr_uri: str) -> None:
    target = local_path_for_uri(output_zarr_uri)
    try:
        if target.exists():
            shutil.rmtree(target)
    except OSError:
        return


def temporary_derived_zarr_uri(output_zarr_uri: str) -> str:
    target = local_path_for_uri(output_zarr_uri)
    suffix = target.suffix
    tmp_name = f".{target.stem}.tmp-{uuid.uuid4().hex}{suffix}"
    return str(target.with_name(tmp_name))


def promote_derived_zarr(*, temp_zarr_uri: str, final_zarr_uri: str, overwrite: bool = True) -> str:
    temp_path = local_path_for_uri(temp_zarr_uri)
    final_path = local_path_for_uri(final_zarr_uri)
    if not temp_path.exists():
        raise ConversionError(f"Temporary masked Zarr store not found: {temp_zarr_uri}")
    if temp_path.resolve() == final_path.resolve():
        raise ConversionError("Temporary and final masked Zarr paths must differ.")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path: Path | None = None
    if final_path.exists():
        if not overwrite:
            raise ConversionError(f"Masked Zarr store already exists: {final_zarr_uri}")
        backup_path = final_path.with_name(
            f".{final_path.stem}.backup-{uuid.uuid4().hex}{final_path.suffix}"
        )
        if backup_path.exists():
            shutil.rmtree(backup_path)
        final_path.rename(backup_path)
    try:
        temp_path.rename(final_path)
    except Exception:
        if backup_path is not None and backup_path.exists() and not final_path.exists():
            backup_path.rename(final_path)
        raise
    if backup_path is not None and backup_path.exists():
        shutil.rmtree(backup_path)
    return str(final_path)


def _copytree_via_cp(*, source_path: Path, output_path: Path) -> None:
    cp_binary = shutil.which("cp")
    if not cp_binary:
        raise RuntimeError("cp is not available")
    output_path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [cp_binary, "-a", "--reflink=auto", f"{source_path}/.", str(output_path)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _copytree_via_rsync(*, source_path: Path, output_path: Path) -> None:
    rsync_binary = shutil.which("rsync")
    if not rsync_binary:
        raise RuntimeError("rsync is not available")
    output_path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [rsync_binary, "-a", f"{source_path}/", f"{output_path}/"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _copytree_via_python(*, source_path: Path, output_path: Path) -> None:
    shutil.copytree(source_path, output_path)


__all__ = [
    "cleanup_derived_zarr",
    "copy_source_zarr",
    "promote_derived_zarr",
    "temporary_derived_zarr_uri",
]
