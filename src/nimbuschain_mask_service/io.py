from __future__ import annotations

import shutil

from nimbuschain_mask_service.channel_reader import (
    read_required_channels,
    read_required_channels_window,
)
from nimbuschain_mask_service.derived_store import (
    cleanup_derived_zarr,
    _copytree_via_cp,
    _copytree_via_python,
    _copytree_via_rsync,
    promote_derived_zarr,
    temporary_derived_zarr_uri,
)
from nimbuschain_mask_service.path_resolution import local_path_for_uri
from nimbuschain_mask_service.zarr_context import (
    ZarrMaskContext,
    delete_mask_layers,
    open_zarr_group,
    read_context,
)
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


__all__ = [
    "ZarrMaskContext",
    "cleanup_derived_zarr",
    "copy_source_zarr",
    "delete_mask_layers",
    "local_path_for_uri",
    "open_zarr_group",
    "promote_derived_zarr",
    "read_context",
    "read_required_channels",
    "read_required_channels_window",
    "temporary_derived_zarr_uri",
]
