from __future__ import annotations

from typing import Any

import numpy as np

from nimbuschain_mask_service.models import MaskWriterMetadata
from nimbuschain_shared.zarr import ConversionError


def storage_mode_from_metadata(
    *,
    metadata: MaskWriterMetadata | dict[str, Any] | None,
    output_uri: str,
) -> str:
    payload = (
        metadata.to_dict()
        if isinstance(metadata, MaskWriterMetadata)
        else dict(metadata or {})
    )
    explicit = str(payload.get("storage_mode") or "").strip()
    if explicit:
        return explicit
    input_zarr_uri = str(payload.get("input_zarr_uri") or "").strip()
    output_zarr_uri = str(payload.get("output_zarr_uri") or output_uri).strip()
    if input_zarr_uri and input_zarr_uri == output_zarr_uri:
        return "in_place_zarr_masking"
    return "derived_zarr_copy"


def ensure_writable_target(group: Any, name: str, *, overwrite: bool) -> None:
    if name not in group:
        return
    if not overwrite:
        raise ConversionError(
            f"Cloud output '{name}' already exists. Set overwrite=true to replace it."
        )
    del group[name]


def as_time_y_x(array: np.ndarray, *, dtype: Any) -> np.ndarray:
    data = np.asarray(array, dtype=dtype)
    if data.ndim == 2:
        return data[np.newaxis, :, :]
    if data.ndim == 3:
        return data
    raise ConversionError(
        f"Cloud output array must be 2D or 3D (time,y,x). Got shape={data.shape}."
    )


__all__ = ["as_time_y_x", "ensure_writable_target", "storage_mode_from_metadata"]
