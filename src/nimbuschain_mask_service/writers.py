from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np

from nimbuschain_mask_service.schema import WATER_MASK_CLASSES, WATER_MASK_NAME, WATER_MASK_PATH
from nimbuschain_zarr_service.core import (
    ChunkShape,
    ConversionDependencyError,
    ConversionError,
    _coerce_timestamp,
    _open_existing_output_store,
)


def write_binary_mask_to_zarr(
    *,
    output_uri: str,
    mask_name: str,
    mask_path: str,
    mask: np.ndarray,
    acquisition_datetime: str | None = None,
    model_name: str,
    model_version: str | None = None,
    input_bands: list[str] | None = None,
    classes: dict[str, str] | None = None,
    overwrite: bool = True,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "Mask writing dependencies are unavailable "
            f"({exc}). Ensure zarr is installed in the runtime."
        ) from exc

    array = np.asarray(mask)
    if array.ndim == 3 and array.shape[0] == 1:
        array = array[0]
    if array.ndim != 2:
        raise ConversionError(
            f"Binary mask '{mask_name}' must be 2D or shaped (1, y, x), got {array.shape}."
        )

    array = array.astype(np.uint8, copy=False)
    unique_values = {int(value) for value in np.unique(array)}
    if not unique_values.issubset({0, 1}):
        raise ConversionError(
            f"Binary mask '{mask_name}' must contain only values 0 or 1, got {sorted(unique_values)}."
        )

    output_store = _open_existing_output_store(output_uri)
    root = zarr.open_group(output_store, mode="a", zarr_format=2)
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    expected_height = int(imagery.shape[2])
    expected_width = int(imagery.shape[3])
    if array.shape != (expected_height, expected_width):
        raise ConversionError(
            f"Binary mask '{mask_name}' shape mismatch: expected "
            f"({expected_height}, {expected_width}), got {array.shape}."
        )

    masks_group = root.require_group("masks")
    chunk_spec = ChunkShape()
    chunks = (
        1,
        min(chunk_spec.y, expected_height),
        min(chunk_spec.x, expected_width),
    )
    target_name = mask_path.split("/", 1)[1] if "/" in mask_path else mask_path
    if overwrite and target_name in masks_group:
        del masks_group[target_name]
    target = masks_group.create_array(
        target_name,
        shape=(1, expected_height, expected_width),
        chunks=chunks,
        dtype=np.uint8,
        overwrite=overwrite,
    )
    target[0, :, :] = array

    timestamp = _coerce_timestamp(acquisition_datetime or datetime.now(timezone.utc).isoformat())
    target.attrs.update(
        {
            "mask_name": mask_name,
            "mask_path": mask_path,
            "classes": dict(classes or WATER_MASK_CLASSES),
            "model_name": model_name,
            "model_version": model_version,
            "input_bands": list(input_bands or []),
            "written_at": timestamp.isoformat(),
            "metadata": dict(metadata or {}),
        }
    )

    artifact_uri = str((metadata or {}).get("artifact_uri") or "").strip()
    status_path = str((metadata or {}).get("status_path") or "").strip()
    work_dir = str((metadata or {}).get("work_dir") or "").strip()
    input_zarr_uri = str((metadata or {}).get("input_zarr_uri") or "").strip()
    output_zarr_uri = str((metadata or {}).get("output_zarr_uri") or output_uri).strip()
    storage_mode = str((metadata or {}).get("storage_mode") or "").strip()

    root.attrs["water_mask_path"] = WATER_MASK_PATH
    root.attrs["water_mask_written"] = True
    root.attrs["water_mask_status"] = "written"
    root.attrs["water_mask_reason"] = ""
    root.attrs["water_mask_artifact_uri"] = artifact_uri
    root.attrs["water_mask_status_path"] = status_path
    root.attrs["water_mask_work_dir"] = work_dir
    root.attrs["water_mask_source_zarr_uri"] = input_zarr_uri
    root.attrs["water_mask_output_zarr_uri"] = output_zarr_uri
    root.attrs["water_mask_storage_mode"] = storage_mode
    zarr.consolidate_metadata(output_store)
    return {
        "mask_name": mask_name,
        "mask_path": mask_path,
        "shape": [1, expected_height, expected_width],
        "dtype": "uint8",
        "classes": dict(classes or WATER_MASK_CLASSES),
        "model_name": model_name,
        "model_version": model_version,
        "input_bands": list(input_bands or []),
        "written_at": timestamp.isoformat(),
        "unique_values": sorted(unique_values),
        "input_zarr_uri": input_zarr_uri,
        "output_zarr_uri": output_zarr_uri,
        "storage_mode": storage_mode or "in_place_zarr_enrichment",
    }


def write_water_mask_to_zarr(
    *,
    output_uri: str,
    mask: np.ndarray,
    acquisition_datetime: str | None = None,
    model_name: str = "omniwatermask",
    model_version: str | None = None,
    input_bands: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return write_binary_mask_to_zarr(
        output_uri=output_uri,
        mask_name=WATER_MASK_NAME,
        mask_path=WATER_MASK_PATH,
        mask=mask,
        acquisition_datetime=acquisition_datetime,
        model_name=model_name,
        model_version=model_version,
        input_bands=input_bands,
        classes=WATER_MASK_CLASSES,
        metadata=metadata,
    )


def write_water_mask_tiles_to_zarr(
    *,
    output_uri: str,
    tiles: list[dict[str, int]],
    mask_paths: list[str],
    height: int,
    width: int,
    acquisition_datetime: str | None = None,
    model_name: str = "omniwatermask",
    model_version: str | None = None,
    input_bands: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import rasterio
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "Mask writing dependencies are unavailable "
            f"({exc}). Ensure zarr and rasterio are installed in the runtime."
        ) from exc

    if len(tiles) != len(mask_paths):
        raise ConversionError(
            f"Water-mask tile mismatch: got {len(tiles)} tiles and {len(mask_paths)} mask raster(s)."
        )

    output_store = _open_existing_output_store(output_uri)
    root = zarr.open_group(output_store, mode="a", zarr_format=2)
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")
    expected_height = int(imagery.shape[2])
    expected_width = int(imagery.shape[3])
    if expected_height != int(height) or expected_width != int(width):
        raise ConversionError(
            f"Water-mask canvas mismatch: expected ({expected_height}, {expected_width}), "
            f"got ({height}, {width})."
        )

    masks_group = root.require_group("masks")
    chunk_spec = ChunkShape()
    chunks = (1, min(chunk_spec.y, expected_height), min(chunk_spec.x, expected_width))
    target_name = WATER_MASK_PATH.split("/", 1)[1]
    if target_name in masks_group:
        del masks_group[target_name]
    target = masks_group.create_array(
        target_name,
        shape=(1, expected_height, expected_width),
        chunks=chunks,
        dtype=np.uint8,
        overwrite=True,
    )

    unique_values: set[int] = set()
    for tile, mask_path in zip(tiles, mask_paths, strict=True):
        row_start = int(tile["row_start"])
        row_stop = int(tile["row_stop"])
        col_start = int(tile["col_start"])
        col_stop = int(tile["col_stop"])
        expected_shape = (row_stop - row_start, col_stop - col_start)
        with rasterio.open(mask_path) as src:
            tile_mask = src.read(1)
        if tuple(tile_mask.shape) != expected_shape:
            raise ConversionError(
                f"Binary mask tile shape mismatch for '{mask_path}': expected {expected_shape}, got {tuple(tile_mask.shape)}."
            )
        tile_mask = np.asarray(tile_mask, dtype=np.uint8)
        unique_values.update(int(value) for value in np.unique(tile_mask))
        target[0, row_start:row_stop, col_start:col_stop] = tile_mask

    if not unique_values.issubset({0, 1}):
        raise ConversionError(
            f"Binary mask '{WATER_MASK_NAME}' must contain only values 0 or 1, got {sorted(unique_values)}."
        )

    timestamp = _coerce_timestamp(acquisition_datetime or datetime.now(timezone.utc).isoformat())
    target.attrs.update(
        {
            "mask_name": WATER_MASK_NAME,
            "mask_path": WATER_MASK_PATH,
            "classes": dict(WATER_MASK_CLASSES),
            "model_name": model_name,
            "model_version": model_version,
            "input_bands": list(input_bands or []),
            "written_at": timestamp.isoformat(),
            "metadata": dict(metadata or {}),
        }
    )

    artifact_uri = str((metadata or {}).get("artifact_uri") or "").strip()
    status_path = str((metadata or {}).get("status_path") or "").strip()
    work_dir = str((metadata or {}).get("work_dir") or "").strip()
    input_zarr_uri = str((metadata or {}).get("input_zarr_uri") or "").strip()
    output_zarr_uri = str((metadata or {}).get("output_zarr_uri") or output_uri).strip()
    storage_mode = str((metadata or {}).get("storage_mode") or "").strip()

    root.attrs["water_mask_path"] = WATER_MASK_PATH
    root.attrs["water_mask_written"] = True
    root.attrs["water_mask_status"] = "written"
    root.attrs["water_mask_reason"] = ""
    root.attrs["water_mask_artifact_uri"] = artifact_uri
    root.attrs["water_mask_status_path"] = status_path
    root.attrs["water_mask_work_dir"] = work_dir
    root.attrs["water_mask_source_zarr_uri"] = input_zarr_uri
    root.attrs["water_mask_output_zarr_uri"] = output_zarr_uri
    root.attrs["water_mask_storage_mode"] = storage_mode
    zarr.consolidate_metadata(output_store)
    return {
        "mask_name": WATER_MASK_NAME,
        "mask_path": WATER_MASK_PATH,
        "shape": [1, expected_height, expected_width],
        "dtype": "uint8",
        "classes": dict(WATER_MASK_CLASSES),
        "model_name": model_name,
        "model_version": model_version,
        "input_bands": list(input_bands or []),
        "written_at": timestamp.isoformat(),
        "unique_values": sorted(unique_values),
        "input_zarr_uri": input_zarr_uri,
        "output_zarr_uri": output_zarr_uri,
        "storage_mode": storage_mode or "in_place_zarr_enrichment",
    }
