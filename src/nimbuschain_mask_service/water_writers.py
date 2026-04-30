from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np

from nimbuschain_mask_service.models import MaskWriterMetadata, WaterRuntimeSummary
from nimbuschain_mask_service.path_resolution import local_path_for_uri
from nimbuschain_mask_service.schema import (
    WATER_MASK_CLASSES,
    WATER_MASK_NAME,
    WATER_MASK_PATH,
    WATER_PROBABILITY_PATH,
)
from nimbuschain_mask_service.writer_support import (
    ensure_writable_target,
    storage_mode_from_metadata,
)
from nimbuschain_shared.zarr import ConversionDependencyError, ConversionError


def write_water_mask_to_zarr(
    *,
    output_uri: str,
    mask: np.ndarray,
    probability: np.ndarray | None = None,
    acquisition_datetime: str | None = None,
    model_name: str = "omniwatermask",
    model_version: str | None = None,
    input_bands: list[str] | None = None,
    metadata: MaskWriterMetadata | dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "Mask writing dependencies are unavailable "
            f"({exc}). Ensure zarr is installed in the runtime."
        ) from exc

    metadata_record = (
        metadata if isinstance(metadata, MaskWriterMetadata) else MaskWriterMetadata.from_mapping(metadata)
    )

    mask_data = np.asarray(mask, dtype=np.uint8)
    if mask_data.ndim == 3 and mask_data.shape[0] == 1:
        mask_data = mask_data[0]
    if mask_data.ndim != 2:
        raise ConversionError(
            f"Binary mask '{WATER_MASK_NAME}' must be 2D or shaped (1, y, x), got {mask_data.shape}."
        )
    unique_values = {int(value) for value in np.unique(mask_data)}
    if not unique_values.issubset({0, 1}):
        raise ConversionError(
            f"Binary mask '{WATER_MASK_NAME}' must contain only values 0 or 1, got {sorted(unique_values)}."
        )

    probability_data: np.ndarray | None
    if probability is None:
        probability_data = None
    else:
        probability_data = np.asarray(probability, dtype=np.float32)
        if probability_data.ndim == 3 and probability_data.shape[0] == 1:
            probability_data = probability_data[0]
        if probability_data.ndim != 2:
            raise ConversionError(
                f"Water probability must be 2D or shaped (1, y, x), got {probability_data.shape}."
            )
        if probability_data.shape != mask_data.shape:
            raise ConversionError(
                f"Water mask/probability shape mismatch: {mask_data.shape} vs {probability_data.shape}."
            )

    output_path = local_path_for_uri(output_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    root = zarr.open_group(str(output_path), mode="a", zarr_format=2)
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")
    expected_height = int(imagery.shape[2])
    expected_width = int(imagery.shape[3])
    if mask_data.shape != (expected_height, expected_width):
        raise ConversionError(
            f"Binary mask '{WATER_MASK_NAME}' shape mismatch: expected "
            f"({expected_height}, {expected_width}), got {mask_data.shape}."
        )

    water_arr, water_prob_arr = prepare_water_output_arrays(root, overwrite=True)
    water_arr[0, :, :] = mask_data
    if probability_data is None:
        chunk_y = int(water_prob_arr.chunks[1])
        chunk_x = int(water_prob_arr.chunks[2])
        for row_start in range(0, expected_height, chunk_y):
            row_stop = min(expected_height, row_start + chunk_y)
            for col_start in range(0, expected_width, chunk_x):
                col_stop = min(expected_width, col_start + chunk_x)
                water_prob_arr[0, row_start:row_stop, col_start:col_stop] = mask_data[
                    row_start:row_stop,
                    col_start:col_stop,
                ].astype(np.float32)
    else:
        water_prob_arr[0, :, :] = probability_data.astype(np.float32, copy=False)

    water_fraction = float(mask_data.mean()) if mask_data.size else 0.0
    probability_source = str(metadata_record.probability_source or "water_score").strip() or "water_score"
    threshold_used = metadata_record.threshold_used
    runtime_mode = str(metadata_record.runtime_mode or "").strip() or None
    sensor_recipe = str(metadata_record.sensor_recipe or "").strip() or None
    write_summary = finalize_water_outputs(
        root,
        runtime_mode=runtime_mode or model_name,
        sensor_key=sensor_recipe or "unknown",
        threshold=(float(threshold_used) if threshold_used is not None else None),
        input_bands=tuple(input_bands or ()),
        metadata=metadata_record,
        water_fraction=water_fraction,
        water_arr=water_arr,
        water_prob_arr=water_prob_arr,
        summary=WaterRuntimeSummary(
            runtime_mode=runtime_mode or model_name,
            probability_source=probability_source,
            sensor_recipe=sensor_recipe,
            threshold_used=(float(threshold_used) if threshold_used is not None else None),
            water_fraction=water_fraction,
        ),
    )
    water_arr.attrs.update(
        {
            "model_name": model_name,
            "model_version": model_version,
            "input_bands": list(input_bands or []),
        }
    )
    water_prob_arr.attrs.update(
        {
            "model_name": model_name,
            "model_version": model_version,
            "input_bands": list(input_bands or []),
        }
    )
    zarr.consolidate_metadata(root.store)
    return {
        "mask_name": WATER_MASK_NAME,
        "mask_path": WATER_MASK_PATH,
        "probability_path": WATER_PROBABILITY_PATH,
        "shape": [1, expected_height, expected_width],
        "dtype": "uint8",
        "probability_dtype": "float32",
        "classes": dict(WATER_MASK_CLASSES),
        "model_name": model_name,
        "model_version": model_version,
        "input_bands": list(input_bands or []),
        "written_at": write_summary["written_at"],
        "unique_values": sorted(unique_values),
        "input_zarr_uri": str(metadata_record.input_zarr_uri or "").strip(),
        "output_zarr_uri": str(metadata_record.output_zarr_uri or output_uri).strip(),
        "storage_mode": storage_mode_from_metadata(metadata=metadata_record, output_uri=output_uri),
        "water_fraction": water_fraction,
    }


def prepare_water_output_arrays(root: Any, *, overwrite: bool) -> tuple[Any, Any]:
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    y_size = int(imagery.shape[2])
    x_size = int(imagery.shape[3])
    masks_group = root.require_group("masks")
    ensure_writable_target(masks_group, "water", overwrite=overwrite)
    ensure_writable_target(masks_group, "water_probability", overwrite=overwrite)

    chunks = (1, min(1024, y_size), min(1024, x_size))
    water_arr = masks_group.create_array(
        "water",
        shape=(1, y_size, x_size),
        chunks=chunks,
        dtype=np.uint8,
        overwrite=True,
    )
    water_prob_arr = masks_group.create_array(
        "water_probability",
        shape=(1, y_size, x_size),
        chunks=chunks,
        dtype=np.float32,
        overwrite=True,
    )
    return water_arr, water_prob_arr


def finalize_water_outputs(
    root: Any,
    *,
    runtime_mode: str,
    sensor_key: str,
    threshold: float | None,
    input_bands: tuple[str, ...],
    metadata: MaskWriterMetadata | dict[str, Any] | None = None,
    water_fraction: float,
    water_arr: Any | None = None,
    water_prob_arr: Any | None = None,
    summary: WaterRuntimeSummary | dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise RuntimeError(f"Water masking requires zarr runtime support ({exc}).") from exc

    if water_arr is None or water_prob_arr is None:
        masks_group = root.get("masks")
        if masks_group is None or "water" not in masks_group or "water_probability" not in masks_group:
            raise ConversionError(
                "Water outputs cannot be finalized because masks/water or masks/water_probability is missing."
            )
        water_arr = masks_group["water"]
        water_prob_arr = masks_group["water_probability"]

    metadata_record = (
        metadata if isinstance(metadata, MaskWriterMetadata) else MaskWriterMetadata.from_mapping(metadata)
    )
    summary_record = (
        summary if isinstance(summary, WaterRuntimeSummary) else WaterRuntimeSummary.from_mapping(summary)
    )
    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "runtime_mode": runtime_mode,
        "sensor": sensor_key,
        "threshold": (float(threshold) if threshold is not None else None),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": metadata_record.to_dict(),
        "summary": summary_record.to_dict(),
    }
    water_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": WATER_MASK_NAME,
            "mask_path": WATER_MASK_PATH,
            "classes": dict(WATER_MASK_CLASSES),
            "description": "Water mask written by Nimbus mask service",
        }
    )
    water_prob_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": "water_probability",
            "mask_path": WATER_PROBABILITY_PATH,
            "description": "Water probability [0,1]",
        }
    )
    root.attrs["water_mask"] = {
        "available": True,
        "runtime_mode": runtime_mode,
        "sensor": sensor_key,
        "path": WATER_MASK_PATH,
        "probability_path": WATER_PROBABILITY_PATH,
        "threshold_used": (float(threshold) if threshold is not None else None),
        "water_fraction": float(water_fraction),
        "tile_size": summary_record.tile_size,
        "tile_sizing": dict(summary_record.tile_sizing),
        "updated_at": common_attrs["created_at"],
        "probability_source": str(summary_record.probability_source or "water_score"),
    }
    root.attrs["water_mask_path"] = WATER_MASK_PATH
    root.attrs["water_mask_probability_path"] = WATER_PROBABILITY_PATH
    root.attrs["water_mask_written"] = True
    root.attrs["water_mask_status"] = "written"
    root.attrs["water_mask_reason"] = ""
    root.attrs["water_mask_artifact_uri"] = str(metadata_record.artifact_uri or "").strip()
    root.attrs["water_mask_status_path"] = str(metadata_record.status_path or "").strip()
    root.attrs["water_mask_work_dir"] = str(metadata_record.work_dir or "").strip()
    root.attrs["water_mask_source_zarr_uri"] = str(metadata_record.input_zarr_uri or "").strip()
    root.attrs["water_mask_output_zarr_uri"] = str(metadata_record.output_zarr_uri or "").strip()
    root.attrs["water_mask_storage_mode"] = storage_mode_from_metadata(
        metadata=metadata_record,
        output_uri=str(metadata_record.output_zarr_uri or ""),
    )
    root.attrs["water_mask_runtime_mode"] = runtime_mode
    root.attrs["water_mask_threshold_used"] = (float(threshold) if threshold is not None else None)
    root.attrs["water_mask_sensor_recipe"] = sensor_key
    root.attrs["water_mask_fraction"] = float(water_fraction)
    root.attrs["water_mask_tile_size"] = summary_record.tile_size
    root.attrs["water_mask_tile_sizing"] = dict(summary_record.tile_sizing)

    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass

    return {
        "water_fraction": float(water_fraction),
        "mask_shape": [int(v) for v in water_arr.shape],
        "mask_dtype": str(water_arr.dtype),
        "probability_dtype": str(water_prob_arr.dtype),
        "mask_path": WATER_MASK_PATH,
        "probability_path": WATER_PROBABILITY_PATH,
        "classes": dict(WATER_MASK_CLASSES),
        "written_at": common_attrs["created_at"],
    }


__all__ = [
    "finalize_water_outputs",
    "prepare_water_output_arrays",
    "write_water_mask_to_zarr",
]
