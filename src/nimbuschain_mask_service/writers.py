from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np

from nimbuschain_mask_service.io import local_path_for_uri
from nimbuschain_mask_service.schema import (
    CLOUD_MASK_CLASSES,
    CLOUD_MASK_NAME,
    CLOUD_MASK_PATH,
    CLOUD_PROBABILITY_PATH,
    WATER_MASK_CLASSES,
    WATER_MASK_NAME,
    WATER_MASK_PATH,
    WATER_PROBABILITY_PATH,
)
from nimbuschain_shared.zarr import (
    ChunkShape,
    ConversionDependencyError,
    ConversionError,
    _coerce_timestamp,
)


def _storage_mode_from_metadata(*, metadata: dict[str, Any] | None, output_uri: str) -> str:
    payload = dict(metadata or {})
    explicit = str(payload.get("storage_mode") or "").strip()
    if explicit:
        return explicit
    input_zarr_uri = str(payload.get("input_zarr_uri") or "").strip()
    output_zarr_uri = str(payload.get("output_zarr_uri") or output_uri).strip()
    if input_zarr_uri and input_zarr_uri == output_zarr_uri:
        return "in_place_zarr_masking"
    return "derived_zarr_copy"


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

    output_path = local_path_for_uri(output_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    root = zarr.open_group(str(output_path), mode="a", zarr_format=2)
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
    storage_mode = _storage_mode_from_metadata(metadata=metadata, output_uri=output_uri)

    root.attrs[f"{mask_name}_mask_path"] = mask_path
    root.attrs[f"{mask_name}_mask_written"] = True
    root.attrs[f"{mask_name}_mask_status"] = "written"
    root.attrs[f"{mask_name}_mask_reason"] = ""
    root.attrs[f"{mask_name}_mask_artifact_uri"] = artifact_uri
    root.attrs[f"{mask_name}_mask_status_path"] = status_path
    root.attrs[f"{mask_name}_mask_work_dir"] = work_dir
    root.attrs[f"{mask_name}_mask_source_zarr_uri"] = input_zarr_uri
    root.attrs[f"{mask_name}_mask_output_zarr_uri"] = output_zarr_uri
    root.attrs[f"{mask_name}_mask_storage_mode"] = storage_mode
    zarr.consolidate_metadata(root.store)
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
        "storage_mode": storage_mode,
    }


def write_water_mask_to_zarr(
    *,
    output_uri: str,
    mask: np.ndarray,
    probability: np.ndarray | None = None,
    acquisition_datetime: str | None = None,
    model_name: str = "omniwatermask",
    model_version: str | None = None,
    input_bands: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "Mask writing dependencies are unavailable "
            f"({exc}). Ensure zarr is installed in the runtime."
        ) from exc

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
    probability_source = str((metadata or {}).get("probability_source") or "water_score").strip() or "water_score"
    threshold_used = (metadata or {}).get("threshold_used")
    runtime_mode = str((metadata or {}).get("runtime_mode") or "").strip() or None
    sensor_recipe = str((metadata or {}).get("sensor_recipe") or "").strip() or None
    write_summary = finalize_water_outputs(
        root,
        runtime_mode=runtime_mode or model_name,
        sensor_key=sensor_recipe or "unknown",
        threshold=(float(threshold_used) if threshold_used is not None else None),
        input_bands=tuple(input_bands or ()),
        metadata=metadata,
        water_fraction=water_fraction,
        water_arr=water_arr,
        water_prob_arr=water_prob_arr,
        summary={
            "runtime_mode": runtime_mode or model_name,
            "probability_source": probability_source,
            "sensor_recipe": sensor_recipe,
            "threshold_used": threshold_used,
        },
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
        "input_zarr_uri": str((metadata or {}).get("input_zarr_uri") or "").strip(),
        "output_zarr_uri": str((metadata or {}).get("output_zarr_uri") or output_uri).strip(),
        "storage_mode": _storage_mode_from_metadata(metadata=metadata, output_uri=output_uri),
        "water_fraction": water_fraction,
    }


def prepare_water_output_arrays(root: Any, *, overwrite: bool) -> tuple[Any, Any]:
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    y_size = int(imagery.shape[2])
    x_size = int(imagery.shape[3])
    masks_group = root.require_group("masks")
    _ensure_writable_target(masks_group, "water", overwrite=overwrite)
    _ensure_writable_target(masks_group, "water_probability", overwrite=overwrite)

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
    metadata: dict[str, Any] | None = None,
    water_fraction: float,
    water_arr: Any | None = None,
    water_prob_arr: Any | None = None,
    summary: dict[str, Any] | None = None,
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

    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "runtime_mode": runtime_mode,
        "sensor": sensor_key,
        "threshold": (float(threshold) if threshold is not None else None),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": dict(metadata or {}),
        "summary": dict(summary or {}),
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
        "tile_size": (summary or {}).get("tile_size"),
        "tile_sizing": dict((summary or {}).get("tile_sizing") or {}),
        "updated_at": common_attrs["created_at"],
        "probability_source": str((summary or {}).get("probability_source") or "water_score"),
    }
    root.attrs["water_mask_path"] = WATER_MASK_PATH
    root.attrs["water_mask_probability_path"] = WATER_PROBABILITY_PATH
    root.attrs["water_mask_written"] = True
    root.attrs["water_mask_status"] = "written"
    root.attrs["water_mask_reason"] = ""
    root.attrs["water_mask_artifact_uri"] = str((metadata or {}).get("artifact_uri") or "").strip()
    root.attrs["water_mask_status_path"] = str((metadata or {}).get("status_path") or "").strip()
    root.attrs["water_mask_work_dir"] = str((metadata or {}).get("work_dir") or "").strip()
    root.attrs["water_mask_source_zarr_uri"] = str((metadata or {}).get("input_zarr_uri") or "").strip()
    root.attrs["water_mask_output_zarr_uri"] = str((metadata or {}).get("output_zarr_uri") or "").strip()
    root.attrs["water_mask_storage_mode"] = _storage_mode_from_metadata(metadata=metadata, output_uri=str((metadata or {}).get("output_zarr_uri") or ""))
    root.attrs["water_mask_runtime_mode"] = runtime_mode
    root.attrs["water_mask_threshold_used"] = (float(threshold) if threshold is not None else None)
    root.attrs["water_mask_sensor_recipe"] = sensor_key
    root.attrs["water_mask_fraction"] = float(water_fraction)
    root.attrs["water_mask_tile_size"] = (summary or {}).get("tile_size")
    root.attrs["water_mask_tile_sizing"] = dict((summary or {}).get("tile_sizing") or {})

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


def write_cloud_outputs(
    root: Any,
    *,
    mask: np.ndarray,
    probability: np.ndarray,
    threshold: float,
    backend: str,
    sensor_key: str,
    input_bands: tuple[str, ...],
    overwrite: bool,
    metadata: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise RuntimeError(f"Cloud masking requires zarr runtime support ({exc}).") from exc

    cloud_data = _as_time_y_x(mask, dtype=np.uint8)
    prob_data = _as_time_y_x(probability, dtype=np.float32)
    if cloud_data.shape != prob_data.shape:
        raise ConversionError(
            f"Cloud mask/probability shape mismatch: {cloud_data.shape} vs {prob_data.shape}"
        )

    masks_group = root.require_group("masks")
    _ensure_writable_target(masks_group, "cloud", overwrite=overwrite)
    _ensure_writable_target(masks_group, "cloud_probability", overwrite=overwrite)

    y_size = int(cloud_data.shape[1])
    x_size = int(cloud_data.shape[2])
    chunks = (1, min(1024, y_size), min(1024, x_size))

    cloud_arr = masks_group.create_array(
        "cloud",
        data=cloud_data,
        chunks=chunks,
        overwrite=True,
    )
    cloud_prob_arr = masks_group.create_array(
        "cloud_probability",
        data=prob_data,
        chunks=chunks,
        overwrite=True,
    )

    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backend": backend,
        "sensor": sensor_key,
        "threshold": float(threshold),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": dict(metadata or {}),
        "summary": dict(summary or {}),
    }
    cloud_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": CLOUD_MASK_NAME,
            "mask_path": CLOUD_MASK_PATH,
            "classes": dict(CLOUD_MASK_CLASSES),
            "description": "Cloud mask written by Nimbus mask service",
        }
    )
    cloud_prob_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": "cloud_probability",
            "mask_path": CLOUD_PROBABILITY_PATH,
            "description": "Cloud probability [0,1]",
        }
    )
    cloud_fraction = float(cloud_data.mean()) if cloud_data.size else 0.0
    root.attrs["cloud_mask"] = {
        "available": True,
        "threshold": float(threshold),
        "backend": backend,
        "sensor": sensor_key,
        "mask_source": str((summary or {}).get("mask_source") or ""),
        "probability_source": str((summary or {}).get("probability_source") or ""),
        "requested_threshold": (summary or {}).get("requested_threshold"),
        "threshold_for_mask": (summary or {}).get("threshold_for_mask"),
        "path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "cloud_fraction": float(cloud_fraction),
        "includes_shadows": bool((summary or {}).get("includes_shadows", False)),
        "shadow_fraction": float((summary or {}).get("shadow_fraction", 0.0)),
        "cloud_only_fraction": float((summary or {}).get("cloud_only_fraction", 0.0)),
        "class_histogram": dict((summary or {}).get("class_histogram") or {}),
        "tile_size": (summary or {}).get("tile_size"),
        "tile_sizing": dict((summary or {}).get("tile_sizing") or {}),
        "updated_at": common_attrs["created_at"],
    }
    root.attrs["cloud_mask_path"] = CLOUD_MASK_PATH
    root.attrs["cloud_mask_probability_path"] = CLOUD_PROBABILITY_PATH
    root.attrs["cloud_mask_written"] = True
    root.attrs["cloud_mask_status"] = "written"
    root.attrs["cloud_mask_reason"] = ""
    root.attrs["cloud_mask_artifact_uri"] = str((metadata or {}).get("artifact_uri") or "").strip()
    root.attrs["cloud_mask_status_path"] = str((metadata or {}).get("status_path") or "").strip()
    root.attrs["cloud_mask_work_dir"] = str((metadata or {}).get("work_dir") or "").strip()
    root.attrs["cloud_mask_source_zarr_uri"] = str((metadata or {}).get("input_zarr_uri") or "").strip()
    root.attrs["cloud_mask_output_zarr_uri"] = str((metadata or {}).get("output_zarr_uri") or "").strip()
    root.attrs["cloud_mask_fraction"] = float(cloud_fraction)
    root.attrs["cloud_mask_mask_source"] = str((summary or {}).get("mask_source") or "")
    root.attrs["cloud_mask_probability_source"] = str((summary or {}).get("probability_source") or "")
    root.attrs["cloud_mask_requested_threshold"] = (summary or {}).get("requested_threshold")
    root.attrs["cloud_mask_threshold_for_mask"] = (summary or {}).get("threshold_for_mask")
    root.attrs["cloud_mask_tile_size"] = (summary or {}).get("tile_size")
    root.attrs["cloud_mask_tile_sizing"] = dict((summary or {}).get("tile_sizing") or {})

    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass
    return {
        "cloud_fraction": cloud_fraction,
        "mask_shape": [int(v) for v in cloud_data.shape],
        "mask_dtype": str(cloud_data.dtype),
        "probability_dtype": str(prob_data.dtype),
        "mask_path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "classes": dict(CLOUD_MASK_CLASSES),
        "written_at": common_attrs["created_at"],
    }


def prepare_cloud_output_arrays(
    root: Any,
    *,
    overwrite: bool,
) -> tuple[Any, Any]:
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    y_size = int(imagery.shape[2])
    x_size = int(imagery.shape[3])
    masks_group = root.require_group("masks")
    _ensure_writable_target(masks_group, "cloud", overwrite=overwrite)
    _ensure_writable_target(masks_group, "cloud_probability", overwrite=overwrite)

    chunks = (1, min(1024, y_size), min(1024, x_size))
    cloud_arr = masks_group.create_array(
        "cloud",
        shape=(1, y_size, x_size),
        chunks=chunks,
        dtype=np.uint8,
        overwrite=True,
    )
    cloud_prob_arr = masks_group.create_array(
        "cloud_probability",
        shape=(1, y_size, x_size),
        chunks=chunks,
        dtype=np.float32,
        overwrite=True,
    )
    return cloud_arr, cloud_prob_arr


def finalize_cloud_outputs(
    root: Any,
    *,
    threshold: float,
    backend: str,
    sensor_key: str,
    input_bands: tuple[str, ...],
    metadata: dict[str, Any] | None = None,
    cloud_fraction: float,
    cloud_arr: Any | None = None,
    cloud_prob_arr: Any | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise RuntimeError(f"Cloud masking requires zarr runtime support ({exc}).") from exc

    if cloud_arr is None or cloud_prob_arr is None:
        masks_group = root.get("masks")
        if masks_group is None or "cloud" not in masks_group or "cloud_probability" not in masks_group:
            raise ConversionError(
                "Cloud outputs cannot be finalized because masks/cloud or masks/cloud_probability is missing."
            )
        cloud_arr = masks_group["cloud"]
        cloud_prob_arr = masks_group["cloud_probability"]
    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backend": backend,
        "sensor": sensor_key,
        "threshold": float(threshold),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": dict(metadata or {}),
        "summary": dict(summary or {}),
    }
    cloud_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": CLOUD_MASK_NAME,
            "mask_path": CLOUD_MASK_PATH,
            "classes": dict(CLOUD_MASK_CLASSES),
            "description": "Cloud mask written by Nimbus mask service",
        }
    )
    cloud_prob_arr.attrs.update(
        {
            **common_attrs,
            "mask_name": "cloud_probability",
            "mask_path": CLOUD_PROBABILITY_PATH,
            "description": "Cloud probability [0,1]",
        }
    )
    root.attrs["cloud_mask"] = {
        "available": True,
        "threshold": float(threshold),
        "backend": backend,
        "sensor": sensor_key,
        "mask_source": str((summary or {}).get("mask_source") or ""),
        "probability_source": str((summary or {}).get("probability_source") or ""),
        "requested_threshold": (summary or {}).get("requested_threshold"),
        "threshold_for_mask": (summary or {}).get("threshold_for_mask"),
        "path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "cloud_fraction": float(cloud_fraction),
        "includes_shadows": bool((summary or {}).get("includes_shadows", False)),
        "shadow_fraction": float((summary or {}).get("shadow_fraction", 0.0)),
        "cloud_only_fraction": float((summary or {}).get("cloud_only_fraction", 0.0)),
        "class_histogram": dict((summary or {}).get("class_histogram") or {}),
        "tile_size": (summary or {}).get("tile_size"),
        "tile_sizing": dict((summary or {}).get("tile_sizing") or {}),
        "updated_at": common_attrs["created_at"],
    }
    root.attrs["cloud_mask_path"] = CLOUD_MASK_PATH
    root.attrs["cloud_mask_probability_path"] = CLOUD_PROBABILITY_PATH
    root.attrs["cloud_mask_written"] = True
    root.attrs["cloud_mask_status"] = "written"
    root.attrs["cloud_mask_reason"] = ""
    root.attrs["cloud_mask_artifact_uri"] = str((metadata or {}).get("artifact_uri") or "").strip()
    root.attrs["cloud_mask_status_path"] = str((metadata or {}).get("status_path") or "").strip()
    root.attrs["cloud_mask_work_dir"] = str((metadata or {}).get("work_dir") or "").strip()
    root.attrs["cloud_mask_source_zarr_uri"] = str((metadata or {}).get("input_zarr_uri") or "").strip()
    root.attrs["cloud_mask_output_zarr_uri"] = str((metadata or {}).get("output_zarr_uri") or "").strip()
    root.attrs["cloud_mask_fraction"] = float(cloud_fraction)
    root.attrs["cloud_mask_mask_source"] = str((summary or {}).get("mask_source") or "")
    root.attrs["cloud_mask_probability_source"] = str((summary or {}).get("probability_source") or "")
    root.attrs["cloud_mask_requested_threshold"] = (summary or {}).get("requested_threshold")
    root.attrs["cloud_mask_threshold_for_mask"] = (summary or {}).get("threshold_for_mask")
    root.attrs["cloud_mask_tile_size"] = (summary or {}).get("tile_size")
    root.attrs["cloud_mask_tile_sizing"] = dict((summary or {}).get("tile_sizing") or {})

    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass

    return {
        "cloud_fraction": float(cloud_fraction),
        "mask_shape": [int(v) for v in cloud_arr.shape],
        "mask_dtype": str(cloud_arr.dtype),
        "probability_dtype": str(cloud_prob_arr.dtype),
        "mask_path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "classes": dict(CLOUD_MASK_CLASSES),
        "written_at": common_attrs["created_at"],
    }


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

    output_path = local_path_for_uri(output_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    root = zarr.open_group(str(output_path), mode="a", zarr_format=2)
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
    storage_mode = _storage_mode_from_metadata(metadata=metadata, output_uri=output_uri)

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
    zarr.consolidate_metadata(root.store)
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
        "storage_mode": storage_mode,
    }


def _ensure_writable_target(group: Any, name: str, *, overwrite: bool) -> None:
    if name not in group:
        return
    if not overwrite:
        raise ConversionError(
            f"Cloud output '{name}' already exists. Set overwrite=true to replace it."
        )
    del group[name]


def _as_time_y_x(array: np.ndarray, *, dtype: Any) -> np.ndarray:
    data = np.asarray(array, dtype=dtype)
    if data.ndim == 2:
        return data[np.newaxis, :, :]
    if data.ndim == 3:
        return data
    raise ConversionError(
        f"Cloud output array must be 2D or 3D (time,y,x). Got shape={data.shape}."
    )
