from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np

from nimbuschain_mask_service.models import CloudInferenceSummary, MaskWriterMetadata
from nimbuschain_mask_service.schema import (
    CLOUD_MASK_CLASSES,
    CLOUD_MASK_NAME,
    CLOUD_MASK_PATH,
    CLOUD_PROBABILITY_PATH,
)
from nimbuschain_mask_service.writer_support import (
    as_time_y_x,
    ensure_writable_target,
)
from nimbuschain_shared.zarr import ConversionError


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
    metadata: MaskWriterMetadata | dict[str, Any] | None = None,
    summary: CloudInferenceSummary | dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        import zarr
    except ImportError as exc:
        raise RuntimeError(f"Cloud masking requires zarr runtime support ({exc}).") from exc

    cloud_data = as_time_y_x(mask, dtype=np.uint8)
    prob_data = as_time_y_x(probability, dtype=np.float32)
    if cloud_data.shape != prob_data.shape:
        raise ConversionError(
            f"Cloud mask/probability shape mismatch: {cloud_data.shape} vs {prob_data.shape}"
        )

    masks_group = root.require_group("masks")
    ensure_writable_target(masks_group, "cloud", overwrite=overwrite)
    ensure_writable_target(masks_group, "cloud_probability", overwrite=overwrite)

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

    metadata_record = (
        metadata if isinstance(metadata, MaskWriterMetadata) else MaskWriterMetadata.from_mapping(metadata)
    )
    summary_record = (
        summary if isinstance(summary, CloudInferenceSummary) else CloudInferenceSummary.from_mapping(summary)
    )
    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backend": backend,
        "sensor": sensor_key,
        "threshold": float(threshold),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": metadata_record.to_dict(),
        "summary": summary_record.to_dict(),
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
        "mask_source": summary_record.mask_source,
        "probability_source": summary_record.probability_source,
        "requested_threshold": summary_record.requested_threshold,
        "threshold_for_mask": summary_record.threshold_for_mask,
        "path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "cloud_fraction": float(cloud_fraction),
        "includes_shadows": bool(summary_record.includes_shadows),
        "shadow_fraction": float(summary_record.shadow_fraction),
        "cloud_only_fraction": float(summary_record.cloud_only_fraction),
        "class_histogram": dict(summary_record.class_histogram),
        "tile_size": summary_record.tile_size,
        "tile_sizing": dict(summary_record.tile_sizing),
        "updated_at": common_attrs["created_at"],
    }
    root.attrs["cloud_mask_path"] = CLOUD_MASK_PATH
    root.attrs["cloud_mask_probability_path"] = CLOUD_PROBABILITY_PATH
    root.attrs["cloud_mask_written"] = True
    root.attrs["cloud_mask_status"] = "written"
    root.attrs["cloud_mask_reason"] = ""
    root.attrs["cloud_mask_artifact_uri"] = str(metadata_record.artifact_uri or "").strip()
    root.attrs["cloud_mask_status_path"] = str(metadata_record.status_path or "").strip()
    root.attrs["cloud_mask_work_dir"] = str(metadata_record.work_dir or "").strip()
    root.attrs["cloud_mask_source_zarr_uri"] = str(metadata_record.input_zarr_uri or "").strip()
    root.attrs["cloud_mask_output_zarr_uri"] = str(metadata_record.output_zarr_uri or "").strip()
    root.attrs["cloud_mask_fraction"] = float(cloud_fraction)
    root.attrs["cloud_mask_mask_source"] = summary_record.mask_source
    root.attrs["cloud_mask_probability_source"] = summary_record.probability_source
    root.attrs["cloud_mask_requested_threshold"] = summary_record.requested_threshold
    root.attrs["cloud_mask_threshold_for_mask"] = summary_record.threshold_for_mask
    root.attrs["cloud_mask_tile_size"] = summary_record.tile_size
    root.attrs["cloud_mask_tile_sizing"] = dict(summary_record.tile_sizing)

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
    ensure_writable_target(masks_group, "cloud", overwrite=overwrite)
    ensure_writable_target(masks_group, "cloud_probability", overwrite=overwrite)

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
    metadata: MaskWriterMetadata | dict[str, Any] | None = None,
    cloud_fraction: float,
    cloud_arr: Any | None = None,
    cloud_prob_arr: Any | None = None,
    summary: CloudInferenceSummary | dict[str, Any] | None = None,
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
    metadata_record = (
        metadata if isinstance(metadata, MaskWriterMetadata) else MaskWriterMetadata.from_mapping(metadata)
    )
    summary_record = (
        summary if isinstance(summary, CloudInferenceSummary) else CloudInferenceSummary.from_mapping(summary)
    )
    common_attrs = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backend": backend,
        "sensor": sensor_key,
        "threshold": float(threshold),
        "input_bands": list(input_bands),
        "dimensions": ["time", "y", "x"],
        "metadata": metadata_record.to_dict(),
        "summary": summary_record.to_dict(),
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
        "mask_source": summary_record.mask_source,
        "probability_source": summary_record.probability_source,
        "requested_threshold": summary_record.requested_threshold,
        "threshold_for_mask": summary_record.threshold_for_mask,
        "path": CLOUD_MASK_PATH,
        "probability_path": CLOUD_PROBABILITY_PATH,
        "cloud_fraction": float(cloud_fraction),
        "includes_shadows": bool(summary_record.includes_shadows),
        "shadow_fraction": float(summary_record.shadow_fraction),
        "cloud_only_fraction": float(summary_record.cloud_only_fraction),
        "class_histogram": dict(summary_record.class_histogram),
        "tile_size": summary_record.tile_size,
        "tile_sizing": dict(summary_record.tile_sizing),
        "updated_at": common_attrs["created_at"],
    }
    root.attrs["cloud_mask_path"] = CLOUD_MASK_PATH
    root.attrs["cloud_mask_probability_path"] = CLOUD_PROBABILITY_PATH
    root.attrs["cloud_mask_written"] = True
    root.attrs["cloud_mask_status"] = "written"
    root.attrs["cloud_mask_reason"] = ""
    root.attrs["cloud_mask_artifact_uri"] = str(metadata_record.artifact_uri or "").strip()
    root.attrs["cloud_mask_status_path"] = str(metadata_record.status_path or "").strip()
    root.attrs["cloud_mask_work_dir"] = str(metadata_record.work_dir or "").strip()
    root.attrs["cloud_mask_source_zarr_uri"] = str(metadata_record.input_zarr_uri or "").strip()
    root.attrs["cloud_mask_output_zarr_uri"] = str(metadata_record.output_zarr_uri or "").strip()
    root.attrs["cloud_mask_fraction"] = float(cloud_fraction)
    root.attrs["cloud_mask_mask_source"] = summary_record.mask_source
    root.attrs["cloud_mask_probability_source"] = summary_record.probability_source
    root.attrs["cloud_mask_requested_threshold"] = summary_record.requested_threshold
    root.attrs["cloud_mask_threshold_for_mask"] = summary_record.threshold_for_mask
    root.attrs["cloud_mask_tile_size"] = summary_record.tile_size
    root.attrs["cloud_mask_tile_sizing"] = dict(summary_record.tile_sizing)

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


__all__ = [
    "finalize_cloud_outputs",
    "prepare_cloud_output_arrays",
    "write_cloud_outputs",
]
