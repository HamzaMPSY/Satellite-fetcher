from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dataclasses import dataclass
import errno
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
from typing import Any
import importlib

import numpy as np

from nimbuschain_mask_service.channel_reader import read_required_channels_window
from nimbuschain_mask_service.derived_store import copy_source_zarr
from nimbuschain_mask_service.path_resolution import local_path_for_uri
from nimbuschain_mask_service.zarr_context import (
    delete_mask_layers,
    open_zarr_group,
    read_context,
)
from nimbuschain_mask_service.models import (
    DatasetSummaryRecord,
    MaskWriteSummary,
    MaskWriterMetadata,
    StageEventPayload,
    WaterMaskState,
    WaterRuntimeSummary,
)
from nimbuschain_mask_service.progress import MaskJobCancelled, raise_if_cancel_requested
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec, resolve_sensor_mask_spec
from nimbuschain_mask_service.tile_sizing import choose_mask_tile_sizing, water_tile_sizing_policy_status
from nimbuschain_mask_service.runtime import (
    batch_size_for_device,
    normalize_device_name,
    parallel_worker_count,
    resolve_inference_device,
)
from nimbuschain_mask_service.water_writers import (
    finalize_water_outputs,
    prepare_water_output_arrays,
)
from nimbuschain_shared.zarr import ConversionDependencyError, ConversionError


_S2_INPUT_BANDS = ["B04", "B03", "B02", "B08"]
_LANDSAT_L1_INPUT_BANDS = ["B4", "B3", "B2", "B5"]
_LANDSAT_L2_INPUT_BANDS = ["SR_B4", "SR_B3", "SR_B2", "SR_B5"]

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class OmniWaterPlan:
    required: bool
    supported: bool
    sensor: SensorMaskSpec | None
    input_bands: list[str]
    fallback_bands: list[str]
    threshold: float | None = None
    reason: str | None = None


@dataclass(frozen=True)
class OmniWaterTile:
    path: Path
    row_start: int
    row_stop: int
    col_start: int
    col_stop: int


@dataclass(frozen=True)
class OmniWaterModelProfile:
    name: str
    batch_size: int
    inference_patch_size: int
    inference_overlap_size: int
    optimise_model: bool


def omniwater_support_status() -> dict[str, Any]:
    return {
        "available": _omniwater_module_available(),
        "module": "omniwatermask",
        "tile_sizing": water_tile_sizing_policy_status(
            model_patch_size=_watermask_inference_patch_size(),
        ).to_dict(),
    }


def apply_omniwatermask_to_zarr(
    *,
    job_id: str | None = None,
    zarr_uri: str,
    source_zarr_uri: str | None = None,
    provider: str,
    collection: str,
    product_type: str | None,
    scene_id: str,
    acquisition_datetime: str | None,
    dataset_summary: dict[str, Any],
    output_zarr_uri: str | None = None,
    runtime_preference: str | None = None,
    overwrite: bool = True,
    inference_device: str | None = None,
    fail_on_error: bool = False,
    stage_callback: Any = None,
) -> dict[str, Any]:
    raise_if_cancel_requested(job_id)
    resolved_device = resolve_inference_device(
        explicit=inference_device,
        env_var="NIMBUS_WATERMASK_DEVICE",
    )
    source_lineage_uri = str(source_zarr_uri or zarr_uri).strip()
    masked_zarr_uri = str(output_zarr_uri or "").strip() or source_lineage_uri
    storage_mode = _water_storage_mode(
        source_zarr_uri=source_lineage_uri,
        output_zarr_uri=masked_zarr_uri,
    )
    dataset_summary_record = DatasetSummaryRecord.from_mapping(
        _dataset_summary_with_zarr_context(dataset_summary, zarr_uri=source_lineage_uri)
    )
    plan = _build_plan(
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=dataset_summary_record.to_dict(),
    )
    if not plan.supported:
        status = "failed" if plan.required else "skipped"
        result = WaterMaskState(
            status=status,
            reason=plan.reason or "unsupported",
            input_zarr_uri=source_lineage_uri,
            output_zarr_uri=masked_zarr_uri,
            storage_mode=storage_mode,
            input_bands=list(plan.input_bands),
            fallback_bands=list(plan.fallback_bands),
            threshold_used=plan.threshold,
        ).to_dict()
        if fail_on_error and plan.required:
            raise ConversionError(
                "OmniWaterMask is required for this product, but the mask plan is not supported "
                f"({result['reason']})."
            )
        return result

    module_version: str | None = None
    make_water_mask: Any | None = None
    preferred_runtime = _watermask_runtime_mode(runtime_preference)
    if preferred_runtime == "model":
        try:
            make_water_mask, _make_water_mask_debug, module_version = _load_make_water_mask()
        except ConversionDependencyError:
            if fail_on_error and plan.required:
                raise
            make_water_mask = None

    target_root: Any | None = None
    runtime_warning = ""

    try:
        raise_if_cancel_requested(job_id)
        prepared_output_zarr_uri = (
            source_lineage_uri
            if storage_mode == "in_place_zarr_masking"
            else _prepare_masked_zarr_output(
                source_zarr_uri=zarr_uri,
                output_zarr_uri=masked_zarr_uri,
                overwrite=overwrite,
            )
        )
        target_root = open_zarr_group(prepared_output_zarr_uri, mode="a")
        water_arr = None
        water_prob_arr = None
        if stage_callback is not None:
            stage_callback(
                "water_masking_started",
                StageEventPayload.from_mapping({
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                }),
            )
        if preferred_runtime == "model" and make_water_mask is not None:
            scratch_root = _watermask_scratch_root(output_zarr_uri=prepared_output_zarr_uri)
            try:
                with tempfile.TemporaryDirectory(
                    prefix=f"nimbus-water-{scene_id}-",
                    dir=str(scratch_root),
                ) as tmp_dir:
                    scene_dir = Path(tmp_dir)
                    tiles_dir = scene_dir / "tiles"
                    cache_dir = scene_dir / "cache"
                    output_dir = scene_dir / "outputs"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    cache_dir.mkdir(parents=True, exist_ok=True)
                    tile_manifest = _export_rgbnir_tiles(
                        job_id=job_id,
                        zarr_uri=prepared_output_zarr_uri,
                        tiles_dir=tiles_dir,
                        dataset_summary=dataset_summary_record.to_dict(),
                        input_bands=plan.input_bands,
                        sensor=plan.sensor,
                        provider=provider,
                        collection=collection,
                        product_type=product_type,
                        inference_device=resolved_device,
                    )
                    tile_paths = [tile.path for tile in tile_manifest["tiles"]]
                    mask_output, model_runtime = _run_omniwater_model(
                        job_id=job_id,
                        make_water_mask=make_water_mask,
                        scene_paths=tile_paths,
                        output_dir=output_dir,
                        cache_dir=cache_dir,
                        scene_dir=scene_dir,
                        tile_size=int(tile_manifest["tile_size"]),
                        tile_sizing=dict(tile_manifest.get("tile_sizing") or {}),
                        inference_device=resolved_device,
                        stage_callback=stage_callback,
                        source_zarr_uri=source_lineage_uri,
                        target_zarr_uri=prepared_output_zarr_uri,
                        scene_id=scene_id,
                    )
                    mask_paths = _normalize_mask_outputs(
                        mask_output,
                        output_dir=output_dir,
                        expected_count=len(tile_paths),
                    )
                    water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
                    runtime_summary = _write_model_water_outputs(
                        job_id=job_id,
                        output_zarr_uri=prepared_output_zarr_uri,
                        tiles=tile_manifest["tiles"],
                        mask_paths=mask_paths,
                        water_arr=water_arr,
                        water_prob_arr=water_prob_arr,
                        sensor=plan.sensor,
                        input_bands=tuple(plan.input_bands),
                    )
                    runtime_summary.update(
                        {
                            "tile_size": int(tile_manifest["tile_size"]),
                            "tile_sizing": dict(tile_manifest.get("tile_sizing") or {}),
                            "model_profile": str(model_runtime.get("profile") or ""),
                            "model_attempt_count": int(model_runtime.get("attempt_count") or 0),
                            "model_attempts": list(model_runtime.get("attempts") or []),
                            "model_auxiliary_options": dict(model_runtime.get("auxiliary_options") or {}),
                            "scratch_root": str(scratch_root),
                        }
                    )
                    runtime_mode = "model"
                    input_bands = list(plan.input_bands)
            except Exception as exc:
                if fail_on_error or not _should_fail_open_model_runtime(exc=exc, device=resolved_device):
                    raise
                runtime_warning = _format_model_runtime_warning(exc=exc, device=resolved_device)
                water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
                runtime_summary = _run_water_fallback_tiled(
                    job_id=job_id,
                    zarr_uri=prepared_output_zarr_uri,
                    provider=provider,
                    collection=collection,
                    product_type=product_type,
                    dataset_summary=dataset_summary_record.to_dict(),
                    sensor=plan.sensor,
                    threshold=float(plan.threshold or 0.0),
                    water_arr=water_arr,
                    water_prob_arr=water_prob_arr,
                    inference_device=resolved_device,
                    stage_callback=stage_callback,
                    source_zarr_uri=source_lineage_uri,
                    target_zarr_uri=prepared_output_zarr_uri,
                    scene_id=scene_id,
                )
                attempt_summaries = _model_attempt_summaries(exc)
                fallback_trigger = "mps_fail_open"
                if _is_legacy_model_dependency_error(exc):
                    fallback_trigger = "legacy_dependency"
                elif _is_watermask_scratch_capacity_error(exc):
                    fallback_trigger = "scratch_capacity"
                runtime_summary.update(
                    {
                        "model_profile": "",
                        "model_attempt_count": len(attempt_summaries),
                        "model_attempts": attempt_summaries,
                        "runtime_warning": runtime_warning,
                        "model_error": str(exc),
                        "fallback_trigger": fallback_trigger,
                        "scratch_root": str(scratch_root),
                    }
                )
                runtime_mode = "heuristic_fallback"
                input_bands = list(plan.fallback_bands)
        else:
            water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
            runtime_summary = _run_water_fallback_tiled(
                job_id=job_id,
                zarr_uri=prepared_output_zarr_uri,
                provider=provider,
                collection=collection,
                product_type=product_type,
                dataset_summary=dataset_summary_record.to_dict(),
                sensor=plan.sensor,
                threshold=float(plan.threshold or 0.0),
                water_arr=water_arr,
                water_prob_arr=water_prob_arr,
                inference_device=resolved_device,
                stage_callback=stage_callback,
                source_zarr_uri=source_lineage_uri,
                target_zarr_uri=prepared_output_zarr_uri,
                scene_id=scene_id,
            )
            runtime_mode = "heuristic_fallback"
            input_bands = list(plan.fallback_bands)

        runtime_summary_record = WaterRuntimeSummary.from_mapping(runtime_summary)
        result = MaskWriteSummary.from_mapping(finalize_water_outputs(
            target_root,
            runtime_mode=runtime_mode,
            sensor_key=str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
            threshold=(runtime_summary_record.threshold_used if runtime_mode != "model" else None),
            input_bands=tuple(input_bands),
            metadata=MaskWriterMetadata.from_mapping({
                "provider": provider,
                "collection": collection,
                "product_type": product_type,
                "scene_id": scene_id,
                "source_mask_raster": "",
                "artifact_uri": "",
                "status_path": "",
                "work_dir": "",
                "runtime_mode": runtime_mode,
                "inference_device": resolved_device,
                "input_zarr_uri": source_lineage_uri,
                "output_zarr_uri": prepared_output_zarr_uri,
                "storage_mode": storage_mode,
                "threshold_used": runtime_summary_record.threshold_used,
                "sensor_recipe": str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
                "probability_source": runtime_summary_record.probability_source,
                "model_profile": runtime_summary_record.model_profile,
                "model_attempt_count": int(runtime_summary_record.model_attempt_count),
                "model_attempts": list(runtime_summary_record.model_attempts),
                "model_auxiliary_options": dict(runtime_summary_record.model_auxiliary_options),
                "runtime_warning": runtime_summary_record.runtime_warning or runtime_warning,
                "fallback_trigger": runtime_summary_record.fallback_trigger,
                "tile_size": runtime_summary_record.tile_size,
                "tile_sizing": dict(runtime_summary_record.tile_sizing),
                "scratch_root": runtime_summary_record.scratch_root,
            }),
            water_fraction=float(runtime_summary_record.water_fraction),
            water_arr=water_arr,
            water_prob_arr=water_prob_arr,
            summary=runtime_summary_record.to_dict(),
        ))
        target_root.attrs["source_zarr_uri"] = source_lineage_uri
        target_root.attrs["masked_zarr_uri"] = prepared_output_zarr_uri
        payload = WaterMaskState(
            status="written",
            reason=None,
            input_zarr_uri=source_lineage_uri,
            working_zarr_uri=prepared_output_zarr_uri,
            output_zarr_uri=prepared_output_zarr_uri,
            storage_mode=storage_mode,
            input_bands=list(input_bands),
            fallback_bands=list(plan.fallback_bands),
            threshold_used=runtime_summary_record.threshold_used,
            mask_path=result.mask_path,
            probability_path=result.probability_path,
            shape=list(result.mask_shape),
            dtype=result.mask_dtype,
            probability_dtype=result.probability_dtype,
            classes=dict(result.classes),
            model_name="omniwatermask" if runtime_mode == "model" else "omniwatermask_heuristic_fallback",
            model_version=module_version,
            written_at=result.written_at,
            runtime_mode=runtime_mode,
            sensor_recipe=str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
            water_fraction=float(runtime_summary_record.water_fraction),
            probability_source=runtime_summary_record.probability_source,
            cloud_blocked_fraction=float(runtime_summary_record.cloud_blocked_fraction),
            runtime_warning=runtime_summary_record.runtime_warning or runtime_warning,
            fallback_trigger=runtime_summary_record.fallback_trigger,
            model_profile=runtime_summary_record.model_profile,
            model_attempt_count=int(runtime_summary_record.model_attempt_count),
            model_auxiliary_options=dict(runtime_summary_record.model_auxiliary_options),
            tile_size=int(runtime_summary_record.tile_size),
            tile_sizing=dict(runtime_summary_record.tile_sizing),
            scratch_root=runtime_summary_record.scratch_root,
        )
        payload_dict = payload.to_dict()
        _sync_zarr_mask_attrs(zarr_uri=prepared_output_zarr_uri, payload=payload_dict)
        if stage_callback is not None:
            stage_callback(
                "water_masking_finished",
                StageEventPayload.from_mapping({
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload_dict,
                }),
            )
        return payload_dict
    except Exception as exc:
        cancelled = isinstance(exc, MaskJobCancelled)
        payload = WaterMaskState(
            status="cancelled" if cancelled else "failed",
            reason=(
                "Water mask request was cancelled before completion."
                if cancelled
                else str(exc)
            ),
            input_zarr_uri=source_lineage_uri,
            output_zarr_uri=masked_zarr_uri,
            storage_mode=storage_mode,
            input_bands=list(plan.input_bands),
        )
        payload_dict = payload.to_dict()
        if storage_mode == "in_place_zarr_masking":
            target_root = None
            _delete_water_layers(zarr_uri=masked_zarr_uri)
            _sync_zarr_mask_attrs(zarr_uri=masked_zarr_uri, payload=payload_dict)
        else:
            _cleanup_masked_zarr_copy(masked_zarr_uri)
        if stage_callback is not None:
            stage_callback(
                "water_masking_failed",
                StageEventPayload.from_mapping({
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": masked_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload_dict,
                }),
            )
        if fail_on_error and plan.required and not cancelled:
            raise ConversionError(f"OmniWaterMask failed for scene '{scene_id}' ({exc}).") from exc
        return payload_dict


def maybe_write_omniwater_mask(
    *,
    job_id: str | None = None,
    output_uri: str,
    provider: str,
    collection: str,
    product_type: str | None,
    scene_id: str,
    acquisition_datetime: str | None,
    dataset_summary: dict[str, Any],
    output_zarr_uri: str | None = None,
    fail_on_error: bool = False,
) -> dict[str, Any]:
    return apply_omniwatermask_to_zarr(
        job_id=job_id,
        zarr_uri=output_uri,
        provider=provider,
        collection=collection,
        product_type=product_type,
        scene_id=scene_id,
        acquisition_datetime=acquisition_datetime,
        dataset_summary=dataset_summary,
        output_zarr_uri=output_zarr_uri,
        fail_on_error=fail_on_error,
    )


def _build_plan(
    *,
    provider: str,
    collection: str,
    product_type: str | None,
    dataset_summary: dict[str, Any],
) -> OmniWaterPlan:
    band_names = [str(value) for value in list(dataset_summary.get("band_names") or [])]
    try:
        sensor = resolve_sensor_mask_spec(
            provider=provider,
            collection=collection,
            product_type=product_type,
        )
    except ValueError:
        return OmniWaterPlan(
            required=is_omniwater_required(provider=provider, collection=collection),
            supported=False,
            sensor=None,
            input_bands=[],
            fallback_bands=[],
            reason="unsupported_collection_for_omniwatermask",
        )

    required_bands = tuple(dict.fromkeys((*sensor.water_input_bands, *sensor.water_fallback_bands)))
    missing = [name for name in required_bands if name not in band_names]
    if missing:
        return OmniWaterPlan(
            required=True,
            supported=False,
            sensor=sensor,
            input_bands=list(sensor.water_input_bands),
            fallback_bands=list(sensor.water_fallback_bands),
            threshold=float(sensor.water_threshold_default),
            reason="required_water_bands_missing:" + ",".join(missing),
        )
    return OmniWaterPlan(
        required=True,
        supported=True,
        sensor=sensor,
        input_bands=list(sensor.water_input_bands),
        fallback_bands=list(sensor.water_fallback_bands),
        threshold=float(sensor.water_threshold_default),
    )


def _dataset_summary_with_zarr_context(
    dataset_summary: dict[str, Any],
    *,
    zarr_uri: str,
) -> dict[str, Any]:
    summary = dict(dataset_summary or {})
    if summary.get("band_names") and summary.get("shape"):
        return summary

    try:
        root = open_zarr_group(zarr_uri, mode="r")
        context = read_context(root, zarr_uri=zarr_uri)
    except Exception:
        return summary

    if not summary.get("band_names"):
        summary["band_names"] = list(context.band_names)
    if not summary.get("shape"):
        summary["shape"] = list(context.imagery_shape)
    for key in ("pixel_size", "reference_pixel_size", "transform", "crs"):
        value = root.attrs.get(key)
        if value is not None and not summary.get(key):
            summary[key] = value
    return summary


def is_omniwater_required(*, provider: str, collection: str) -> bool:
    normalized_provider = str(provider or "").strip().lower()
    normalized_collection = str(collection or "").strip().upper()
    if normalized_provider == "copernicus" and normalized_collection == "SENTINEL-2":
        return True
    if normalized_provider == "usgs" and normalized_collection.startswith("LANDSAT"):
        return True
    return False


def _load_make_water_mask() -> tuple[Any, Any | None, str | None]:
    try:
        module = importlib.import_module("omniwatermask")
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask import failed "
            f"({exc}). Ensure omniwatermask is installed and OpenCV system libraries are present."
        ) from exc
    make_water_mask = getattr(module, "make_water_mask", None)
    if make_water_mask is None:
        raise ConversionDependencyError(
            "Installed omniwatermask package does not expose make_water_mask()."
        )
    return make_water_mask, getattr(module, "make_water_mask_debug", None), getattr(module, "__version__", None)


def _export_rgbnir_tiles(
    *,
    job_id: str | None = None,
    zarr_uri: str,
    tiles_dir: Path,
    dataset_summary: dict[str, Any] | DatasetSummaryRecord,
    input_bands: list[str],
    sensor: SensorMaskSpec,
    provider: str,
    collection: str,
    product_type: str | None,
    inference_device: str | None = None,
) -> dict[str, Any]:
    raise_if_cancel_requested(job_id)
    try:
        import numpy as np
        import rasterio
        from rasterio.transform import Affine
        from rasterio.windows import Window, transform as window_transform
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask export dependencies are unavailable "
            f"({exc}). Ensure rasterio and zarr are installed."
        ) from exc

    output_path = local_path_for_uri(zarr_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    root = zarr.open_group(str(output_path), mode="r")
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    context = read_context(root, zarr_uri=zarr_uri)
    missing = [name for name in input_bands if name not in context.band_names]
    if missing:
        raise ConversionError(
            f"Cannot export OmniWaterMask input because the Zarr store is missing bands: {missing}."
        )

    summary_record = (
        dataset_summary
        if isinstance(dataset_summary, DatasetSummaryRecord)
        else DatasetSummaryRecord.from_mapping(dataset_summary)
    )
    transform_values = list(summary_record.transform)
    crs = summary_record.crs
    if len(transform_values) < 6 or not crs:
        raise ConversionError("The Zarr summary is missing transform/crs for OmniWaterMask export.")

    height = int(summary_record.shape[2])
    width = int(summary_record.shape[3])
    transform = Affine(*transform_values[:6])
    tile_sizing = _watermask_tile_sizing(
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=summary_record,
        device=inference_device,
        model_patch_size=_watermask_inference_patch_size(),
    )
    tile_size = int(tile_sizing["tile_size"])
    tiles: list[OmniWaterTile] = []
    tiles_dir.mkdir(parents=True, exist_ok=True)
    tile_index = 0
    for row_start in range(0, height, tile_size):
        raise_if_cancel_requested(job_id)
        row_stop = min(height, row_start + tile_size)
        for col_start in range(0, width, tile_size):
            raise_if_cancel_requested(job_id)
            col_stop = min(width, col_start + tile_size)
            tile_index += 1
            tile_path = tiles_dir / f"rgbnir_tile_{tile_index:04d}.tif"
            tile_window = Window(
                col_off=col_start,
                row_off=row_start,
                width=col_stop - col_start,
                height=row_stop - row_start,
            )
            with rasterio.open(
                tile_path,
                "w",
                driver="GTiff",
                height=row_stop - row_start,
                width=col_stop - col_start,
                count=4,
                dtype=np.uint16,
                crs=crs,
                transform=window_transform(tile_window, transform),
                nodata=0,
            ) as dataset:
                try:
                    channels_result = read_required_channels_window(
                        root,
                        band_names=context.band_names,
                        required_bands=tuple(input_bands),
                        scale_hint=sensor.scale_hint,
                        row_start=row_start,
                        row_stop=row_stop,
                        col_start=col_start,
                        col_stop=col_stop,
                        normalize=True,
                        include_validity=True,
                    )
                except TypeError as exc:
                    if "include_validity" not in str(exc):
                        raise
                    channels_result = read_required_channels_window(
                        root,
                        band_names=context.band_names,
                        required_bands=tuple(input_bands),
                        scale_hint=sensor.scale_hint,
                        row_start=row_start,
                        row_stop=row_stop,
                        col_start=col_start,
                        col_stop=col_stop,
                        normalize=True,
                    )
                if len(channels_result) == 3:
                    channels, _missing, valid_mask = channels_result
                else:
                    channels, _missing = channels_result
                    valid_mask = None
                for output_band_index, band_name in enumerate(input_bands, start=1):
                    scaled = np.clip(
                        np.round(channels[band_name] * 10000.0),
                        0.0,
                        10000.0,
                    ).astype(np.uint16)
                    if valid_mask is not None:
                        scaled = np.where(valid_mask, scaled, 0).astype(np.uint16, copy=False)
                    dataset.write(scaled, output_band_index)
            tiles.append(
                OmniWaterTile(
                    path=tile_path,
                    row_start=row_start,
                    row_stop=row_stop,
                    col_start=col_start,
                    col_stop=col_stop,
                )
            )
    return {
        "tiles": tiles,
        "tile_size": tile_size,
        "tile_sizing": tile_sizing.to_dict(),
        "height": height,
        "width": width,
        "transform": transform,
        "crs": crs,
    }


def _run_omniwater_model(
    *,
    job_id: str | None = None,
    make_water_mask: Any,
    scene_paths: list[Path],
    output_dir: Path,
    cache_dir: Path,
    scene_dir: Path,
    tile_size: int,
    tile_sizing: dict[str, Any] | None = None,
    inference_device: str | None,
    stage_callback: Any = None,
    source_zarr_uri: str = "",
    target_zarr_uri: str = "",
    scene_id: str = "",
) -> tuple[Any, dict[str, Any]]:
    raise_if_cancel_requested(job_id)
    device = resolve_inference_device(
        explicit=inference_device,
        env_var="NIMBUS_WATERMASK_DEVICE",
    )
    signature = getattr(make_water_mask, "__signature__", None)
    inspect_module = None
    if signature is None:
        try:
            import inspect

            inspect_module = inspect
            signature = inspect.signature(make_water_mask)
        except Exception:
            signature = None
    else:
        try:
            import inspect

            inspect_module = inspect
        except Exception:
            inspect_module = None
    accepted = set(signature.parameters.keys()) if signature is not None else set()
    accepts_var_kwargs = False
    if signature is not None and inspect_module is not None:
        accepts_var_kwargs = any(
            getattr(parameter, "kind", None) == inspect_module.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
    normalized_device = normalize_device_name(device)
    _configure_osmnx_cache(cache_dir=cache_dir)
    auxiliary_options, auxiliary_summary = _omniwater_auxiliary_options(tile_sizing=tile_sizing)
    attempt_summaries: list[dict[str, Any]] = []
    last_exc: Exception | None = None
    for profile in _omniwater_model_profiles(device=device, tile_size=tile_size):
        kwargs: dict[str, Any] = {
            "scene_paths": scene_paths,
            "band_order": [1, 2, 3, 4],
            "output_dir": output_dir,
            "overwrite": True,
            "cache_dir": cache_dir,
            "batch_size": int(profile.batch_size),
            "mosaic_device": device,
            "inference_device": device,
            "inference_patch_size": int(profile.inference_patch_size),
            "inference_overlap_size": int(profile.inference_overlap_size),
            "destination_model_dir": _watermask_model_dir(scene_dir),
            "model_download_source": "hugging_face",
        }
        optional_kwargs = {
            "use_cache": _bool_env("NIMBUS_WATERMASK_OSMNX_USE_CACHE", default=True),
            "use_osm_water": auxiliary_options["use_osm_water"],
            "use_osm_building": auxiliary_options["use_osm_building"],
            "use_osm_roads": auxiliary_options["use_osm_roads"],
            "optimise_model": bool(profile.optimise_model),
            "use_model": True,
            "use_ndwi": True,
        }
        for key, value in optional_kwargs.items():
            if accepts_var_kwargs or not accepted or key in accepted:
                kwargs[key] = value
        attempt_summaries.append(
            {
                "profile": profile.name,
                "device": normalized_device,
                "batch_size": int(profile.batch_size),
                "inference_patch_size": int(profile.inference_patch_size),
                "inference_overlap_size": int(profile.inference_overlap_size),
                "optimise_model": bool(profile.optimise_model),
                "auxiliary_options": dict(auxiliary_summary),
            }
        )
        try:
            raise_if_cancel_requested(job_id)
            stop_progress = _start_model_progress_monitor(
                output_dir=output_dir,
                tiles_total=len(scene_paths),
                stage_callback=stage_callback,
                source_zarr_uri=source_zarr_uri,
                target_zarr_uri=target_zarr_uri,
                scene_id=scene_id,
            )
            try:
                raise_if_cancel_requested(job_id)
                result = make_water_mask(**kwargs)
            finally:
                if stop_progress is not None:
                    stop_progress()
            raise_if_cancel_requested(job_id)
            return result, {
                "device": normalized_device,
                "profile": profile.name,
                "attempt_count": len(attempt_summaries),
                "attempts": [dict(item) for item in attempt_summaries],
                "auxiliary_options": dict(auxiliary_summary),
            }
        except Exception as exc:
            last_exc = exc
            setattr(last_exc, "nimbus_attempts", [dict(item) for item in attempt_summaries])
            _LOGGER.warning(
                "OmniWater model attempt '%s' failed on device '%s' for %d tile(s): %s",
                profile.name,
                normalized_device,
                len(scene_paths),
                exc,
            )
    if last_exc is None:
        raise ConversionError("OmniWaterMask did not produce a model execution attempt.")
    raise last_exc


def _omniwater_auxiliary_options(*, tile_sizing: dict[str, Any] | None) -> tuple[dict[str, bool], dict[str, Any]]:
    requested = {
        "use_osm_water": _bool_env("NIMBUS_WATERMASK_USE_OSM_WATER", default=False),
        "use_osm_building": _bool_env("NIMBUS_WATERMASK_USE_OSM_BUILDING", default=False),
        "use_osm_roads": _bool_env("NIMBUS_WATERMASK_USE_OSM_ROADS", default=False),
    }
    summary: dict[str, Any] = {
        **requested,
        "default": "disabled",
        "guard_env": "NIMBUS_WATERMASK_OSM_MAX_SCENE_SPAN_METERS",
    }
    span_m = _float_or_none(dict(tile_sizing or {}).get("scene_ground_span_meters"))
    max_span_m = _watermask_osm_max_scene_span_meters()
    summary["scene_ground_span_meters"] = span_m
    summary["max_scene_span_meters"] = max_span_m
    if any(requested.values()) and max_span_m > 0 and span_m is not None and span_m > max_span_m:
        disabled = {key: False for key in requested}
        summary.update(
            {
                **disabled,
                "disabled_reason": (
                    "scene_ground_span_exceeds_osm_guard:"
                    f"{span_m:.1f}>{max_span_m:.1f}"
                ),
            }
        )
        return disabled, summary
    return requested, summary


def _watermask_osm_max_scene_span_meters() -> float:
    raw = str(os.getenv("NIMBUS_WATERMASK_OSM_MAX_SCENE_SPAN_METERS") or "").strip()
    try:
        return float(raw) if raw else 50000.0
    except ValueError:
        return 50000.0


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _bool_env(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _start_model_progress_monitor(
    *,
    output_dir: Path,
    tiles_total: int,
    stage_callback: Any,
    source_zarr_uri: str,
    target_zarr_uri: str,
    scene_id: str,
) -> Any | None:
    if stage_callback is None or tiles_total <= 0:
        return None
    interval = _model_progress_interval_seconds()
    stop_event = threading.Event()
    last_completed = -1
    started_at = datetime.now(timezone.utc)

    def completed_count() -> int:
        return min(tiles_total, len(_model_output_candidates(output_dir)))

    def emit(*, force: bool = False) -> None:
        nonlocal last_completed
        completed = completed_count()
        changed = completed != last_completed
        last_completed = completed
        stage_callback(
            "water_masking_progress",
            StageEventPayload.from_mapping({
                "zarr_uri": source_zarr_uri,
                "output_zarr_uri": target_zarr_uri,
                "scene_id": scene_id,
                "tiles_completed": completed,
                "tiles_total": tiles_total,
                "progress": round(completed / max(1, tiles_total), 4),
                "status": "model_inference",
                "heartbeat": not changed and not force,
                "elapsed_seconds": (
                    datetime.now(timezone.utc) - started_at
                ).total_seconds(),
            }),
        )

    def run() -> None:
        while not stop_event.wait(interval):
            emit()

    emit(force=True)
    thread = threading.Thread(target=run, name="omniwater-progress", daemon=True)
    thread.start()

    def stop() -> None:
        stop_event.set()
        thread.join(timeout=max(1.0, interval + 1.0))
        emit(force=True)

    return stop


def _model_output_candidates(output_dir: Path) -> list[Path]:
    return [
        path
        for path in output_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}
    ]


def _model_progress_interval_seconds() -> float:
    raw = str(os.getenv("NIMBUS_WATERMASK_MODEL_PROGRESS_SECONDS") or "").strip()
    try:
        value = float(raw) if raw else 5.0
    except ValueError:
        value = 5.0
    return max(0.25, value)


def _run_internal_ndwi(*, scene_paths: list[Path], output_dir: Path) -> list[Path]:
    try:
        import numpy as np
        import rasterio
    except ImportError as exc:
        raise ConversionDependencyError(
            f"Internal NDWI fallback requires numpy and rasterio ({exc})."
        ) from exc

    outputs: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for scene_path in scene_paths:
        mask_path = output_dir / f"{scene_path.stem}_water_mask.tif"
        with rasterio.open(scene_path) as src:
            green = src.read(2).astype(np.float32)
            nir = src.read(4).astype(np.float32)
            denom = green + nir
            ndwi = np.divide(
                green - nir,
                denom,
                out=np.zeros_like(green, dtype=np.float32),
                where=np.abs(denom) > 1e-6,
            )
            mask = (ndwi > 0.0).astype(np.uint8)
            profile = src.profile.copy()
            profile.update(count=1, dtype="uint8")
            with rasterio.open(mask_path, "w", **profile) as dst:
                dst.write(mask, 1)
        outputs.append(mask_path)
    return outputs


def _run_water_fallback_tiled(
    *,
    job_id: str | None = None,
    zarr_uri: str,
    provider: str,
    collection: str,
    product_type: str | None,
    dataset_summary: dict[str, Any],
    sensor: SensorMaskSpec,
    threshold: float,
    water_arr: Any,
    water_prob_arr: Any,
    inference_device: str | None,
    stage_callback: Any = None,
    source_zarr_uri: str = "",
    target_zarr_uri: str = "",
    scene_id: str = "",
) -> dict[str, Any]:
    raise_if_cancel_requested(job_id)
    root = open_zarr_group(zarr_uri, mode="a")
    context = read_context(root, zarr_uri=zarr_uri)
    imagery = root["imagery"]
    height = int(imagery.shape[2])
    width = int(imagery.shape[3])
    cloud_arr = None
    if "masks" in root and "cloud" in root["masks"]:
        cloud_arr = root["masks"]["cloud"]

    total_pixels = max(1, height * width)
    valid_pixels_total = 0
    water_pixels = 0
    cloud_blocked_pixels = 0
    probability_sum = 0.0
    effective_threshold = float(threshold)
    tile_sizing = _watermask_tile_sizing(
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=dataset_summary,
        device=inference_device,
        model_patch_size=_watermask_inference_patch_size(),
    )
    tile_size = int(tile_sizing["tile_size"])
    windows = [
        (
            row_start,
            min(height, row_start + tile_size),
            col_start,
            min(width, col_start + tile_size),
        )
        for row_start in range(0, height, tile_size)
        for col_start in range(0, width, tile_size)
    ]
    raise_if_cancel_requested(job_id)
    tile_workers = parallel_worker_count(
        device=resolve_inference_device(explicit=inference_device, env_var="NIMBUS_WATERMASK_DEVICE"),
        env_var="NIMBUS_WATERMASK_TILE_WORKERS",
        cpu_default=2,
        gpu_default=1,
        hard_limit=4,
    )

    def process_window(window: tuple[int, int, int, int]) -> tuple[tuple[int, int, int, int], np.ndarray, np.ndarray, dict[str, Any]]:
        raise_if_cancel_requested(job_id)
        row_start, row_stop, col_start, col_stop = window
        try:
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=sensor.water_fallback_bands,
                scale_hint=sensor.scale_hint,
                row_start=row_start,
                row_stop=row_stop,
                col_start=col_start,
                col_stop=col_stop,
                normalize=True,
                include_validity=True,
            )
        except TypeError as exc:
            if "include_validity" not in str(exc):
                raise
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=sensor.water_fallback_bands,
                scale_hint=sensor.scale_hint,
                row_start=row_start,
                row_stop=row_stop,
                col_start=col_start,
                col_stop=col_stop,
                normalize=True,
            )
        if len(channels_result) == 3:
            channels, _missing, valid_mask = channels_result
        else:
            channels, _missing = channels_result
            valid_mask = None
        raise_if_cancel_requested(job_id)
        cloud_window = None
        if cloud_arr is not None:
            cloud_window = np.asarray(
                cloud_arr[0, row_start:row_stop, col_start:col_stop],
                dtype=np.uint8,
            )
        probability_tile, mask_tile, tile_summary = _run_water_fallback_window(
            sensor=sensor,
            channels=channels,
            threshold=effective_threshold,
            cloud_mask=cloud_window,
            valid_mask=valid_mask,
        )
        return window, probability_tile, mask_tile, tile_summary

    executor: ThreadPoolExecutor | None = None
    if tile_workers <= 1 or len(windows) <= 1:
        results_iter = map(process_window, windows)
    else:
        executor = ThreadPoolExecutor(max_workers=tile_workers, thread_name_prefix="water-mask")
        results_iter = executor.map(process_window, windows)

    tile_index = 0
    try:
        for window, probability_tile, mask_tile, tile_summary in results_iter:
            raise_if_cancel_requested(job_id)
            row_start, row_stop, col_start, col_stop = window
            tile_index += 1
            water_arr[0, row_start:row_stop, col_start:col_stop] = mask_tile
            water_prob_arr[0, row_start:row_stop, col_start:col_stop] = probability_tile
            water_pixels += int(np.asarray(mask_tile, dtype=np.uint64).sum())
            probability_sum += float(np.asarray(probability_tile, dtype=np.float64).sum())
            cloud_blocked_pixels += int(tile_summary["cloud_blocked_pixels"])
            valid_pixels_total += int(tile_summary.get("valid_pixels") or 0)

            if stage_callback is not None and (tile_index == 1 or tile_index == len(windows) or tile_index % 8 == 0):
                stage_callback(
                    "water_masking_progress",
                    StageEventPayload.from_mapping({
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                        "tiles_completed": tile_index,
                        "tiles_total": len(windows),
                        "progress": round(tile_index / max(1, len(windows)), 4),
                    }),
                )
    finally:
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)

    raise_if_cancel_requested(job_id)
    denominator = max(1, valid_pixels_total or total_pixels)
    return {
        "runtime_mode": "heuristic_fallback",
        "water_fraction": float(water_pixels / denominator),
        "probability_mean": float(probability_sum / denominator),
        "threshold_used": effective_threshold,
        "sensor_recipe": sensor.sensor_key,
        "input_bands": list(sensor.water_fallback_bands),
        "probability_source": "water_score",
        "cloud_blocked_fraction": float(cloud_blocked_pixels / denominator),
        "valid_pixel_fraction": float(denominator / total_pixels),
        "tile_size": tile_size,
        "tile_sizing": tile_sizing,
        "tile_workers": tile_workers,
    }


def _run_water_fallback_window(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    cloud_mask: np.ndarray | None,
    valid_mask: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    blue, green, red, nir, swir1, swir2 = _water_recipe_channels(sensor=sensor, channels=channels)
    ndwi = _safe_index(green, nir)
    mndwi = _safe_index(green, swir1)
    ndvi = _safe_index(nir, red)
    swir_mean = (swir1 + swir2) / 2.0
    brightness = np.clip((blue + green + red) / 3.0, 0.0, 1.0)
    darkness = 1.0 - np.clip((0.55 * nir) + (0.45 * swir_mean), 0.0, 1.0)
    awei_like = (4.0 * (green - swir1)) - (0.25 * nir + 2.75 * swir2)

    ndwi_score = np.clip((ndwi + 0.18) / 0.78, 0.0, 1.0)
    mndwi_score = np.clip((mndwi + 0.22) / 0.82, 0.0, 1.0)
    awei_score = np.clip((awei_like + 0.25) / 1.35, 0.0, 1.0)
    darkness_score = np.clip(darkness, 0.0, 1.0)
    vegetation_penalty = np.clip((ndvi - 0.08) / 0.42, 0.0, 1.0)
    dry_bright_penalty = np.clip((brightness - 0.46) / 0.34, 0.0, 1.0) * (1.0 - mndwi_score)

    probability = (
        0.34 * ndwi_score
        + 0.34 * mndwi_score
        + 0.20 * awei_score
        + 0.12 * darkness_score
        - 0.18 * vegetation_penalty
        - 0.10 * dry_bright_penalty
    )
    probability = np.clip(probability, 0.0, 1.0).astype(np.float32, copy=False)

    cloud_blocked_pixels = 0
    if cloud_mask is not None:
        cloud_block = np.asarray(cloud_mask, dtype=np.uint8) > 0
        if valid_mask is not None:
            cloud_block = np.logical_and(cloud_block, np.asarray(valid_mask, dtype=bool))
        cloud_blocked_pixels = int(cloud_block.sum())
        probability = np.where(cloud_block, 0.0, probability)

    raw_mask = probability >= float(threshold)
    refined_mask = _refine_binary_mask(raw_mask)
    if cloud_mask is not None:
        refined_mask = np.logical_and(refined_mask, np.asarray(cloud_mask, dtype=np.uint8) == 0)
    if valid_mask is not None:
        valid = np.asarray(valid_mask, dtype=bool)
        if valid.shape != probability.shape:
            raise ValueError(
                f"valid_mask shape {valid.shape} does not match water tile shape {probability.shape}."
            )
        probability = np.where(valid, probability, 0.0).astype(np.float32, copy=False)
        refined_mask = np.logical_and(refined_mask, valid)
        valid_pixels = int(valid.sum())
    else:
        valid_pixels = int(probability.size)
    return (
        probability.astype(np.float32, copy=False),
        refined_mask.astype(np.uint8, copy=False),
        {
            "cloud_blocked_pixels": cloud_blocked_pixels,
            "valid_pixels": valid_pixels,
        },
    )


def _write_model_water_outputs(
    *,
    job_id: str | None = None,
    output_zarr_uri: str,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    water_arr: Any,
    water_prob_arr: Any,
    sensor: SensorMaskSpec,
    input_bands: tuple[str, ...],
) -> dict[str, Any]:
    raise_if_cancel_requested(job_id)
    root = open_zarr_group(output_zarr_uri, mode="a")
    context = read_context(root, zarr_uri=output_zarr_uri)
    height = int(root["imagery"].shape[2])
    width = int(root["imagery"].shape[3])
    cloud_arr = None
    if "masks" in root and "cloud" in root["masks"]:
        cloud_arr = root["masks"]["cloud"]

    total_pixels = max(1, height * width)
    water_pixels = 0
    cloud_blocked_pixels = 0
    valid_pixels_total = 0
    for tile, mask_path in zip(tiles, mask_paths, strict=True):
        raise_if_cancel_requested(job_id)
        tile_mask = _read_mask(mask_path).astype(np.uint8, copy=False)
        try:
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=input_bands,
                scale_hint=sensor.scale_hint,
                row_start=tile.row_start,
                row_stop=tile.row_stop,
                col_start=tile.col_start,
                col_stop=tile.col_stop,
                normalize=False,
                include_validity=True,
            )
        except TypeError as exc:
            if "include_validity" not in str(exc):
                raise
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=input_bands,
                scale_hint=sensor.scale_hint,
                row_start=tile.row_start,
                row_stop=tile.row_stop,
                col_start=tile.col_start,
                col_stop=tile.col_stop,
                normalize=False,
            )
        if len(channels_result) == 3:
            _channels, _missing, valid_mask = channels_result
        else:
            _channels, _missing = channels_result
            valid_mask = None
        if cloud_arr is not None:
            cloud_window = np.asarray(
                cloud_arr[0, tile.row_start:tile.row_stop, tile.col_start:tile.col_stop],
                dtype=np.uint8,
            )
            cloud_block = cloud_window > 0
            if valid_mask is not None:
                cloud_block = np.logical_and(cloud_block, valid_mask)
            cloud_blocked_pixels += int(cloud_block.sum())
            tile_mask = np.where(cloud_block, 0, tile_mask).astype(np.uint8, copy=False)
        if valid_mask is not None:
            tile_mask = np.where(valid_mask, tile_mask, 0).astype(np.uint8, copy=False)
            tile_valid_pixels = int(valid_mask.sum())
        else:
            tile_valid_pixels = int(tile_mask.size)
        water_arr[0, tile.row_start:tile.row_stop, tile.col_start:tile.col_stop] = tile_mask
        water_prob_arr[0, tile.row_start:tile.row_stop, tile.col_start:tile.col_stop] = tile_mask.astype(np.float32)
        water_pixels += int(np.asarray(tile_mask, dtype=np.uint64).sum())
        valid_pixels_total += tile_valid_pixels

    denominator = max(1, valid_pixels_total or total_pixels)
    return {
        "runtime_mode": "model",
        "water_fraction": float(water_pixels / denominator),
        "probability_mean": float(water_pixels / denominator),
        "threshold_used": None,
        "probability_source": "model_binary_mask",
        "cloud_blocked_fraction": float(cloud_blocked_pixels / denominator),
        "valid_pixel_fraction": float(denominator / total_pixels),
    }


def _water_recipe_channels(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    if sensor.sensor_key == "sentinel-2":
        return (
            channels["B02"],
            channels["B03"],
            channels["B04"],
            channels["B08"],
            channels["B11"],
            channels["B12"],
        )
    if sensor.sensor_key == "landsat-8-9-l1":
        return (
            channels["B2"],
            channels["B3"],
            channels["B4"],
            channels["B5"],
            channels["B6"],
            channels["B7"],
        )
    if sensor.sensor_key == "landsat-8-9-l2":
        return (
            channels["SR_B2"],
            channels["SR_B3"],
            channels["SR_B4"],
            channels["SR_B5"],
            channels["SR_B6"],
            channels["SR_B7"],
        )
    raise ConversionError(f"Unsupported water-mask sensor recipe: {sensor.sensor_key}")


def _safe_index(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    denom = a + b
    return np.divide(a - b, denom, out=np.zeros_like(a, dtype=np.float32), where=np.abs(denom) > 1e-6)


def _delete_water_layers(*, zarr_uri: str) -> None:
    delete_mask_layers(zarr_uri, layer_names=("water", "water_probability"))


def _refine_binary_mask(mask: np.ndarray) -> np.ndarray:
    mask_bool = np.asarray(mask, dtype=bool)
    neighbor_count = _box_filter2d(mask_bool.astype(np.uint8), radius=1)
    filled = np.logical_or(mask_bool, neighbor_count >= 7)
    keep = np.logical_and(filled, neighbor_count >= 2)
    return keep


def _box_filter2d(array: np.ndarray, *, radius: int) -> np.ndarray:
    if radius <= 0:
        return np.asarray(array, dtype=np.uint8)
    src = np.asarray(array, dtype=np.uint8)
    padded = np.pad(src, radius, mode="edge")
    height, width = src.shape
    result = np.zeros_like(src, dtype=np.uint8)
    for row_offset in range(0, 2 * radius + 1):
        row_slice = slice(row_offset, row_offset + height)
        for col_offset in range(0, 2 * radius + 1):
            col_slice = slice(col_offset, col_offset + width)
            result = result + padded[row_slice, col_slice]
    return result


def _normalize_mask_outputs(result: Any, *, output_dir: Path, expected_count: int) -> list[Path]:
    outputs: list[Path] = []
    if isinstance(result, (str, Path)):
        candidate = Path(result)
        if candidate.exists():
            outputs.append(candidate)
    elif isinstance(result, list):
        for item in result:
            if isinstance(item, (str, Path)):
                candidate = Path(item)
                if candidate.exists():
                    outputs.append(candidate)
    if len(outputs) != expected_count:
        tif_candidates = sorted(output_dir.rglob("*.tif")) + sorted(output_dir.rglob("*.tiff"))
        outputs = [candidate for candidate in tif_candidates if candidate.exists()]
    if len(outputs) != expected_count:
        raise ConversionError(
            f"OmniWaterMask produced {len(outputs)} readable output raster(s), expected {expected_count}."
        )
    return outputs


def _read_mask(mask_path: Path) -> Any:
    try:
        import rasterio
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask mask-read dependencies are unavailable "
            f"({exc}). Ensure rasterio is installed."
        ) from exc

    with rasterio.open(mask_path) as src:
        return src.read(1)


def _stitch_masks(
    *,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    height: int,
    width: int,
) -> Any:
    try:
        import numpy as np
    except ImportError as exc:
        raise ConversionDependencyError(f"numpy is required to stitch tile masks ({exc}).") from exc

    stitched = np.zeros((height, width), dtype=np.uint8)
    for tile, mask_path in zip(tiles, mask_paths, strict=True):
        tile_mask = _read_mask(mask_path)
        expected_shape = (tile.row_stop - tile.row_start, tile.col_stop - tile.col_start)
        if tuple(tile_mask.shape) != expected_shape:
            raise ConversionError(
                f"Tile mask shape mismatch for '{mask_path.name}': expected {expected_shape}, got {tuple(tile_mask.shape)}."
            )
        stitched[tile.row_start:tile.row_stop, tile.col_start:tile.col_stop] = tile_mask
    return stitched


def _normalize_coord_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _watermask_work_root() -> str | None:
    configured_root = str(os.getenv("NIMBUS_WATERMASK_DIR") or "").strip()
    data_root = str(os.getenv("NIMBUS_DATA_DIR") or "").strip()
    candidates: list[str] = []
    if configured_root:
        candidates.append(configured_root)
    if data_root:
        candidates.append(str(Path(data_root) / "watermask"))
    candidates.extend(
        [
            "/data/downloads/watermask",
            str(Path.cwd() / "data" / "downloads" / "watermask"),
        ]
    )
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        return str(path)
    return None


def _masked_zarr_root() -> str | None:
    configured_root = str(os.getenv("NIMBUS_ZARRMASK_DIR") or "").strip()
    data_root = str(os.getenv("NIMBUS_DATA_DIR") or "").strip()
    candidates: list[str] = []
    if configured_root:
        candidates.append(configured_root)
    if data_root:
        candidates.append(str(Path(data_root) / "zarrmask"))
    candidates.extend(
        [
            "/data/downloads/zarrmask",
            str(Path.cwd() / "data" / "downloads" / "zarrmask"),
            str(Path.cwd() / "download" / "zarrmask"),
        ]
    )
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        return str(path)
    return None


def _watermask_scene_dir(*, job_id: str | None, scene_id: str) -> Path:
    root = Path(_watermask_work_root() or "/tmp")
    safe_job_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(job_id or "").strip()).strip("._")
    safe_job_id = safe_job_id or "standalone"
    safe_scene_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in scene_id).strip("._")
    safe_scene_id = safe_scene_id or "unknown_scene"
    path = root / safe_job_id / safe_scene_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _water_storage_mode(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    source_value = str(source_zarr_uri or "").strip()
    output_value = str(output_zarr_uri or "").strip()
    if source_value and output_value and source_value == output_value:
        return "in_place_zarr_masking"
    return "derived_zarr_copy"


def _masked_zarr_output_uri(*, source_zarr_uri: str, scene_dir: Path, scene_id: str) -> str:
    source_path = local_path_for_uri(source_zarr_uri)
    safe_scene_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in scene_id).strip("._")
    safe_scene_id = safe_scene_id or "unknown_scene"
    if source_path.suffix == ".zarr":
        source_parent = source_path.parent
        if source_parent.name.lower() == "zarr":
            root = source_parent.parent / "zarrmask"
        else:
            root = source_parent / "zarrmask"
        root.mkdir(parents=True, exist_ok=True)
        return str(root / f"{source_path.stem}__watermask.zarr")
    root = Path(_masked_zarr_root() or scene_dir)
    return str(root / f"{safe_scene_id}__watermask.zarr")


def _prepare_masked_zarr_copy(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    return copy_source_zarr(source_zarr_uri=source_zarr_uri, output_zarr_uri=output_zarr_uri)


def _prepare_masked_zarr_output(*, source_zarr_uri: str, output_zarr_uri: str, overwrite: bool = True) -> str:
    source_path = local_path_for_uri(source_zarr_uri)
    output_path = local_path_for_uri(output_zarr_uri)
    if source_path.resolve() == output_path.resolve():
        if not output_path.exists():
            raise ConversionError(f"Masked Zarr store not found: {output_zarr_uri}")
        return str(output_path)
    if output_path.exists() and not overwrite:
        return str(output_path)
    return _prepare_masked_zarr_copy(source_zarr_uri=source_zarr_uri, output_zarr_uri=output_zarr_uri)


def _cleanup_masked_zarr_copy(output_zarr_uri: str) -> None:
    target = local_path_for_uri(output_zarr_uri)
    try:
        if target.exists():
            shutil.rmtree(target)
    except OSError:
        return


def _prepare_scene_dir(scene_dir: Path) -> None:
    for candidate in (scene_dir / "cache", scene_dir / "outputs", scene_dir / "tiles"):
        if candidate.exists():
            shutil.rmtree(candidate, ignore_errors=True)
    for candidate in (
        scene_dir / "water_mask.tif",
        scene_dir / "water_mask_status.json",
    ):
        if candidate.exists():
            candidate.unlink()

def _write_mask_artifact(
    *,
    scene_dir: Path,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    height: int,
    width: int,
    crs: Any,
    transform: Any,
) -> Path:
    try:
        import numpy as np
        import rasterio
        from rasterio.windows import Window
    except ImportError as exc:
        raise ConversionDependencyError(
            f"rasterio is required to persist the final water-mask artifact ({exc})."
        ) from exc
    artifact_path = scene_dir / "water_mask.tif"
    with rasterio.open(
        artifact_path,
        "w",
        driver="GTiff",
        height=int(height),
        width=int(width),
        count=1,
        dtype="uint8",
        crs=crs,
        transform=transform,
    ) as dst:
        for tile, mask_path in zip(tiles, mask_paths, strict=True):
            with rasterio.open(mask_path) as src:
                tile_mask = src.read(1)
            expected_shape = (tile.row_stop - tile.row_start, tile.col_stop - tile.col_start)
            if tuple(tile_mask.shape) != expected_shape:
                raise ConversionError(
                    f"Tile mask shape mismatch for '{mask_path.name}': expected {expected_shape}, got {tuple(tile_mask.shape)}."
                )
            dst.write(
                np.asarray(tile_mask, dtype=np.uint8),
                1,
                window=Window(
                    col_off=tile.col_start,
                    row_off=tile.row_start,
                    width=tile.col_stop - tile.col_start,
                    height=tile.row_stop - tile.row_start,
                ),
            )
    return artifact_path


def _is_legacy_model_dependency_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "fastai is not installed" in message
        or "legacy model support" in message
        or "versions 1-3" in message
        or "must enable use_model" in message
    )


def _is_watermask_scratch_capacity_error(exc: Exception) -> bool:
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)
        if getattr(current, "errno", None) == errno.ENOSPC:
            return True
        message = str(current).strip().lower()
        if (
            "no space left on device" in message
            or "disk full" in message
            or "not enough space" in message
            or "filesystem full" in message
        ):
            return True
        cause = getattr(current, "__cause__", None)
        context = getattr(current, "__context__", None)
        if cause is not None:
            pending.append(cause)
        if context is not None:
            pending.append(context)
    return False


def _should_fail_open_model_runtime(*, exc: Exception, device: str | None) -> bool:
    if _is_legacy_model_dependency_error(exc):
        return True
    if _is_watermask_scratch_capacity_error(exc):
        return True
    return normalize_device_name(device) == "mps" and _watermask_mps_fail_open()


def _format_model_runtime_warning(*, exc: Exception, device: str | None) -> str:
    normalized_device = normalize_device_name(device)
    reason = str(exc).strip()
    if _is_watermask_scratch_capacity_error(exc):
        return (
            "OmniWater scratch export ran out of local disk space; Nimbus switched this scene "
            f"to the tiled heuristic fallback to keep the pipeline running ({reason})."
        )
    if normalized_device == "mps":
        return (
            "OmniWater model runtime failed on MPS; Nimbus switched this scene to the tiled "
            f"heuristic fallback to keep the pipeline running ({reason})."
        )
    return (
        "OmniWater model runtime fell back to the tiled heuristic water mask "
        f"({reason})."
    )


def _model_attempt_summaries(exc: Exception) -> list[dict[str, Any]]:
    raw = getattr(exc, "nimbus_attempts", None)
    if not isinstance(raw, list):
        return []
    summaries: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        summaries.append(dict(item))
    return summaries


def _watermask_tile_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_TILE_SIZE") or "").strip()
    if raw:
        try:
            return max(256, int(raw))
        except ValueError:
            pass
    return 512


def _watermask_tile_sizing(
    *,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
    dataset_summary: dict[str, Any] | DatasetSummaryRecord | None,
    device: str | None,
    model_patch_size: int,
) -> Any:
    return choose_mask_tile_sizing(
        mask_kind="water",
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=(
            dataset_summary
            if isinstance(dataset_summary, DatasetSummaryRecord)
            else DatasetSummaryRecord.from_mapping(dataset_summary)
        ),
        device=device,
        model_patch_size=model_patch_size,
        env_var="NIMBUS_WATERMASK_TILE_SIZE",
    )


def _watermask_runtime_mode(explicit: str | None = None) -> str:
    raw = str(explicit or os.getenv("NIMBUS_WATERMASK_RUNTIME_MODE") or "").strip().lower()
    if raw in {"fallback", "heuristic", "ndwi"}:
        return "fallback"
    if _omniwater_module_available():
        return "model"
    return "fallback"


def _watermask_mps_safe_mode() -> bool:
    return _bool_env("NIMBUS_WATERMASK_MPS_SAFE_MODE", default=True)


def _watermask_mps_fail_open() -> bool:
    return _bool_env("NIMBUS_WATERMASK_MPS_FAIL_OPEN", default=True)


def _omniwater_model_profiles(*, device: str | None, tile_size: int) -> list[OmniWaterModelProfile]:
    normalized_device = normalize_device_name(device)
    requested_batch_size = batch_size_for_device(
        device=device,
        env_var="NIMBUS_WATERMASK_BATCH_SIZE",
        cpu_default=1,
        gpu_default=2,
        hard_limit=8,
    )
    base_patch_size = min(_watermask_inference_patch_size(), tile_size)
    base_overlap_size = min(_watermask_inference_overlap_size(), max(0, tile_size - 1))
    if normalized_device == "mps":
        safe_mode = _watermask_mps_safe_mode()
        primary = OmniWaterModelProfile(
            name="mps_safe",
            batch_size=1 if safe_mode else requested_batch_size,
            inference_patch_size=base_patch_size,
            inference_overlap_size=base_overlap_size,
            optimise_model=False if safe_mode else True,
        )
        profiles = [primary]
        compact_patch_size = min(base_patch_size, 256)
        compact_overlap_size = min(base_overlap_size, max(0, min(64, compact_patch_size // 4)))
        compact = OmniWaterModelProfile(
            name="mps_compact_retry",
            batch_size=1,
            inference_patch_size=compact_patch_size,
            inference_overlap_size=compact_overlap_size,
            optimise_model=False,
        )
        if compact != primary:
            profiles.append(compact)
        return profiles
    return [
        OmniWaterModelProfile(
            name=f"{normalized_device or 'cpu'}_default",
            batch_size=requested_batch_size,
            inference_patch_size=base_patch_size,
            inference_overlap_size=base_overlap_size,
            optimise_model=normalized_device == "cuda",
        )
    ]


def _omniwater_module_available() -> bool:
    if "omniwatermask" in sys.modules:
        return True
    try:
        importlib.import_module("omniwatermask")
    except Exception:
        return False
    return True


def _configure_osmnx_cache(*, cache_dir: Path) -> dict[str, Any]:
    """Point OSMnx at a writable cache before OmniWater starts worker threads."""
    try:
        import osmnx as ox
    except Exception as exc:
        return {"configured": False, "reason": f"osmnx_import_failed:{exc}"}

    use_cache = _bool_env("NIMBUS_WATERMASK_OSMNX_USE_CACHE", default=True)
    cache_root = _watermask_osmnx_cache_dir(cache_dir=cache_dir)
    if use_cache:
        try:
            cache_root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _LOGGER.warning(
                "OSMnx cache directory '%s' is not writable; disabling OSMnx cache for this run: %s",
                cache_root,
                exc,
            )
            ox.settings.use_cache = False
            return {
                "configured": False,
                "enabled": False,
                "cache_folder": str(cache_root),
                "reason": str(exc),
            }
        ox.settings.cache_folder = str(cache_root)
    ox.settings.use_cache = bool(use_cache)
    return {
        "configured": True,
        "enabled": bool(use_cache),
        "cache_folder": str(cache_root),
    }


def _watermask_osmnx_cache_dir(*, cache_dir: Path) -> Path:
    configured = str(os.getenv("NIMBUS_WATERMASK_OSMNX_CACHE_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return cache_dir / "osmnx"


def _watermask_scratch_root(*, output_zarr_uri: str) -> Path:
    configured = str(os.getenv("NIMBUS_WATERMASK_TMP_DIR") or "").strip()
    if configured:
        target = Path(configured).expanduser()
    else:
        output_path = local_path_for_uri(output_zarr_uri)
        target = output_path.parent / ".nimbus-mask-tmp" / "water"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _watermask_model_dir(scene_dir: Path) -> Path:
    configured = str(os.getenv("NIMBUS_WATERMASK_MODEL_DIR") or "").strip()
    target = Path(configured) if configured else scene_dir.parent / "_models"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _watermask_inference_patch_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 512
    except ValueError:
        value = 512
    return max(128, value)


def _watermask_inference_overlap_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_INFERENCE_OVERLAP_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 128
    except ValueError:
        value = 128
    return max(0, value)


def _persist_status_artifacts(
    *,
    scene_dir: Path,
    status_path: Path,
    payload: WaterMaskState | dict[str, Any],
) -> None:
    scene_dir.mkdir(parents=True, exist_ok=True)
    serializable_payload = payload.to_dict() if hasattr(payload, "to_dict") else dict(payload)
    serializable_payload.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    status_path.write_text(json.dumps(serializable_payload, indent=2, sort_keys=True))


def _sync_zarr_mask_attrs(*, zarr_uri: str, payload: WaterMaskState | dict[str, Any]) -> None:
    payload_dict = payload.to_dict() if hasattr(payload, "to_dict") else dict(payload)
    try:
        import zarr
    except ImportError:
        return

    try:
        output_path = local_path_for_uri(zarr_uri)
        if not output_path.exists():
            return
    except ConversionError:
        return
    root = zarr.open_group(str(output_path), mode="a", zarr_format=2, use_consolidated=False)
    root.attrs["water_mask_status"] = str(payload_dict.get("status") or "")
    root.attrs["water_mask_reason"] = str(payload_dict.get("reason") or "")
    root.attrs["water_mask_status_path"] = str(payload_dict.get("status_path") or "")
    root.attrs["water_mask_artifact_uri"] = str(payload_dict.get("artifact_uri") or "")
    root.attrs["water_mask_work_dir"] = str(payload_dict.get("work_dir") or "")
    if str(payload_dict.get("status") or "").strip().lower() != "written":
        root.attrs["water_mask_written"] = False
    else:
        model_attrs = {
            "model_name": payload_dict.get("model_name"),
            "model_version": payload_dict.get("model_version"),
            "runtime_mode": payload_dict.get("runtime_mode"),
            "sensor_recipe": payload_dict.get("sensor_recipe"),
            "probability_source": payload_dict.get("probability_source"),
            "model_profile": payload_dict.get("model_profile"),
            "model_attempt_count": payload_dict.get("model_attempt_count"),
            "model_auxiliary_options": dict(payload_dict.get("model_auxiliary_options") or {}),
            "tile_size": payload_dict.get("tile_size"),
            "tile_sizing": dict(payload_dict.get("tile_sizing") or {}),
            "runtime_warning": payload_dict.get("runtime_warning"),
            "fallback_trigger": payload_dict.get("fallback_trigger"),
        }
        existing_water_mask = root.attrs.get("water_mask")
        water_mask_attrs = dict(existing_water_mask) if isinstance(existing_water_mask, dict) else {}
        water_mask_attrs.update(model_attrs)
        root.attrs["water_mask"] = water_mask_attrs
        root.attrs["water_mask_model_name"] = str(payload_dict.get("model_name") or "")
        root.attrs["water_mask_model_version"] = str(payload_dict.get("model_version") or "")
        root.attrs["water_mask_model_auxiliary_options"] = dict(
            payload_dict.get("model_auxiliary_options") or {}
        )
        root.attrs["water_mask_runtime_mode"] = str(payload_dict.get("runtime_mode") or "")
        root.attrs["water_mask_tile_size"] = int(payload_dict.get("tile_size") or 0)

        masks_group = root.get("masks")
        if masks_group is not None:
            for array_name in ("water", "water_probability"):
                try:
                    mask_array = masks_group[array_name]
                except Exception:
                    mask_array = None
                if mask_array is not None:
                    mask_array.attrs.update(model_attrs)
    zarr.consolidate_metadata(root.store)
