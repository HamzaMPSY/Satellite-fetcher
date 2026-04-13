from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any
import importlib

import numpy as np

from nimbuschain_mask_service.io import (
    copy_source_zarr,
    delete_mask_layers,
    local_path_for_uri,
    open_zarr_group,
    read_context,
    read_required_channels_window,
)
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec, resolve_sensor_mask_spec
from nimbuschain_mask_service.runtime import (
    batch_size_for_device,
    normalize_device_name,
    parallel_worker_count,
    resolve_inference_device,
)
from nimbuschain_mask_service.writers import (
    finalize_water_outputs,
    prepare_water_output_arrays,
    write_water_mask_to_zarr,
)
from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError


_S2_INPUT_BANDS = ["B04", "B03", "B02", "B08"]
_LANDSAT_L1_INPUT_BANDS = ["B4", "B3", "B2", "B5"]
_LANDSAT_L2_INPUT_BANDS = ["SR_B4", "SR_B3", "SR_B2", "SR_B5"]


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


def omniwater_support_status() -> dict[str, Any]:
    return {
        "available": _omniwater_module_available(),
        "module": "omniwatermask",
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
    plan = _build_plan(
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=dataset_summary,
    )
    if not plan.supported:
        status = "failed" if plan.required else "skipped"
        result = {
            "status": status,
            "reason": plan.reason or "unsupported",
            "input_zarr_uri": source_lineage_uri,
            "output_zarr_uri": masked_zarr_uri,
            "storage_mode": storage_mode,
            "input_bands": list(plan.input_bands),
            "fallback_bands": list(plan.fallback_bands),
            "threshold_used": plan.threshold,
            "mask_path": None,
            "probability_path": None,
            "artifact_uri": None,
            "status_path": None,
            "work_dir": None,
        }
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
            make_water_mask = None

    target_root: Any | None = None

    try:
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
                {
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                },
            )
        if preferred_runtime == "model" and make_water_mask is not None:
            with tempfile.TemporaryDirectory(prefix=f"nimbus-water-{scene_id}-") as tmp_dir:
                scene_dir = Path(tmp_dir)
                tiles_dir = scene_dir / "tiles"
                cache_dir = scene_dir / "cache"
                output_dir = scene_dir / "outputs"
                output_dir.mkdir(parents=True, exist_ok=True)
                cache_dir.mkdir(parents=True, exist_ok=True)
                tile_manifest = _export_rgbnir_tiles(
                    zarr_uri=prepared_output_zarr_uri,
                    tiles_dir=tiles_dir,
                    dataset_summary=dataset_summary,
                    input_bands=plan.input_bands,
                    sensor=plan.sensor,
                )
                tile_paths = [tile.path for tile in tile_manifest["tiles"]]
                try:
                    mask_output = _run_omniwater_model(
                        make_water_mask=make_water_mask,
                        scene_paths=tile_paths,
                        output_dir=output_dir,
                        cache_dir=cache_dir,
                        scene_dir=scene_dir,
                        tile_size=int(tile_manifest["tile_size"]),
                        inference_device=resolved_device,
                    )
                    mask_paths = _normalize_mask_outputs(
                        mask_output,
                        output_dir=output_dir,
                        expected_count=len(tile_paths),
                    )
                    water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
                    runtime_summary = _write_model_water_outputs(
                        output_zarr_uri=prepared_output_zarr_uri,
                        tiles=tile_manifest["tiles"],
                        mask_paths=mask_paths,
                        water_arr=water_arr,
                        water_prob_arr=water_prob_arr,
                        sensor=plan.sensor,
                        input_bands=tuple(plan.input_bands),
                    )
                    runtime_mode = "model"
                    input_bands = list(plan.input_bands)
                except Exception as exc:
                    if not _is_legacy_model_dependency_error(exc):
                        raise
                    water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
                    runtime_summary = _run_water_fallback_tiled(
                        zarr_uri=prepared_output_zarr_uri,
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
        else:
            water_arr, water_prob_arr = prepare_water_output_arrays(target_root, overwrite=True)
            runtime_summary = _run_water_fallback_tiled(
                zarr_uri=prepared_output_zarr_uri,
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

        result = finalize_water_outputs(
            target_root,
            runtime_mode=runtime_mode,
            sensor_key=str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
            threshold=(runtime_summary.get("threshold_used") if runtime_mode != "model" else None),
            input_bands=tuple(input_bands),
            metadata={
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
                "threshold_used": runtime_summary.get("threshold_used"),
                "sensor_recipe": str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
                "probability_source": str(runtime_summary.get("probability_source") or "water_score"),
            },
            water_fraction=float(runtime_summary.get("water_fraction") or 0.0),
            water_arr=water_arr,
            water_prob_arr=water_prob_arr,
            summary=runtime_summary,
        )
        target_root.attrs["source_zarr_uri"] = source_lineage_uri
        target_root.attrs["masked_zarr_uri"] = prepared_output_zarr_uri
        result.update(
            {
                "model_name": "omniwatermask" if runtime_mode == "model" else "omniwatermask_heuristic_fallback",
                "model_version": module_version,
                "input_bands": list(input_bands),
            }
        )
        payload = {
            "status": "written",
            "reason": None,
            "input_zarr_uri": source_lineage_uri,
            "working_zarr_uri": prepared_output_zarr_uri,
            "output_zarr_uri": prepared_output_zarr_uri,
            "storage_mode": storage_mode,
            "input_bands": list(input_bands),
            "fallback_bands": list(plan.fallback_bands),
            "mask_path": result["mask_path"],
            "probability_path": result["probability_path"],
            "artifact_uri": None,
            "status_path": None,
            "work_dir": None,
            "shape": result["mask_shape"],
            "dtype": result["mask_dtype"],
            "probability_dtype": result["probability_dtype"],
            "classes": result["classes"],
            "model_name": "omniwatermask" if runtime_mode == "model" else "omniwatermask_heuristic_fallback",
            "model_version": module_version,
            "written_at": result["written_at"],
            "runtime_mode": runtime_mode,
            "threshold_used": runtime_summary.get("threshold_used"),
            "sensor_recipe": str(plan.sensor.sensor_key if plan.sensor is not None else "unknown"),
            "water_fraction": float(runtime_summary.get("water_fraction") or 0.0),
            "probability_source": str(runtime_summary.get("probability_source") or "water_score"),
            "cloud_blocked_fraction": float(runtime_summary.get("cloud_blocked_fraction") or 0.0),
        }
        _sync_zarr_mask_attrs(zarr_uri=prepared_output_zarr_uri, payload=payload)
        if stage_callback is not None:
            stage_callback(
                "water_masking_finished",
                {
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload,
                },
            )
        return payload
    except Exception as exc:
        payload = {
            "status": "failed",
            "reason": str(exc),
            "input_zarr_uri": source_lineage_uri,
            "output_zarr_uri": masked_zarr_uri,
            "storage_mode": storage_mode,
            "input_bands": list(plan.input_bands),
            "mask_path": None,
            "probability_path": None,
            "artifact_uri": None,
            "status_path": None,
            "work_dir": None,
        }
        if storage_mode == "in_place_zarr_masking":
            target_root = None
            _delete_water_layers(zarr_uri=masked_zarr_uri)
            _sync_zarr_mask_attrs(zarr_uri=masked_zarr_uri, payload=payload)
        else:
            _cleanup_masked_zarr_copy(masked_zarr_uri)
        if stage_callback is not None:
            stage_callback(
                "water_masking_failed",
                {
                    "zarr_uri": source_lineage_uri,
                    "output_zarr_uri": masked_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload,
                },
            )
        if fail_on_error and plan.required:
            raise ConversionError(f"OmniWaterMask failed for scene '{scene_id}' ({exc}).") from exc
        return payload


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
    zarr_uri: str,
    tiles_dir: Path,
    dataset_summary: dict[str, Any],
    input_bands: list[str],
    sensor: SensorMaskSpec,
) -> dict[str, Any]:
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

    transform_values = list(dataset_summary.get("transform") or [])
    crs = dataset_summary.get("crs")
    if len(transform_values) < 6 or not crs:
        raise ConversionError("The Zarr summary is missing transform/crs for OmniWaterMask export.")

    height = int(dataset_summary["shape"][2])
    width = int(dataset_summary["shape"][3])
    transform = Affine(*transform_values[:6])
    tile_size = _watermask_tile_size()
    tiles: list[OmniWaterTile] = []
    tiles_dir.mkdir(parents=True, exist_ok=True)
    tile_index = 0
    for row_start in range(0, height, tile_size):
        row_stop = min(height, row_start + tile_size)
        for col_start in range(0, width, tile_size):
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
        "height": height,
        "width": width,
        "transform": transform,
        "crs": crs,
    }


def _run_omniwater_model(
    *,
    make_water_mask: Any,
    scene_paths: list[Path],
    output_dir: Path,
    cache_dir: Path,
    scene_dir: Path,
    tile_size: int,
    inference_device: str | None,
) -> Any:
    device = resolve_inference_device(
        explicit=inference_device,
        env_var="NIMBUS_WATERMASK_DEVICE",
    )
    kwargs: dict[str, Any] = {
        "scene_paths": scene_paths,
        "band_order": [1, 2, 3, 4],
        "output_dir": output_dir,
        "overwrite": True,
        "cache_dir": cache_dir,
        "batch_size": batch_size_for_device(
            device=device,
            env_var="NIMBUS_WATERMASK_BATCH_SIZE",
            cpu_default=1,
            gpu_default=2,
            hard_limit=8,
        ),
        "mosaic_device": device,
        "inference_device": device,
        "inference_patch_size": min(_watermask_inference_patch_size(), tile_size),
        "inference_overlap_size": min(_watermask_inference_overlap_size(), max(0, tile_size - 1)),
        "destination_model_dir": _watermask_model_dir(scene_dir),
        "model_download_source": "hugging_face",
    }
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
    optional_kwargs = {
        "use_cache": False,
        "use_osm_water": _bool_env("NIMBUS_WATERMASK_USE_OSM_WATER", default=True),
        "use_osm_building": _bool_env("NIMBUS_WATERMASK_USE_OSM_BUILDING", default=True),
        "use_osm_roads": _bool_env("NIMBUS_WATERMASK_USE_OSM_ROADS", default=True),
        "optimise_model": normalize_device_name(device) in {"cuda", "mps"},
        "use_model": True,
        "use_ndwi": True,
    }
    for key, value in optional_kwargs.items():
        if accepts_var_kwargs or not accepted or key in accepted:
            kwargs[key] = value
    return make_water_mask(**kwargs)


def _bool_env(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


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
    zarr_uri: str,
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
    tile_size = _watermask_tile_size()
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
    tile_workers = parallel_worker_count(
        device=resolve_inference_device(explicit=inference_device, env_var="NIMBUS_WATERMASK_DEVICE"),
        env_var="NIMBUS_WATERMASK_TILE_WORKERS",
        cpu_default=2,
        gpu_default=1,
        hard_limit=4,
    )

    def process_window(window: tuple[int, int, int, int]) -> tuple[tuple[int, int, int, int], np.ndarray, np.ndarray, dict[str, Any]]:
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

    if tile_workers <= 1 or len(windows) <= 1:
        results_iter = map(process_window, windows)
    else:
        executor = ThreadPoolExecutor(max_workers=tile_workers, thread_name_prefix="water-mask")
        results_iter = executor.map(process_window, windows)

    tile_index = 0
    try:
        for window, probability_tile, mask_tile, tile_summary in results_iter:
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
                    {
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                        "tiles_completed": tile_index,
                        "tiles_total": len(windows),
                        "progress": round(tile_index / max(1, len(windows)), 4),
                    },
                )
    finally:
        if tile_workers > 1 and len(windows) > 1:
            executor.shutdown(wait=True)

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
    output_zarr_uri: str,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    water_arr: Any,
    water_prob_arr: Any,
    sensor: SensorMaskSpec,
    input_bands: tuple[str, ...],
) -> dict[str, Any]:
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
        return np.asarray(array, dtype=np.uint8, copy=False)
    src = np.asarray(array, dtype=np.uint8, copy=False)
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


def _watermask_tile_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_TILE_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 2048
    except ValueError:
        value = 2048
    return max(256, value)


def _watermask_runtime_mode(explicit: str | None = None) -> str:
    raw = str(explicit or os.getenv("NIMBUS_WATERMASK_RUNTIME_MODE") or "").strip().lower()
    if raw in {"fallback", "heuristic", "ndwi"}:
        return "fallback"
    if _omniwater_module_available():
        return "model"
    return "fallback"


def _omniwater_module_available() -> bool:
    if "omniwatermask" in sys.modules:
        return True
    return importlib.util.find_spec("omniwatermask") is not None


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


def _persist_status_artifacts(*, scene_dir: Path, status_path: Path, payload: dict[str, Any]) -> None:
    scene_dir.mkdir(parents=True, exist_ok=True)
    serializable_payload = dict(payload)
    serializable_payload.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    status_path.write_text(json.dumps(serializable_payload, indent=2, sort_keys=True))


def _sync_zarr_mask_attrs(*, zarr_uri: str, payload: dict[str, Any]) -> None:
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
    root = zarr.open_group(str(output_path), mode="a", zarr_format=2)
    root.attrs["water_mask_status"] = str(payload.get("status") or "")
    root.attrs["water_mask_reason"] = str(payload.get("reason") or "")
    root.attrs["water_mask_status_path"] = str(payload.get("status_path") or "")
    root.attrs["water_mask_artifact_uri"] = str(payload.get("artifact_uri") or "")
    root.attrs["water_mask_work_dir"] = str(payload.get("work_dir") or "")
    if str(payload.get("status") or "").strip().lower() != "written":
        root.attrs["water_mask_written"] = False
    zarr.consolidate_metadata(root.store)
