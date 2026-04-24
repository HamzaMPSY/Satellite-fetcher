from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gc
import json
import os
from pathlib import Path
from typing import Any

import numpy as np

from nimbuschain_mask_service.registry import registry_status, resolve_cloud_backend, resolve_water_backend
from nimbuschain_mask_service.io import (
    cleanup_derived_zarr,
    copy_source_zarr,
    delete_mask_layers,
    local_path_for_uri,
    open_zarr_group,
    read_context,
    read_required_channels_window,
)
from nimbuschain_mask_service.inference import CloudMaskResult
from nimbuschain_mask_service.runtime import (
    normalize_device_name,
    parallel_worker_count,
    resolve_inference_device,
    runtime_device_status,
)
from nimbuschain_mask_service.sensor_mapping import resolve_sensor_mask_spec
from nimbuschain_mask_service.tile_sizing import (
    choose_mask_tile_sizing,
    cloud_tile_sizing_policy_status,
    water_tile_sizing_policy_status,
)
from nimbuschain_shared.zarr import ConversionError
from nimbuschain_mask_service.writer import (
    finalize_cloud_outputs,
    prepare_cloud_output_arrays,
    write_cloud_outputs,
    write_water_mask_to_zarr,
)


class MaskService:
    """Internal masking runtime used by the unified backend pipeline."""

    def write_water_mask(
        self,
        *,
        output_uri: str,
        mask: np.ndarray,
        acquisition_datetime: str | None = None,
        model_name: str = "omniwatermask",
        model_version: str | None = None,
        input_bands: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return write_water_mask_to_zarr(
            output_uri=output_uri,
            mask=mask,
            acquisition_datetime=acquisition_datetime,
            model_name=model_name,
            model_version=model_version,
            input_bands=input_bands,
            metadata=metadata,
        )

    def apply_omniwater_to_zarr(
        self,
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
        backend: str | None = None,
        overwrite: bool = True,
        inference_device: str | None = None,
        fail_on_error: bool = False,
        stage_callback: Any = None,
    ) -> dict[str, Any]:
        descriptor = resolve_water_backend(backend)
        result = descriptor.run(
            job_id=job_id,
            zarr_uri=zarr_uri,
            source_zarr_uri=source_zarr_uri,
            provider=provider,
            collection=collection,
            product_type=product_type,
            scene_id=scene_id,
            acquisition_datetime=acquisition_datetime,
            dataset_summary=dataset_summary,
            output_zarr_uri=output_zarr_uri,
            overwrite=overwrite,
            inference_device=inference_device,
            fail_on_error=fail_on_error,
            stage_callback=stage_callback,
        )
        result.setdefault("backend", descriptor.name)
        result.setdefault("mask_contract_version", "v2")
        return result

    def apply_masks_to_zarr(
        self,
        *,
        job_id: str | None = None,
        zarr_uri: str,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, Any],
        mask_types: list[str],
        output_zarr_uri: str | None = None,
        fail_on_error: bool = False,
        stage_callback: Any = None,
        backend: str = "auto",
        threshold: float = 0.45,
        overwrite: bool = True,
        inference_device: str | None = None,
        include_shadows: bool = True,
        water_backend: str | None = None,
        water_overwrite: bool = True,
        water_inference_device: str | None = None,
    ) -> dict[str, Any]:
        normalized_mask_types = _normalize_mask_types(mask_types)
        effective_water_backend = _effective_water_backend_request(
            backend=backend,
            water_backend=water_backend,
        )
        masked_zarr_uri = str(zarr_uri).strip()
        storage_mode = _storage_mode_for_paths(
            source_zarr_uri=zarr_uri,
            output_zarr_uri=masked_zarr_uri,
        )
        water_mask: dict[str, Any] = {}
        cloud_mask: dict[str, Any] = {}

        if "cloud" in normalized_mask_types:
            cloud_mask = self.apply_cloud_to_zarr(
                job_id=job_id,
                source_zarr_uri=zarr_uri,
                output_zarr_uri=masked_zarr_uri,
                provider=provider,
                collection=collection,
                product_type=product_type,
                scene_id=scene_id,
                acquisition_datetime=acquisition_datetime,
                dataset_summary=dataset_summary,
                fail_on_error=fail_on_error,
                stage_callback=stage_callback,
                threshold=threshold,
                backend=backend,
                overwrite=overwrite,
                inference_device=inference_device,
                include_shadows=include_shadows,
            )

        cloud_written = str(cloud_mask.get("status") or "").strip().lower() == "written"
        if "water" in normalized_mask_types and ("cloud" not in normalized_mask_types or cloud_written):
            water_input_zarr_uri = (
                masked_zarr_uri
                if ("cloud" in normalized_mask_types and local_path_for_uri(masked_zarr_uri).exists())
                else zarr_uri
            )
            water_mask = self.apply_omniwater_to_zarr(
                job_id=job_id,
                zarr_uri=water_input_zarr_uri,
                source_zarr_uri=zarr_uri,
                provider=provider,
                collection=collection,
                product_type=product_type,
                scene_id=scene_id,
                acquisition_datetime=acquisition_datetime,
                dataset_summary=dataset_summary,
                output_zarr_uri=masked_zarr_uri,
                backend=effective_water_backend,
                overwrite=water_overwrite,
                inference_device=water_inference_device,
                fail_on_error=fail_on_error,
                stage_callback=stage_callback,
            )
        elif "water" in normalized_mask_types and "cloud" in normalized_mask_types:
            water_mask = {
                "status": "failed",
                "reason": "Cloud mask failed before water masking could start.",
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": masked_zarr_uri,
                "storage_mode": storage_mode,
            }

        status = _combine_status(normalized_mask_types, water_mask=water_mask, cloud_mask=cloud_mask)
        if status != "written" and storage_mode == "in_place_zarr_masking":
            delete_mask_layers(
                masked_zarr_uri,
                layer_names=_requested_mask_layer_names(normalized_mask_types),
            )
        masked_outputs = [masked_zarr_uri] if status == "written" and local_path_for_uri(masked_zarr_uri).exists() else []
        target_zarr_uri = masked_outputs[0] if masked_outputs else None
        return {
            "status": status,
            "mask_contract_version": "v2",
            "mask_types": normalized_mask_types,
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": target_zarr_uri or "",
            "masked_zarr_uri": target_zarr_uri,
            "masked_zarr_outputs": masked_outputs,
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            "watermask_outputs": [],
            "cloudmask_outputs": [],
        }

    def apply_cloud_to_zarr(
        self,
        *,
        job_id: str | None,
        source_zarr_uri: str,
        output_zarr_uri: str,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, Any],
        fail_on_error: bool = False,
        stage_callback: Any = None,
        threshold: float = 0.45,
        backend: str = "auto",
        overwrite: bool = True,
        inference_device: str | None = None,
        include_shadows: bool = True,
    ) -> dict[str, Any]:
        target_zarr_uri = str(output_zarr_uri or source_zarr_uri).strip()
        source_path = local_path_for_uri(source_zarr_uri)
        output_path = local_path_for_uri(target_zarr_uri)
        storage_mode = _storage_mode_for_paths(
            source_zarr_uri=source_zarr_uri,
            output_zarr_uri=target_zarr_uri,
        )
        try:
            if stage_callback is not None:
                stage_callback(
                    "cloud_masking_started",
                    {
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                    },
                )
            if not output_path.exists() and source_path.resolve() != output_path.resolve():
                copy_source_zarr(source_zarr_uri=source_zarr_uri, output_zarr_uri=target_zarr_uri)
            root = open_zarr_group(target_zarr_uri, mode="a")
            context = read_context(root, zarr_uri=target_zarr_uri)
            sensor = resolve_sensor_mask_spec(
                provider=provider or context.provider,
                collection=collection or context.collection,
                product_type=product_type or context.product_type,
            )
            backend_request = _effective_cloud_backend_request(
                backend=backend,
                inference_device=inference_device,
            )
            backend_descriptor = resolve_cloud_backend(backend_request)
            backend_name = backend_descriptor.name
            effective_threshold = _effective_cloud_threshold(
                sensor=sensor,
                backend=backend_name,
                threshold=threshold,
            )
            metadata = {
                "provider": provider,
                "collection": collection,
                "product_type": product_type,
                "scene_id": scene_id,
                "input_zarr_uri": source_zarr_uri,
                "output_zarr_uri": target_zarr_uri,
                "storage_mode": storage_mode,
                "include_shadows": bool(include_shadows),
            }
            crs = dataset_summary.get("crs") or root.attrs.get("crs")
            transform = dataset_summary.get("transform") or root.attrs.get("transform")

            write_summary, inference_summary = _run_cloud_inference_tiled(
                root=root,
                context=context,
                sensor=sensor,
                backend_descriptor=backend_descriptor,
                threshold=effective_threshold,
                metadata=metadata,
                include_shadows=include_shadows,
                inference_device=inference_device,
                provider=provider,
                collection=collection,
                product_type=product_type,
                dataset_summary=dataset_summary,
                stage_callback=stage_callback,
                source_zarr_uri=source_zarr_uri,
                target_zarr_uri=target_zarr_uri,
                scene_id=scene_id,
            )

            payload = {
                "status": "written",
                "mask_contract_version": "v2",
                "reason": None,
                "input_zarr_uri": source_zarr_uri,
                "output_zarr_uri": target_zarr_uri,
                "storage_mode": storage_mode,
                "mask_path": write_summary["mask_path"],
                "probability_path": write_summary["probability_path"],
                "artifact_uri": None,
                "status_path": None,
                "work_dir": None,
                "shape": write_summary["mask_shape"],
                "dtype": write_summary["mask_dtype"],
                "classes": write_summary["classes"],
                "threshold": float(effective_threshold),
                "backend": backend_name,
                "sensor": sensor.sensor_key,
                "input_bands": list(backend_descriptor.required_bands(sensor)),
                "written_at": write_summary["written_at"],
                "inference": inference_summary,
                "tile_size": int(inference_summary.get("tile_size") or 0),
                "tile_sizing": dict(inference_summary.get("tile_sizing") or {}),
                "include_shadows": bool(include_shadows),
                "cloud_fraction": float(inference_summary.get("cloud_fraction") or 0.0),
                "cloud_only_fraction": float(inference_summary.get("cloud_only_fraction") or 0.0),
                "shadow_fraction": float(inference_summary.get("shadow_fraction") or 0.0),
                "mask_source": str(inference_summary.get("mask_source") or ""),
                "probability_source": str(inference_summary.get("probability_source") or ""),
                "sensor_recipe": sensor.sensor_key,
                "backend_request": str(backend_request or backend or "auto"),
            }
            if stage_callback is not None:
                stage_callback(
                    "cloud_masking_finished",
                    {
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                        "cloud_mask": payload,
                    },
                )
            return payload
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            cancelled = isinstance(exc, asyncio.CancelledError) or isinstance(exc, (BrokenPipeError, ConnectionResetError))
            failure = {
                "status": "cancelled" if cancelled else "failed",
                "mask_contract_version": "v2",
                "reason": (
                    "Cloud mask request was cancelled or disconnected before completion."
                    if cancelled
                    else str(exc)
                ),
                "input_zarr_uri": source_zarr_uri,
                "output_zarr_uri": target_zarr_uri,
                "storage_mode": storage_mode,
                "mask_path": None,
                "artifact_uri": None,
                "status_path": None,
                "work_dir": None,
            }
            _sync_cloud_mask_attrs(target_zarr_uri, failure)
            delete_mask_layers(target_zarr_uri, layer_names=("cloud", "cloud_probability"))
            if storage_mode != "in_place_zarr_masking" and _should_cleanup_failed_cloud_output(output_path):
                cleanup_derived_zarr(target_zarr_uri)
            if stage_callback is not None:
                stage_callback(
                    "cloud_masking_failed",
                    {
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                        "cloud_mask": failure,
                    },
                )
            if fail_on_error:
                raise ConversionError(f"Cloud mask failed for scene '{scene_id}' ({exc}).") from exc
            return failure


def run_cloud_inference(
    *,
    sensor: Any,
    channels: dict[str, Any],
    threshold: float,
    backend_descriptor: Any | None = None,
    backend: str | None = None,
    inference_device: str | None = None,
    include_shadows: bool = True,
    valid_mask: np.ndarray | None = None,
) -> Any:
    descriptor = backend_descriptor or resolve_cloud_backend(backend)
    kwargs = {
        "sensor": sensor,
        "channels": channels,
        "threshold": threshold,
        "inference_device": inference_device,
        "include_shadows": include_shadows,
        "valid_mask": valid_mask,
    }
    try:
        return descriptor.run(**kwargs)
    except TypeError as exc:
        if "valid_mask" not in str(exc):
            raise
        kwargs.pop("valid_mask", None)
        return descriptor.run(**kwargs)


def support_status() -> dict[str, Any]:
    registry = registry_status()
    return {
        "omniwatermask_available": any(
            item.get("name") == "omniwatermask" and bool(item.get("available"))
            for item in list(registry.get("water") or [])
        ),
        "omnicloudmask_available": any(
            item.get("name") == "omnicloudmask" and bool(item.get("available"))
            for item in list(registry.get("cloud") or [])
        ),
        "runtime": {
            "cloud": runtime_device_status(explicit=None, env_var="NIMBUS_CLOUDMASK_DEVICE"),
            "water": runtime_device_status(explicit=None, env_var="NIMBUS_WATERMASK_DEVICE"),
        },
        "tile_sizing": {
            "cloud": cloud_tile_sizing_policy_status(),
            "water": water_tile_sizing_policy_status(),
        },
        "registry": registry,
    }


def _resolved_cloud_backend(backend: str | None) -> str:
    value = str(backend or "auto").strip().lower()
    if value in {"", "auto"}:
        return "omnicloudmask"
    return value


def _effective_cloud_threshold(*, sensor: Any, backend: str, threshold: float) -> float:
    if backend != "heuristic":
        return float(threshold)
    requested = float(threshold)
    if abs(requested - 0.45) < 1e-6:
        return float(getattr(sensor, "cloud_threshold_default", requested))
    return requested


def _normalize_mask_types(mask_types: list[str]) -> list[str]:
    normalized: list[str] = []
    for item in mask_types:
        value = str(item or "").strip().lower()
        if value not in {"water", "cloud"}:
            raise ValueError(f"Unsupported mask type: {item}")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("mask_types cannot be empty")
    return normalized


def _combine_status(mask_types: list[str], *, water_mask: dict[str, Any], cloud_mask: dict[str, Any]) -> str:
    statuses: list[str] = []
    if "water" in mask_types:
        statuses.append(str(water_mask.get("status") or "").strip().lower())
    if "cloud" in mask_types:
        statuses.append(str(cloud_mask.get("status") or "").strip().lower())
    normalized = [status for status in statuses if status]
    if normalized and all(status == "written" for status in normalized):
        return "written"
    if normalized and all(status == "skipped" for status in normalized):
        return "skipped"
    return "failed"


def _collect_mask_outputs(mask_payload: dict[str, Any]) -> list[str]:
    return []


def _requested_mask_layer_names(mask_types: list[str]) -> tuple[str, ...]:
    names: list[str] = []
    if "cloud" in mask_types:
        names.extend(["cloud", "cloud_probability"])
    if "water" in mask_types:
        names.extend(["water", "water_probability"])
    return tuple(names)


def _storage_mode_for_paths(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    source_value = str(source_zarr_uri or "").strip()
    output_value = str(output_zarr_uri or "").strip()
    if source_value and output_value and source_value == output_value:
        return "in_place_zarr_masking"
    return "derived_zarr_copy"


def _mask_root(env_key: str, default_leaf: str) -> Path:
    configured = str(os.getenv(env_key) or "").strip()
    data_root = str(os.getenv("NIMBUS_DATA_DIR") or "").strip()
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured))
    if data_root:
        candidates.append(Path(data_root) / default_leaf)
    candidates.extend([
        Path("/data/downloads") / default_leaf,
        Path.cwd() / "data" / "downloads" / default_leaf,
    ])
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        return candidate
    return Path("/tmp") / default_leaf


def _cloudmask_scene_dir(*, job_id: str | None, scene_id: str) -> Path:
    root = _mask_root("NIMBUS_CLOUDMASK_DIR", "cloudmask")
    safe_job_id = _safe_component(job_id or "standalone")
    safe_scene_id = _safe_component(scene_id or "unknown_scene")
    path = root / safe_job_id / safe_scene_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _derived_zarr_uri(*, source_zarr_uri: str, scene_id: str, mask_types: list[str]) -> str:
    source_path = local_path_for_uri(source_zarr_uri)
    suffix = "-".join(mask_types)
    if source_path.suffix == ".zarr":
        if source_path.parent.name.lower() == "zarr":
            root = source_path.parent.parent / "zarrmask"
        else:
            root = source_path.parent / "zarrmask"
        root.mkdir(parents=True, exist_ok=True)
        return str(root / f"{source_path.stem}__mask-{suffix}.zarr")
    root = _mask_root("NIMBUS_ZARRMASK_DIR", "zarrmask")
    return str(root / f"{_safe_component(scene_id)}__mask-{suffix}.zarr")


def _write_cloud_mask_artifact(*, scene_dir: Path, mask: np.ndarray, crs: Any, transform: Any) -> Path:
    try:
        import rasterio
        from rasterio.transform import Affine
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise ConversionError(f"Cloud mask artifact requires rasterio ({exc}).") from exc

    if not crs or not transform or len(list(transform)) < 6:
        raise ConversionError("Cloud mask export requires CRS and affine transform from the source Zarr summary.")
    artifact_path = scene_dir / "cloud_mask.tif"
    affine = Affine(*list(transform)[:6])
    with rasterio.open(
        artifact_path,
        "w",
        driver="GTiff",
        height=int(mask.shape[0]),
        width=int(mask.shape[1]),
        count=1,
        dtype="uint8",
        crs=crs,
        transform=affine,
    ) as dst:
        dst.write(np.asarray(mask, dtype=np.uint8), 1)
    return artifact_path


def _run_cloud_inference_tiled(
    *,
    root: Any,
    context: Any,
    sensor: Any,
    backend_descriptor: Any,
    threshold: float,
    metadata: dict[str, Any],
    include_shadows: bool,
    inference_device: str | None,
    provider: str | None = None,
    collection: str | None = None,
    product_type: str | None = None,
    dataset_summary: dict[str, Any] | None = None,
    stage_callback: Any = None,
    source_zarr_uri: str = "",
    target_zarr_uri: str = "",
    scene_id: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    imagery = root["imagery"]
    height = int(imagery.shape[2])
    width = int(imagery.shape[3])
    effective_dataset_summary = dict(dataset_summary or {})
    if not effective_dataset_summary.get("shape"):
        effective_dataset_summary["shape"] = [1, len(context.band_names), height, width]
    if not effective_dataset_summary.get("pixel_size"):
        pixel_size = root.attrs.get("reference_pixel_size") or root.attrs.get("pixel_size")
        if pixel_size is not None:
            effective_dataset_summary["pixel_size"] = list(pixel_size) if isinstance(pixel_size, (list, tuple)) else pixel_size
    cloud_arr, cloud_prob_arr = prepare_cloud_output_arrays(root, overwrite=True)
    total_pixels = max(1, height * width)
    valid_pixels_total = 0
    cloud_pixels = 0
    shadow_pixels = 0
    cloud_only_pixels = 0
    tile_index = 0
    required_bands = backend_descriptor.required_bands(sensor)
    normalize = backend_descriptor.normalize_inputs(sensor)
    confidence_available = False
    class_histogram: dict[str, int] = {}
    mask_source = "class_map" if backend_descriptor.name == "omnicloudmask" else "threshold"
    probability_source = "class_map" if backend_descriptor.name == "omnicloudmask" else "heuristic_score"
    resolved_device = resolve_inference_device(
        explicit=inference_device,
        env_var="NIMBUS_CLOUDMASK_DEVICE",
    )
    try:
        tile_sizing = _cloud_tile_sizing(
            backend_name=str(backend_descriptor.name),
            device=resolved_device,
            provider=provider,
            collection=collection,
            product_type=product_type,
            dataset_summary=effective_dataset_summary,
        )
        tile_size = int(tile_sizing["tile_size"])
    except TypeError as exc:
        if (
            "backend_name" not in str(exc)
            and "device" not in str(exc)
            and "provider" not in str(exc)
            and "collection" not in str(exc)
            and "product_type" not in str(exc)
            and "dataset_summary" not in str(exc)
        ):
            raise
        try:
            tile_size = _cloud_tile_size(backend_name=str(backend_descriptor.name))
        except TypeError as nested_exc:
            if "backend_name" not in str(nested_exc):
                raise
            tile_size = _cloud_tile_size()
        tile_sizing = {
            "source": "legacy_wrapper",
            "tile_size": int(tile_size),
            "mask_kind": "cloud",
            "provider": str(provider or "").strip().lower(),
            "collection": str(collection or "").strip(),
            "collection_family": None,
            "product_type": str(product_type or "").strip() or None,
            "backend": str(backend_descriptor.name),
            "device": normalize_device_name(resolved_device),
            "scene_shape": list((effective_dataset_summary or {}).get("shape")[-2:] if (effective_dataset_summary or {}).get("shape") else []),
            "scene_max_dimension": None,
            "scene_area_pixels": None,
            "target_pixel_size_meters": None,
            "scene_ground_span_meters": None,
            "tile_ground_span_meters": None,
            "target_tiles_long_axis": None,
            "target_tile_pixels": None,
            "target_tile_ground_span_meters": None,
            "estimated_tiles_long_axis": None,
            "model_patch_size": 256,
            "snap_multiple": 256,
            "patch_multiple": None,
            "min_patch_multiple": None,
            "max_patch_multiple": None,
            "requested_env_value": None,
            "invalid_env_value": None,
        }
    total_tiles = ((height + tile_size - 1) // tile_size) * ((width + tile_size - 1) // tile_size)
    tile_workers = parallel_worker_count(
        device=resolved_device,
        env_var="NIMBUS_CLOUDMASK_TILE_WORKERS",
        cpu_default=1 if str(backend_descriptor.name) == "omnicloudmask" else 2,
        gpu_default=1,
        hard_limit=4,
    )
    windows = list(_iter_windows(height=height, width=width, tile_size=tile_size))
    should_trim_memory = (
        str(backend_descriptor.name) == "omnicloudmask"
        and normalize_device_name(resolved_device) == "cpu"
    )

    def process_window(window: tuple[int, int, int, int]) -> tuple[tuple[int, int, int, int], Any]:
        row_start, row_stop, col_start, col_stop = window
        try:
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=required_bands,
                scale_hint=sensor.scale_hint,
                row_start=row_start,
                row_stop=row_stop,
                col_start=col_start,
                col_stop=col_stop,
                normalize=normalize,
                include_validity=True,
            )
        except TypeError as exc:
            if "include_validity" not in str(exc):
                raise
            channels_result = read_required_channels_window(
                root,
                band_names=context.band_names,
                required_bands=required_bands,
                scale_hint=sensor.scale_hint,
                row_start=row_start,
                row_stop=row_stop,
                col_start=col_start,
                col_stop=col_stop,
                normalize=normalize,
            )
        if len(channels_result) == 3:
            channels, _missing, valid_mask = channels_result
        else:
            channels, _missing = channels_result
            valid_mask = None
        if valid_mask is not None and not bool(np.asarray(valid_mask, dtype=bool).any()):
            tile_height = row_stop - row_start
            tile_width = col_stop - col_start
            zeros_probability = np.zeros((tile_height, tile_width), dtype=np.float32)
            zeros_mask = np.zeros((tile_height, tile_width), dtype=np.uint8)
            return window, CloudMaskResult(
                probability=zeros_probability,
                mask=zeros_mask,
                summary={
                    "backend": backend_descriptor.name,
                    "sensor": sensor.sensor_key,
                    "cloud_fraction": 0.0,
                    "cloud_only_fraction": 0.0,
                    "shadow_fraction": 0.0,
                    "includes_shadows": bool(include_shadows),
                    "valid_pixels": 0,
                    "mask_source": "class_map" if backend_descriptor.name == "omnicloudmask" else "threshold",
                    "probability_source": "class_map" if backend_descriptor.name == "omnicloudmask" else "heuristic_score",
                    "confidence_available": False,
                    "class_histogram": {},
                },
            )
        try:
            result = run_cloud_inference(
                sensor=sensor,
                channels=channels,
                threshold=threshold,
                backend=backend_descriptor.name,
                inference_device=resolved_device,
                include_shadows=include_shadows,
                valid_mask=valid_mask,
            )
        except TypeError as exc:
            if "valid_mask" not in str(exc):
                raise
            result = run_cloud_inference(
                sensor=sensor,
                channels=channels,
                threshold=threshold,
                backend=backend_descriptor.name,
                inference_device=resolved_device,
                include_shadows=include_shadows,
            )
        return window, result

    if tile_workers <= 1 or len(windows) <= 1:
        results_iter = map(process_window, windows)
    else:
        executor = ThreadPoolExecutor(max_workers=tile_workers, thread_name_prefix="cloud-mask")
        results_iter = executor.map(process_window, windows)

    try:
        for window, result in results_iter:
            row_start, row_stop, col_start, col_stop = window
            tile_index += 1
            cloud_arr[0, row_start:row_stop, col_start:col_stop] = result.mask
            cloud_prob_arr[0, row_start:row_stop, col_start:col_stop] = result.probability
            cloud_pixels += int(np.asarray(result.mask, dtype=np.uint64).sum())
            tile_pixels = max(
                1,
                int(result.summary.get("valid_pixels") or 0)
                or (row_stop - row_start) * (col_stop - col_start),
            )
            valid_pixels_total += tile_pixels
            shadow_pixels += int(round(float(result.summary.get("shadow_fraction") or 0.0) * tile_pixels))
            cloud_only_pixels += int(round(float(result.summary.get("cloud_only_fraction") or 0.0) * tile_pixels))
            confidence_available = confidence_available or bool(result.summary.get("confidence_available"))
            mask_source = str(result.summary.get("mask_source") or mask_source)
            probability_source = str(result.summary.get("probability_source") or probability_source)
            for key, value in dict(result.summary.get("class_histogram") or {}).items():
                class_histogram[str(key)] = int(class_histogram.get(str(key), 0)) + int(value)

            if stage_callback is not None and (tile_index == 1 or tile_index == total_tiles or tile_index % 8 == 0):
                stage_callback(
                    "cloud_masking_progress",
                    {
                        "zarr_uri": source_zarr_uri,
                        "output_zarr_uri": target_zarr_uri,
                        "scene_id": scene_id,
                        "tiles_completed": tile_index,
                        "tiles_total": total_tiles,
                        "progress": round(tile_index / max(1, total_tiles), 4),
                    },
                )
            if should_trim_memory and (tile_index == total_tiles or tile_index % 4 == 0):
                gc.collect()
    finally:
        if tile_workers > 1 and len(windows) > 1:
            executor.shutdown(wait=True)
        if should_trim_memory:
            gc.collect()

    total_pixels = max(1, valid_pixels_total or total_pixels)
    cloud_fraction = float(cloud_pixels / total_pixels)
    backend_name = str(backend_descriptor.name)
    write_summary = finalize_cloud_outputs(
        root,
        threshold=threshold,
        backend=backend_name,
        sensor_key=sensor.sensor_key,
        input_bands=required_bands,
        metadata=metadata,
        cloud_fraction=cloud_fraction,
        cloud_arr=cloud_arr,
        cloud_prob_arr=cloud_prob_arr,
        summary={
            "cloud_fraction": cloud_fraction,
            "cloud_only_fraction": float(cloud_only_pixels / total_pixels),
            "shadow_fraction": float(shadow_pixels / total_pixels),
            "includes_shadows": bool(include_shadows),
            "tile_size": tile_size,
            "tiles_total": total_tiles,
            "tile_workers": tile_workers,
            "confidence_available": confidence_available,
            "class_histogram": class_histogram,
            "mask_source": mask_source,
            "probability_source": probability_source,
            "requested_threshold": float(threshold),
            "threshold_for_mask": None if backend_name == "omnicloudmask" else float(threshold),
            "tile_sizing": dict(tile_sizing),
        },
    )
    return (
        write_summary,
        {
            "backend": backend_name,
            "sensor": sensor.sensor_key,
            "cloud_fraction": cloud_fraction,
            "cloud_only_fraction": float(cloud_only_pixels / total_pixels),
            "shadow_fraction": float(shadow_pixels / total_pixels),
            "includes_shadows": bool(include_shadows),
            "tile_size": tile_size,
            "tiles_total": total_tiles,
            "tile_workers": tile_workers,
            "confidence_available": confidence_available,
            "class_histogram": class_histogram,
            "mask_source": mask_source,
            "probability_source": probability_source,
            "requested_threshold": float(threshold),
            "threshold_for_mask": None if backend_name == "omnicloudmask" else float(threshold),
            "tile_sizing": dict(tile_sizing),
        },
    )


def _persist_status(status_path: Path, payload: dict[str, Any]) -> None:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    serializable = dict(payload)
    serializable.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    status_path.write_text(json.dumps(serializable, indent=2, sort_keys=True), encoding="utf-8")


def _sync_cloud_mask_attrs(zarr_uri: str, payload: dict[str, Any]) -> None:
    try:
        root = open_zarr_group(zarr_uri, mode="a")
    except Exception:
        return
    root.attrs["cloud_mask_status"] = str(payload.get("status") or "")
    root.attrs["cloud_mask_reason"] = str(payload.get("reason") or "")
    root.attrs["cloud_mask_status_path"] = str(payload.get("status_path") or "")
    root.attrs["cloud_mask_artifact_uri"] = str(payload.get("artifact_uri") or "")
    root.attrs["cloud_mask_work_dir"] = str(payload.get("work_dir") or "")
    root.attrs["cloud_mask_source_zarr_uri"] = str(payload.get("input_zarr_uri") or "")
    root.attrs["cloud_mask_output_zarr_uri"] = str(payload.get("output_zarr_uri") or "")
    if str(payload.get("status") or "").strip().lower() != "written":
        root.attrs["cloud_mask_written"] = False
    try:
        import zarr
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass


def _rewrite_mask_payload_output_zarr_uri(
    payload: dict[str, Any],
    *,
    final_output_zarr_uri: str,
) -> dict[str, Any]:
    if not payload:
        return payload
    updated = dict(payload)
    if "output_zarr_uri" in updated:
        updated["output_zarr_uri"] = str(final_output_zarr_uri)
    if "working_zarr_uri" in updated:
        updated["working_zarr_uri"] = str(final_output_zarr_uri)
    status_path = str(updated.get("status_path") or "").strip()
    if status_path:
        _persist_status(Path(status_path), updated)
    return updated


def _sync_promoted_mask_attrs(
    *,
    zarr_uri: str,
    source_zarr_uri: str,
    final_output_zarr_uri: str,
    water_mask: dict[str, Any],
    cloud_mask: dict[str, Any],
) -> None:
    try:
        import zarr

        root = open_zarr_group(zarr_uri, mode="a")
    except Exception:
        return

    root.attrs["source_zarr_uri"] = str(source_zarr_uri)
    root.attrs["masked_zarr_uri"] = str(final_output_zarr_uri)
    if water_mask:
        root.attrs["water_mask_output_zarr_uri"] = str(final_output_zarr_uri)
        root.attrs["water_mask_source_zarr_uri"] = str(source_zarr_uri)
    if cloud_mask:
        root.attrs["cloud_mask_output_zarr_uri"] = str(final_output_zarr_uri)
        root.attrs["cloud_mask_source_zarr_uri"] = str(source_zarr_uri)
    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass


def _should_cleanup_failed_cloud_output(output_path: Path) -> bool:
    if not output_path.exists() or not output_path.is_dir():
        return False
    try:
        root = open_zarr_group(str(output_path), mode="r")
    except Exception:
        return True
    masks = root.get("masks")
    if masks is None:
        return True
    return "water" not in masks


def _safe_component(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value).strip()).strip("._") or "unknown"


def _effective_cloud_backend_request(*, backend: str | None, inference_device: str | None) -> str:
    del inference_device
    requested = str(backend or "auto").strip().lower()
    if requested in {"", "auto"}:
        return "omnicloudmask"
    return requested


def _effective_water_backend_request(
    *,
    backend: str | None,
    water_backend: str | None,
) -> str | None:
    explicit_water_backend = str(water_backend or "").strip().lower()
    if explicit_water_backend:
        return explicit_water_backend

    legacy_backend = str(backend or "").strip().lower()
    if legacy_backend in {"", "auto"}:
        return "auto"
    if legacy_backend in {"heuristic", "fallback", "ndwi", "omniwatermask"}:
        return legacy_backend
    return None


def _cloud_tile_size(*, backend_name: str | None = None, device: str | None = None) -> int:
    raw = str(os.getenv("NIMBUS_CLOUDMASK_TILE_SIZE") or "").strip()
    if raw:
        try:
            return max(256, min(int(raw), 2048))
        except ValueError:
            pass
    return int(
        _cloud_tile_sizing(
            backend_name=backend_name,
            device=device,
            provider=None,
            collection=None,
            product_type=None,
            dataset_summary=None,
        )["tile_size"]
    )


def _cloud_tile_sizing(
    *,
    backend_name: str | None = None,
    device: str | None = None,
    provider: str | None = None,
    collection: str | None = None,
    product_type: str | None = None,
    dataset_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return choose_mask_tile_sizing(
        mask_kind="cloud",
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=dataset_summary,
        device=device,
        backend_name=backend_name,
        model_patch_size=256,
        env_var="NIMBUS_CLOUDMASK_TILE_SIZE",
    )


def _iter_windows(*, height: int, width: int, tile_size: int):
    for row_start in range(0, height, tile_size):
        row_stop = min(height, row_start + tile_size)
        for col_start in range(0, width, tile_size):
            col_stop = min(width, col_start + tile_size)
            yield row_start, row_stop, col_start, col_stop
