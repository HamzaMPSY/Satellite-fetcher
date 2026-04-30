from __future__ import annotations

from dataclasses import dataclass
import importlib
import os
import threading
from typing import Any

import numpy as np

from nimbuschain_mask_service.models import CloudInferenceSummary
from nimbuschain_mask_service.runtime import batch_size_for_device, resolve_inference_device
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec


@dataclass(frozen=True)
class CloudMaskResult:
    probability: np.ndarray
    mask: np.ndarray
    summary: CloudInferenceSummary


_OMNICLOUDMASK_MODEL_CACHE: dict[tuple[str, int, str, bool, str, str], tuple[Any, ...]] = {}
_OMNICLOUDMASK_MODEL_CACHE_LOCK = threading.Lock()


def run_cloud_inference(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    backend: str,
    inference_device: str | None = None,
    include_shadows: bool = True,
    valid_mask: np.ndarray | None = None,
) -> CloudMaskResult:
    backend_name = _resolve_backend_name(backend)
    if backend_name in {"heuristic", "default", "fallback"}:
        return _run_heuristic(
            sensor=sensor,
            channels=channels,
            threshold=threshold,
            include_shadows=include_shadows,
            valid_mask=valid_mask,
        )
    if backend_name == "omnicloudmask":
        return _run_omnicloudmask(
            sensor=sensor,
            channels=channels,
            threshold=threshold,
            inference_device=inference_device,
            include_shadows=include_shadows,
            valid_mask=valid_mask,
        )
    raise ValueError(f"Unsupported cloud-mask backend: {backend}")


def run_heuristic_cloud_inference(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    include_shadows: bool,
    valid_mask: np.ndarray | None = None,
) -> CloudMaskResult:
    return _run_heuristic(
        sensor=sensor,
        channels=channels,
        threshold=threshold,
        include_shadows=include_shadows,
        valid_mask=valid_mask,
    )


def run_omnicloudmask_cloud_inference(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    inference_device: str | None = None,
    include_shadows: bool,
    valid_mask: np.ndarray | None = None,
) -> CloudMaskResult:
    return _run_omnicloudmask(
        sensor=sensor,
        channels=channels,
        threshold=threshold,
        inference_device=inference_device,
        include_shadows=include_shadows,
        valid_mask=valid_mask,
    )


def _run_heuristic(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    include_shadows: bool,
    valid_mask: np.ndarray | None,
) -> CloudMaskResult:
    effective_threshold = _effective_heuristic_threshold(sensor=sensor, threshold=threshold)
    if sensor.sensor_key == "sentinel-2":
        blue = channels["B02"]
        green = channels["B03"]
        red = channels["B04"]
        nir = channels["B08"]
        swir1 = channels["B11"]
        swir2 = channels["B12"]
    elif sensor.sensor_key == "landsat-8-9-l1":
        blue = channels["B2"]
        green = channels["B3"]
        red = channels["B4"]
        nir = channels["B5"]
        swir1 = channels["B6"]
        swir2 = channels["B7"]
    elif sensor.sensor_key == "landsat-8-9-l2":
        blue = channels["SR_B2"]
        green = channels["SR_B3"]
        red = channels["SR_B4"]
        nir = channels["SR_B5"]
        swir1 = channels["SR_B6"]
        swir2 = channels["SR_B7"]
    else:
        raise ValueError(f"No heuristic cloud recipe for sensor '{sensor.sensor_key}'.")

    visible_mean = (blue + green + red) / 3.0
    visible_spread = (np.abs(red - green) + np.abs(red - blue) + np.abs(green - blue)) / 3.0
    whiteness = 1.0 - np.clip(visible_spread / (visible_mean + 1e-6), 0.0, 1.0)
    swir_mean = (swir1 + swir2) / 2.0
    brightness = np.clip((visible_mean + swir_mean) / 2.0, 0.0, 1.0)
    ndvi = (nir - red) / (nir + red + 1e-6)
    ndwi = (green - nir) / (green + nir + 1e-6)

    cloud_probability = (
        0.30 * visible_mean
        + 0.20 * brightness
        + 0.18 * whiteness
        + 0.18 * swir_mean
        + 0.10 * blue
        + 0.06 * red
        - 0.22 * nir
        - 0.06 * np.clip(ndvi, -1.0, 1.0)
    )
    cloud_probability = np.clip(cloud_probability, 0.0, 1.0).astype(np.float32, copy=False)
    cloud_only_mask = cloud_probability >= float(effective_threshold)

    visible_darkness = 1.0 - np.clip(visible_mean, 0.0, 1.0)
    nir_darkness = 1.0 - np.clip(nir, 0.0, 1.0)
    swir_darkness = 1.0 - np.clip(swir_mean, 0.0, 1.0)
    water_suppression = np.clip((ndwi + 0.12) / 0.62, 0.0, 1.0)
    cloud_adjacency = _max_filter2d(cloud_only_mask.astype(np.float32, copy=False), radius=5)
    adjacency_gate = np.clip((cloud_adjacency - 0.08) / 0.92, 0.0, 1.0)
    shadow_seed = np.clip(
        0.34 * visible_darkness
        + 0.22 * nir_darkness
        + 0.18 * swir_darkness
        + 0.08 * (1.0 - np.clip(brightness, 0.0, 1.0))
        - 0.34 * water_suppression,
        0.0,
        1.0,
    ).astype(np.float32, copy=False)
    shadow_probability = np.clip(
        (0.30 * adjacency_gate) + (0.70 * shadow_seed * adjacency_gate),
        0.0,
        1.0,
    ).astype(np.float32, copy=False)
    shadow_probability *= (1.0 - cloud_probability)
    shadow_threshold = min(float(effective_threshold), 0.34) - 0.04
    shadow_threshold = max(shadow_threshold, 0.22)
    shadow_mask = np.logical_and(adjacency_gate >= 0.12, shadow_probability >= shadow_threshold)

    probability = cloud_probability
    if include_shadows:
        probability = np.maximum(cloud_probability, np.clip(shadow_probability * 0.92, 0.0, 1.0))
        mask = np.logical_or(cloud_only_mask, shadow_mask).astype(np.uint8, copy=False)
    else:
        shadow_mask = np.zeros_like(cloud_only_mask, dtype=bool)
        mask = cloud_only_mask.astype(np.uint8, copy=False)
    probability, mask, valid_pixels = _apply_validity_to_outputs(
        probability=probability,
        mask=mask,
        valid_mask=valid_mask,
    )
    valid = np.asarray(valid_mask, dtype=bool) if valid_mask is not None else None
    cloud_pixels = int(np.asarray(mask, dtype=np.uint64).sum())
    shadow_pixels = (
        int(np.logical_and(shadow_mask, valid).sum())
        if valid is not None
        else int(shadow_mask.astype(np.uint64, copy=False).sum())
    )
    cloud_only_pixels = (
        int(np.logical_and(cloud_only_mask, valid).sum())
        if valid is not None
        else int(cloud_only_mask.astype(np.uint64, copy=False).sum())
    )
    cloud_fraction = float(cloud_pixels / valid_pixels) if valid_pixels else 0.0
    shadow_fraction = float(shadow_pixels / valid_pixels) if valid_pixels else 0.0
    cloud_only_fraction = float(cloud_only_pixels / valid_pixels) if valid_pixels else 0.0
    return CloudMaskResult(
        probability=probability.astype(np.float32, copy=False),
        mask=mask,
        summary=CloudInferenceSummary(
            backend="heuristic",
            sensor=sensor.sensor_key,
            cloud_fraction=cloud_fraction,
            cloud_only_fraction=cloud_only_fraction,
            shadow_fraction=shadow_fraction,
            includes_shadows=bool(include_shadows),
            shadow_threshold=float(shadow_threshold),
            threshold_used=float(effective_threshold),
            sensor_recipe=sensor.sensor_key,
            valid_pixels=int(valid_pixels),
        ),
    )


def _run_omnicloudmask(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
    threshold: float,
    inference_device: str | None,
    include_shadows: bool,
    valid_mask: np.ndarray | None,
) -> CloudMaskResult:
    try:
        import omnicloudmask  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "OmniCloudMask backend requested but 'omnicloudmask' is not installed in this runtime."
        ) from exc

    rgbnir = _select_omnicloudmask_rgbnir(sensor=sensor, channels=channels)
    device = _configured_device(explicit=inference_device)
    batch_size = _omnicloudmask_batch_size(device=device)
    preloaded_models = _omnicloudmask_preloaded_models(
        omnicloudmask_module=omnicloudmask,
        device=device,
        batch_size=batch_size,
    )
    class_map = _invoke_omnicloudmask(
        omnicloudmask,
        rgbnir=rgbnir,
        device=device,
        batch_size=batch_size,
        preloaded_models=preloaded_models,
    )
    confidence_cube = None
    if _cloud_confidence_enabled():
        confidence_cube = _invoke_omnicloudmask_confidence(
            omnicloudmask,
            rgbnir=rgbnir,
            device=device,
            batch_size=batch_size,
            preloaded_models=preloaded_models,
        )
    cloud_only_mask = _omnicloudmask_cloud_only_mask(class_map)
    shadow_mask = _omnicloudmask_shadow_mask(class_map)
    obstruction_mask = _omnicloudmask_obstruction_mask(
        class_map=class_map,
        include_shadows=include_shadows,
    )
    probability = _omnicloudmask_probability(
        class_map=class_map,
        confidence_cube=confidence_cube,
        include_shadows=include_shadows,
        obstruction_mask=obstruction_mask,
    )
    probability, mask, valid_pixels = _apply_validity_to_outputs(
        probability=probability,
        mask=obstruction_mask.astype(np.uint8, copy=False),
        valid_mask=valid_mask,
    )
    valid = np.asarray(valid_mask, dtype=bool) if valid_mask is not None else None
    cloud_pixels = int(np.asarray(mask, dtype=np.uint64).sum())
    shadow_pixels = (
        int(np.logical_and(shadow_mask, valid).sum())
        if valid is not None
        else int(shadow_mask.astype(np.uint64, copy=False).sum())
    )
    cloud_only_pixels = (
        int(np.logical_and(cloud_only_mask, valid).sum())
        if valid is not None
        else int(cloud_only_mask.astype(np.uint64, copy=False).sum())
    )
    cloud_fraction = float(cloud_pixels / valid_pixels) if valid_pixels else 0.0
    shadow_fraction = float(shadow_pixels / valid_pixels) if valid_pixels else 0.0
    cloud_only_fraction = float(cloud_only_pixels / valid_pixels) if valid_pixels else 0.0
    return CloudMaskResult(
        probability=probability,
        mask=mask,
        summary=CloudInferenceSummary(
            backend="omnicloudmask",
            sensor=sensor.sensor_key,
            cloud_fraction=cloud_fraction,
            cloud_only_fraction=cloud_only_fraction,
            shadow_fraction=shadow_fraction,
            includes_shadows=bool(include_shadows),
            class_labels={
                "0": "clear",
                "1": "thick_cloud",
                "2": "thin_cloud",
                "3": "cloud_shadow",
            },
            class_histogram=_class_histogram(class_map, valid_mask=valid_mask),
            confidence_available=confidence_cube is not None,
            inference_device=device or "auto",
            mask_source="class_map",
            probability_source="confidence_cube" if confidence_cube is not None else "class_map",
            threshold_for_mask=None,
            requested_threshold=float(threshold),
            valid_pixels=int(valid_pixels),
            batch_size=int(batch_size),
            preloaded_models=bool(preloaded_models),
        ),
    )


def _resolve_backend_name(backend: str | None) -> str:
    value = str(backend or "auto").strip().lower()
    if value in {"", "auto"}:
        return "omnicloudmask"
    return value


def _effective_heuristic_threshold(*, sensor: SensorMaskSpec, threshold: float) -> float:
    requested = float(threshold)
    if abs(requested - 0.45) < 1e-6:
        return float(sensor.cloud_threshold_default)
    return requested


def _invoke_omnicloudmask(
    module: Any,
    *,
    rgbnir: np.ndarray,
    device: str | None,
    batch_size: int,
    preloaded_models: tuple[Any, ...] | None,
) -> np.ndarray:
    predict = getattr(module, "predict_from_array", None)
    if not callable(predict):
        available = sorted(name for name in dir(module) if not name.startswith("_"))
        raise RuntimeError(
            "Installed omnicloudmask package does not expose predict_from_array(). "
            f"Discovered exports: {available[:30]}"
        )

    output = predict(
        rgbnir,
        batch_size=batch_size,
        inference_device=device,
        mosaic_device=device,
        apply_no_data_mask=True,
        model_download_source=os.getenv("NIMBUS_OMNICLOUDMASK_MODEL_SOURCE", "hugging_face"),
        custom_models=list(preloaded_models) if preloaded_models else None,
    )
    return _coerce_class_map(output)


def _invoke_omnicloudmask_confidence(
    module: Any,
    *,
    rgbnir: np.ndarray,
    device: str | None,
    batch_size: int,
    preloaded_models: tuple[Any, ...] | None,
) -> np.ndarray | None:
    predict = getattr(module, "predict_from_array", None)
    if not callable(predict):
        return None

    try:
        output = predict(
            rgbnir,
            batch_size=batch_size,
            inference_device=device,
            mosaic_device=device,
            apply_no_data_mask=True,
            export_confidence=True,
            softmax_output=True,
            model_download_source=os.getenv("NIMBUS_OMNICLOUDMASK_MODEL_SOURCE", "hugging_face"),
            custom_models=list(preloaded_models) if preloaded_models else None,
        )
    except TypeError:
        return None
    except Exception:
        return None
    return _coerce_confidence_cube(output)


def _select_omnicloudmask_rgbnir(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, np.ndarray],
) -> np.ndarray:
    required = sensor.cloud_rgbnir_bands
    missing = [name for name in required if name not in channels]
    if missing:
        raise ValueError(
            "Cannot run OmniCloudMask because required RGB/NIR channels are missing: " + ", ".join(missing)
        )
    stacked = np.stack([channels[name] for name in required], axis=0).astype(np.float32, copy=False)
    return _restore_omnicloudmask_dynamic_range(stacked)


def _restore_omnicloudmask_dynamic_range(stacked: np.ndarray) -> np.ndarray:
    finite_max = float(np.nanmax(stacked)) if stacked.size else 0.0
    if finite_max <= 2.0:
        return (stacked * 10000.0).astype(np.float32, copy=False)
    return stacked


def _coerce_class_map(output: Any) -> np.ndarray:
    if isinstance(output, dict):
        for key in ("mask", "prediction", "pred", "cloud_mask", "classes"):
            if key in output:
                return _as_2d(output[key]).astype(np.uint8)
    return _as_2d(output)


def _coerce_confidence_cube(output: Any) -> np.ndarray | None:
    candidate = output
    if isinstance(output, dict):
        for key in ("confidence", "probabilities", "scores", "softmax", "prediction", "pred"):
            if key in output:
                candidate = output[key]
                break
    array = np.asarray(candidate, dtype=np.float32)
    if array.ndim == 3 and array.shape[0] >= 4:
        return array
    if array.ndim == 3 and array.shape[-1] >= 4:
        return np.moveaxis(array, -1, 0)
    return None


def _as_2d(value: Any) -> np.ndarray:
    array = np.asarray(value)
    if array.ndim == 2:
        return array.astype(np.uint8)
    if array.ndim == 3:
        if array.shape[0] == 1:
            return array[0, :, :].astype(np.uint8)
        if array.shape[-1] == 1:
            return array[:, :, 0].astype(np.uint8)
    raise ValueError(f"Expected OmniCloudMask output as 2D or singleton-3D array, got shape={array.shape}")


def _configured_device(*, explicit: str | None) -> str | None:
    return resolve_inference_device(explicit=explicit, env_var="NIMBUS_CLOUDMASK_DEVICE")


def _omnicloudmask_batch_size(*, device: str | None) -> int:
    return batch_size_for_device(
        device=device,
        env_var="NIMBUS_CLOUDMASK_BATCH_SIZE",
        cpu_default=1,
        gpu_default=2,
        hard_limit=8,
    )


def _omnicloudmask_preloaded_models(
    *,
    omnicloudmask_module: Any,
    device: str | None,
    batch_size: int,
) -> tuple[Any, ...] | None:
    try:
        cloud_mask_module = importlib.import_module("omnicloudmask.cloud_mask")
    except Exception:
        return None

    collect_models = getattr(cloud_mask_module, "collect_models", None)
    if not callable(collect_models):
        return None

    try:
        torch = importlib.import_module("torch")
    except Exception:
        return None

    compile_models = _omnicloudmask_compile_enabled()
    compile_mode = _omnicloudmask_compile_mode()
    source = os.getenv("NIMBUS_OMNICLOUDMASK_MODEL_SOURCE", "hugging_face")
    model_version = str(os.getenv("NIMBUS_OMNICLOUDMASK_MODEL_VERSION") or "").strip() or ""
    normalized_device = str(device or "cpu")
    cache_key = (
        normalized_device,
        int(batch_size),
        source,
        bool(compile_models),
        compile_mode,
        model_version,
    )
    with _OMNICLOUDMASK_MODEL_CACHE_LOCK:
        cached = _OMNICLOUDMASK_MODEL_CACHE.get(cache_key)
        if cached is not None:
            return cached

    try:
        inference_device = torch.device(normalized_device)
        models = collect_models(
            custom_models=None,
            inference_device=inference_device,
            inference_dtype=torch.float32,
            source=source,
            destination_model_dir=os.getenv("NIMBUS_OMNICLOUDMASK_MODEL_DIR") or None,
            model_version=float(model_version) if model_version else None,
            compile_models=compile_models,
            patch_size=1000,
            batch_size=int(batch_size),
            compile_mode=compile_mode,
        )
    except Exception:
        return None

    frozen_models = tuple(models)
    with _OMNICLOUDMASK_MODEL_CACHE_LOCK:
        _OMNICLOUDMASK_MODEL_CACHE[cache_key] = frozen_models
    return frozen_models


def _omnicloudmask_compile_enabled() -> bool:
    raw = str(os.getenv("NIMBUS_OMNICLOUDMASK_COMPILE_MODELS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _omnicloudmask_compile_mode() -> str:
    raw = str(os.getenv("NIMBUS_OMNICLOUDMASK_COMPILE_MODE") or "").strip()
    return raw or "default"


def _cloud_confidence_enabled() -> bool:
    raw = str(os.getenv("NIMBUS_CLOUDMASK_EXPORT_CONFIDENCE") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _omnicloudmask_probability(
    *,
    class_map: np.ndarray,
    confidence_cube: np.ndarray | None,
    include_shadows: bool,
    obstruction_mask: np.ndarray,
) -> np.ndarray:
    probability = _omnicloudmask_class_floor(class_map=class_map, include_shadows=include_shadows)
    if confidence_cube is not None:
        cloud_probability = np.maximum(confidence_cube[1], confidence_cube[2]).astype(np.float32, copy=False)
        if include_shadows and confidence_cube.shape[0] > 3:
            cloud_probability = np.maximum(cloud_probability, confidence_cube[3] * 0.92)
        probability = np.maximum(probability, np.clip(cloud_probability, 0.0, 1.0))

    # Never let the debug probability layer erase pixels already classified as an
    # obstruction by OmniCloudMask itself.
    probability[obstruction_mask] = np.maximum(probability[obstruction_mask], np.float32(1.0))
    return probability.astype(np.float32, copy=False)


def _omnicloudmask_class_floor(*, class_map: np.ndarray, include_shadows: bool) -> np.ndarray:
    probability = np.zeros_like(class_map, dtype=np.float32)
    if set(np.unique(class_map.astype(np.uint8, copy=False)).tolist()).issubset({0, 1}):
        probability[class_map > 0] = 1.0
        return probability
    probability[class_map == 1] = 0.98
    probability[class_map == 2] = 0.90
    if include_shadows:
        probability[class_map == 3] = 0.82
    return probability


def _omnicloudmask_cloud_only_mask(class_map: np.ndarray) -> np.ndarray:
    values = set(np.unique(class_map.astype(np.uint8, copy=False)).tolist())
    if values.issubset({0, 1}):
        return class_map > 0
    return np.isin(class_map, (1, 2))


def _omnicloudmask_shadow_mask(class_map: np.ndarray) -> np.ndarray:
    return class_map == 3


def _omnicloudmask_obstruction_mask(*, class_map: np.ndarray, include_shadows: bool) -> np.ndarray:
    cloud_only_mask = _omnicloudmask_cloud_only_mask(class_map)
    if include_shadows:
        return class_map > 0
    return cloud_only_mask


def _class_histogram(class_map: np.ndarray, *, valid_mask: np.ndarray | None = None) -> dict[str, int]:
    values_source = class_map.astype(np.uint8, copy=False)
    if valid_mask is not None:
        values_source = values_source[np.asarray(valid_mask, dtype=bool)]
    if values_source.size == 0:
        return {}
    values, counts = np.unique(values_source, return_counts=True)
    return {str(int(value)): int(count) for value, count in zip(values, counts, strict=False)}


def _apply_validity_to_outputs(
    *,
    probability: np.ndarray,
    mask: np.ndarray,
    valid_mask: np.ndarray | None,
) -> tuple[np.ndarray, np.ndarray, int]:
    probability_array = np.asarray(probability, dtype=np.float32)
    mask_array = np.asarray(mask, dtype=np.uint8)
    if valid_mask is None:
        return probability_array, mask_array, int(mask_array.size)
    valid = np.asarray(valid_mask, dtype=bool)
    if valid.shape != probability_array.shape:
        raise ValueError(
            f"valid_mask shape {valid.shape} does not match cloud output shape {probability_array.shape}."
        )
    return (
        np.where(valid, probability_array, 0.0).astype(np.float32, copy=False),
        np.where(valid, mask_array, 0).astype(np.uint8, copy=False),
        int(valid.sum()),
    )


def _max_filter2d(array: np.ndarray, *, radius: int) -> np.ndarray:
    if radius <= 0:
        return np.asarray(array, dtype=np.float32)
    src = np.asarray(array, dtype=np.float32)
    padded = np.pad(src, radius, mode="edge")
    height, width = src.shape
    result = np.zeros_like(src, dtype=np.float32)
    for row_offset in range(0, 2 * radius + 1):
        row_slice = slice(row_offset, row_offset + height)
        for col_offset in range(0, 2 * radius + 1):
            col_slice = slice(col_offset, col_offset + width)
            result = np.maximum(result, padded[row_slice, col_slice])
    return result
