from __future__ import annotations

import math
import os
from typing import Any

from nimbuschain_mask_service.models import (
    DatasetSummaryRecord,
    TileSizingDecision,
    TileSizingPolicyStatus,
)
from nimbuschain_mask_service.runtime import normalize_device_name
from nimbuschain_shared.resolution import target_pixel_size_for

_CLOUD_PATCH_QUANTUM = 256
_DEFAULT_WATER_PATCH_SIZE = 512
_FIXED_DEFAULT_TILE_SIZE = 512


def choose_mask_tile_sizing(
    *,
    mask_kind: str,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
    dataset_summary: dict[str, Any] | DatasetSummaryRecord | None,
    device: str | None,
    backend_name: str | None = None,
    model_patch_size: int | None = None,
    env_var: str,
) -> TileSizingDecision:
    normalized_kind = str(mask_kind or "").strip().lower()
    summary_record = (
        dataset_summary
        if isinstance(dataset_summary, DatasetSummaryRecord)
        else DatasetSummaryRecord.from_mapping(dataset_summary)
    )
    if normalized_kind not in {"cloud", "water"}:
        raise ValueError(f"Unsupported mask kind for tile sizing: {mask_kind}")

    normalized_provider = str(provider or "").strip().lower()
    normalized_collection = str(collection or "").strip()
    normalized_device = normalize_device_name(device)
    normalized_backend = str(backend_name or "").strip().lower() or None
    collection_family = _collection_family(normalized_provider, normalized_collection)
    scene_height, scene_width = _scene_shape(summary_record)
    scene_max_dim = max(scene_height, scene_width, 1)
    target_pixel_size_meters = _target_pixel_size_meters(
        dataset_summary=summary_record,
        provider=normalized_provider,
        collection=normalized_collection,
    )
    patch_quantum = _patch_quantum(
        mask_kind=normalized_kind,
        model_patch_size=model_patch_size,
    )
    requested_override = str(os.getenv(env_var) or "").strip()
    if requested_override:
        explicit = _parse_int(requested_override)
        if explicit is not None:
            value = _normalize_override_tile_size(
                mask_kind=normalized_kind,
                explicit=explicit,
            )
            return _decision_payload(
                source="env_override",
                tile_size=value,
                mask_kind=normalized_kind,
                provider=normalized_provider,
                collection=normalized_collection,
                product_type=product_type,
                device=normalized_device,
                backend_name=normalized_backend,
                collection_family=collection_family,
                scene_height=scene_height,
                scene_width=scene_width,
                target_pixel_size_meters=target_pixel_size_meters,
                patch_quantum=patch_quantum,
                target_tiles_long_axis=None,
                patch_multiple=None,
                min_patch_multiple=None,
                max_patch_multiple=None,
                requested_env_value=explicit,
                invalid_env_value=None,
            )
        invalid_env_value = requested_override
    else:
        invalid_env_value = None

    tile_size = _normalize_override_tile_size(
        mask_kind=normalized_kind,
        explicit=_FIXED_DEFAULT_TILE_SIZE,
    )
    patch_multiple = max(1, int(tile_size // max(1, patch_quantum)))
    target_tiles_long_axis = max(1, int(math.ceil(scene_max_dim / max(1, tile_size))))
    return _decision_payload(
        source="fixed_default",
        tile_size=tile_size,
        mask_kind=normalized_kind,
        provider=normalized_provider,
        collection=normalized_collection,
        product_type=product_type,
        device=normalized_device,
        backend_name=normalized_backend,
        collection_family=collection_family,
        scene_height=scene_height,
        scene_width=scene_width,
        target_pixel_size_meters=target_pixel_size_meters,
        patch_quantum=patch_quantum,
        target_tiles_long_axis=target_tiles_long_axis,
        patch_multiple=patch_multiple,
        min_patch_multiple=patch_multiple,
        max_patch_multiple=patch_multiple,
        requested_env_value=None,
        invalid_env_value=invalid_env_value,
    )


def cloud_tile_sizing_policy_status() -> TileSizingPolicyStatus:
    return TileSizingPolicyStatus(
        mode="fixed_default",
        env_var="NIMBUS_CLOUDMASK_TILE_SIZE",
        env_override=str(os.getenv("NIMBUS_CLOUDMASK_TILE_SIZE") or "").strip() or None,
        default_tile_size=_FIXED_DEFAULT_TILE_SIZE,
        patch_quantum=_CLOUD_PATCH_QUANTUM,
        selection_rule="Use 512 unless NIMBUS_CLOUDMASK_TILE_SIZE overrides it.",
    )


def water_tile_sizing_policy_status(*, model_patch_size: int | None = None) -> TileSizingPolicyStatus:
    if model_patch_size is None:
        raw_patch_size = str(os.getenv("NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE") or "").strip()
        parsed_patch_size = _parse_int(raw_patch_size)
    else:
        parsed_patch_size = model_patch_size
    patch_size = _patch_quantum(mask_kind="water", model_patch_size=parsed_patch_size)
    return TileSizingPolicyStatus(
        mode="fixed_default",
        env_var="NIMBUS_WATERMASK_TILE_SIZE",
        env_override=str(os.getenv("NIMBUS_WATERMASK_TILE_SIZE") or "").strip() or None,
        default_tile_size=_FIXED_DEFAULT_TILE_SIZE,
        patch_quantum=patch_size,
        selection_rule="Use 512 unless NIMBUS_WATERMASK_TILE_SIZE overrides it.",
    )


def _decision_payload(
    *,
    source: str,
    tile_size: int,
    mask_kind: str,
    provider: str,
    collection: str,
    product_type: str | None,
    device: str,
    backend_name: str | None,
    collection_family: str,
    scene_height: int,
    scene_width: int,
    target_pixel_size_meters: float | None,
    patch_quantum: int,
    target_tiles_long_axis: int | None,
    patch_multiple: int | None,
    min_patch_multiple: int | None,
    max_patch_multiple: int | None,
    requested_env_value: int | None,
    invalid_env_value: str | None,
) -> TileSizingDecision:
    scene_max_dim = max(scene_height, scene_width, 1)
    scene_area_pixels = max(1, scene_height * scene_width)
    scene_ground_span_meters = (
        float(scene_max_dim * target_pixel_size_meters)
        if target_pixel_size_meters is not None
        else None
    )
    tile_ground_span_meters = (
        float(tile_size * target_pixel_size_meters)
        if target_pixel_size_meters is not None
        else None
    )
    target_tile_pixels = (
        float(scene_max_dim / max(1, target_tiles_long_axis))
        if target_tiles_long_axis is not None
        else None
    )
    target_tile_ground_span_meters = (
        float(target_tile_pixels * target_pixel_size_meters)
        if target_tile_pixels is not None and target_pixel_size_meters is not None
        else None
    )
    estimated_tiles_long_axis = max(1, int(math.ceil(scene_max_dim / max(1, tile_size))))
    return TileSizingDecision(
        source=source,
        mask_kind=mask_kind,
        provider=provider,
        collection=collection,
        collection_family=collection_family,
        product_type=str(product_type or "").strip() or None,
        backend=backend_name,
        device=device,
        tile_size=int(tile_size),
        default_tile_size=_FIXED_DEFAULT_TILE_SIZE,
        scene_shape=[int(scene_height), int(scene_width)],
        scene_max_dimension=int(scene_max_dim),
        scene_area_pixels=int(scene_area_pixels),
        target_pixel_size_meters=target_pixel_size_meters,
        scene_ground_span_meters=scene_ground_span_meters,
        tile_ground_span_meters=tile_ground_span_meters,
        target_tiles_long_axis=target_tiles_long_axis,
        target_tile_pixels=target_tile_pixels,
        target_tile_ground_span_meters=target_tile_ground_span_meters,
        estimated_tiles_long_axis=estimated_tiles_long_axis,
        model_patch_size=int(patch_quantum),
        snap_multiple=int(patch_quantum),
        patch_multiple=patch_multiple,
        min_patch_multiple=min_patch_multiple,
        max_patch_multiple=max_patch_multiple,
        requested_env_value=requested_env_value,
        invalid_env_value=invalid_env_value,
    )


def _target_pixel_size_meters(
    *,
    dataset_summary: DatasetSummaryRecord,
    provider: str,
    collection: str,
) -> float | None:
    pixel_size = dataset_summary.pixel_size or dataset_summary.reference_pixel_size
    if isinstance(pixel_size, (list, tuple)):
        for value in pixel_size:
            try:
                parsed = abs(float(value))
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
    try:
        resolved = target_pixel_size_for(provider, collection)
    except Exception:
        resolved = None
    return float(resolved) if resolved is not None else None


def _scene_shape(dataset_summary: DatasetSummaryRecord) -> tuple[int, int]:
    shape = list(dataset_summary.shape)
    if len(shape) >= 4:
        return max(0, int(shape[2])), max(0, int(shape[3]))
    if len(shape) >= 2:
        return max(0, int(shape[-2])), max(0, int(shape[-1]))
    return 0, 0


def _collection_family(provider: str, collection: str) -> str:
    normalized_provider = str(provider or "").strip().lower()
    normalized_collection = str(collection or "").strip()
    upper_collection = normalized_collection.upper()
    lower_collection = normalized_collection.lower()
    if normalized_provider == "copernicus" and upper_collection == "SENTINEL-2":
        return "sentinel-2"
    if normalized_provider == "usgs" and lower_collection in {"landsat_ot_c2_l1", "landsat_ot_c2_l2"}:
        return "landsat-8-9"
    return lower_collection or "default"


def _patch_quantum(*, mask_kind: str, model_patch_size: int | None) -> int:
    if str(mask_kind).strip().lower() == "cloud":
        return _CLOUD_PATCH_QUANTUM
    try:
        value = int(model_patch_size) if model_patch_size is not None else _DEFAULT_WATER_PATCH_SIZE
    except (TypeError, ValueError):
        value = _DEFAULT_WATER_PATCH_SIZE
    return max(1, value)


def _parse_int(raw: str) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _normalize_override_tile_size(*, mask_kind: str, explicit: int) -> int:
    value = int(explicit)
    if str(mask_kind).strip().lower() == "cloud":
        return max(256, min(value, 2048))
    return max(256, value)
