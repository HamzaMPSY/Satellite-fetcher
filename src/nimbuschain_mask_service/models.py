from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


def _text(value: Any) -> str:
    return str(value or "").strip()


def _maybe_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _str_list(values: Any) -> list[str]:
    return [str(item) for item in list(values or [])]


def _int_list(values: Any) -> list[int]:
    return [_int(item) for item in list(values or [])]


def _dict(value: Any) -> dict[str, Any]:
    return dict(value or {})


def _str_int_dict(value: Any) -> dict[str, int]:
    payload = dict(value or {})
    return {str(key): _int(item) for key, item in payload.items()}


@dataclass(frozen=True, slots=True)
class CloudInferenceSummary:
    backend: str = ""
    sensor: str = ""
    cloud_fraction: float = 0.0
    cloud_only_fraction: float = 0.0
    shadow_fraction: float = 0.0
    includes_shadows: bool = False
    confidence_available: bool = False
    class_labels: dict[str, str] = field(default_factory=dict)
    class_histogram: dict[str, int] = field(default_factory=dict)
    mask_source: str = ""
    probability_source: str = ""
    inference_device: str | None = None
    batch_size: int = 0
    preloaded_models: bool = False
    requested_threshold: float | None = None
    threshold_for_mask: float | None = None
    threshold_used: float | None = None
    shadow_threshold: float | None = None
    sensor_recipe: str | None = None
    valid_pixels: int = 0
    tile_size: int = 0
    tiles_total: int = 0
    tile_workers: int = 0
    tile_sizing: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> CloudInferenceSummary:
        data = dict(payload or {})
        return cls(
            backend=_text(data.get("backend")),
            sensor=_text(data.get("sensor")),
            cloud_fraction=_float(data.get("cloud_fraction")),
            cloud_only_fraction=_float(data.get("cloud_only_fraction")),
            shadow_fraction=_float(data.get("shadow_fraction")),
            includes_shadows=bool(data.get("includes_shadows")),
            confidence_available=bool(data.get("confidence_available")),
            class_labels={str(key): str(value) for key, value in dict(data.get("class_labels") or {}).items()},
            class_histogram=_str_int_dict(data.get("class_histogram")),
            mask_source=_text(data.get("mask_source")),
            probability_source=_text(data.get("probability_source")),
            inference_device=_maybe_text(data.get("inference_device")),
            batch_size=_int(data.get("batch_size")),
            preloaded_models=bool(data.get("preloaded_models")),
            requested_threshold=(
                None if data.get("requested_threshold") is None else _float(data.get("requested_threshold"))
            ),
            threshold_for_mask=(
                None if data.get("threshold_for_mask") is None else _float(data.get("threshold_for_mask"))
            ),
            threshold_used=(
                None if data.get("threshold_used") is None else _float(data.get("threshold_used"))
            ),
            shadow_threshold=(
                None if data.get("shadow_threshold") is None else _float(data.get("shadow_threshold"))
            ),
            sensor_recipe=_maybe_text(data.get("sensor_recipe")),
            valid_pixels=_int(data.get("valid_pixels")),
            tile_size=_int(data.get("tile_size")),
            tiles_total=_int(data.get("tiles_total")),
            tile_workers=_int(data.get("tile_workers")),
            tile_sizing=_dict(data.get("tile_sizing")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "sensor": self.sensor,
            "cloud_fraction": float(self.cloud_fraction),
            "cloud_only_fraction": float(self.cloud_only_fraction),
            "shadow_fraction": float(self.shadow_fraction),
            "includes_shadows": bool(self.includes_shadows),
            "confidence_available": bool(self.confidence_available),
            "class_labels": dict(self.class_labels),
            "class_histogram": dict(self.class_histogram),
            "mask_source": self.mask_source,
            "probability_source": self.probability_source,
            "inference_device": self.inference_device,
            "batch_size": int(self.batch_size),
            "preloaded_models": bool(self.preloaded_models),
            "requested_threshold": self.requested_threshold,
            "threshold_for_mask": self.threshold_for_mask,
            "threshold_used": self.threshold_used,
            "shadow_threshold": self.shadow_threshold,
            "sensor_recipe": self.sensor_recipe,
            "valid_pixels": int(self.valid_pixels),
            "tile_size": int(self.tile_size),
            "tiles_total": int(self.tiles_total),
            "tile_workers": int(self.tile_workers),
            "tile_sizing": dict(self.tile_sizing),
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


@dataclass(frozen=True, slots=True)
class DatasetSummaryRecord:
    shape: list[int] = field(default_factory=list)
    pixel_size: list[float] = field(default_factory=list)
    reference_pixel_size: list[float] = field(default_factory=list)
    transform: list[float] = field(default_factory=list)
    crs: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "DatasetSummaryRecord":
        data = dict(payload or {})
        known = {"shape", "pixel_size", "reference_pixel_size", "transform", "crs"}
        return cls(
            shape=_int_list(data.get("shape")),
            pixel_size=[_float(item) for item in list(data.get("pixel_size") or [])],
            reference_pixel_size=[_float(item) for item in list(data.get("reference_pixel_size") or [])],
            transform=[_float(item) for item in list(data.get("transform") or [])],
            crs=_maybe_text(data.get("crs")),
            extras={key: value for key, value in data.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.extras)
        payload.update(
            {
                "shape": list(self.shape),
                "pixel_size": list(self.pixel_size),
                "reference_pixel_size": list(self.reference_pixel_size),
                "transform": list(self.transform),
                "crs": self.crs,
            }
        )
        return payload

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


@dataclass(frozen=True, slots=True)
class TileSizingDecision:
    source: str
    mask_kind: str
    provider: str
    collection: str
    collection_family: str
    product_type: str | None
    backend: str | None
    device: str
    tile_size: int
    default_tile_size: int
    scene_shape: list[int]
    scene_max_dimension: int
    scene_area_pixels: int
    target_pixel_size_meters: float | None
    scene_ground_span_meters: float | None
    tile_ground_span_meters: float | None
    target_tiles_long_axis: int | None
    target_tile_pixels: float | None
    target_tile_ground_span_meters: float | None
    estimated_tiles_long_axis: int
    model_patch_size: int
    snap_multiple: int
    patch_multiple: int | None
    min_patch_multiple: int | None
    max_patch_multiple: int | None
    requested_env_value: int | None
    invalid_env_value: str | None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "TileSizingDecision":
        data = dict(payload or {})
        return cls(
            source=_text(data.get("source")),
            mask_kind=_text(data.get("mask_kind")),
            provider=_text(data.get("provider")),
            collection=_text(data.get("collection")),
            collection_family=_text(data.get("collection_family")),
            product_type=_maybe_text(data.get("product_type")),
            backend=_maybe_text(data.get("backend")),
            device=_text(data.get("device")),
            tile_size=_int(data.get("tile_size")),
            default_tile_size=_int(data.get("default_tile_size")),
            scene_shape=_int_list(data.get("scene_shape")),
            scene_max_dimension=_int(data.get("scene_max_dimension")),
            scene_area_pixels=_int(data.get("scene_area_pixels")),
            target_pixel_size_meters=(
                None if data.get("target_pixel_size_meters") is None else _float(data.get("target_pixel_size_meters"))
            ),
            scene_ground_span_meters=(
                None if data.get("scene_ground_span_meters") is None else _float(data.get("scene_ground_span_meters"))
            ),
            tile_ground_span_meters=(
                None if data.get("tile_ground_span_meters") is None else _float(data.get("tile_ground_span_meters"))
            ),
            target_tiles_long_axis=(
                None if data.get("target_tiles_long_axis") is None else _int(data.get("target_tiles_long_axis"))
            ),
            target_tile_pixels=(
                None if data.get("target_tile_pixels") is None else _float(data.get("target_tile_pixels"))
            ),
            target_tile_ground_span_meters=(
                None
                if data.get("target_tile_ground_span_meters") is None
                else _float(data.get("target_tile_ground_span_meters"))
            ),
            estimated_tiles_long_axis=_int(data.get("estimated_tiles_long_axis")),
            model_patch_size=_int(data.get("model_patch_size")),
            snap_multiple=_int(data.get("snap_multiple")),
            patch_multiple=None if data.get("patch_multiple") is None else _int(data.get("patch_multiple")),
            min_patch_multiple=(
                None if data.get("min_patch_multiple") is None else _int(data.get("min_patch_multiple"))
            ),
            max_patch_multiple=(
                None if data.get("max_patch_multiple") is None else _int(data.get("max_patch_multiple"))
            ),
            requested_env_value=(
                None if data.get("requested_env_value") is None else _int(data.get("requested_env_value"))
            ),
            invalid_env_value=_maybe_text(data.get("invalid_env_value")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "mask_kind": self.mask_kind,
            "provider": self.provider,
            "collection": self.collection,
            "collection_family": self.collection_family,
            "product_type": self.product_type,
            "backend": self.backend,
            "device": self.device,
            "tile_size": int(self.tile_size),
            "default_tile_size": int(self.default_tile_size),
            "scene_shape": list(self.scene_shape),
            "scene_max_dimension": int(self.scene_max_dimension),
            "scene_area_pixels": int(self.scene_area_pixels),
            "target_pixel_size_meters": self.target_pixel_size_meters,
            "scene_ground_span_meters": self.scene_ground_span_meters,
            "tile_ground_span_meters": self.tile_ground_span_meters,
            "target_tiles_long_axis": self.target_tiles_long_axis,
            "target_tile_pixels": self.target_tile_pixels,
            "target_tile_ground_span_meters": self.target_tile_ground_span_meters,
            "estimated_tiles_long_axis": int(self.estimated_tiles_long_axis),
            "model_patch_size": int(self.model_patch_size),
            "snap_multiple": int(self.snap_multiple),
            "patch_multiple": self.patch_multiple,
            "min_patch_multiple": self.min_patch_multiple,
            "max_patch_multiple": self.max_patch_multiple,
            "requested_env_value": self.requested_env_value,
            "invalid_env_value": self.invalid_env_value,
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


@dataclass(frozen=True, slots=True)
class TileSizingPolicyStatus:
    mode: str
    env_var: str
    env_override: str | None
    default_tile_size: int
    patch_quantum: int
    selection_rule: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "env_var": self.env_var,
            "env_override": self.env_override,
            "default_tile_size": int(self.default_tile_size),
            "patch_quantum": int(self.patch_quantum),
            "selection_rule": self.selection_rule,
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    job_id: str
    stage_name: str
    payload: dict[str, Any]
    status: str
    updated_at: str
    sequence: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "stage_name": self.stage_name,
            "payload": dict(self.payload),
            "status": self.status,
            "updated_at": self.updated_at,
            "sequence": int(self.sequence),
        }


@dataclass(frozen=True, slots=True)
class ProgressRecord:
    job_id: str
    stage_name: str
    payload: dict[str, Any]
    status: str
    updated_at: str
    sequence: int
    history: list[ProgressEvent] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "stage_name": self.stage_name,
            "payload": dict(self.payload),
            "status": self.status,
            "updated_at": self.updated_at,
            "sequence": int(self.sequence),
            "history": [item.to_dict() for item in self.history],
        }


@dataclass(frozen=True, slots=True)
class StageEventPayload:
    zarr_uri: str | None = None
    output_zarr_uri: str | None = None
    source_zarr_uri: str | None = None
    scene_id: str | None = None
    provider: str | None = None
    collection: str | None = None
    product_type: str | None = None
    tiles_completed: int | None = None
    tiles_total: int | None = None
    progress: float | None = None
    status: str | None = None
    mask_types: list[str] = field(default_factory=list)
    masked_zarr_uri: str | None = None
    cloud_mask: dict[str, Any] | None = None
    water_mask: dict[str, Any] | None = None
    error: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "StageEventPayload":
        data = dict(payload or {})
        known = {
            "zarr_uri",
            "output_zarr_uri",
            "source_zarr_uri",
            "scene_id",
            "provider",
            "collection",
            "product_type",
            "tiles_completed",
            "tiles_total",
            "progress",
            "status",
            "mask_types",
            "masked_zarr_uri",
            "cloud_mask",
            "water_mask",
            "error",
        }
        return cls(
            zarr_uri=_maybe_text(data.get("zarr_uri")),
            output_zarr_uri=_maybe_text(data.get("output_zarr_uri")),
            source_zarr_uri=_maybe_text(data.get("source_zarr_uri")),
            scene_id=_maybe_text(data.get("scene_id")),
            provider=_maybe_text(data.get("provider")),
            collection=_maybe_text(data.get("collection")),
            product_type=_maybe_text(data.get("product_type")),
            tiles_completed=None if data.get("tiles_completed") is None else _int(data.get("tiles_completed")),
            tiles_total=None if data.get("tiles_total") is None else _int(data.get("tiles_total")),
            progress=None if data.get("progress") is None else _float(data.get("progress")),
            status=_maybe_text(data.get("status")),
            mask_types=_str_list(data.get("mask_types")),
            masked_zarr_uri=_maybe_text(data.get("masked_zarr_uri")),
            cloud_mask=None if data.get("cloud_mask") is None else _dict(data.get("cloud_mask")),
            water_mask=None if data.get("water_mask") is None else _dict(data.get("water_mask")),
            error=_maybe_text(data.get("error")),
            extras={key: value for key, value in data.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.extras)
        payload.update(
            {
                "zarr_uri": self.zarr_uri,
                "output_zarr_uri": self.output_zarr_uri,
                "source_zarr_uri": self.source_zarr_uri,
                "scene_id": self.scene_id,
                "provider": self.provider,
                "collection": self.collection,
                "product_type": self.product_type,
                "tiles_completed": self.tiles_completed,
                "tiles_total": self.tiles_total,
                "progress": self.progress,
                "status": self.status,
                "mask_types": list(self.mask_types),
                "masked_zarr_uri": self.masked_zarr_uri,
                "cloud_mask": None if self.cloud_mask is None else dict(self.cloud_mask),
                "water_mask": None if self.water_mask is None else dict(self.water_mask),
                "error": self.error,
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class MaskWriterMetadata:
    provider: str | None = None
    collection: str | None = None
    product_type: str | None = None
    scene_id: str | None = None
    artifact_uri: str | None = None
    status_path: str | None = None
    work_dir: str | None = None
    input_zarr_uri: str | None = None
    output_zarr_uri: str | None = None
    storage_mode: str | None = None
    runtime_mode: str | None = None
    inference_device: str | None = None
    threshold_used: float | None = None
    sensor_recipe: str | None = None
    probability_source: str | None = None
    model_profile: str | None = None
    model_attempt_count: int = 0
    model_attempts: list[dict[str, Any]] = field(default_factory=list)
    runtime_warning: str | None = None
    fallback_trigger: str | None = None
    tile_size: int | None = None
    tile_sizing: dict[str, Any] = field(default_factory=dict)
    scratch_root: str | None = None
    source_mask_raster: str | None = None
    include_shadows: bool | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "MaskWriterMetadata":
        data = dict(payload or {})
        known = {
            "provider",
            "collection",
            "product_type",
            "scene_id",
            "artifact_uri",
            "status_path",
            "work_dir",
            "input_zarr_uri",
            "output_zarr_uri",
            "storage_mode",
            "runtime_mode",
            "inference_device",
            "threshold_used",
            "sensor_recipe",
            "probability_source",
            "model_profile",
            "model_attempt_count",
            "model_attempts",
            "runtime_warning",
            "fallback_trigger",
            "tile_size",
            "tile_sizing",
            "scratch_root",
            "source_mask_raster",
            "include_shadows",
        }
        return cls(
            provider=_maybe_text(data.get("provider")),
            collection=_maybe_text(data.get("collection")),
            product_type=_maybe_text(data.get("product_type")),
            scene_id=_maybe_text(data.get("scene_id")),
            artifact_uri=_maybe_text(data.get("artifact_uri")),
            status_path=_maybe_text(data.get("status_path")),
            work_dir=_maybe_text(data.get("work_dir")),
            input_zarr_uri=_maybe_text(data.get("input_zarr_uri")),
            output_zarr_uri=_maybe_text(data.get("output_zarr_uri")),
            storage_mode=_maybe_text(data.get("storage_mode")),
            runtime_mode=_maybe_text(data.get("runtime_mode")),
            inference_device=_maybe_text(data.get("inference_device")),
            threshold_used=None if data.get("threshold_used") is None else _float(data.get("threshold_used")),
            sensor_recipe=_maybe_text(data.get("sensor_recipe")),
            probability_source=_maybe_text(data.get("probability_source")),
            model_profile=_maybe_text(data.get("model_profile")),
            model_attempt_count=_int(data.get("model_attempt_count")),
            model_attempts=[_dict(item) for item in list(data.get("model_attempts") or [])],
            runtime_warning=_maybe_text(data.get("runtime_warning")),
            fallback_trigger=_maybe_text(data.get("fallback_trigger")),
            tile_size=None if data.get("tile_size") is None else _int(data.get("tile_size")),
            tile_sizing=_dict(data.get("tile_sizing")),
            scratch_root=_maybe_text(data.get("scratch_root")),
            source_mask_raster=_maybe_text(data.get("source_mask_raster")),
            include_shadows=None if data.get("include_shadows") is None else bool(data.get("include_shadows")),
            extras={key: value for key, value in data.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.extras)
        payload.update(
            {
                "provider": self.provider,
                "collection": self.collection,
                "product_type": self.product_type,
                "scene_id": self.scene_id,
                "artifact_uri": self.artifact_uri,
                "status_path": self.status_path,
                "work_dir": self.work_dir,
                "input_zarr_uri": self.input_zarr_uri,
                "output_zarr_uri": self.output_zarr_uri,
                "storage_mode": self.storage_mode,
                "runtime_mode": self.runtime_mode,
                "inference_device": self.inference_device,
                "threshold_used": self.threshold_used,
                "sensor_recipe": self.sensor_recipe,
                "probability_source": self.probability_source,
                "model_profile": self.model_profile,
                "model_attempt_count": int(self.model_attempt_count),
                "model_attempts": [dict(item) for item in self.model_attempts],
                "runtime_warning": self.runtime_warning,
                "fallback_trigger": self.fallback_trigger,
                "tile_size": self.tile_size,
                "tile_sizing": dict(self.tile_sizing),
                "scratch_root": self.scratch_root,
                "source_mask_raster": self.source_mask_raster,
                "include_shadows": self.include_shadows,
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class CloudBackendRunRequest:
    sensor: Any
    channels: dict[str, Any]
    threshold: float
    inference_device: str | None = None
    include_shadows: bool = True
    valid_mask: Any | None = None


@dataclass(frozen=True, slots=True)
class WaterBackendRunRequest:
    job_id: str | None = None
    zarr_uri: str = ""
    source_zarr_uri: str | None = None
    provider: str = ""
    collection: str = ""
    product_type: str | None = None
    scene_id: str = ""
    acquisition_datetime: str | None = None
    dataset_summary: DatasetSummaryRecord = field(default_factory=DatasetSummaryRecord)
    output_zarr_uri: str | None = None
    overwrite: bool = True
    inference_device: str | None = None
    fail_on_error: bool = False
    stage_callback: Any | None = None


@dataclass(frozen=True, slots=True)
class BackendAvailabilityRecord:
    name: str
    available: bool
    primary: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "BackendAvailabilityRecord":
        data = dict(payload or {})
        return cls(
            name=_text(data.get("name")),
            available=bool(data.get("available")),
            primary=bool(data.get("primary")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": bool(self.available),
            "primary": bool(self.primary),
        }


@dataclass(frozen=True, slots=True)
class RegistryStatusRecord:
    cloud: list[BackendAvailabilityRecord] = field(default_factory=list)
    water: list[BackendAvailabilityRecord] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "RegistryStatusRecord":
        data = dict(payload or {})
        return cls(
            cloud=[BackendAvailabilityRecord.from_mapping(item) for item in list(data.get("cloud") or [])],
            water=[BackendAvailabilityRecord.from_mapping(item) for item in list(data.get("water") or [])],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "cloud": [item.to_dict() for item in self.cloud],
            "water": [item.to_dict() for item in self.water],
        }


@dataclass(frozen=True, slots=True)
class RuntimeDeviceStatusRecord:
    explicit: str
    env: str
    auto_detected: str
    resolved: str
    available: dict[str, bool]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "RuntimeDeviceStatusRecord":
        data = dict(payload or {})
        return cls(
            explicit=_text(data.get("explicit")),
            env=_text(data.get("env")),
            auto_detected=_text(data.get("auto_detected")),
            resolved=_text(data.get("resolved")),
            available={str(key): bool(value) for key, value in dict(data.get("available") or {}).items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "explicit": self.explicit,
            "env": self.env,
            "auto_detected": self.auto_detected,
            "resolved": self.resolved,
            "available": dict(self.available),
        }


@dataclass(frozen=True, slots=True)
class IntegrationPolicyRecord:
    public_api: str
    pipeline_stage: str
    storage_policy: str
    mask_contract_version: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "public_api": self.public_api,
            "pipeline_stage": self.pipeline_stage,
            "storage_policy": self.storage_policy,
            "mask_contract_version": self.mask_contract_version,
        }


@dataclass(frozen=True, slots=True)
class SupportStatusRecord:
    omniwatermask_available: bool
    omnicloudmask_available: bool
    runtime_cloud: RuntimeDeviceStatusRecord
    runtime_water: RuntimeDeviceStatusRecord
    tile_sizing_cloud: TileSizingPolicyStatus
    tile_sizing_water: TileSizingPolicyStatus
    registry: RegistryStatusRecord

    def to_dict(self) -> dict[str, Any]:
        return {
            "omniwatermask_available": bool(self.omniwatermask_available),
            "omnicloudmask_available": bool(self.omnicloudmask_available),
            "runtime": {
                "cloud": self.runtime_cloud.to_dict(),
                "water": self.runtime_water.to_dict(),
            },
            "tile_sizing": {
                "cloud": self.tile_sizing_cloud.to_dict(),
                "water": self.tile_sizing_water.to_dict(),
            },
            "registry": self.registry.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class MaskModelSchemaRecord:
    status: str
    integration_policy: IntegrationPolicyRecord
    backends: RegistryStatusRecord
    water: dict[str, Any]
    cloud: dict[str, Any]
    cloud_probability: dict[str, Any]
    water_probability: dict[str, Any]
    input_policy: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "integration_policy": self.integration_policy.to_dict(),
            "backends": self.backends.to_dict(),
            "water": dict(self.water),
            "cloud": dict(self.cloud),
            "cloud_probability": dict(self.cloud_probability),
            "water_probability": dict(self.water_probability),
            "input_policy": dict(self.input_policy),
        }


@dataclass(frozen=True, slots=True)
class MaskWriteSummary:
    mask_path: str
    probability_path: str
    mask_shape: list[int]
    mask_dtype: str
    probability_dtype: str
    classes: dict[str, str]
    written_at: str

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> MaskWriteSummary:
        data = dict(payload or {})
        return cls(
            mask_path=_text(data.get("mask_path")),
            probability_path=_text(data.get("probability_path")),
            mask_shape=_int_list(data.get("mask_shape") or data.get("shape")),
            mask_dtype=_text(data.get("mask_dtype") or data.get("dtype")),
            probability_dtype=_text(data.get("probability_dtype")),
            classes={str(key): str(value) for key, value in dict(data.get("classes") or {}).items()},
            written_at=_text(data.get("written_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mask_path": self.mask_path,
            "probability_path": self.probability_path,
            "mask_shape": list(self.mask_shape),
            "mask_dtype": self.mask_dtype,
            "probability_dtype": self.probability_dtype,
            "classes": dict(self.classes),
            "written_at": self.written_at,
        }


@dataclass(frozen=True, slots=True)
class WaterRuntimeSummary:
    runtime_mode: str = ""
    water_fraction: float = 0.0
    probability_mean: float = 0.0
    threshold_used: float | None = None
    sensor_recipe: str | None = None
    input_bands: list[str] = field(default_factory=list)
    probability_source: str = "water_score"
    cloud_blocked_fraction: float = 0.0
    valid_pixel_fraction: float = 0.0
    tile_size: int = 0
    tile_sizing: dict[str, Any] = field(default_factory=dict)
    tile_workers: int = 0
    model_profile: str = ""
    model_attempt_count: int = 0
    model_attempts: list[dict[str, Any]] = field(default_factory=list)
    runtime_warning: str = ""
    fallback_trigger: str = ""
    scratch_root: str = ""

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> WaterRuntimeSummary:
        data = dict(payload or {})
        return cls(
            runtime_mode=_text(data.get("runtime_mode")),
            water_fraction=_float(data.get("water_fraction")),
            probability_mean=_float(data.get("probability_mean")),
            threshold_used=None if data.get("threshold_used") is None else _float(data.get("threshold_used")),
            sensor_recipe=_maybe_text(data.get("sensor_recipe")),
            input_bands=_str_list(data.get("input_bands")),
            probability_source=_text(data.get("probability_source") or "water_score"),
            cloud_blocked_fraction=_float(data.get("cloud_blocked_fraction")),
            valid_pixel_fraction=_float(data.get("valid_pixel_fraction")),
            tile_size=_int(data.get("tile_size")),
            tile_sizing=_dict(data.get("tile_sizing")),
            tile_workers=_int(data.get("tile_workers")),
            model_profile=_text(data.get("model_profile")),
            model_attempt_count=_int(data.get("model_attempt_count")),
            model_attempts=[_dict(item) for item in list(data.get("model_attempts") or [])],
            runtime_warning=_text(data.get("runtime_warning")),
            fallback_trigger=_text(data.get("fallback_trigger")),
            scratch_root=_text(data.get("scratch_root")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_mode": self.runtime_mode,
            "water_fraction": float(self.water_fraction),
            "probability_mean": float(self.probability_mean),
            "threshold_used": self.threshold_used,
            "sensor_recipe": self.sensor_recipe,
            "input_bands": list(self.input_bands),
            "probability_source": self.probability_source,
            "cloud_blocked_fraction": float(self.cloud_blocked_fraction),
            "valid_pixel_fraction": float(self.valid_pixel_fraction),
            "tile_size": int(self.tile_size),
            "tile_sizing": dict(self.tile_sizing),
            "tile_workers": int(self.tile_workers),
            "model_profile": self.model_profile,
            "model_attempt_count": int(self.model_attempt_count),
            "model_attempts": [dict(item) for item in self.model_attempts],
            "runtime_warning": self.runtime_warning,
            "fallback_trigger": self.fallback_trigger,
            "scratch_root": self.scratch_root,
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self):
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def keys(self):
        return self.to_dict().keys()

    def items(self):
        return self.to_dict().items()

    def values(self):
        return self.to_dict().values()


@dataclass(frozen=True, slots=True)
class CloudMaskState:
    status: str
    input_zarr_uri: str
    output_zarr_uri: str
    storage_mode: str
    reason: str | None = None
    mask_contract_version: str = "v2"
    mask_path: str | None = None
    probability_path: str | None = None
    artifact_uri: str | None = None
    status_path: str | None = None
    work_dir: str | None = None
    shape: list[int] = field(default_factory=list)
    dtype: str | None = None
    classes: dict[str, str] = field(default_factory=dict)
    threshold: float | None = None
    backend: str | None = None
    sensor: str | None = None
    input_bands: list[str] = field(default_factory=list)
    written_at: str | None = None
    inference: CloudInferenceSummary | None = None
    tile_size: int = 0
    tile_sizing: dict[str, Any] = field(default_factory=dict)
    include_shadows: bool = False
    cloud_fraction: float = 0.0
    cloud_only_fraction: float = 0.0
    shadow_fraction: float = 0.0
    mask_source: str = ""
    probability_source: str = ""
    sensor_recipe: str | None = None
    backend_request: str = "auto"

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> CloudMaskState:
        data = dict(payload or {})
        inference_payload = data.get("inference")
        return cls(
            status=_text(data.get("status")),
            reason=_maybe_text(data.get("reason")),
            mask_contract_version=_text(data.get("mask_contract_version") or "v2"),
            input_zarr_uri=_text(data.get("input_zarr_uri")),
            output_zarr_uri=_text(data.get("output_zarr_uri")),
            storage_mode=_text(data.get("storage_mode")),
            mask_path=_maybe_text(data.get("mask_path")),
            probability_path=_maybe_text(data.get("probability_path")),
            artifact_uri=_maybe_text(data.get("artifact_uri")),
            status_path=_maybe_text(data.get("status_path")),
            work_dir=_maybe_text(data.get("work_dir")),
            shape=_int_list(data.get("shape")),
            dtype=_maybe_text(data.get("dtype")),
            classes={str(key): str(value) for key, value in dict(data.get("classes") or {}).items()},
            threshold=None if data.get("threshold") is None else _float(data.get("threshold")),
            backend=_maybe_text(data.get("backend")),
            sensor=_maybe_text(data.get("sensor")),
            input_bands=_str_list(data.get("input_bands")),
            written_at=_maybe_text(data.get("written_at")),
            inference=None if inference_payload is None else CloudInferenceSummary.from_mapping(_dict(inference_payload)),
            tile_size=_int(data.get("tile_size")),
            tile_sizing=_dict(data.get("tile_sizing")),
            include_shadows=bool(data.get("include_shadows")),
            cloud_fraction=_float(data.get("cloud_fraction")),
            cloud_only_fraction=_float(data.get("cloud_only_fraction")),
            shadow_fraction=_float(data.get("shadow_fraction")),
            mask_source=_text(data.get("mask_source")),
            probability_source=_text(data.get("probability_source")),
            sensor_recipe=_maybe_text(data.get("sensor_recipe")),
            backend_request=_text(data.get("backend_request") or "auto"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mask_contract_version": self.mask_contract_version,
            "reason": self.reason,
            "input_zarr_uri": self.input_zarr_uri,
            "output_zarr_uri": self.output_zarr_uri,
            "storage_mode": self.storage_mode,
            "mask_path": self.mask_path,
            "probability_path": self.probability_path,
            "artifact_uri": self.artifact_uri,
            "status_path": self.status_path,
            "work_dir": self.work_dir,
            "shape": list(self.shape),
            "dtype": self.dtype,
            "classes": dict(self.classes),
            "threshold": self.threshold,
            "backend": self.backend,
            "sensor": self.sensor,
            "input_bands": list(self.input_bands),
            "written_at": self.written_at,
            "inference": None if self.inference is None else self.inference.to_dict(),
            "tile_size": int(self.tile_size),
            "tile_sizing": dict(self.tile_sizing),
            "include_shadows": bool(self.include_shadows),
            "cloud_fraction": float(self.cloud_fraction),
            "cloud_only_fraction": float(self.cloud_only_fraction),
            "shadow_fraction": float(self.shadow_fraction),
            "mask_source": self.mask_source,
            "probability_source": self.probability_source,
            "sensor_recipe": self.sensor_recipe,
            "backend_request": self.backend_request,
        }


@dataclass(frozen=True, slots=True)
class WaterMaskState:
    status: str
    input_zarr_uri: str
    output_zarr_uri: str
    storage_mode: str
    reason: str | None = None
    input_bands: list[str] = field(default_factory=list)
    fallback_bands: list[str] = field(default_factory=list)
    threshold_used: float | None = None
    mask_path: str | None = None
    probability_path: str | None = None
    artifact_uri: str | None = None
    status_path: str | None = None
    work_dir: str | None = None
    shape: list[int] = field(default_factory=list)
    dtype: str | None = None
    probability_dtype: str | None = None
    classes: dict[str, str] = field(default_factory=dict)
    model_name: str | None = None
    model_version: str | None = None
    written_at: str | None = None
    runtime_mode: str | None = None
    sensor_recipe: str | None = None
    water_fraction: float = 0.0
    probability_source: str = "water_score"
    cloud_blocked_fraction: float = 0.0
    runtime_warning: str = ""
    fallback_trigger: str = ""
    model_profile: str = ""
    model_attempt_count: int = 0
    tile_size: int = 0
    tile_sizing: dict[str, Any] = field(default_factory=dict)
    scratch_root: str = ""
    working_zarr_uri: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> WaterMaskState:
        data = dict(payload or {})
        return cls(
            status=_text(data.get("status")),
            reason=_maybe_text(data.get("reason")),
            input_zarr_uri=_text(data.get("input_zarr_uri")),
            output_zarr_uri=_text(data.get("output_zarr_uri")),
            storage_mode=_text(data.get("storage_mode")),
            input_bands=_str_list(data.get("input_bands")),
            fallback_bands=_str_list(data.get("fallback_bands")),
            threshold_used=None if data.get("threshold_used") is None else _float(data.get("threshold_used")),
            mask_path=_maybe_text(data.get("mask_path")),
            probability_path=_maybe_text(data.get("probability_path")),
            artifact_uri=_maybe_text(data.get("artifact_uri")),
            status_path=_maybe_text(data.get("status_path")),
            work_dir=_maybe_text(data.get("work_dir")),
            shape=_int_list(data.get("shape")),
            dtype=_maybe_text(data.get("dtype")),
            probability_dtype=_maybe_text(data.get("probability_dtype")),
            classes={str(key): str(value) for key, value in dict(data.get("classes") or {}).items()},
            model_name=_maybe_text(data.get("model_name")),
            model_version=_maybe_text(data.get("model_version")),
            written_at=_maybe_text(data.get("written_at")),
            runtime_mode=_maybe_text(data.get("runtime_mode")),
            sensor_recipe=_maybe_text(data.get("sensor_recipe")),
            water_fraction=_float(data.get("water_fraction")),
            probability_source=_text(data.get("probability_source") or "water_score"),
            cloud_blocked_fraction=_float(data.get("cloud_blocked_fraction")),
            runtime_warning=_text(data.get("runtime_warning")),
            fallback_trigger=_text(data.get("fallback_trigger")),
            model_profile=_text(data.get("model_profile")),
            model_attempt_count=_int(data.get("model_attempt_count")),
            tile_size=_int(data.get("tile_size")),
            tile_sizing=_dict(data.get("tile_sizing")),
            scratch_root=_text(data.get("scratch_root")),
            working_zarr_uri=_maybe_text(data.get("working_zarr_uri")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "input_zarr_uri": self.input_zarr_uri,
            "working_zarr_uri": self.working_zarr_uri,
            "output_zarr_uri": self.output_zarr_uri,
            "storage_mode": self.storage_mode,
            "input_bands": list(self.input_bands),
            "fallback_bands": list(self.fallback_bands),
            "threshold_used": self.threshold_used,
            "mask_path": self.mask_path,
            "probability_path": self.probability_path,
            "artifact_uri": self.artifact_uri,
            "status_path": self.status_path,
            "work_dir": self.work_dir,
            "shape": list(self.shape),
            "dtype": self.dtype,
            "probability_dtype": self.probability_dtype,
            "classes": dict(self.classes),
            "model_name": self.model_name,
            "model_version": self.model_version,
            "written_at": self.written_at,
            "runtime_mode": self.runtime_mode,
            "sensor_recipe": self.sensor_recipe,
            "water_fraction": float(self.water_fraction),
            "probability_source": self.probability_source,
            "cloud_blocked_fraction": float(self.cloud_blocked_fraction),
            "runtime_warning": self.runtime_warning,
            "fallback_trigger": self.fallback_trigger,
            "model_profile": self.model_profile,
            "model_attempt_count": int(self.model_attempt_count),
            "tile_size": int(self.tile_size),
            "tile_sizing": dict(self.tile_sizing),
            "scratch_root": self.scratch_root,
        }


@dataclass(frozen=True, slots=True)
class CombinedMaskState:
    status: str
    mask_types: list[str]
    input_zarr_uri: str
    output_zarr_uri: str
    masked_zarr_uri: str | None
    masked_zarr_outputs: list[str]
    water_mask: WaterMaskState
    cloud_mask: CloudMaskState
    mask_contract_version: str = "v2"
    watermask_outputs: list[str] = field(default_factory=list)
    cloudmask_outputs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mask_contract_version": self.mask_contract_version,
            "mask_types": list(self.mask_types),
            "input_zarr_uri": self.input_zarr_uri,
            "output_zarr_uri": self.output_zarr_uri,
            "masked_zarr_uri": self.masked_zarr_uri,
            "masked_zarr_outputs": list(self.masked_zarr_outputs),
            "water_mask": self.water_mask.to_dict(),
            "cloud_mask": self.cloud_mask.to_dict(),
            "watermask_outputs": list(self.watermask_outputs),
            "cloudmask_outputs": list(self.cloudmask_outputs),
        }


__all__ = [
    "BackendAvailabilityRecord",
    "CloudBackendRunRequest",
    "DatasetSummaryRecord",
    "CloudInferenceSummary",
    "CloudMaskState",
    "CombinedMaskState",
    "IntegrationPolicyRecord",
    "MaskWriterMetadata",
    "MaskWriteSummary",
    "MaskModelSchemaRecord",
    "ProgressEvent",
    "ProgressRecord",
    "RegistryStatusRecord",
    "RuntimeDeviceStatusRecord",
    "StageEventPayload",
    "SupportStatusRecord",
    "TileSizingDecision",
    "TileSizingPolicyStatus",
    "WaterBackendRunRequest",
    "WaterMaskState",
    "WaterRuntimeSummary",
]
