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


def _dict(value: Any) -> dict[str, Any]:
    return dict(value or {})


def _str_list(value: Any) -> list[str]:
    return [str(item) for item in list(value or [])]


@dataclass(frozen=True, slots=True)
class ConversionRequestRecord:
    provider: str
    collection: str
    scene_id: str
    raw_uri: str
    output_uri: str
    product_type: str | None = None


@dataclass(frozen=True, slots=True)
class ConversionProgressRecord:
    stage: str = ""
    fraction: float = 0.0
    array_name: str = ""
    band_name: str = ""
    blocks_written: int = 0
    total_blocks: int = 0
    scene_id: str | None = None
    scene_index: int | None = None
    scene_total: int | None = None
    cube_output_uri: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "ConversionProgressRecord":
        data = dict(payload or {})
        known = {
            "stage",
            "fraction",
            "array_name",
            "band_name",
            "blocks_written",
            "total_blocks",
            "scene_id",
            "scene_index",
            "scene_total",
            "cube_output_uri",
        }
        return cls(
            stage=_text(data.get("stage")),
            fraction=_float(data.get("fraction")),
            array_name=_text(data.get("array_name")),
            band_name=_text(data.get("band_name")),
            blocks_written=_int(data.get("blocks_written")),
            total_blocks=_int(data.get("total_blocks")),
            scene_id=_maybe_text(data.get("scene_id")),
            scene_index=(None if data.get("scene_index") is None else _int(data.get("scene_index"))),
            scene_total=(None if data.get("scene_total") is None else _int(data.get("scene_total"))),
            cube_output_uri=_maybe_text(data.get("cube_output_uri")),
            extras={key: value for key, value in data.items() if key not in known},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.extras)
        payload.update(
            {
                "stage": self.stage,
                "fraction": float(self.fraction),
                "array_name": self.array_name,
                "band_name": self.band_name,
                "blocks_written": int(self.blocks_written),
                "total_blocks": int(self.total_blocks),
                "scene_id": self.scene_id,
                "scene_index": self.scene_index,
                "scene_total": self.scene_total,
                "cube_output_uri": self.cube_output_uri,
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class ConversionOutcomeRecord:
    written_uri: str
    data_family: str
    summary: dict[str, Any] = field(default_factory=dict)
    dataset_summary: dict[str, Any] = field(default_factory=dict)

    def as_tuple(self) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        return (
            self.written_uri,
            self.data_family,
            dict(self.summary),
            dict(self.dataset_summary),
        )


@dataclass(frozen=True, slots=True)
class CubeSummaryRecord:
    status: str = ""
    zarr_uri: str | None = None
    cube_outputs: list[dict[str, Any]] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "CubeSummaryRecord":
        data = dict(payload or {})
        return cls(
            status=_text(data.get("status")),
            zarr_uri=_maybe_text(data.get("zarr_uri")),
            cube_outputs=[dict(item or {}) for item in list(data.get("cube_outputs") or [])],
            payload=data,
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass(frozen=True, slots=True)
class DatasetInspectionRecord:
    dimensions: list[str] = field(default_factory=list)
    shape: list[int] = field(default_factory=list)
    band_names: list[str] = field(default_factory=list)
    ancillary_layer_names: list[str] = field(default_factory=list)
    acquisition_datetime: str | None = None
    crs: str | None = None
    transform: list[float] = field(default_factory=list)
    dtype: str = ""
    pixel_size: list[float] = field(default_factory=list)
    reference_pixel_size: list[float] = field(default_factory=list)
    band_metadata: dict[str, Any] = field(default_factory=dict)
    ancillary_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimensions": list(self.dimensions),
            "shape": list(self.shape),
            "band_names": list(self.band_names),
            "ancillary_layer_names": list(self.ancillary_layer_names),
            "acquisition_datetime": self.acquisition_datetime,
            "crs": self.crs,
            "transform": list(self.transform),
            "dtype": self.dtype,
            "pixel_size": list(self.pixel_size),
            "reference_pixel_size": list(self.reference_pixel_size),
            "band_metadata": dict(self.band_metadata),
            "ancillary_metadata": dict(self.ancillary_metadata),
        }


@dataclass(frozen=True, slots=True)
class ConverterConfigRecord:
    copernicus: dict[str, Any] = field(default_factory=dict)
    usgs: dict[str, Any] = field(default_factory=dict)
    resolution_policy: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "ConverterConfigRecord":
        data = dict(payload or {})
        return cls(
            copernicus=_dict(data.get("copernicus")),
            usgs=_dict(data.get("usgs")),
            resolution_policy=_dict(data.get("resolution_policy")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "copernicus": dict(self.copernicus),
            "usgs": dict(self.usgs),
            "resolution_policy": dict(self.resolution_policy),
        }


@dataclass(frozen=True, slots=True)
class CollectionProductSpecRecord:
    provider: str
    collection: str
    product_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass(frozen=True, slots=True)
class GridMetadataRecord:
    height: int
    width: int
    dtype: str
    crs: str | None = None
    transform: Any = None
    pixel_size: Any = None
    reference_band: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "GridMetadataRecord":
        data = dict(payload or {})
        return cls(
            height=_int(data.get("height")),
            width=_int(data.get("width")),
            dtype=_text(data.get("dtype")),
            crs=_maybe_text(data.get("crs")),
            transform=data.get("transform"),
            pixel_size=data.get("pixel_size"),
            reference_band=_maybe_text(data.get("reference_band")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "height": int(self.height),
            "width": int(self.width),
            "dtype": self.dtype,
            "crs": self.crs,
            "transform": self.transform,
            "pixel_size": self.pixel_size,
            "reference_band": self.reference_band,
        }


@dataclass(frozen=True, slots=True)
class RasterBandMetadataRecord:
    path: str
    source_layer: str
    source_band_index: int
    source_raster_band_count: int
    dtype: str
    source_height: int
    source_width: int
    source_crs: str | None = None
    source_transform: list[float] = field(default_factory=list)
    source_pixel_size: list[float] = field(default_factory=list)
    reference_native_pixel_size: list[float] = field(default_factory=list)
    reference_pixel_size: list[float] = field(default_factory=list)
    target_pixel_size_requested: float | None = None
    resampled_to_reference: bool = False
    categorical: bool = False
    source_nodata: float | int | None = None
    target_nodata: float | int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "source_layer": self.source_layer,
            "source_band_index": int(self.source_band_index),
            "source_raster_band_count": int(self.source_raster_band_count),
            "dtype": self.dtype,
            "source_height": int(self.source_height),
            "source_width": int(self.source_width),
            "source_crs": self.source_crs,
            "source_transform": list(self.source_transform),
            "source_pixel_size": list(self.source_pixel_size),
            "reference_native_pixel_size": list(self.reference_native_pixel_size),
            "reference_pixel_size": list(self.reference_pixel_size),
            "target_pixel_size_requested": self.target_pixel_size_requested,
            "resampled_to_reference": bool(self.resampled_to_reference),
            "categorical": bool(self.categorical),
            "source_nodata": self.source_nodata,
            "target_nodata": self.target_nodata,
        }


@dataclass(frozen=True, slots=True)
class RasterStackRecord:
    band_names: list[str]
    height: int
    width: int
    dtype: str
    crs: str | None = None
    transform: list[float] = field(default_factory=list)
    pixel_size: list[float] = field(default_factory=list)
    reference_band: str | None = None
    band_metadata: dict[str, RasterBandMetadataRecord] = field(default_factory=dict)
    arrays: Any | None = None

    @property
    def band_metadata_dict(self) -> dict[str, dict[str, Any]]:
        return {key: value.to_dict() for key, value in self.band_metadata.items()}


@dataclass(frozen=True, slots=True)
class DatasetZarrSummaryRecord:
    data_family: str
    zarr_uri: str
    dimensions: list[str]
    shape: list[int]
    band_names: list[str]
    time_values: list[str]
    dtype: str = ""
    crs: str | None = None
    transform: Any = None
    pixel_size: Any = None
    band_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    ancillary_layer_names: list[str] = field(default_factory=list)
    ancillary_dimensions: list[str] = field(default_factory=list)
    ancillary_shape: list[int] = field(default_factory=list)
    ancillary_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    quadkey_schema_version: Any = None
    quadkey_coverage_mode: Any = None
    quadkey_zoom_index: Any = None
    quadkeys_index: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "data_family": self.data_family,
            "zarr_uri": self.zarr_uri,
            "dimensions": list(self.dimensions),
            "shape": list(self.shape),
            "band_names": list(self.band_names),
            "time_values": list(self.time_values),
            "dtype": self.dtype,
            "crs": self.crs,
            "transform": self.transform,
            "pixel_size": self.pixel_size,
            "band_metadata": dict(self.band_metadata),
        }
        if self.ancillary_layer_names or self.ancillary_dimensions or self.ancillary_shape or self.ancillary_metadata:
            payload.update(
                {
                    "ancillary_layer_names": list(self.ancillary_layer_names),
                    "ancillary_dimensions": list(self.ancillary_dimensions),
                    "ancillary_shape": list(self.ancillary_shape),
                    "ancillary_metadata": dict(self.ancillary_metadata),
                }
            )
        if self.quadkey_schema_version is not None or self.quadkeys_index:
            payload.update(
                {
                    "quadkey_schema_version": self.quadkey_schema_version,
                    "quadkey_coverage_mode": self.quadkey_coverage_mode,
                    "quadkey_zoom_index": self.quadkey_zoom_index,
                    "quadkeys_index": list(self.quadkeys_index),
                }
            )
        return payload

@dataclass(frozen=True, slots=True)
class ProductNormalizationSummaryRecord:
    provider: str
    collection: str
    scene_id: str
    product_id: str
    product_type: str
    product_level: str
    data_family: str
    source_kind: str
    raw_path: str
    acquisition_datetime: str | None = None
    normalized_band_order: list[str] = field(default_factory=list)
    resolution_policy_meters: float | None = None
    band_sources: dict[str, str] = field(default_factory=dict)
    band_resampling: dict[str, Any] = field(default_factory=dict)
    band_native_pixel_size: dict[str, Any] = field(default_factory=dict)
    ancillary_layer_names: list[str] = field(default_factory=list)
    ancillary_sources: dict[str, str] = field(default_factory=dict)
    ancillary_resampling: dict[str, Any] = field(default_factory=dict)
    ancillary_native_pixel_size: dict[str, Any] = field(default_factory=dict)
    grid: dict[str, Any] = field(default_factory=dict)
    validation: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.extras)
        payload.update(
            {
                "provider": self.provider,
                "collection": self.collection,
                "scene_id": self.scene_id,
                "product_id": self.product_id,
                "product_type": self.product_type,
                "product_level": self.product_level,
                "data_family": self.data_family,
                "source_kind": self.source_kind,
                "raw_path": self.raw_path,
                "acquisition_datetime": self.acquisition_datetime,
                "normalized_band_order": list(self.normalized_band_order),
                "resolution_policy_meters": self.resolution_policy_meters,
                "band_sources": dict(self.band_sources),
                "band_resampling": dict(self.band_resampling),
                "band_native_pixel_size": dict(self.band_native_pixel_size),
                "ancillary_layer_names": list(self.ancillary_layer_names),
                "ancillary_sources": dict(self.ancillary_sources),
                "ancillary_resampling": dict(self.ancillary_resampling),
                "ancillary_native_pixel_size": dict(self.ancillary_native_pixel_size),
                "grid": dict(self.grid),
                "validation": dict(self.validation),
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class LandsatNormalizationSummaryRecord(ProductNormalizationSummaryRecord):
    product_type_short: str = ""
    satellite: str = ""
    radiometric_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = ProductNormalizationSummaryRecord.to_dict(self)
        payload.update(
            {
                "product_type_short": self.product_type_short,
                "satellite": self.satellite,
                "radiometric_metadata": dict(self.radiometric_metadata),
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class CubeBuildSummaryRecord:
    zarr_uri: str
    cube_kind: str
    source_scene_count: int
    source_zarr_uris: list[str] = field(default_factory=list)
    band_names: list[str] = field(default_factory=list)
    shape: list[int] = field(default_factory=list)
    time_values: list[str] = field(default_factory=list)
    scene_ids: list[str] = field(default_factory=list)
    provider: str | None = None
    collection: str | None = None
    product_type: str | None = None
    data_family: str | None = None
    crs: Any = None
    transform: Any = None
    pixel_size: Any = None
    dimensions: list[str] = field(default_factory=lambda: ["time", "band", "y", "x"])
    ancillary_written: bool = False
    ancillary_layer_names: list[str] = field(default_factory=list)
    masks_written: bool = False
    mask_layer_names: list[str] = field(default_factory=list)
    quadkey_schema_version: Any = None
    quadkey_coverage_mode: Any = None
    quadkey_zoom_index: Any = None
    quadkeys_index: list[str] = field(default_factory=list)
    time_granularity: str | None = None
    mosaic_overlap_policy: str | None = None
    mosaic_crs_policy: str | None = None
    mosaic_resolution_m: int | None = None
    nodata: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "zarr_uri": self.zarr_uri,
            "cube_kind": self.cube_kind,
            "source_scene_count": int(self.source_scene_count),
            "source_zarr_uris": list(self.source_zarr_uris),
            "band_names": list(self.band_names),
            "shape": list(self.shape),
            "time_values": list(self.time_values),
            "scene_ids": list(self.scene_ids),
            "provider": self.provider,
            "collection": self.collection,
            "product_type": self.product_type,
            "data_family": self.data_family,
            "crs": self.crs,
            "transform": self.transform,
            "pixel_size": self.pixel_size,
            "dimensions": list(self.dimensions),
            "ancillary_written": bool(self.ancillary_written),
            "ancillary_layer_names": list(self.ancillary_layer_names),
            "masks_written": bool(self.masks_written),
            "mask_layer_names": list(self.mask_layer_names),
            "quadkey_schema_version": self.quadkey_schema_version,
            "quadkey_coverage_mode": self.quadkey_coverage_mode,
            "quadkey_zoom_index": self.quadkey_zoom_index,
            "quadkeys_index": list(self.quadkeys_index),
            "time_granularity": self.time_granularity,
            "mosaic_overlap_policy": self.mosaic_overlap_policy,
            "mosaic_crs_policy": self.mosaic_crs_policy,
            "mosaic_resolution_m": self.mosaic_resolution_m,
            "nodata": self.nodata,
        }


@dataclass(frozen=True, slots=True)
class GroupedCubeItemRecord:
    summary: CubeBuildSummaryRecord
    group_key: str
    skipped_duplicate_scene_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = self.summary.to_dict()
        payload.update(
            {
                "group_key": self.group_key,
                "skipped_duplicate_scene_ids": list(self.skipped_duplicate_scene_ids),
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class GroupedCubeSkippedRecord:
    group_key: str
    reason: str
    candidate_scene_ids: list[str] = field(default_factory=list)
    error_code: str | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "group_key": self.group_key,
            "reason": self.reason,
            "candidate_scene_ids": list(self.candidate_scene_ids),
        }
        if self.error_code:
            payload["error_code"] = self.error_code
        if self.message:
            payload["message"] = self.message
        return payload


@dataclass(frozen=True, slots=True)
class GroupedCubeSummaryRecord:
    status: str
    reason: str
    cube_outputs: list[str] = field(default_factory=list)
    items: list[dict[str, Any]] = field(default_factory=list)
    tiles_built: list[str] = field(default_factory=list)
    tiles_skipped: list[dict[str, Any]] = field(default_factory=list)
    stage_label: str | None = None
    date_range: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    diagnostics: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "error_code": self.error_code,
            "cube_outputs": list(self.cube_outputs),
            "items": list(self.items),
            "tiles_built": list(self.tiles_built),
            "tiles_skipped": list(self.tiles_skipped),
            "stage_label": self.stage_label,
            "date_range": dict(self.date_range),
            "diagnostics": list(self.diagnostics),
        }
