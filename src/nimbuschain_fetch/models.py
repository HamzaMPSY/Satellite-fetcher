from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from nimbuschain_fetch.domain.metadata import (
    ConversionMetadataRecord,
    PayloadRecord,
    PipelineMetadataRecord,
    StringMapRecord,
)
from nimbuschain_fetch.geometry.aoi import validate_aoi_payload


COLLECTION_RE = re.compile(r"^[A-Za-z0-9._\-/]{1,120}$")


def _normalize_mask_type_values(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    for value in list(values or []):
        candidate = str(value or "").strip().lower()
        if candidate not in {"water", "cloud"}:
            raise ValueError("mask_types must contain only 'water' and/or 'cloud'.")
        if candidate not in normalized:
            normalized.append(candidate)
    return normalized


class ProviderName(str, Enum):
    copernicus = "copernicus"
    usgs = "usgs"


class JobState(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"


class PipelineState(str, Enum):
    queued = "queued"
    searching = "searching"
    downloading = "downloading"
    downloaded = "downloaded"
    sen2like_queued = "sen2like_queued"
    sen2like_running = "sen2like_running"
    sen2like_written = "sen2like_written"
    zarr_queued = "zarr_queued"
    zarr_converting = "zarr_converting"
    zarr_written = "zarr_written"
    cube_queued = "cube_queued"
    cube_building = "cube_building"
    cube_written = "cube_written"
    cube_failed = "cube_failed"
    resolving_source_zarr = "resolving_source_zarr"
    copying_source_zarr = "copying_source_zarr"
    running_water_inference = "running_water_inference"
    running_cloud_inference = "running_cloud_inference"
    writing_mask_artifacts = "writing_mask_artifacts"
    writing_masked_zarr = "writing_masked_zarr"
    registering_artifacts = "registering_artifacts"
    masked_zarr_written = "masked_zarr_written"
    sen2like_failed = "sen2like_failed"
    zarr_failed = "zarr_failed"
    failed = "failed"
    cancelled = "cancelled"


class AOIInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    wkt: str | None = None
    geojson: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_geometry(self) -> "AOIInput":
        validate_aoi_payload(self.model_dump())
        return self


class SearchDownloadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: Literal["search_download"]
    provider: ProviderName
    collection: str
    product_type: str
    start_date: date
    end_date: date
    aoi: AOIInput
    tile_id: str | None = None
    output_dir: str | None = None
    download_only: bool = False
    mask_types: list[Literal["water", "cloud"]] = Field(default_factory=list)
    download_strategy: Literal["default", "copernicus_account_pool"] = "default"
    cube_mode: Literal["none", "before_mask", "after_mask"] = "none"
    cube_start_date: date | None = None
    cube_end_date: date | None = None
    cube_layout: Literal["grouped_time", "daily_mosaic"] = "grouped_time"
    cube_target_crs: str | None = None
    cube_target_resolution_m: int = Field(default=10, ge=1, le=1000)
    cube_overlap_policy: Literal["least_cloud", "latest", "earliest", "first_valid"] = "least_cloud"

    @field_validator("collection", "product_type")
    @classmethod
    def _validate_collection_like(cls, value: str) -> str:
        if not COLLECTION_RE.match(value):
            raise ValueError("Invalid collection/product_type format.")
        return value

    @field_validator("output_dir")
    @classmethod
    def _validate_output_dir(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if value.startswith("/"):
            raise ValueError("output_dir must be relative.")
        if ".." in value.split("/"):
            raise ValueError("output_dir traversal is not allowed.")
        return value

    @field_validator("cube_target_crs")
    @classmethod
    def _normalize_cube_target_crs(cls, value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @field_validator("mask_types")
    @classmethod
    def _normalize_mask_types(cls, values: list[str]) -> list[str]:
        return _normalize_mask_type_values(values)

    @model_validator(mode="after")
    def _validate_dates(self) -> "SearchDownloadRequest":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater or equal to start_date.")
        if self.provider != ProviderName.copernicus and self.download_strategy != "default":
            raise ValueError("download_strategy is only supported for Copernicus jobs.")
        if self.download_only and self.mask_types:
            raise ValueError("download_only cannot be combined with mask_types.")
        if self.download_only and self.cube_mode != "none":
            raise ValueError("download_only requires cube_mode='none'.")
        if self.cube_mode == "after_mask" and not self.mask_types:
            raise ValueError("cube_mode='after_mask' requires at least one mask_type.")
        has_cube_options = (
            self.cube_layout != "grouped_time"
            or self.cube_target_crs is not None
            or self.cube_target_resolution_m != 10
            or self.cube_overlap_policy != "least_cloud"
        )
        if self.cube_mode == "none" and has_cube_options:
            raise ValueError("cube layout options require cube_mode != 'none'.")
        if self.cube_start_date is None:
            self.cube_start_date = self.start_date
        if self.cube_end_date is None:
            self.cube_end_date = self.end_date
        if self.cube_mode != "none" and self.cube_end_date < self.cube_start_date:
            raise ValueError("cube_end_date must be greater or equal to cube_start_date.")
        return self


class DownloadProductsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: Literal["download_products"]
    provider: ProviderName
    collection: str
    product_ids: list[str] = Field(min_length=1)
    output_dir: str | None = None
    download_only: bool = False
    download_strategy: Literal["default", "copernicus_account_pool"] = "default"

    @field_validator("collection")
    @classmethod
    def _validate_collection(cls, value: str) -> str:
        if not COLLECTION_RE.match(value):
            raise ValueError("Invalid collection format.")
        return value

    @field_validator("product_ids")
    @classmethod
    def _validate_product_ids(cls, values: list[str]) -> list[str]:
        filtered = [v.strip() for v in values if v and v.strip()]
        if not filtered:
            raise ValueError("product_ids cannot be empty.")
        return filtered

    @field_validator("output_dir")
    @classmethod
    def _validate_output_dir(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if value.startswith("/"):
            raise ValueError("output_dir must be relative.")
        if ".." in value.split("/"):
            raise ValueError("output_dir traversal is not allowed.")
        return value

    @model_validator(mode="after")
    def _validate_download_strategy(self) -> "DownloadProductsRequest":
        if self.provider != ProviderName.copernicus and self.download_strategy != "default":
            raise ValueError("download_strategy is only supported for Copernicus jobs.")
        return self


JobCreateRequest = Annotated[
    SearchDownloadRequest | DownloadProductsRequest,
    Field(discriminator="job_type"),
]


class BatchJobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    jobs: list[JobCreateRequest] = Field(min_length=1)


class JobCreatedResponse(BaseModel):
    job_id: str


class BatchJobCreatedResponse(BaseModel):
    job_ids: list[str]


class JobEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    job_id: str
    type: str
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class JobStatusResponse(BaseModel):
    job_id: str
    job_type: str | None = None
    job_kind: Literal["fetch", "mask"] | None = None
    service_name: str | None = None
    source_job_id: str | None = None
    state: JobState
    pipeline_state: PipelineState = PipelineState.queued
    pipeline_step: str | None = None
    pipeline_progress: float | None = Field(default=None, ge=0, le=100)
    pipeline_timeline: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    pipeline_metadata: PipelineMetadataRecord | dict[str, Any] = Field(default_factory=PipelineMetadataRecord)
    conversion_metadata: ConversionMetadataRecord | dict[str, Any] = Field(default_factory=ConversionMetadataRecord)
    raw_outputs: list[str] = Field(default_factory=list)
    zarr_outputs: list[str] = Field(default_factory=list)
    cube_outputs: list[str] = Field(default_factory=list)
    masked_zarr_outputs: list[str] = Field(default_factory=list)
    watermask_outputs: list[str] = Field(default_factory=list)
    cloudmask_outputs: list[str] = Field(default_factory=list)
    progress: float = Field(default=0, ge=0, le=100)
    bytes_downloaded: int = 0
    bytes_total: int = 0
    retry_count: int = 0
    last_retry_at: datetime | None = None
    product_type: str | None = None
    tile_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    errors: list[str] = Field(default_factory=list)
    can_resume: bool = False
    resume_action: str | None = None
    resume_label: str | None = None
    resume_reason: str | None = None
    provider: ProviderName
    collection: str

    @field_validator("pipeline_timeline", mode="before")
    @classmethod
    def _coerce_pipeline_timeline(cls, value: Any) -> PayloadRecord:
        if isinstance(value, PayloadRecord):
            return value
        return PayloadRecord.from_mapping(value)

    @field_validator("pipeline_metadata", mode="before")
    @classmethod
    def _coerce_pipeline_metadata(cls, value: Any) -> PipelineMetadataRecord:
        if isinstance(value, PipelineMetadataRecord):
            return value
        return PipelineMetadataRecord.from_mapping(value)

    @field_validator("conversion_metadata", mode="before")
    @classmethod
    def _coerce_conversion_metadata(cls, value: Any) -> ConversionMetadataRecord:
        if isinstance(value, ConversionMetadataRecord):
            return value
        return ConversionMetadataRecord.from_mapping(value)

    @field_serializer("pipeline_timeline", "pipeline_metadata", "conversion_metadata")
    def _serialize_payload_field(self, value: PayloadRecord | dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, PayloadRecord):
            return value.to_dict()
        return dict(value or {})


class JobResultResponse(BaseModel):
    job_id: str
    job_type: str | None = None
    job_kind: Literal["fetch", "mask"] | None = None
    service_name: str | None = None
    source_job_id: str | None = None
    paths: list[str] = Field(default_factory=list)
    raw_outputs: list[str] = Field(default_factory=list)
    zarr_outputs: list[str] = Field(default_factory=list)
    cube_outputs: list[str] = Field(default_factory=list)
    masked_zarr_outputs: list[str] = Field(default_factory=list)
    watermask_outputs: list[str] = Field(default_factory=list)
    cloudmask_outputs: list[str] = Field(default_factory=list)
    checksums: StringMapRecord | dict[str, str] = Field(default_factory=StringMapRecord)
    metadata: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    manifest_entry: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    pipeline_metadata: PipelineMetadataRecord | dict[str, Any] = Field(default_factory=PipelineMetadataRecord)
    conversion_metadata: ConversionMetadataRecord | dict[str, Any] = Field(default_factory=ConversionMetadataRecord)

    @field_validator("checksums", mode="before")
    @classmethod
    def _coerce_checksums(cls, value: Any) -> StringMapRecord:
        if isinstance(value, StringMapRecord):
            return value
        return StringMapRecord.from_mapping(value)

    @field_validator("metadata", "manifest_entry", mode="before")
    @classmethod
    def _coerce_payload_fields(cls, value: Any) -> PayloadRecord:
        if isinstance(value, PayloadRecord):
            return value
        return PayloadRecord.from_mapping(value)

    @field_validator("pipeline_metadata", mode="before")
    @classmethod
    def _coerce_result_pipeline_metadata(cls, value: Any) -> PipelineMetadataRecord:
        if isinstance(value, PipelineMetadataRecord):
            return value
        return PipelineMetadataRecord.from_mapping(value)

    @field_validator("conversion_metadata", mode="before")
    @classmethod
    def _coerce_result_conversion_metadata(cls, value: Any) -> ConversionMetadataRecord:
        if isinstance(value, ConversionMetadataRecord):
            return value
        return ConversionMetadataRecord.from_mapping(value)

    @field_serializer("checksums")
    def _serialize_checksums(self, value: StringMapRecord | dict[str, str]) -> dict[str, str]:
        if isinstance(value, StringMapRecord):
            return value.to_dict()
        return {str(key): str(item) for key, item in dict(value or {}).items()}

    @field_serializer("metadata", "manifest_entry", "pipeline_metadata", "conversion_metadata")
    def _serialize_result_payload_field(
        self,
        value: PayloadRecord | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(value, PayloadRecord):
            return value.to_dict()
        return dict(value or {})


class JobConvertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_uri: str | None = None
    output_uri: str | None = None
    scene_id: str | None = None
    product_type: str | None = None


class JobMaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    zarr_uri: str | None = None
    scene_id: str | None = None
    product_type: str | None = None
    mask_types: list[Literal["water", "cloud"]] = Field(default_factory=lambda: ["water"], min_length=1)
    backend: Literal["auto", "omnicloudmask"] = "auto"
    threshold: float = Field(default=0.62, ge=0.0, le=1.0)
    overwrite: bool = True
    inference_device: str | None = None
    include_shadows: bool = True
    water_backend: Literal["auto", "heuristic", "omniwatermask", "fallback", "ndwi"] = "auto"
    water_overwrite: bool | None = None
    water_inference_device: str | None = None
    fail_on_error: bool = False

    @field_validator("mask_types")
    @classmethod
    def _normalize_mask_types(cls, values: list[str]) -> list[str]:
        normalized = _normalize_mask_type_values(values)
        if not normalized:
            raise ValueError("mask_types cannot be empty.")
        return normalized

    @field_validator("backend", mode="before")
    @classmethod
    def _normalize_cloud_backend(cls, value: str | None) -> str:
        candidate = str(value or "auto").strip().lower()
        if candidate in {"heuristic", "default", "fallback"}:
            return "omnicloudmask"
        return candidate or "auto"


class JobWaterMaskRequest(JobMaskRequest):
    mask_types: list[Literal["water"]] = Field(default_factory=lambda: ["water"])


class JobCloudMaskRequest(JobMaskRequest):
    mask_types: list[Literal["cloud"]] = Field(default_factory=lambda: ["cloud"])
    backend: Literal["auto", "omnicloudmask"] = "auto"
    include_shadows: bool = True


class JobMaskResponse(BaseModel):
    job_id: str
    source_job_id: str
    source_zarr_uri: str
    masked_zarr_uri: str | None = None
    mask_types: list[str] = Field(default_factory=list)
    water_mask: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    cloud_mask: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    masked_zarr_outputs: list[str] = Field(default_factory=list)
    watermask_outputs: list[str] = Field(default_factory=list)
    cloudmask_outputs: list[str] = Field(default_factory=list)
    job: JobStatusResponse

    @field_validator("water_mask", "cloud_mask", mode="before")
    @classmethod
    def _coerce_mask_payload(cls, value: Any) -> PayloadRecord:
        if isinstance(value, PayloadRecord):
            return value
        return PayloadRecord.from_mapping(value)

    @field_serializer("water_mask", "cloud_mask")
    def _serialize_mask_payload(self, value: PayloadRecord | dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, PayloadRecord):
            return value.to_dict()
        return dict(value or {})


class JobWaterMaskResponse(JobMaskResponse):
    pass


class JobCloudMaskResponse(JobMaskResponse):
    pass


class JobResumeResponse(BaseModel):
    source_job_id: str
    resumed_job_id: str
    resume_action: str
    resume_label: str
    spawned_new_job: bool = False
    message: str
    job: JobStatusResponse


class JobListResponse(BaseModel):
    items: list[JobStatusResponse]
    total: int
    page: int
    page_size: int


class ArtifactType(str, Enum):
    zarr = "zarr"
    zarr_cube = "zarr_cube"
    zarr_masked = "zarr_masked"
    watermask = "watermask"
    cloudmask = "cloudmask"


class ArtifactUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_type: ArtifactType
    artifact_uri: str = Field(min_length=1)
    provider: ProviderName | None = None
    collection: str | None = None
    scene_id: str | None = None
    source_uri: str | None = None
    created_by_job_id: str | None = None
    source_job_id: str | None = None
    data_family: str | None = None
    band_names: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    shape: list[int] = Field(default_factory=list)
    size_bytes: int | None = Field(default=None, ge=0)
    metadata: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)

    @field_validator("collection")
    @classmethod
    def _validate_collection_optional(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not COLLECTION_RE.match(value):
            raise ValueError("Invalid collection format.")
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def _coerce_artifact_upsert_metadata(cls, value: Any) -> PayloadRecord:
        if isinstance(value, PayloadRecord):
            return value
        return PayloadRecord.from_mapping(value)

    @field_serializer("metadata")
    def _serialize_artifact_upsert_metadata(self, value: PayloadRecord | dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, PayloadRecord):
            return value.to_dict()
        return dict(value or {})


class ArtifactRecord(BaseModel):
    artifact_id: str
    artifact_type: ArtifactType
    artifact_uri: str
    provider: ProviderName | None = None
    collection: str | None = None
    scene_id: str | None = None
    source_uri: str | None = None
    created_by_job_id: str | None = None
    source_job_id: str | None = None
    data_family: str | None = None
    band_names: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    shape: list[int] = Field(default_factory=list)
    size_bytes: int | None = None
    metadata: PayloadRecord | dict[str, Any] = Field(default_factory=PayloadRecord)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("metadata", mode="before")
    @classmethod
    def _coerce_artifact_metadata(cls, value: Any) -> PayloadRecord:
        if isinstance(value, PayloadRecord):
            return value
        return PayloadRecord.from_mapping(value)

    @field_serializer("metadata")
    def _serialize_artifact_metadata(self, value: PayloadRecord | dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, PayloadRecord):
            return value.to_dict()
        return dict(value or {})


class ArtifactListResponse(BaseModel):
    items: list[ArtifactRecord]
    total: int
    page: int
    page_size: int
