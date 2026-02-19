from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nimbuschain_fetch.geometry.aoi import validate_aoi_payload


COLLECTION_RE = re.compile(r"^[A-Za-z0-9._\-/]{1,120}$")


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

    @model_validator(mode="after")
    def _validate_dates(self) -> "SearchDownloadRequest":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater or equal to start_date.")
        return self


class DownloadProductsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: Literal["download_products"]
    provider: ProviderName
    collection: str
    product_ids: list[str] = Field(min_length=1)
    output_dir: str | None = None

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
    state: JobState
    progress: float = Field(default=0, ge=0, le=100)
    bytes_downloaded: int = 0
    bytes_total: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    errors: list[str] = Field(default_factory=list)
    provider: ProviderName
    collection: str


class JobResultResponse(BaseModel):
    job_id: str
    paths: list[str] = Field(default_factory=list)
    checksums: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    manifest_entry: dict[str, Any] = Field(default_factory=dict)


class JobListResponse(BaseModel):
    items: list[JobStatusResponse]
    total: int
    page: int
    page_size: int
