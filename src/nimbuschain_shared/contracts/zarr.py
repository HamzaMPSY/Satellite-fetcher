from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ConvertRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    pipeline_id: str = Field(..., min_length=1)
    trace_id: str = Field(..., min_length=1)
    provider: Literal["copernicus", "usgs"]
    collection: str = Field(..., min_length=1)
    product_type: str | None = None
    scene_id: str = Field(..., min_length=1)
    raw_uri: str = Field(..., min_length=1)
    raw_format: str = Field(..., min_length=1)
    output_uri: str = Field(..., min_length=1)


class ConvertResponse(BaseModel):
    job_id: str
    pipeline_id: str
    status: Literal["accepted", "normalized", "written"]
    stage: Literal["zarr_converting"]
    service: Literal["zarr-converter-service"]
    message: str
    accepted_at: str
    zarr_uri: str | None = None
    data_family: str | None = None
    band_names: list[str] | None = None
    dimensions: list[str] | None = None
    ancillary_layer_names: list[str] | None = None
    ancillary_dimensions: list[str] | None = None
    normalization_summary: dict[str, object] | None = None


class BuildGroupedCubesRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    pipeline_id: str = Field(..., min_length=1)
    trace_id: str = Field(..., min_length=1)
    source_zarr_uris: list[str] = Field(..., min_length=1)
    output_dir: str = Field(..., min_length=1)
    include_ancillary: bool = True
    include_masks: bool | None = None
    start_date: str | None = None
    end_date: str | None = None
    stage_label: str | None = None


class BuildGroupedCubesResponse(BaseModel):
    job_id: str
    pipeline_id: str
    status: Literal["written", "skipped"]
    service: Literal["zarr-converter-service"]
    cube_summary: dict[str, Any]


class BuildCubeRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    pipeline_id: str = Field(..., min_length=1)
    trace_id: str = Field(..., min_length=1)
    source_zarr_uris: list[str] = Field(..., min_length=1)
    output_uri: str = Field(..., min_length=1)
    include_ancillary: bool = True
    include_masks: bool = False


class BuildCubeResponse(BaseModel):
    job_id: str
    pipeline_id: str
    status: Literal["written"]
    service: Literal["zarr-converter-service"]
    cube_summary: dict[str, Any]


class InspectDatasetRequest(BaseModel):
    zarr_uri: str = Field(..., min_length=1)


class InspectDatasetResponse(BaseModel):
    service: Literal["zarr-converter-service"]
    zarr_uri: str
    dataset_summary: dict[str, Any]
