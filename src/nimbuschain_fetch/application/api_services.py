from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Protocol

from nimbuschain_fetch.models import (
    ArtifactListResponse,
    ArtifactRecord,
    ArtifactUpsertRequest,
    BatchJobCreateRequest,
    JobCloudMaskRequest,
    JobCloudMaskResponse,
    JobConvertRequest,
    JobCreateRequest,
    JobListResponse,
    JobMaskRequest,
    JobMaskResponse,
    JobResumeResponse,
    JobResultResponse,
    JobStatusResponse,
    JobWaterMaskRequest,
    JobWaterMaskResponse,
)


class JobSubmissionService(Protocol):
    async def submit_job(self, request: JobCreateRequest) -> str:
        ...

    async def submit_batch(self, request: BatchJobCreateRequest) -> list[str]:
        ...


class JobQueryService(Protocol):
    def get_job(self, job_id: str) -> JobStatusResponse:
        ...

    def get_result(self, job_id: str) -> JobResultResponse:
        ...

    def list_jobs(
        self,
        *,
        state: str | None,
        states: tuple[str, ...] = (),
        provider: str | None,
        collection: str | None = None,
        product_type: str | None = None,
        job_id_query: str | None = None,
        date_from: datetime | None,
        date_to: datetime | None,
        updated_from: datetime | None = None,
        updated_to: datetime | None = None,
        sort_by: str = "updated_at",
        sort_desc: bool = True,
        page: int,
        page_size: int,
    ) -> JobListResponse:
        ...


class JobControlService(Protocol):
    async def cancel_job(self, job_id: str) -> bool:
        ...

    def resume_job(self, job_id: str) -> JobResumeResponse:
        ...

    async def reset_runtime_state(self) -> dict[str, object]:
        ...


class EventStreamService(Protocol):
    def stream_events(self, *, job_id: str | None, since: int | None) -> AsyncIterator[object]:
        ...


class ArtifactCatalogService(Protocol):
    def upsert_artifact(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        ...

    def list_artifacts(
        self,
        *,
        artifact_type: str | None,
        provider: str | None,
        collection: str | None,
        scene_id: str | None,
        job_id: str | None,
        uri_query: str | None,
        date_from: datetime | None,
        date_to: datetime | None,
        page: int,
        page_size: int,
    ) -> ArtifactListResponse:
        ...


class ConversionService(Protocol):
    def convert_existing_job(
        self,
        job_id: str,
        request: JobConvertRequest,
        continue_pipeline: bool = False,
    ) -> JobStatusResponse:
        ...

    def apply_mask_existing_job(
        self,
        job_id: str,
        request: JobMaskRequest,
    ) -> JobMaskResponse:
        ...

    def apply_watermask_existing_job(
        self,
        job_id: str,
        request: JobWaterMaskRequest,
    ) -> JobWaterMaskResponse:
        ...

    def apply_cloud_mask_existing_job(
        self,
        job_id: str,
        request: JobCloudMaskRequest,
    ) -> JobCloudMaskResponse:
        ...
