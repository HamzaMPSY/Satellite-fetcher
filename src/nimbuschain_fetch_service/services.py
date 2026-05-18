from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from nimbuschain_fetch.application.api_services import (
    ArtifactCatalogService,
    ConversionService,
    EventStreamService,
    JobControlService,
    JobQueryService,
    JobSubmissionService,
)
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


@dataclass(slots=True)
class FetcherJobSubmissionService:
    fetcher: Any

    async def submit_job(self, request: JobCreateRequest) -> str:
        return await self.fetcher.submit_job(request)

    async def submit_batch(self, request: BatchJobCreateRequest) -> list[str]:
        return await self.fetcher.submit_batch(request)


@dataclass(slots=True)
class FetcherJobQueryService:
    fetcher: Any

    def get_job(self, job_id: str) -> JobStatusResponse:
        return self.fetcher.get_job(job_id)

    def get_result(self, job_id: str) -> JobResultResponse:
        return self.fetcher.get_result(job_id)

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
        return self.fetcher.list_jobs(
            state=state,
            states=states,
            provider=provider,
            collection=collection,
            product_type=product_type,
            job_id_query=job_id_query,
            date_from=date_from,
            date_to=date_to,
            updated_from=updated_from,
            updated_to=updated_to,
            sort_by=sort_by,
            sort_desc=sort_desc,
            page=page,
            page_size=page_size,
        )


@dataclass(slots=True)
class FetcherJobControlService:
    fetcher: Any

    async def cancel_job(self, job_id: str) -> bool:
        return await self.fetcher.cancel_job(job_id)

    def resume_job(self, job_id: str) -> JobResumeResponse:
        return self.fetcher.resume_job(job_id)

    async def reset_runtime_state(self) -> dict[str, object]:
        return await self.fetcher.reset_runtime_state()


@dataclass(slots=True)
class FetcherEventStreamService:
    fetcher: Any

    def stream_events(self, *, job_id: str | None, since: int | None):
        return self.fetcher.stream_events(job_id=job_id, since=since)


@dataclass(slots=True)
class FetcherArtifactCatalogService:
    fetcher: Any

    def upsert_artifact(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        return self.fetcher.upsert_artifact(request)

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
        return self.fetcher.list_artifacts(
            artifact_type=artifact_type,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
            job_id=job_id,
            uri_query=uri_query,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
        )


@dataclass(slots=True)
class FetcherConversionService:
    fetcher: Any

    def convert_existing_job(
        self,
        job_id: str,
        request: JobConvertRequest,
        continue_pipeline: bool = False,
    ) -> JobStatusResponse:
        return self.fetcher.convert_existing_job(
            job_id,
            request,
            continue_pipeline=continue_pipeline,
        )

    def apply_mask_existing_job(
        self,
        job_id: str,
        request: JobMaskRequest,
    ) -> JobMaskResponse:
        return self.fetcher.apply_mask_existing_job(job_id, request)

    def apply_watermask_existing_job(
        self,
        job_id: str,
        request: JobWaterMaskRequest,
    ) -> JobWaterMaskResponse:
        return self.fetcher.apply_watermask_existing_job(job_id, request)

    def apply_cloud_mask_existing_job(
        self,
        job_id: str,
        request: JobCloudMaskRequest,
    ) -> JobCloudMaskResponse:
        return self.fetcher.apply_cloud_mask_existing_job(job_id, request)


@dataclass(slots=True)
class FetchServiceContainer:
    job_submission: JobSubmissionService
    job_query: JobQueryService
    job_control: JobControlService
    event_stream: EventStreamService
    artifact_catalog: ArtifactCatalogService
    conversion: ConversionService


def create_fetch_service_container(fetcher: Any) -> FetchServiceContainer:
    return FetchServiceContainer(
        job_submission=FetcherJobSubmissionService(fetcher),
        job_query=FetcherJobQueryService(fetcher),
        job_control=FetcherJobControlService(fetcher),
        event_stream=FetcherEventStreamService(fetcher),
        artifact_catalog=FetcherArtifactCatalogService(fetcher),
        conversion=FetcherConversionService(fetcher),
    )


__all__ = [
    "FetchServiceContainer",
    "FetcherArtifactCatalogService",
    "FetcherConversionService",
    "FetcherEventStreamService",
    "FetcherJobControlService",
    "FetcherJobQueryService",
    "FetcherJobSubmissionService",
    "create_fetch_service_container",
]
