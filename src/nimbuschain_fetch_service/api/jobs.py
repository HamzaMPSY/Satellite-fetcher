from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status

from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError, NimbusFetcher
from nimbuschain_fetch.models import (
    BatchJobCreateRequest,
    BatchJobCreatedResponse,
    JobCreateRequest,
    JobCreatedResponse,
    JobListResponse,
    JobResultResponse,
    JobStatusResponse,
)
from nimbuschain_fetch_service.dependencies import get_fetcher
from nimbuschain_fetch_service.observability import (
    record_job_cancellation,
    record_job_submission,
)

router = APIRouter(prefix="/v1", tags=["jobs"])


@router.post("/jobs", response_model=JobCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    request: JobCreateRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobCreatedResponse:
    job_id = await fetcher.submit_job(request)
    record_job_submission(str(request.job_type), str(request.provider.value))
    return JobCreatedResponse(job_id=job_id)


@router.post("/jobs/batch", response_model=BatchJobCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_batch_jobs(
    request: BatchJobCreateRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> BatchJobCreatedResponse:
    job_ids = await fetcher.submit_batch(request)
    for item in request.jobs:
        record_job_submission(str(item.job_type), str(item.provider.value))
    return BatchJobCreatedResponse(job_ids=job_ids)


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job(
    job_id: str,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobStatusResponse:
    try:
        return fetcher.get_job(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.") from exc


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: str,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> dict[str, object]:
    try:
        status = fetcher.get_job(job_id)
        cancel_requested = await fetcher.cancel_job(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.") from exc
    if cancel_requested:
        record_job_cancellation(status.provider.value)
    return {"job_id": job_id, "cancel_requested": cancel_requested}


@router.get("/jobs/{job_id}/result", response_model=JobResultResponse)
def get_job_result(
    job_id: str,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobResultResponse:
    try:
        return fetcher.get_result(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Result for '{job_id}' not found.") from exc


@router.get("/jobs", response_model=JobListResponse)
def list_jobs(
    state: str | None = None,
    provider: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    page: int = 1,
    page_size: int = 20,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobListResponse:
    return fetcher.list_jobs(
        state=state,
        provider=provider,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
