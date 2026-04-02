from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status

from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError, NimbusFetcher
from nimbuschain_fetch.models import ProviderName
from nimbuschain_fetch.provider_status import get_provider_status
from nimbuschain_fetch.models import (
    BatchJobCreateRequest,
    BatchJobCreatedResponse,
    JobCreateRequest,
    JobCreatedResponse,
    JobListResponse,
    JobResultResponse,
    JobStatusResponse,
)
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.dependencies import get_fetcher, get_runtime_settings
from nimbuschain_fetch_service.observability import (
    record_job_cancellation,
    record_job_submission,
)

router = APIRouter(prefix="/v1", tags=["jobs"])


@router.post("/jobs", response_model=JobCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    request: JobCreateRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
    settings: Settings = Depends(get_runtime_settings),
) -> JobCreatedResponse:
    _ensure_provider_submission_ready(request, settings)
    job_id = await fetcher.submit_job(request)
    record_job_submission(str(request.job_type), str(request.provider.value))
    return JobCreatedResponse(job_id=job_id)


@router.post("/jobs/batch", response_model=BatchJobCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_batch_jobs(
    request: BatchJobCreateRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
    settings: Settings = Depends(get_runtime_settings),
) -> BatchJobCreatedResponse:
    for item in request.jobs:
        _ensure_provider_submission_ready(item, settings)
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
    state_in: str | None = None,
    provider: str | None = None,
    collection: str | None = None,
    product_type: str | None = None,
    job_id_query: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    updated_from: datetime | None = None,
    updated_to: datetime | None = None,
    sort_by: str = "updated_at",
    sort_desc: bool = True,
    page: int = 1,
    page_size: int = 20,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobListResponse:
    states = tuple(
        item.strip()
        for item in (state_in or "").split(",")
        if item and item.strip()
    )
    return fetcher.list_jobs(
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


def _ensure_provider_submission_ready(request: JobCreateRequest, settings: Settings) -> None:
    if request.provider != ProviderName.usgs:
        return
    provider_status = get_provider_status(settings, "usgs")
    if bool(provider_status.get("configured")) and bool(provider_status.get("auth_valid")):
        return

    error_kind = str(provider_status.get("error_kind") or "").strip().lower()
    message = str(provider_status.get("message") or "USGS job submission is blocked.").strip()
    detail = str(provider_status.get("detail") or message).strip()

    if error_kind in {"credentials_missing", "credentials_invalid"}:
        action = "Update NIMBUS_USGS_TOKEN in the runtime environment and restart the API/worker services."
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{message} {action} Detail: {detail}",
        )

    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=f"{message} Detail: {detail}",
    )
