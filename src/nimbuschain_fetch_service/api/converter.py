from __future__ import annotations

import anyio
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError, NimbusFetcher
from nimbuschain_fetch.models import JobConvertRequest, JobStatusResponse
from nimbuschain_fetch_service.dependencies import get_fetcher
from nimbuschain_zarr_service.main import (
    health as converter_health,
    readiness as converter_readiness,
    schema as converter_schema,
)

router = APIRouter(prefix="/v1", tags=["converter"])


@router.get("/converter/health")
def get_converter_health() -> JSONResponse:
    return converter_health()


@router.get("/converter/readiness")
def get_converter_readiness() -> JSONResponse:
    return converter_readiness()


@router.get("/converter/schema")
def get_converter_schema() -> dict[str, object]:
    return converter_schema()


@router.post("/jobs/{job_id}/convert", response_model=JobStatusResponse)
async def convert_job_output(
    job_id: str,
    request: JobConvertRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobStatusResponse:
    try:
        return await anyio.to_thread.run_sync(fetcher.convert_existing_job, job_id, request)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
