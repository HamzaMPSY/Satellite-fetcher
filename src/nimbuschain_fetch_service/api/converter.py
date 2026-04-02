from __future__ import annotations

import anyio
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError, NimbusFetcher
from nimbuschain_fetch.models import (
    JobCloudMaskRequest,
    JobCloudMaskResponse,
    JobConvertRequest,
    JobMaskRequest,
    JobMaskResponse,
    JobStatusResponse,
    JobWaterMaskRequest,
    JobWaterMaskResponse,
)
from nimbuschain_fetch.settings import get_settings
from nimbuschain_mask_service.client import MaskServiceClient
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


@router.get("/mask/health")
def get_mask_health() -> dict[str, object]:
    client = MaskServiceClient(service_url=get_settings().nimbus_mask_service_url)
    try:
        return client.health()
    finally:
        client.close()


@router.get("/mask/schema")
def get_mask_schema() -> dict[str, object]:
    client = MaskServiceClient(service_url=get_settings().nimbus_mask_service_url)
    try:
        return client.schema()
    finally:
        client.close()


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


@router.post("/jobs/{job_id}/mask", response_model=JobMaskResponse)
async def mask_job_output(
    job_id: str,
    request: JobMaskRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobMaskResponse:
    try:
        return await anyio.to_thread.run_sync(fetcher.apply_mask_existing_job, job_id, request)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.post("/jobs/{job_id}/water-mask", response_model=JobWaterMaskResponse)
@router.post("/jobs/{job_id}/watermask", response_model=JobWaterMaskResponse, include_in_schema=False)
async def watermask_job_output(
    job_id: str,
    request: JobWaterMaskRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobWaterMaskResponse:
    try:
        return await anyio.to_thread.run_sync(fetcher.apply_watermask_existing_job, job_id, request)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.post("/jobs/{job_id}/mask-cloud", response_model=JobCloudMaskResponse)
async def cloudmask_job_output(
    job_id: str,
    request: JobCloudMaskRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JobCloudMaskResponse:
    try:
        return await anyio.to_thread.run_sync(fetcher.apply_cloud_mask_existing_job, job_id, request)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
