from __future__ import annotations

import anyio
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
import requests

from nimbuschain_fetch.application.api_services import ConversionService
from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError
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
from nimbuschain_fetch.settings import Settings
from nimbuschain_shared.clients.mask import MaskServiceClient
from nimbuschain_shared.clients.zarr import ZarrServiceClient
from nimbuschain_fetch_service.dependencies import get_conversion_service, get_runtime_settings

router = APIRouter(prefix="/v1", tags=["converter"])


def _require_mask_service_url(settings: Settings) -> str:
    service_url = str(settings.nimbus_mask_service_url or "").strip()
    if not service_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Mask service URL is not configured.",
        )
    return service_url


def _require_zarr_service_url(settings: Settings) -> str:
    service_url = str(settings.nimbus_zarr_service_url or "").strip()
    if not service_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Zarr service URL is not configured.",
        )
    return service_url


@router.get("/converter/health")
def get_converter_health(settings: Settings = Depends(get_runtime_settings)) -> JSONResponse:
    client = ZarrServiceClient(service_url=_require_zarr_service_url(settings))
    try:
        status_code, payload = client.health()
        return JSONResponse(status_code=status_code, content=payload)
    except (requests.RequestException, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Zarr service health request failed: {exc}",
        ) from exc
    finally:
        client.close()


@router.get("/converter/readiness")
def get_converter_readiness(settings: Settings = Depends(get_runtime_settings)) -> JSONResponse:
    client = ZarrServiceClient(service_url=_require_zarr_service_url(settings))
    try:
        status_code, payload = client.readiness()
        return JSONResponse(status_code=status_code, content=payload)
    except (requests.RequestException, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Zarr service readiness request failed: {exc}",
        ) from exc
    finally:
        client.close()


@router.get("/converter/schema")
def get_converter_schema(settings: Settings = Depends(get_runtime_settings)) -> JSONResponse:
    client = ZarrServiceClient(service_url=_require_zarr_service_url(settings))
    try:
        status_code, payload = client.schema()
        return JSONResponse(status_code=status_code, content=payload)
    except (requests.RequestException, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Zarr service schema request failed: {exc}",
        ) from exc
    finally:
        client.close()


@router.get("/mask/health")
def get_mask_health(settings: Settings = Depends(get_runtime_settings)) -> dict[str, object]:
    client = MaskServiceClient(service_url=_require_mask_service_url(settings))
    try:
        return client.health()
    finally:
        client.close()


@router.get("/mask/schema")
def get_mask_schema(settings: Settings = Depends(get_runtime_settings)) -> dict[str, object]:
    client = MaskServiceClient(service_url=_require_mask_service_url(settings))
    try:
        return client.schema()
    finally:
        client.close()


@router.post("/jobs/{job_id}/convert", response_model=JobStatusResponse)
async def convert_job_output(
    job_id: str,
    request: JobConvertRequest,
    fetcher: ConversionService = Depends(get_conversion_service),
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
    fetcher: ConversionService = Depends(get_conversion_service),
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
    fetcher: ConversionService = Depends(get_conversion_service),
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
    fetcher: ConversionService = Depends(get_conversion_service),
) -> JobCloudMaskResponse:
    try:
        return await anyio.to_thread.run_sync(fetcher.apply_cloud_mask_existing_job, job_id, request)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
