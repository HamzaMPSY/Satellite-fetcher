from __future__ import annotations

from fastapi import HTTPException, Request, status

from nimbuschain_fetch.application.api_services import (
    ArtifactCatalogService,
    ConversionService,
    EventStreamService,
    JobControlService,
    JobQueryService,
    JobSubmissionService,
)
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.artifact_service import LocalArtifactOverlayService
from nimbuschain_fetch_service.services import (
    FetchServiceContainer,
    create_fetch_service_container,
)


def get_fetcher(request: Request) -> NimbusFetcher:
    fetcher = getattr(request.app.state, "fetcher", None)
    if fetcher is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Fetcher is not ready.",
        )
    return fetcher


def get_service_container(request: Request) -> FetchServiceContainer:
    services = getattr(request.app.state, "services", None)
    if services is not None:
        return services
    fetcher = get_fetcher(request)
    return create_fetch_service_container(fetcher)


def get_runtime_settings(request: Request) -> Settings:
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Settings are not ready.",
        )
    return settings


def get_job_submission_service(request: Request) -> JobSubmissionService:
    return get_service_container(request).job_submission


def get_job_query_service(request: Request) -> JobQueryService:
    return get_service_container(request).job_query


def get_job_control_service(request: Request) -> JobControlService:
    return get_service_container(request).job_control


def get_event_stream_service(request: Request) -> EventStreamService:
    return get_service_container(request).event_stream


def get_artifact_catalog_service(request: Request) -> ArtifactCatalogService:
    return get_service_container(request).artifact_catalog


def get_conversion_service(request: Request) -> ConversionService:
    return get_service_container(request).conversion


def get_local_artifact_overlay_service(request: Request) -> LocalArtifactOverlayService:
    settings = get_runtime_settings(request)
    return LocalArtifactOverlayService(data_dir=settings.nimbus_data_dir)
