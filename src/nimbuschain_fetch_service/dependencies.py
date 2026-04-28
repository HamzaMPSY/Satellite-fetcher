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


def get_fetcher(request: Request) -> NimbusFetcher:
    fetcher = getattr(request.app.state, "fetcher", None)
    if fetcher is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Fetcher is not ready.",
        )
    return fetcher


def get_runtime_settings(request: Request) -> Settings:
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Settings are not ready.",
        )
    return settings


def get_job_submission_service(request: Request) -> JobSubmissionService:
    return get_fetcher(request)


def get_job_query_service(request: Request) -> JobQueryService:
    return get_fetcher(request)


def get_job_control_service(request: Request) -> JobControlService:
    return get_fetcher(request)


def get_event_stream_service(request: Request) -> EventStreamService:
    return get_fetcher(request)


def get_artifact_catalog_service(request: Request) -> ArtifactCatalogService:
    return get_fetcher(request)


def get_conversion_service(request: Request) -> ConversionService:
    return get_fetcher(request)
