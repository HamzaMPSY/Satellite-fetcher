from __future__ import annotations

from fastapi import HTTPException, Request, status

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
