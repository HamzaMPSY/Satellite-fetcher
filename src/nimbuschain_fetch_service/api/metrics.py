from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.dependencies import get_fetcher, get_runtime_settings
from nimbuschain_fetch_service.observability import render_metrics

router = APIRouter(prefix="/v1", tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
def metrics(
    fetcher: NimbusFetcher = Depends(get_fetcher),
    settings: Settings = Depends(get_runtime_settings),
) -> Response:
    if not settings.nimbus_enable_metrics:
        raise HTTPException(status_code=404, detail="Metrics disabled.")
    body = render_metrics(fetcher)
    return Response(content=body, media_type=CONTENT_TYPE_LATEST)

