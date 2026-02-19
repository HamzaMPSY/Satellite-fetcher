from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.dependencies import get_runtime_settings

router = APIRouter(prefix="/v1", tags=["health"])


@router.get("/health")
def healthcheck(settings: Settings = Depends(get_runtime_settings)) -> dict[str, str]:
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "runtime_role": settings.runtime_role,
        "db_backend": settings.nimbus_db_backend.strip().lower(),
        "metrics_enabled": str(bool(settings.nimbus_enable_metrics)).lower(),
    }
