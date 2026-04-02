from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from nimbuschain_fetch.provider_status import get_provider_statuses
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.dependencies import get_runtime_settings

router = APIRouter(prefix="/v1", tags=["providers"])


class ProviderStatusItem(BaseModel):
    provider: str
    configured: bool
    auth_valid: bool | None = None
    error_kind: str = ""
    message: str = ""
    detail: str = ""
    last_checked_at: str
    credential_source: str = "runtime_env"
    username_present: bool = False
    token_present: bool = False
    password_present: bool = False


class ProviderStatusResponse(BaseModel):
    providers: list[ProviderStatusItem] = Field(default_factory=list)


@router.get("/providers/status", response_model=ProviderStatusResponse)
def list_provider_statuses(
    provider: Literal["copernicus", "usgs"] | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(get_runtime_settings),
) -> ProviderStatusResponse:
    return ProviderStatusResponse(
        providers=[
            ProviderStatusItem.model_validate(item)
            for item in get_provider_statuses(
                settings,
                provider=provider,
                force_refresh=force_refresh,
            )
        ]
    )
