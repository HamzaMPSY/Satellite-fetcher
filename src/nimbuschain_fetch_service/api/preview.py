from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from nimbuschain_fetch.preview import preview_products_from_env

router = APIRouter(prefix="/v1", tags=["preview"])


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    collection: str
    product_type: str
    start_date: str
    end_date: str
    aoi_wkt: str
    max_items: int = Field(default=50, ge=1, le=200)
    tile_ids: list[str] | None = None


class PreviewResponse(BaseModel):
    items: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    error: str = ""
    error_kind: str = ""
    error_detail: str = ""


@router.post("/preview", response_model=PreviewResponse)
def preview_products(request: PreviewRequest) -> PreviewResponse:
    payload = preview_products_from_env(
        provider=request.provider,
        collection=request.collection,
        product_type=request.product_type,
        start_date=request.start_date,
        end_date=request.end_date,
        aoi_wkt=request.aoi_wkt,
        max_items=request.max_items,
        tile_ids=request.tile_ids,
    )
    return PreviewResponse(**payload)
