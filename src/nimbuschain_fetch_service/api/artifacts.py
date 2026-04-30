from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends

from nimbuschain_fetch.application.api_services import ArtifactCatalogService
from nimbuschain_fetch.models import (
    ArtifactListResponse,
    ArtifactRecord,
    ArtifactUpsertRequest,
)
from nimbuschain_fetch_service.artifact_service import LocalArtifactOverlayService
from nimbuschain_fetch_service.dependencies import (
    get_artifact_catalog_service,
    get_local_artifact_overlay_service,
)

router = APIRouter(prefix="/v1", tags=["artifacts"])


@router.post("/artifacts", response_model=ArtifactRecord)
def upsert_artifact(
    request: ArtifactUpsertRequest,
    fetcher: ArtifactCatalogService = Depends(get_artifact_catalog_service),
) -> ArtifactRecord:
    payload = request.model_copy(update={"artifact_uri": request.artifact_uri.strip()})
    payload = payload.model_copy(
        update={
            "metadata": {
                **payload.metadata,
                "registered_via": "api",
            }
        }
    )
    return fetcher.upsert_artifact(
        payload.model_copy(
            update={"metadata": payload.metadata, "artifact_uri": payload.artifact_uri}
        )
    )


@router.get("/artifacts", response_model=ArtifactListResponse)
def list_artifacts(
    artifact_type: str | None = None,
    provider: str | None = None,
    collection: str | None = None,
    scene_id: str | None = None,
    job_id: str | None = None,
    uri_query: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    include_local: bool = False,
    page: int = 1,
    page_size: int = 20,
    fetcher: ArtifactCatalogService = Depends(get_artifact_catalog_service),
    local_overlay: LocalArtifactOverlayService = Depends(get_local_artifact_overlay_service),
) -> ArtifactListResponse:
    if include_local:
        registered = fetcher.list_artifacts(
            artifact_type=artifact_type,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
            job_id=job_id,
            uri_query=uri_query,
            date_from=date_from,
            date_to=date_to,
            page=1,
            page_size=1000,
        )
        return local_overlay.merge_with_local_artifacts(
            registered,
            artifact_type=artifact_type,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
            job_id=job_id,
            uri_query=uri_query,
            page=page,
            page_size=page_size,
        )

    return fetcher.list_artifacts(
        artifact_type=artifact_type,
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        job_id=job_id,
        uri_query=uri_query,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
