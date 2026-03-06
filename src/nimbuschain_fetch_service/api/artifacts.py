from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import (
    ArtifactListResponse,
    ArtifactRecord,
    ArtifactUpsertRequest,
)
from nimbuschain_fetch_service.dependencies import get_fetcher, get_runtime_settings
from nimbuschain_fetch.settings import Settings

router = APIRouter(prefix="/v1", tags=["artifacts"])


def _artifact_id_for_uri(uri: str) -> str:
    return hashlib.md5(uri.encode("utf-8"), usedforsecurity=False).hexdigest()


def _safe_parse_iso(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _discover_local_zarr_artifacts(data_dir: Path) -> list[dict[str, Any]]:
    zarr_root = data_dir / "zarr"
    if not zarr_root.exists():
        return []

    items: list[dict[str, Any]] = []
    for store_path in sorted(zarr_root.rglob("*.zarr")):
        if not store_path.is_dir():
            continue
        try:
            stat = store_path.stat()
        except OSError:
            continue

        root_meta: dict[str, Any] = {}
        zarr_json_path = store_path / "zarr.json"
        if zarr_json_path.exists():
            try:
                root_meta = json.loads(zarr_json_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                root_meta = {}

        attributes = root_meta.get("attributes", {}) if isinstance(root_meta, dict) else {}
        consolidated = root_meta.get("consolidated_metadata", {}) if isinstance(root_meta, dict) else {}
        nodes = consolidated.get("metadata", {}) if isinstance(consolidated, dict) else {}
        imagery = nodes.get("imagery", {}) if isinstance(nodes, dict) else {}
        items.append(
            {
                "artifact_id": _artifact_id_for_uri(str(store_path)),
                "artifact_type": "zarr",
                "artifact_uri": str(store_path),
                "provider": attributes.get("provider"),
                "collection": attributes.get("collection"),
                "scene_id": attributes.get("scene_id") or store_path.stem,
                "source_uri": attributes.get("source_uri"),
                "created_by_job_id": None,
                "source_job_id": None,
                "data_family": attributes.get("data_family"),
                "band_names": list(attributes.get("band_names", [])),
                "dimensions": list(imagery.get("dimension_names", [])),
                "shape": list(imagery.get("shape", [])),
                "size_bytes": None,
                "metadata": {
                    "discovered_local": True,
                    "zarr_format": root_meta.get("zarr_format"),
                    "crs": attributes.get("crs"),
                    "transform": attributes.get("transform"),
                },
                "created_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return items


def _merge_artifacts(registered: list[dict[str, Any]], discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in discovered:
        merged[str(item["artifact_uri"])] = dict(item)
    for item in registered:
        key = str(item["artifact_uri"])
        current = merged.get(key, {})
        metadata = dict(current.get("metadata", {}))
        metadata.update(dict(item.get("metadata", {})))
        merged[key] = {
            **current,
            **item,
            "band_names": list(item.get("band_names") or current.get("band_names") or []),
            "dimensions": list(item.get("dimensions") or current.get("dimensions") or []),
            "shape": list(item.get("shape") or current.get("shape") or []),
            "metadata": metadata,
        }
    values = list(merged.values())
    values.sort(key=lambda row: _safe_parse_iso(row.get("updated_at")).timestamp(), reverse=True)
    return values


@router.post("/artifacts", response_model=ArtifactRecord)
def upsert_artifact(
    request: ArtifactUpsertRequest,
    fetcher: NimbusFetcher = Depends(get_fetcher),
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
    row = fetcher.upsert_artifact(
        payload.model_copy(
            update={"metadata": payload.metadata, "artifact_uri": payload.artifact_uri}
        )
    )
    return row


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
    fetcher: NimbusFetcher = Depends(get_fetcher),
    settings: Settings = Depends(get_runtime_settings),
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
        discovered = _discover_local_zarr_artifacts(settings.nimbus_data_dir)
        merged = _merge_artifacts(
            [item.model_dump(mode="python") for item in registered.items],
            discovered,
        )
        page_value = max(1, page)
        page_size_value = max(1, min(200, page_size))
        offset = (page_value - 1) * page_size_value
        page_items = merged[offset : offset + page_size_value]
        return ArtifactListResponse(
            items=[ArtifactRecord.model_validate(item) for item in page_items],
            total=len(merged),
            page=page_value,
            page_size=page_size_value,
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
