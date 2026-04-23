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


def _is_remote_uri(uri: str) -> bool:
    value = str(uri or "").strip()
    return "://" in value


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


def _enum_or_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "value"):
        return str(value.value).strip()
    return str(value).strip()


def _canonical_runtime_path(uri: Any, data_dir: Path) -> Path | None:
    value = str(uri or "").strip()
    if not value or _is_remote_uri(value):
        return None
    root = data_dir.resolve()
    if value in {"/data/downloads", "/download", "/downloads", "/app/data/downloads", "/app/download", "/app/downloads"}:
        return root
    prefix_map = (
        ("/data/downloads/", root),
        ("/download/", root),
        ("/downloads/", root),
        ("/app/data/downloads/", root),
        ("/app/download/", root),
        ("/app/downloads/", root),
    )
    for prefix, destination_root in prefix_map:
        if value.startswith(prefix):
            suffix = value[len(prefix):]
            return destination_root / suffix if suffix else destination_root
    local_path = Path(value)
    if local_path.is_absolute():
        return local_path
    return None


def _annotate_runtime_existence(item: dict[str, Any], data_dir: Path) -> dict[str, Any]:
    annotated = dict(item)
    metadata = dict(annotated.get("metadata") or {})
    runtime_path = _canonical_runtime_path(annotated.get("artifact_uri"), data_dir)
    if runtime_path is None:
        metadata.setdefault("runtime_exists", True)
        annotated["metadata"] = metadata
        return annotated
    runtime_exists = runtime_path.exists()
    metadata["runtime_exists"] = runtime_exists
    metadata.setdefault("runtime_canonical_path", str(runtime_path))
    annotated["metadata"] = metadata
    return annotated


def _should_keep_include_local_item(item: dict[str, Any], data_dir: Path) -> bool:
    annotated = _annotate_runtime_existence(item, data_dir)
    metadata = dict(annotated.get("metadata") or {})
    if metadata.get("discovered_local"):
        return True
    return bool(metadata.get("runtime_exists", True))


def _discover_local_zarr_artifacts(data_dir: Path) -> list[dict[str, Any]]:
    roots: list[tuple[Path, str]] = []
    direct_root = data_dir / "zarr"
    if direct_root.exists():
        roots.append((direct_root, "zarr"))
    project_fallback = Path(__file__).resolve().parents[3] / "download" / "zarr"
    if project_fallback.exists() and all(project_fallback != root for root, _kind in roots):
        roots.append((project_fallback, "zarr"))
    masked_root = data_dir / "zarrmask"
    if masked_root.exists():
        roots.append((masked_root, "zarr_masked"))
    masked_project_fallbacks = [
        Path(__file__).resolve().parents[3] / "download" / "zarrmask",
        Path(__file__).resolve().parents[3] / "data" / "downloads" / "zarrmask",
    ]
    for masked_project_fallback in masked_project_fallbacks:
        if masked_project_fallback.exists() and all(masked_project_fallback != root for root, _kind in roots):
            roots.append((masked_project_fallback, "zarr_masked"))
    if not roots:
        return []

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for zarr_root, artifact_type in roots:
        for store_path in sorted(zarr_root.rglob("*.zarr")):
            if not store_path.is_dir():
                continue
            store_key = str(store_path.resolve())
            if store_key in seen:
                continue
            seen.add(store_key)
            try:
                stat = store_path.stat()
            except OSError:
                continue

            root_meta: dict[str, Any] = {}
            attributes: dict[str, Any] = {}
            imagery: dict[str, Any] = {}
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
            else:
                zattrs_path = store_path / ".zattrs"
                zmetadata_path = store_path / ".zmetadata"
                if zattrs_path.exists():
                    try:
                        attributes = json.loads(zattrs_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        attributes = {}
                if zmetadata_path.exists():
                    try:
                        zmetadata = json.loads(zmetadata_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        zmetadata = {}
                    imagery = ((zmetadata.get("metadata") or {}).get("imagery/.zarray") or {})
            items.append(
                {
                    "artifact_id": _artifact_id_for_uri(str(store_path)),
                    "artifact_type": artifact_type,
                    "artifact_uri": str(store_path),
                    "provider": attributes.get("provider"),
                    "collection": attributes.get("collection"),
                    "scene_id": attributes.get("scene_id") or store_path.stem,
                    "source_uri": attributes.get("source_uri"),
                    "created_by_job_id": None,
                    "source_job_id": None,
                    "data_family": attributes.get("data_family"),
                    "band_names": list(attributes.get("band_names", [])),
                    "dimensions": list(imagery.get("dimension_names", []) or ["time", "band", "y", "x"]),
                    "shape": list(imagery.get("shape", [])),
                    "size_bytes": None,
                    "metadata": {
                        "discovered_local": True,
                        "zarr_format": root_meta.get("zarr_format", 2),
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


def _matches_artifact_filters(
    item: dict[str, Any],
    *,
    artifact_type: str | None,
    provider: str | None,
    collection: str | None,
    scene_id: str | None,
    job_id: str | None,
    uri_query: str | None,
) -> bool:
    item_artifact_type = _enum_or_str(item.get("artifact_type"))
    item_provider = _enum_or_str(item.get("provider"))
    item_collection = str(item.get("collection") or "").strip()
    item_scene_id = str(item.get("scene_id") or "").strip()
    item_uri = str(item.get("artifact_uri") or "").strip()

    if artifact_type and item_artifact_type != artifact_type:
        return False
    if provider and item_provider != provider:
        return False
    if collection and collection.lower() not in item_collection.lower():
        return False
    if scene_id and scene_id.lower() not in item_scene_id.lower():
        return False
    if job_id:
        created = str(item.get("created_by_job_id") or "").strip()
        source = str(item.get("source_job_id") or "").strip()
        if job_id not in {created, source}:
            return False
    if uri_query:
        query = uri_query.lower()
        if query not in item_uri.lower() and query not in item_scene_id.lower():
            return False
    return True


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
        merged = [
            _annotate_runtime_existence(item, settings.nimbus_data_dir)
            for item in merged
            if _should_keep_include_local_item(item, settings.nimbus_data_dir)
        ]
        merged = [
            item
            for item in merged
            if _matches_artifact_filters(
                item,
                artifact_type=artifact_type,
                provider=provider,
                collection=collection,
                scene_id=scene_id,
                job_id=job_id,
                uri_query=uri_query,
            )
        ]
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
