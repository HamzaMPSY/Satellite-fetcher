from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nimbuschain_fetch.models import ArtifactListResponse, ArtifactRecord


class LocalArtifactOverlayService:
    def __init__(self, *, data_dir: Path):
        self._data_dir = data_dir

    @staticmethod
    def _is_remote_uri(uri: str) -> bool:
        value = str(uri or "").strip()
        return "://" in value

    @staticmethod
    def _artifact_id_for_uri(uri: str) -> str:
        return hashlib.md5(uri.encode("utf-8"), usedforsecurity=False).hexdigest()

    @staticmethod
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

    @staticmethod
    def _enum_or_str(value: Any) -> str:
        if value is None:
            return ""
        if hasattr(value, "value"):
            return str(value.value).strip()
        return str(value).strip()

    def _canonical_runtime_path(self, uri: Any) -> Path | None:
        value = str(uri or "").strip()
        if not value or self._is_remote_uri(value):
            return None
        root = self._data_dir.resolve()
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

    def _annotate_runtime_existence(self, item: ArtifactRecord) -> ArtifactRecord:
        metadata = dict(item.metadata)
        runtime_path = self._canonical_runtime_path(item.artifact_uri)
        if runtime_path is None:
            metadata.setdefault("runtime_exists", True)
            return item.model_copy(update={"metadata": metadata})
        runtime_exists = runtime_path.exists()
        metadata["runtime_exists"] = runtime_exists
        metadata.setdefault("runtime_canonical_path", str(runtime_path))
        return item.model_copy(update={"metadata": metadata})

    @staticmethod
    def _should_keep_include_local_item(item: ArtifactRecord) -> bool:
        if item.metadata.get("discovered_local"):
            return True
        return bool(item.metadata.get("runtime_exists", True))

    def _discover_local_zarr_artifacts(self) -> list[ArtifactRecord]:
        roots: list[tuple[Path, str]] = []
        direct_root = self._data_dir / "zarr"
        if direct_root.exists():
            roots.append((direct_root, "zarr"))
        project_fallback = Path(__file__).resolve().parents[2] / "download" / "zarr"
        if project_fallback.exists() and all(project_fallback != root for root, _kind in roots):
            roots.append((project_fallback, "zarr"))
        masked_root = self._data_dir / "zarrmask"
        if masked_root.exists():
            roots.append((masked_root, "zarr_masked"))
        masked_project_fallbacks = [
            Path(__file__).resolve().parents[2] / "download" / "zarrmask",
            Path(__file__).resolve().parents[2] / "data" / "downloads" / "zarrmask",
        ]
        for masked_project_fallback in masked_project_fallbacks:
            if masked_project_fallback.exists() and all(masked_project_fallback != root for root, _kind in roots):
                roots.append((masked_project_fallback, "zarr_masked"))
        if not roots:
            return []

        items: list[ArtifactRecord] = []
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
                    ArtifactRecord.model_validate(
                        {
                            "artifact_id": self._artifact_id_for_uri(str(store_path)),
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
                )
        return items

    @staticmethod
    def _merge_artifacts(
        registered: list[ArtifactRecord],
        discovered: list[ArtifactRecord],
    ) -> list[ArtifactRecord]:
        merged: dict[str, ArtifactRecord] = {}
        for item in discovered:
            merged[item.artifact_uri] = item
        for item in registered:
            current = merged.get(item.artifact_uri)
            if current is None:
                merged[item.artifact_uri] = item
                continue
            metadata = dict(current.metadata)
            metadata.update(dict(item.metadata))
            payload = current.model_dump(mode="python")
            payload.update(item.model_dump(mode="python"))
            payload["metadata"] = metadata
            payload["band_names"] = list(item.band_names or current.band_names or [])
            payload["dimensions"] = list(item.dimensions or current.dimensions or [])
            payload["shape"] = list(item.shape or current.shape or [])
            merged[item.artifact_uri] = ArtifactRecord.model_validate(payload)
        values = list(merged.values())
        values.sort(
            key=lambda row: LocalArtifactOverlayService._safe_parse_iso(row.updated_at).timestamp(),
            reverse=True,
        )
        return values

    def _matches_filters(
        self,
        item: ArtifactRecord,
        *,
        artifact_type: str | None,
        provider: str | None,
        collection: str | None,
        scene_id: str | None,
        job_id: str | None,
        uri_query: str | None,
    ) -> bool:
        item_artifact_type = self._enum_or_str(item.artifact_type)
        item_provider = self._enum_or_str(item.provider)
        item_collection = str(item.collection or "").strip()
        item_scene_id = str(item.scene_id or "").strip()
        item_uri = str(item.artifact_uri or "").strip()

        if artifact_type and item_artifact_type != artifact_type:
            return False
        if provider and item_provider != provider:
            return False
        if collection and collection.lower() not in item_collection.lower():
            return False
        if scene_id and scene_id.lower() not in item_scene_id.lower():
            return False
        if job_id:
            created = str(item.created_by_job_id or "").strip()
            source = str(item.source_job_id or "").strip()
            if job_id not in {created, source}:
                return False
        if uri_query:
            query = uri_query.lower()
            if query not in item_uri.lower() and query not in item_scene_id.lower():
                return False
        return True

    def merge_with_local_artifacts(
        self,
        registered: ArtifactListResponse,
        *,
        artifact_type: str | None,
        provider: str | None,
        collection: str | None,
        scene_id: str | None,
        job_id: str | None,
        uri_query: str | None,
        page: int,
        page_size: int,
    ) -> ArtifactListResponse:
        discovered = self._discover_local_zarr_artifacts()
        merged = self._merge_artifacts(list(registered.items), discovered)
        merged = [self._annotate_runtime_existence(item) for item in merged]
        merged = [item for item in merged if self._should_keep_include_local_item(item)]
        merged = [
            item
            for item in merged
            if self._matches_filters(
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
            items=page_items,
            total=len(merged),
            page=page_value,
            page_size=page_size_value,
        )


__all__ = ["LocalArtifactOverlayService"]
