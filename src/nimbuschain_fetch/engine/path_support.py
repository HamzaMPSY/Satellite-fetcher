from __future__ import annotations

from pathlib import Path
from typing import Any

from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch.usgs_product_type import canonicalize_usgs_product_type


class FetcherPathSupport:
    """Shared path/default-output helpers used by the fetcher facade."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @staticmethod
    def filter_manifest_paths(paths: list[str]) -> list[str]:
        return [path for path in paths if Path(str(path)).name != "manifest.json"]

    @staticmethod
    def scene_id_from_raw_uri(raw_uri: str) -> str:
        name = Path(str(raw_uri)).name
        for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
        return Path(name).stem or "scene"

    def default_zarr_output_uri(self, scene_id: str) -> str:
        safe_scene = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (scene_id or "scene")).strip(
            "._-"
        )
        if not safe_scene:
            safe_scene = "scene"
        return str(self._settings.nimbus_data_dir / "zarr" / f"{safe_scene}.zarr")

    def default_cube_output_dir(self, job_id: str) -> str:
        safe_job_id = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (job_id or "job")).strip(
            "._-"
        )
        if not safe_job_id:
            safe_job_id = "job"
        return str(self._settings.nimbus_data_dir / "zarr" / "cubes" / safe_job_id)

    @staticmethod
    def path_size_bytes(target_path: str | Path | None) -> int | None:
        if not target_path:
            return None
        try:
            path = Path(target_path)
        except TypeError:
            return None
        try:
            if not path.exists():
                return None
            if path.is_file():
                return int(path.stat().st_size)
            total = 0
            for child in path.rglob("*"):
                if child.is_file():
                    total += int(child.stat().st_size)
            return total
        except OSError:
            return None

    @staticmethod
    def merge_paths(existing: list[str], additions: list[str]) -> list[str]:
        merged: list[str] = []
        for value in [*existing, *additions]:
            item = str(value).strip()
            if item and item not in merged:
                merged.append(item)
        return merged

    def normalize_backend_path(self, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            return normalized
        root = str(self._settings.nimbus_data_dir).rstrip("/")
        legacy_prefixes = (
            "/data/downloads/",
            "/download/",
            "/downloads/",
            "/app/download/",
            "/app/downloads/",
            "/app/data/downloads/",
        )
        if normalized in {
            "/data/downloads",
            "/download",
            "/downloads",
            "/app/download",
            "/app/downloads",
            "/app/data/downloads",
        }:
            return root
        for legacy_prefix in legacy_prefixes:
            if normalized.startswith(legacy_prefix):
                suffix = normalized[len(legacy_prefix) :]
                return f"{root}/{suffix}" if suffix else root
        return normalized

    def normalize_backend_paths_payload(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self.normalize_backend_paths_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.normalize_backend_paths_payload(item) for item in value]
        if isinstance(value, tuple):
            return [self.normalize_backend_paths_payload(item) for item in value]
        return self.normalize_backend_path(value)

    def normalize_backend_paths_in_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        normalized["request"] = dict(self.normalize_backend_paths_payload(dict(row.get("request") or {})))
        normalized["pipeline_metadata"] = dict(
            self.normalize_backend_paths_payload(dict(row.get("pipeline_metadata") or {}))
        )
        normalized["conversion_metadata"] = dict(
            self.normalize_backend_paths_payload(dict(row.get("conversion_metadata") or {}))
        )
        normalized["raw_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(row.get("raw_outputs") or []))),
        )
        normalized["zarr_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(row.get("zarr_outputs") or []))),
        )
        normalized["watermask_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(row.get("watermask_outputs") or []))),
        )
        normalized["cloudmask_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(row.get("cloudmask_outputs") or []))),
        )
        return normalized

    def normalize_backend_paths_in_result_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result or {})
        normalized["paths"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("paths") or []))),
        )
        normalized["raw_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("raw_outputs") or []))),
        )
        normalized["zarr_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("zarr_outputs") or []))),
        )
        normalized["cube_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("cube_outputs") or []))),
        )
        normalized["masked_zarr_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("masked_zarr_outputs") or []))),
        )
        normalized["watermask_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("watermask_outputs") or []))),
        )
        normalized["cloudmask_outputs"] = self.merge_paths(
            [],
            list(self.normalize_backend_paths_payload(list(normalized.get("cloudmask_outputs") or []))),
        )
        normalized["metadata"] = dict(self.normalize_backend_paths_payload(dict(normalized.get("metadata") or {})))
        normalized["manifest_entry"] = dict(
            self.normalize_backend_paths_payload(dict(normalized.get("manifest_entry") or {}))
        )
        normalized["pipeline_metadata"] = dict(
            self.normalize_backend_paths_payload(dict(normalized.get("pipeline_metadata") or {}))
        )
        normalized["conversion_metadata"] = dict(
            self.normalize_backend_paths_payload(dict(normalized.get("conversion_metadata") or {}))
        )
        return normalized

    def normalize_backend_paths_in_artifact_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        normalized["artifact_uri"] = self.normalize_backend_path(row.get("artifact_uri"))
        normalized["source_uri"] = self.normalize_backend_path(row.get("source_uri"))
        normalized["metadata"] = dict(self.normalize_backend_paths_payload(dict(row.get("metadata") or {})))
        return normalized

    @staticmethod
    def normalize_collection_for_zarr(provider_name: str, collection: str) -> str:
        return collection.strip().lower() if provider_name == "usgs" else collection.strip().upper()

    @staticmethod
    def normalize_product_type_for_zarr(product_type: str | None) -> str | None:
        if product_type is None:
            return None
        normalized = str(product_type).strip()
        if not normalized:
            return None
        return canonicalize_usgs_product_type(normalized)
