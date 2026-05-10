from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from nimbuschain_fetch.domain.metadata import ConversionMetadataRecord, PipelineMetadataRecord
from nimbuschain_fetch.models import JobCreateRequest, SearchDownloadRequest


class FetcherMaskPolicySupport:
    """Mask/cube value-policy helpers used across workflows and status reconstruction."""

    @staticmethod
    def normalized_mask_types(values: list[str] | tuple[str, ...] | None) -> list[str]:
        normalized: list[str] = []
        for value in list(values or []):
            candidate = str(value or "").strip().lower()
            if candidate not in {"water", "cloud"}:
                continue
            if candidate not in normalized:
                normalized.append(candidate)
        return normalized

    @staticmethod
    def normalized_cube_mode(value: Any) -> str:
        candidate = str(value or "").strip().lower()
        if candidate in {"before_mask", "after_mask"}:
            return candidate
        return "none"

    @staticmethod
    def normalized_cube_layout(value: Any) -> str:
        candidate = str(value or "").strip().lower()
        if candidate in {"grouped_time", "daily_mosaic"}:
            return candidate
        return "grouped_time"

    @staticmethod
    def normalized_cube_overlap_policy(value: Any) -> str:
        candidate = str(value or "").strip().lower()
        if candidate in {"least_cloud", "latest", "earliest", "first_valid"}:
            return candidate
        return "least_cloud"

    @classmethod
    def timeline_cube_mode_for_row(
        cls,
        row: dict[str, Any],
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> str:
        metadata = PipelineMetadataRecord.from_mapping(row.get("pipeline_metadata")).merged_with(pipeline_metadata)
        return cls.normalized_cube_mode(
            metadata.cube_mode
            or dict(row.get("request") or {}).get("cube_mode")
        )

    @classmethod
    def cube_config_from_request(cls, request: JobCreateRequest) -> dict[str, Any] | None:
        if not isinstance(request, SearchDownloadRequest):
            return None
        cube_mode = cls.normalized_cube_mode(getattr(request, "cube_mode", "none"))
        if cube_mode == "none":
            return None
        return {
            "mode": cube_mode,
            "start_date": getattr(request, "cube_start_date", None),
            "end_date": getattr(request, "cube_end_date", None),
            "layout": cls.normalized_cube_layout(getattr(request, "cube_layout", "grouped_time")),
            "target_crs": str(getattr(request, "cube_target_crs", "") or "").strip() or None,
            "target_resolution_m": int(getattr(request, "cube_target_resolution_m", 10) or 10),
            "overlap_policy": cls.normalized_cube_overlap_policy(
                getattr(request, "cube_overlap_policy", "least_cloud")
            ),
        }

    @classmethod
    def cube_config_from_request_payload(cls, request_payload: dict[str, Any]) -> dict[str, Any] | None:
        cube_mode = cls.normalized_cube_mode(request_payload.get("cube_mode"))
        if cube_mode == "none":
            return None
        return {
            "mode": cube_mode,
            "start_date": request_payload.get("cube_start_date") or request_payload.get("start_date"),
            "end_date": request_payload.get("cube_end_date") or request_payload.get("end_date"),
            "layout": cls.normalized_cube_layout(request_payload.get("cube_layout")),
            "target_crs": str(request_payload.get("cube_target_crs") or "").strip() or None,
            "target_resolution_m": int(request_payload.get("cube_target_resolution_m") or 10),
            "overlap_policy": cls.normalized_cube_overlap_policy(
                request_payload.get("cube_overlap_policy")
            ),
        }

    @staticmethod
    def normalize_mask_failure_step(value: Any) -> str | None:
        candidate = str(value or "").strip().lower()
        if candidate in {"failed", "cloud_failed", "water_failed"}:
            return candidate
        return None

    @classmethod
    def preferred_mask_failure_step(
        cls,
        values: Iterable[Any],
    ) -> str:
        priority = {
            "failed": 0,
            "cloud_failed": 1,
            "water_failed": 2,
        }
        selected = "failed"
        selected_priority = -1
        for raw_value in values:
            candidate = cls.normalize_mask_failure_step(raw_value)
            if candidate is None:
                continue
            candidate_priority = priority.get(candidate, -1)
            if candidate_priority > selected_priority:
                selected = candidate
                selected_priority = candidate_priority
        return selected

    @classmethod
    def mask_failure_step_from_payloads(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        water_mask: dict[str, Any] | None,
        cloud_mask: dict[str, Any] | None,
    ) -> str | None:
        normalized_mask_types = cls.normalized_mask_types(list(mask_types or []))
        failure_steps: list[str] = []
        water_status = str((water_mask or {}).get("status") or "").strip().lower()
        cloud_status = str((cloud_mask or {}).get("status") or "").strip().lower()
        if "water" in normalized_mask_types and water_status == "failed":
            failure_steps.append("water_failed")
        if "cloud" in normalized_mask_types and cloud_status == "failed":
            failure_steps.append("cloud_failed")
        if not failure_steps:
            return None
        return cls.preferred_mask_failure_step(failure_steps)

    @classmethod
    def mask_failure_step_from_items(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        items: list[dict[str, Any]] | None,
    ) -> str:
        failure_steps: list[str] = []
        for item in list(items or []):
            direct_step = cls.normalize_mask_failure_step(item.get("failed_step"))
            if direct_step is not None:
                failure_steps.append(direct_step)
                continue
            conversion_metadata = ConversionMetadataRecord.from_mapping(item.get("conversion_metadata"))
            inferred_step = cls.mask_failure_step_from_payloads(
                mask_types=mask_types,
                water_mask=conversion_metadata.water_mask,
                cloud_mask=conversion_metadata.cloud_mask,
            )
            if inferred_step is not None:
                failure_steps.append(inferred_step)
        return cls.preferred_mask_failure_step(failure_steps)

    @staticmethod
    def build_mask_progress_plan(
        *,
        mask_types: list[str],
        stage_start_progress: float,
        stage_end_progress: float,
    ) -> dict[str, float]:
        start = max(0.0, float(stage_start_progress))
        end = max(start, float(stage_end_progress))
        span = max(1.0, end - start)
        has_cloud = "cloud" in mask_types
        has_water = "water" in mask_types
        if has_cloud and has_water:
            cloud_end = min(end, start + (span * 0.48))
            water_start = min(end, start + (span * 0.58))
            return {
                "cloud_start": start,
                "cloud_end": cloud_end,
                "water_start": water_start,
                "water_end": end,
            }
        if has_cloud:
            return {
                "cloud_start": start,
                "cloud_end": end,
                "water_start": end,
                "water_end": end,
            }
        return {
            "cloud_start": start,
            "cloud_end": start,
            "water_start": start,
            "water_end": end,
        }
