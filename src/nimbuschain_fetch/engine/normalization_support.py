from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from nimbuschain_fetch.engine.status_timeline_support import FetcherStatusTimelineSupport
from nimbuschain_fetch.models import JobState, JobStatusResponse, PipelineState


class FetcherNormalizationSupport:
    """Historical-row normalization and status/timeline facade helpers."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def normalize_historical_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
        state = str(row.get("state") or "")
        pipeline_state = str(row.get("pipeline_state") or PipelineState.queued.value)
        if pipeline_state != PipelineState.queued.value:
            return row

        updates: dict[str, Any] = {}
        progress = float(row.get("progress") or 0.0)
        raw_outputs = list(row.get("raw_outputs") or [])
        zarr_outputs = list(row.get("zarr_outputs") or [])
        conversion_metadata = dict(row.get("conversion_metadata") or {})

        if state == JobState.succeeded.value:
            if zarr_outputs or conversion_metadata.get("status") == "written":
                updates = {
                    "pipeline_state": PipelineState.zarr_written.value,
                    "pipeline_step": "zarr_written",
                    "pipeline_progress": 100.0,
                }
            else:
                updates = {
                    "pipeline_state": PipelineState.downloaded.value,
                    "pipeline_step": "downloaded",
                    "pipeline_progress": 100.0,
                }
        elif state == JobState.failed.value:
            if raw_outputs and conversion_metadata:
                updates = {
                    "pipeline_state": PipelineState.zarr_failed.value,
                    "pipeline_step": "zarr_failed",
                    "pipeline_progress": max(float(row.get("pipeline_progress") or 0.0), max(progress, 72.0)),
                }
            else:
                updates = {
                    "pipeline_state": PipelineState.failed.value,
                    "pipeline_step": "failed",
                    "pipeline_progress": max(float(row.get("pipeline_progress") or 0.0), progress),
                }
        elif state == JobState.cancelled.value:
            updates = {
                "pipeline_state": PipelineState.cancelled.value,
                "pipeline_step": "cancelled",
                "pipeline_progress": float(row.get("pipeline_progress") or progress),
            }

        if not updates:
            return row

        self._rt.store.update_job(row["job_id"], preserve_updated_at=True, **updates)
        normalized = dict(row)
        normalized.update(updates)
        return normalized

    def to_status_response(self, row: dict[str, Any]) -> JobStatusResponse:
        return self._rt._status_timeline_support.to_status_response(self._rt, row)

    def rebuild_pipeline_timeline_from_events(
        self,
        *,
        row: dict[str, Any],
        job_kind: str | None,
        mask_types: list[str],
        cube_mode: str,
        pipeline_progress: float | None,
        timeline_timestamp: str | datetime,
    ) -> dict[str, Any]:
        return self._rt._status_timeline_support.rebuild_pipeline_timeline_from_events(
            self._rt,
            row=row,
            job_kind=job_kind,
            mask_types=mask_types,
            cube_mode=cube_mode,
            pipeline_progress=pipeline_progress,
            timeline_timestamp=timeline_timestamp,
        )

    @staticmethod
    def events_for_current_timeline_attempt(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return FetcherStatusTimelineSupport.events_for_current_timeline_attempt(events)

    @staticmethod
    def pipeline_timeline_needs_rebuild(
        *,
        row: dict[str, Any],
        pipeline_timeline: dict[str, Any],
        mask_types: list[str],
        cube_mode: str,
        normalized_cube_mode: Any,
    ) -> bool:
        return FetcherStatusTimelineSupport.pipeline_timeline_needs_rebuild(
            row=row,
            pipeline_timeline=pipeline_timeline,
            mask_types=mask_types,
            cube_mode=cube_mode,
            normalized_cube_mode=normalized_cube_mode,
        )

    @staticmethod
    def duration_seconds_for_row(
        *,
        state: str,
        started_at: datetime | None,
        finished_at: datetime | None,
        updated_at: datetime | None,
    ) -> float | None:
        return FetcherStatusTimelineSupport.duration_seconds_for_row(
            state=state,
            started_at=started_at,
            finished_at=finished_at,
            updated_at=updated_at,
        )

    def effective_started_at_for_row(self, row: dict[str, Any]) -> datetime | None:
        return self._rt._status_timeline_support.effective_started_at_for_row(row)

    @staticmethod
    def parse_iso(value: str | datetime | None) -> datetime | None:
        return FetcherStatusTimelineSupport.parse_iso(value)

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
