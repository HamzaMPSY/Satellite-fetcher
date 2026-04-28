from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import JobState, JobStatusResponse, PipelineState, ProviderName
from nimbuschain_fetch.pipeline_timeline import advance_pipeline_timeline


class FetcherStatusTimelineSupport:
    """Status response and pipeline timeline helpers for the fetcher facade."""

    def __init__(self, *, store: JobStore, now_iso: Any) -> None:
        self._store = store
        self._now_iso = now_iso

    @staticmethod
    def events_for_current_timeline_attempt(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        last_requeue_index: int | None = None
        last_started_index: int | None = None

        for index, event in enumerate(events):
            event_type = str(event.get("type") or "").strip().lower()
            if event_type in {"job.requeued_after_restart", "job.requeued_stale"}:
                last_requeue_index = index
            if event_type == "job.started":
                last_started_index = index

        boundary_index = 0
        if last_requeue_index is not None:
            boundary_index = last_requeue_index
        if (
            last_started_index is not None
            and (
                last_requeue_index is None
                or last_started_index > last_requeue_index
            )
        ):
            boundary_index = last_started_index
        return list(events[boundary_index:])

    @staticmethod
    def pipeline_timeline_needs_rebuild(
        *,
        row: dict[str, Any],
        pipeline_timeline: dict[str, Any],
        mask_types: list[str],
        cube_mode: str,
        normalized_cube_mode: Any,
    ) -> bool:
        steps = [
            dict(step)
            for step in list(pipeline_timeline.get("steps") or [])
            if isinstance(step, dict)
        ]
        if not steps:
            return True

        active_steps = [
            dict(step)
            for step in steps
            if str(step.get("status") or "").strip().lower() in {"running", "queued"}
        ]
        if len(active_steps) > 1:
            return True

        stage_statuses = {
            str(stage.get("key") or "").strip().lower(): str(stage.get("status") or "").strip().lower()
            for stage in list(pipeline_timeline.get("stages") or [])
            if isinstance(stage, dict)
        }
        pipeline_state = str(row.get("pipeline_state") or "").strip().lower()
        current_stage = str(pipeline_timeline.get("current_stage") or "").strip().lower()
        normalized_mode = normalized_cube_mode(cube_mode or pipeline_timeline.get("cube_mode"))
        cube_related_states = {
            PipelineState.cube_queued.value,
            PipelineState.cube_building.value,
            PipelineState.cube_written.value,
            PipelineState.cube_failed.value,
        }

        if normalized_mode != "none" and "cube" not in stage_statuses:
            return True
        if pipeline_state in cube_related_states and stage_statuses.get("cube", "pending") == "pending":
            return True
        cube_stage_status = stage_statuses.get("cube", "pending")
        if (
            normalized_mode == "before_mask"
            and pipeline_state in {
                PipelineState.queued.value,
                PipelineState.searching.value,
                PipelineState.downloading.value,
                PipelineState.downloaded.value,
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.zarr_written.value,
                PipelineState.zarr_failed.value,
            }
            and cube_stage_status not in {"pending"}
        ):
            return True
        if (
            normalized_mode == "after_mask"
            and pipeline_state in {
                PipelineState.queued.value,
                PipelineState.searching.value,
                PipelineState.downloading.value,
                PipelineState.downloaded.value,
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.zarr_written.value,
                PipelineState.running_cloud_inference.value,
                PipelineState.running_water_inference.value,
                PipelineState.writing_mask_artifacts.value,
                PipelineState.writing_masked_zarr.value,
                PipelineState.registering_artifacts.value,
                PipelineState.zarr_failed.value,
            }
            and cube_stage_status not in {"pending"}
        ):
            return True
        if pipeline_state == PipelineState.searching.value and stage_statuses.get("search", "pending") == "pending":
            return True
        if (
            pipeline_state in {PipelineState.downloading.value, PipelineState.downloaded.value}
            and stage_statuses.get("download", "pending") == "pending"
        ):
            return True
        if (
            pipeline_state in {
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.zarr_written.value,
                PipelineState.running_cloud_inference.value,
                PipelineState.running_water_inference.value,
                PipelineState.zarr_failed.value,
                PipelineState.masked_zarr_written.value,
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_written.value,
                PipelineState.cube_failed.value,
            }
            and stage_statuses.get("convert", "pending") == "pending"
        ):
            return True
        if (
            pipeline_state in {
                PipelineState.running_cloud_inference.value,
                PipelineState.running_water_inference.value,
                PipelineState.masked_zarr_written.value,
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_written.value,
                PipelineState.cube_failed.value,
                PipelineState.failed.value,
            }
            and stage_statuses.get("convert") == "running"
        ):
            return True
        if pipeline_state == PipelineState.resolving_source_zarr.value and stage_statuses.get("resolve", "pending") == "pending":
            return True
        if pipeline_state == PipelineState.running_cloud_inference.value and current_stage not in {"cloud"}:
            return True
        if pipeline_state == PipelineState.running_water_inference.value and current_stage not in {"water"}:
            return True
        if pipeline_state == PipelineState.cube_building.value and current_stage not in {"cube"}:
            return True
        if (
            "cloud" in mask_types
            and pipeline_state in {
                PipelineState.running_cloud_inference.value,
                PipelineState.running_water_inference.value,
                PipelineState.masked_zarr_written.value,
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_written.value,
                PipelineState.cube_failed.value,
            }
            and stage_statuses.get("cloud", "pending") == "pending"
        ):
            return True
        if (
            "water" in mask_types
            and pipeline_state in {
                PipelineState.running_water_inference.value,
                PipelineState.masked_zarr_written.value,
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_written.value,
                PipelineState.cube_failed.value,
            }
            and stage_statuses.get("water", "pending") == "pending"
        ):
            return True
        if (
            "cloud" in mask_types
            and pipeline_state == PipelineState.masked_zarr_written.value
            and stage_statuses.get("cloud", "pending") == "pending"
        ):
            return True
        if (
            normalized_mode == "before_mask"
            and pipeline_state in {
                PipelineState.running_cloud_inference.value,
                PipelineState.running_water_inference.value,
                PipelineState.masked_zarr_written.value,
                PipelineState.failed.value,
            }
            and stage_statuses.get("cube", "pending") in {"pending", "queued", "running"}
        ):
            return True
        return False

    @staticmethod
    def duration_seconds_for_row(
        *,
        state: str,
        started_at: datetime | None,
        finished_at: datetime | None,
        updated_at: datetime | None,
    ) -> float | None:
        if started_at is None:
            return None

        state_value = str(state or "").strip().lower()
        if state_value == JobState.queued.value:
            return None

        if state_value in {JobState.running.value, JobState.cancel_requested.value}:
            end_time = datetime.now(timezone.utc)
        elif state_value in {
            JobState.succeeded.value,
            JobState.failed.value,
            JobState.cancelled.value,
        }:
            end_time = finished_at or updated_at
        else:
            end_time = finished_at or updated_at

        if end_time is None:
            return None
        return max(0.0, (end_time - started_at).total_seconds())

    @staticmethod
    def parse_iso(value: str | datetime | None) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)

    def effective_started_at_for_row(self, row: dict[str, Any]) -> datetime | None:
        started_at = self.parse_iso(row.get("started_at"))
        if started_at is not None:
            return started_at

        state_value = str(row.get("state") or "").strip().lower()
        if state_value == JobState.queued.value:
            return None

        preferred_types = {
            "job.started",
            "job.searching",
            "job.downloading",
            "job.downloaded",
            "job.zarr_queued",
            "job.zarr_converting",
            "job.zarr_written",
            "job.mask_started",
            "job.cloud_masking_started",
            "job.water_masking_started",
        }
        ignored_types = {
            "job.queued",
            "job.requeued_after_restart",
            "job.requeued_stale",
        }
        fallback_event_ts: datetime | None = None
        try:
            events = self._store.list_events(str(row.get("job_id") or ""), None, 200)
        except Exception:
            events = []
        events = self.events_for_current_timeline_attempt(events)
        for event in events:
            event_type = str(event.get("type") or "").strip()
            event_ts = self.parse_iso(event.get("timestamp"))
            if event_ts is None:
                continue
            if event_type in preferred_types:
                return event_ts
            if fallback_event_ts is None and event_type not in ignored_types:
                fallback_event_ts = event_ts
        return fallback_event_ts or self.parse_iso(row.get("created_at"))

    def rebuild_pipeline_timeline_from_events(
        self,
        rt: Any,
        *,
        row: dict[str, Any],
        job_kind: str | None,
        mask_types: list[str],
        cube_mode: str,
        pipeline_progress: float | None,
        timeline_timestamp: str | datetime,
    ) -> dict[str, Any]:
        rebuilt: dict[str, Any] = {}
        try:
            events = self._store.list_events(str(row.get("job_id") or ""), None, 500)
        except Exception:
            events = []
        events = self.events_for_current_timeline_attempt(events)

        for event in events:
            payload = dict(event.get("payload") or {})
            event_pipeline_state = str(payload.get("pipeline_state") or "").strip().lower()
            event_pipeline_step = str(payload.get("pipeline_step") or event_pipeline_state).strip().lower()
            if not event_pipeline_state and not event_pipeline_step:
                continue
            event_progress = payload.get("pipeline_progress")
            event_cube_mode = rt._normalized_cube_mode(payload.get("cube_mode") or cube_mode)
            rebuilt = advance_pipeline_timeline(
                rebuilt,
                job_state=str(row.get("state") or ""),
                pipeline_state=event_pipeline_state or str(row.get("pipeline_state") or ""),
                pipeline_step=event_pipeline_step or event_pipeline_state,
                pipeline_progress=float(event_progress) if event_progress is not None else None,
                timestamp=event.get("timestamp") or timeline_timestamp,
                job_kind=job_kind,
                mask_types=mask_types,
                cube_mode=event_cube_mode,
            )

        return advance_pipeline_timeline(
            rebuilt,
            job_state=str(row.get("state") or ""),
            pipeline_state=str(row.get("pipeline_state") or PipelineState.queued.value),
            pipeline_step=row.get("pipeline_step"),
            pipeline_progress=pipeline_progress,
            timestamp=timeline_timestamp,
            job_kind=job_kind,
            mask_types=mask_types,
            cube_mode=cube_mode,
        )

    def to_status_response(self, rt: Any, row: dict[str, Any]) -> JobStatusResponse:
        started_at = self.effective_started_at_for_row(row)
        finished_at = self.parse_iso(row.get("finished_at"))
        updated_at = self.parse_iso(row.get("updated_at"))
        duration_seconds = self.duration_seconds_for_row(
            state=str(row.get("state") or ""),
            started_at=started_at,
            finished_at=finished_at,
            updated_at=updated_at,
        )
        job_type = row.get("job_type")
        job_kind = rt._job_kind_for_type(job_type)
        service_name = rt._service_name_for_type(job_type)
        masked_zarr_outputs = rt._masked_zarr_outputs_for_job(job_id=row["job_id"], result={}, row=row)
        pipeline_metadata = dict(row.get("pipeline_metadata") or {})
        cube_outputs = rt._cube_outputs_for_job(job_id=row["job_id"], result={}, row=row)
        pipeline_progress = float(row["pipeline_progress"]) if row.get("pipeline_progress") is not None else None
        timeline_cube_mode = rt._timeline_cube_mode_for_row(row, pipeline_metadata)
        if str(row.get("state") or "").strip().lower() in {
            JobState.running.value,
            JobState.cancel_requested.value,
        }:
            timeline_timestamp: str | datetime = self._now_iso()
        else:
            timeline_timestamp = finished_at or updated_at or started_at or self._now_iso()
        existing_timeline = pipeline_metadata.get("timeline")
        timeline_mask_types = rt._normalized_mask_types(
            (row.get("request") or {}).get("mask_types")
            or pipeline_metadata.get("mask_types")
            or dict(row.get("conversion_metadata") or {}).get("mask_types")
            or []
        )
        pipeline_timeline = advance_pipeline_timeline(
            dict(existing_timeline) if isinstance(existing_timeline, dict) else {},
            job_state=str(row.get("state") or ""),
            pipeline_state=str(row.get("pipeline_state") or PipelineState.queued.value),
            pipeline_step=row.get("pipeline_step"),
            pipeline_progress=pipeline_progress,
            timestamp=timeline_timestamp,
            job_kind=job_kind,
            mask_types=timeline_mask_types,
            cube_mode=timeline_cube_mode,
        )
        if self.pipeline_timeline_needs_rebuild(
            row=row,
            pipeline_timeline=pipeline_timeline,
            mask_types=timeline_mask_types,
            cube_mode=timeline_cube_mode,
            normalized_cube_mode=rt._normalized_cube_mode,
        ):
            pipeline_timeline = self.rebuild_pipeline_timeline_from_events(
                rt,
                row=row,
                job_kind=job_kind,
                mask_types=timeline_mask_types,
                cube_mode=timeline_cube_mode,
                pipeline_progress=pipeline_progress,
                timeline_timestamp=timeline_timestamp,
            )
        if pipeline_timeline:
            pipeline_metadata["timeline"] = pipeline_timeline
        source_job_id = str(
            pipeline_metadata.get("source_job_id")
            or (row.get("request") or {}).get("source_job_id")
            or ""
        ).strip() or None
        resume_metadata = rt._resume_metadata_for_row(row)

        return JobStatusResponse(
            job_id=row["job_id"],
            job_type=str(job_type or "") or None,
            job_kind=cast(Any, job_kind),
            service_name=service_name,
            source_job_id=source_job_id,
            state=JobState(row["state"]),
            pipeline_state=PipelineState(str(row.get("pipeline_state") or PipelineState.queued.value)),
            pipeline_step=row.get("pipeline_step"),
            pipeline_progress=pipeline_progress,
            pipeline_timeline=pipeline_timeline,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=dict(row.get("conversion_metadata") or {}),
            raw_outputs=list(row.get("raw_outputs") or []),
            zarr_outputs=list(row.get("zarr_outputs") or []),
            cube_outputs=cube_outputs,
            masked_zarr_outputs=masked_zarr_outputs,
            watermask_outputs=list(row.get("watermask_outputs") or []),
            cloudmask_outputs=list(row.get("cloudmask_outputs") or []),
            progress=float(row["progress"]),
            bytes_downloaded=int(row["bytes_downloaded"]),
            bytes_total=int(row["bytes_total"]),
            retry_count=int(row.get("retry_count", 0) or 0),
            last_retry_at=self.parse_iso(row.get("last_retry_at")),
            product_type=row.get("product_type"),
            tile_id=row.get("tile_id"),
            created_at=self.parse_iso(row.get("created_at")),
            updated_at=updated_at,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            errors=list(row.get("errors", [])),
            can_resume=bool(resume_metadata.get("can_resume")),
            resume_action=cast(Any, resume_metadata.get("resume_action")),
            resume_label=cast(Any, resume_metadata.get("resume_label")),
            resume_reason=cast(Any, resume_metadata.get("resume_reason")),
            provider=ProviderName(row["provider"]),
            collection=str(row["collection"]),
        )
