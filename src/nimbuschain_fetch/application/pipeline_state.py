from __future__ import annotations

from collections.abc import Callable
from threading import RLock
from typing import Any

from nimbuschain_fetch.domain.records import JobRowRecord
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import PipelineState
from nimbuschain_fetch.pipeline_timeline import advance_pipeline_timeline


class PipelineStateService:
    def __init__(
        self,
        *,
        store: JobStore,
        lock: RLock,
        now_iso: Callable[[], str],
        job_kind_for_type: Callable[[str | None], str],
        normalized_mask_types: Callable[[list[str] | tuple[str, ...] | None], list[str]],
        timeline_cube_mode_for_row: Callable[[dict[str, Any], dict[str, Any] | None], str],
    ):
        self._store = store
        self._lock = lock
        self._now_iso = now_iso
        self._job_kind_for_type = job_kind_for_type
        self._normalized_mask_types = normalized_mask_types
        self._timeline_cube_mode_for_row = timeline_cube_mode_for_row

    def update(
        self,
        job_id: str,
        *,
        pipeline_state: PipelineState,
        pipeline_step: str,
        pipeline_progress: float | None = None,
        pipeline_metadata: dict[str, Any] | None = None,
        conversion_metadata: dict[str, Any] | None = None,
        raw_outputs: list[str] | None = None,
        zarr_outputs: list[str] | None = None,
        event_type: str | None = None,
        event_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            row_now = self._store.get_job_record(job_id) or JobRowRecord(job_id=job_id)
            existing_pipeline_metadata = dict(row_now.pipeline_metadata)
            merged_pipeline_metadata = dict(
                pipeline_metadata if pipeline_metadata is not None else existing_pipeline_metadata
            )
            if pipeline_metadata is not None:
                for orchestration_key in ("orchestrator", "stage_plan", "stage_results"):
                    if (
                        orchestration_key in existing_pipeline_metadata
                        and orchestration_key not in merged_pipeline_metadata
                    ):
                        merged_pipeline_metadata[orchestration_key] = existing_pipeline_metadata[
                            orchestration_key
                        ]
            existing_timeline = existing_pipeline_metadata.get("timeline")
            merged_pipeline_metadata["timeline"] = advance_pipeline_timeline(
                dict(existing_timeline) if isinstance(existing_timeline, dict) else {},
                job_state=str(row_now.state or ""),
                pipeline_state=pipeline_state.value,
                pipeline_step=pipeline_step,
                pipeline_progress=pipeline_progress,
                timestamp=self._now_iso(),
                job_kind=self._job_kind_for_type(row_now.job_type),
                mask_types=self._normalized_mask_types(
                    merged_pipeline_metadata.get("mask_types")
                    or existing_pipeline_metadata.get("mask_types")
                    or row_now.request.get("mask_types")
                    or []
                ),
                cube_mode=self._timeline_cube_mode_for_row(
                    {
                        "pipeline_metadata": row_now.pipeline_metadata,
                        "request": row_now.request,
                    },
                    merged_pipeline_metadata,
                ),
            )
            fields: dict[str, Any] = {
                "pipeline_state": pipeline_state.value,
                "pipeline_step": pipeline_step,
                "pipeline_metadata": merged_pipeline_metadata,
            }
            if pipeline_progress is not None:
                fields["pipeline_progress"] = max(0.0, min(100.0, float(pipeline_progress)))
            if conversion_metadata is not None:
                fields["conversion_metadata"] = conversion_metadata
            if raw_outputs is not None:
                fields["raw_outputs"] = list(raw_outputs)
            if zarr_outputs is not None:
                fields["zarr_outputs"] = list(zarr_outputs)
            self._store.update_job(job_id, **fields)
            if event_type:
                self._store.append_event(
                    job_id,
                    event_type,
                    {
                        "pipeline_state": pipeline_state.value,
                        "pipeline_step": pipeline_step,
                        **(event_payload or {}),
                    },
                )

    def merged_metadata(
        self,
        job_id: str,
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row_now = self._store.get_job_record(job_id) or JobRowRecord(job_id=job_id)
        existing_pipeline_metadata = dict(row_now.pipeline_metadata)
        merged = dict(existing_pipeline_metadata)
        if pipeline_metadata is not None:
            merged.update(pipeline_metadata)
        timeline = merged.get("timeline")
        if isinstance(timeline, dict):
            merged["timeline"] = dict(timeline)
        elif isinstance(existing_pipeline_metadata.get("timeline"), dict):
            merged["timeline"] = dict(existing_pipeline_metadata["timeline"])
        return merged
