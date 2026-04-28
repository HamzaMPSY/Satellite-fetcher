from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import anyio

from nimbuschain_fetch.download.coordinator import DownloadCoordinatorStore
from nimbuschain_fetch.jobs.events import stream_events as stream_persisted_events
from nimbuschain_fetch.jobs.store import ArtifactListFilters, JobListFilters
from nimbuschain_fetch.models import ArtifactListResponse, ArtifactRecord, JobListResponse, JobState, PipelineState


class FetcherOperationsSupport:
    """Runtime reset, job listing, artifact listing, and queue-monitoring helpers."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    async def cancel_job(self, job_id: str) -> bool:
        row_record = self._rt._get_job_row_record(job_id)
        if row_record is None:
            raise self._rt.job_not_found_error_cls(job_id)

        state = str(row_record.state or "")
        if state in {JobState.succeeded.value, JobState.failed.value, JobState.cancelled.value}:
            return False

        if state == JobState.queued.value:
            self._rt._mark_cancelled(job_id, "cancelled_while_queued")
            return True

        self._rt.store.update_job(job_id, state=JobState.cancel_requested.value)
        self._rt.store.append_event(job_id, "job.cancel_requested", {"state": JobState.cancel_requested.value})
        if self._rt._execution_enabled and self._rt._executor is not None:
            await self._rt._executor.cancel(job_id)
        return True

    def list_jobs_by_states(self, states: tuple[str, ...]) -> list[dict[str, Any]]:
        page = 1
        rows: list[dict[str, Any]] = []
        while True:
            batch, total = self._rt.store.list_jobs(
                JobListFilters(
                    states=states,
                    sort_by="updated_at",
                    sort_desc=True,
                    page=page,
                    page_size=200,
                )
            )
            rows.extend(batch)
            if len(rows) >= int(total) or not batch:
                break
            page += 1
        return rows

    async def reset_runtime_state(self) -> dict[str, Any]:
        active_rows = self.list_jobs_by_states(
            (
                JobState.queued.value,
                JobState.running.value,
                JobState.cancel_requested.value,
            )
        )
        active_job_ids = [
            str(row.get("job_id") or "").strip()
            for row in active_rows
            if str(row.get("job_id") or "").strip()
        ]

        executor_cancelled = 0
        if self._rt._execution_enabled and self._rt._executor is not None:
            for job_id in active_job_ids:
                try:
                    await self._rt._executor.cancel(job_id)
                    executor_cancelled += 1
                except Exception:
                    continue

        for job_id in active_job_ids:
            self._rt._mark_cancelled(job_id, "runtime_reset")
            self._rt._cancel_check_cache[job_id] = (time.monotonic() + 60.0, True)

        coordinator_reason = "Download cancelled by runtime reset."
        if self._rt._download_coordinator is not None:
            coordinator_reset = self._rt._download_coordinator.reset_runtime_state(reason=coordinator_reason)
        else:
            coordinator_store = DownloadCoordinatorStore(self._rt.settings.download_coordinator_db_path)
            try:
                task_ids = coordinator_store.cancel_all_non_terminal_tasks(reason=coordinator_reason)
            finally:
                coordinator_store.close()
            coordinator_reset = {
                "tasks_cancelled": len(task_ids),
                "task_ids": task_ids,
            }

        workers_cleared = int(self._rt.store.clear_workers())

        return {
            "status": "ok",
            "history_preserved": True,
            "jobs_cancelled": len(active_job_ids),
            "job_ids": active_job_ids,
            "executor_cancellations_requested": executor_cancelled,
            "coordinator_tasks_cancelled": int(coordinator_reset.get("tasks_cancelled", 0) or 0),
            "coordinator_task_ids": list(coordinator_reset.get("task_ids") or []),
            "worker_heartbeats_cleared": workers_cleared,
        }

    def list_jobs(
        self,
        *,
        state: str | None,
        states: tuple[str, ...] = (),
        provider: str | None,
        collection: str | None = None,
        product_type: str | None = None,
        job_id_query: str | None = None,
        date_from: datetime | None,
        date_to: datetime | None,
        updated_from: datetime | None = None,
        updated_to: datetime | None = None,
        sort_by: str = "updated_at",
        sort_desc: bool = True,
        page: int,
        page_size: int,
    ) -> JobListResponse:
        rows, total = self._rt.store.list_job_records(
            JobListFilters(
                state=state,
                states=states,
                provider=provider,
                collection=collection,
                product_type=product_type,
                job_id_query=job_id_query,
                date_from=date_from,
                date_to=date_to,
                updated_from=updated_from,
                updated_to=updated_to,
                sort_by=sort_by,
                sort_desc=sort_desc,
                page=page,
                page_size=page_size,
            )
        )
        return JobListResponse(
            items=[
                self._rt._to_status_response(
                    self._rt._normalize_backend_paths_in_job_row(
                        self._rt._normalize_historical_job_row(row.to_row())
                    )
                )
                for row in rows
            ],
            total=total,
            page=max(1, page),
            page_size=max(1, page_size),
        )

    def list_artifacts(
        self,
        *,
        artifact_type: str | None,
        provider: str | None,
        collection: str | None,
        scene_id: str | None,
        job_id: str | None,
        uri_query: str | None,
        date_from: datetime | None,
        date_to: datetime | None,
        page: int,
        page_size: int,
    ) -> ArtifactListResponse:
        rows, total = self._rt.store.list_artifact_records(
            ArtifactListFilters(
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
        )
        return ArtifactListResponse(
            items=[
                ArtifactRecord.model_validate(
                    self._rt._normalize_backend_paths_in_artifact_row(row.to_row())
                )
                for row in rows
            ],
            total=total,
            page=max(1, page),
            page_size=max(1, page_size),
        )

    async def stream_events(self, *, job_id: str | None, since: int | None):
        async for item in stream_persisted_events(
            self._rt.store,
            job_id=job_id,
            since_id=since,
            heartbeat_seconds=10.0,
            poll_interval=0.4,
        ):
            yield item

    async def monitor_queued_jobs_loop(self) -> None:
        while True:
            stale_job_ids = self._rt.store.requeue_stale_running_jobs(self._rt.settings.nimbus_stale_job_seconds)
            self._rt._fail_interrupted_mask_jobs(
                job_ids=stale_job_ids,
                reason="Mask job marked stale after exceeding the worker timeout. Submit a new mask job from the Mask tab.",
                event_type="job.mask_failed_stale_timeout",
            )
            await self.enqueue_queued_jobs()
            await anyio.sleep(float(self._rt.settings.nimbus_queue_poll_seconds))

    async def enqueue_queued_jobs(self) -> None:
        if self._rt._executor is None:
            return
        queued, _ = self._rt.store.list_jobs(
            JobListFilters(state=JobState.queued.value, page=1, page_size=5000)
        )
        for row in queued:
            await self._rt._executor.submit(str(row["job_id"]))
