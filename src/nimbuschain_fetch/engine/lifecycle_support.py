from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from typing import Any

from nimbuschain_fetch.application.job_execution import JobExecutionContext
from nimbuschain_fetch.models import JobState


class FetcherLifecycleSupport:
    """Lifecycle and job-dispatch helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    async def start(self) -> None:
        if self._rt._started:
            return
        self._rt.settings.ensure_runtime_dirs()
        if self._rt._execution_enabled and self._rt._executor is not None:
            requeued_job_ids = self._rt.store.requeue_incomplete_jobs()
            self._rt._retire_legacy_mask_jobs()
            self._rt._fail_interrupted_mask_jobs(
                job_ids=requeued_job_ids,
                reason="Mask job interrupted during service restart. Submit a new mask job from the Mask tab.",
                event_type="job.mask_failed_after_restart",
            )
            await self._rt._executor.start()
            await self._rt._enqueue_queued_jobs()
            self._rt._publish_worker_heartbeat()
            self._rt._start_worker_heartbeat_thread()
            self._rt._poller_task = self._rt.asyncio.create_task(
                self._rt._monitor_queued_jobs_loop(),
                name="nimbus-queue-poller",
            )
        self._rt._started = True

    async def stop(self) -> None:
        if not self._rt._started:
            return
        if self._rt._poller_task:
            self._rt._poller_task.cancel()
            try:
                await self._rt._poller_task
            except self._rt.asyncio.CancelledError:
                pass
            self._rt._poller_task = None
        self._rt._stop_worker_heartbeat_thread()
        if self._rt._executor is not None:
            await self._rt._executor.stop()
        if self._rt._zarr_converter is not None and hasattr(self._rt._zarr_converter, "close"):
            self._rt._zarr_converter.close()
            self._rt._zarr_converter = None
        if self._rt._mask_service is not None and hasattr(self._rt._mask_service, "close"):
            self._rt._mask_service.close()
            self._rt._mask_service = None
        if self._rt._download_coordinator is not None:
            self._rt._download_coordinator.close()
            self._rt._download_coordinator = None
        self._rt._started = False

    def is_job_cancel_requested(self, job_id: str) -> bool:
        now = time.monotonic()
        cached = self._rt._cancel_check_cache.get(job_id)
        if cached and now < cached[0]:
            return cached[1]

        row = self._rt.store.get_job_record(job_id)
        is_cancelled = bool(
            row
            and row.state in {JobState.cancel_requested.value, JobState.cancelled.value}
        )
        self._rt._cancel_check_cache[job_id] = (now + 0.5, is_cancelled)
        return is_cancelled

    async def execute_job(self, job_id: str, is_cancelled: Callable[[], bool]) -> None:
        row_record = self._rt._get_job_row_record(job_id)
        if row_record is None:
            return
        row = row_record.to_row()

        if not self._rt.store.claim_job_for_execution(job_id, self._rt._worker_id):
            return

        def is_cancelled_now() -> bool:
            return is_cancelled() or self._rt._is_job_cancel_requested(job_id)

        if is_cancelled_now():
            self._rt._mark_cancelled(job_id, "cancelled_before_start")
            return

        self._rt.store.update_job(
            job_id,
            state=JobState.running.value,
            started_at=self._rt._now_iso(),
            finished_at=None,
            progress=0.0,
            errors=[],
        )
        self._rt.store.append_event(job_id, "job.started", {"state": JobState.running.value})

        job_type = str(row.get("job_type") or "").strip().lower()
        handler = self._rt._job_execution_registry.resolve(job_type)
        if handler is not None:
            execution_result = handler.execute(
                JobExecutionContext(
                    job_id=job_id,
                    row=row,
                    is_cancelled_now=is_cancelled_now,
                )
            )
            if inspect.isawaitable(execution_result):
                await execution_result
            return

        await self._rt._fetch_job_workflow.execute(
            job_id=job_id,
            row=row,
            is_cancelled_now=is_cancelled_now,
        )
