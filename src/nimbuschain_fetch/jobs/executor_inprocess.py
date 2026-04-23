from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.jobs.store import JobStore


RunJobCallable = Callable[[str, Callable[[], bool]], Awaitable[None]]


class InProcessExecutor(ExecutorBackend):
    """Async in-process scheduler with global and per-provider limits."""

    def __init__(
        self,
        *,
        store: JobStore,
        run_job: RunJobCallable,
        max_concurrent_jobs: int,
        provider_limits: dict[str, int],
    ):
        self._store = store
        self._run_job = run_job
        self._max_concurrent_jobs = max(1, int(max_concurrent_jobs))
        self._provider_limits = {k.lower(): max(1, int(v)) for k, v in provider_limits.items()}

        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._enqueued_ids: set[str] = set()
        self._active_ids: set[str] = set()
        self._provider_semaphores: dict[str, asyncio.Semaphore] = {}
        self._global_semaphore = asyncio.Semaphore(self._max_concurrent_jobs)
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._workers = [
            asyncio.create_task(self._worker_loop(index), name=f"nimbus-worker-{index}")
            for index in range(self._max_concurrent_jobs)
        ]

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        for task in self._workers:
            task.cancel()
        if self._workers:
            try:
                await asyncio.gather(*self._workers, return_exceptions=True)
            except RuntimeError:
                # Defensive guard for teardown paths where the event loop is already closing.
                pass
        self._workers.clear()
        self._enqueued_ids.clear()
        self._active_ids.clear()

    async def submit(self, job_id: str) -> None:
        if job_id in self._enqueued_ids or job_id in self._active_ids:
            return
        self._enqueued_ids.add(job_id)
        await self._queue.put(job_id)

    async def cancel(self, job_id: str) -> None:
        event = self._cancel_events.setdefault(job_id, asyncio.Event())
        event.set()

    def _provider_semaphore(self, provider: str) -> asyncio.Semaphore:
        key = provider.lower().strip()
        if key not in self._provider_semaphores:
            limit = self._provider_limits.get(key, 1)
            self._provider_semaphores[key] = asyncio.Semaphore(limit)
        return self._provider_semaphores[key]

    async def _worker_loop(self, worker_index: int) -> None:
        _ = worker_index
        while True:
            job_id = await self._queue.get()
            self._enqueued_ids.discard(job_id)
            self._active_ids.add(job_id)
            cancel_event = self._cancel_events.setdefault(job_id, asyncio.Event())
            try:
                row = self._store.get_job(job_id)
                if not row:
                    continue

                provider = str(row.get("provider", "")).lower()
                provider_sem = self._provider_semaphore(provider)

                async with self._global_semaphore:
                    async with provider_sem:
                        await self._run_job(job_id, cancel_event.is_set)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Job errors are handled in run_job and persisted in store.
                pass
            finally:
                self._queue.task_done()
                self._cancel_events.pop(job_id, None)
                self._active_ids.discard(job_id)
