from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, cast

import anyio
from pydantic import TypeAdapter

from nimbuschain_fetch.download.download_manager import DownloadCancelled, DownloadManager
from nimbuschain_fetch.geometry.aoi import parse_aoi
from nimbuschain_fetch.jobs.events import stream_events as stream_persisted_events
from nimbuschain_fetch.jobs.executor_inprocess import InProcessExecutor
from nimbuschain_fetch.jobs.store import ArtifactListFilters, JobListFilters, JobStore
from nimbuschain_fetch.manifest import build_manifest_entry, checksums_for_paths, write_manifest
from nimbuschain_fetch.models import (
    ArtifactListResponse,
    ArtifactRecord,
    ArtifactUpsertRequest,
    BatchJobCreateRequest,
    DownloadProductsRequest,
    JobCreateRequest,
    JobListResponse,
    JobResultResponse,
    JobState,
    JobStatusResponse,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.providers import CopernicusProvider, UsgsProvider
from nimbuschain_fetch.security.paths import sanitize_output_dir
from nimbuschain_fetch.jobs.store_factory import create_job_store
from nimbuschain_fetch.settings import Settings, get_settings


class JobNotFoundError(KeyError):
    pass


class JobCancelledError(RuntimeError):
    pass


class NimbusFetcher:
    """Core orchestrator for submission, execution and tracking of fetch jobs."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        store: JobStore | None = None,
        provider_registry: dict[str, type] | None = None,
    ):
        self.settings = settings or get_settings()
        self.store = store or create_job_store(self.settings)
        self.provider_registry = provider_registry or {
            "copernicus": CopernicusProvider,
            "usgs": UsgsProvider,
        }
        self._runtime_role = self.settings.runtime_role
        self._execution_enabled = self._runtime_role in {"all", "worker"}
        self._request_adapter = TypeAdapter(JobCreateRequest)
        self._executor = (
            InProcessExecutor(
                store=self.store,
                run_job=self._execute_job,
                max_concurrent_jobs=self.settings.nimbus_max_jobs,
                provider_limits=self.settings.provider_limits_map,
            )
            if self._execution_enabled
            else None
        )
        self._poller_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._worker_id = uuid.uuid4().hex
        self._worker_hostname = socket.gethostname()
        self._worker_pid = os.getpid()
        self._worker_started_at = datetime.now(timezone.utc).isoformat()
        self._cancel_check_cache: dict[str, tuple[float, bool]] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self.settings.ensure_runtime_dirs()
        if self._execution_enabled and self._executor is not None:
            self.store.requeue_incomplete_jobs()
            await self._executor.start()
            await self._enqueue_queued_jobs()
            self._publish_worker_heartbeat()
            self._heartbeat_task = asyncio.create_task(
                self._worker_heartbeat_loop(),
                name="nimbus-worker-heartbeat",
            )
            self._poller_task = asyncio.create_task(
                self._monitor_queued_jobs_loop(),
                name="nimbus-queue-poller",
            )
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        if self._poller_task:
            self._poller_task.cancel()
            try:
                await self._poller_task
            except asyncio.CancelledError:
                pass
            self._poller_task = None
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        if self._executor is not None:
            await self._executor.stop()
        self._started = False

    async def submit_job(self, request: JobCreateRequest) -> str:
        if not self._started:
            await self.start()

        request_payload = cast(dict[str, Any], request.model_dump(mode="json"))
        job_id = uuid.uuid4().hex
        self.store.create_job(
            job_id=job_id,
            job_type=request_payload["job_type"],
            provider=request_payload["provider"],
            collection=request_payload["collection"],
            request_payload=request_payload,
        )
        self.store.append_event(job_id, "job.queued", {"state": JobState.queued.value})
        if self._execution_enabled and self._executor is not None:
            await self._executor.submit(job_id)
        return job_id

    async def submit_batch(self, request: BatchJobCreateRequest) -> list[str]:
        job_ids: list[str] = []
        for job in request.jobs:
            job_ids.append(await self.submit_job(job))
        return job_ids

    def get_job(self, job_id: str) -> JobStatusResponse:
        row = self.store.get_job(job_id)
        if not row:
            raise JobNotFoundError(job_id)
        return self._to_status_response(row)

    def get_result(self, job_id: str) -> JobResultResponse:
        row = self.store.get_result(job_id)
        if not row:
            raise JobNotFoundError(job_id)
        return JobResultResponse.model_validate(row)

    async def cancel_job(self, job_id: str) -> bool:
        row = self.store.get_job(job_id)
        if not row:
            raise JobNotFoundError(job_id)

        state = row["state"]
        if state in {JobState.succeeded.value, JobState.failed.value, JobState.cancelled.value}:
            return False

        if state == JobState.queued.value:
            self._mark_cancelled(job_id, "cancelled_while_queued")
            return True

        self.store.update_job(job_id, state=JobState.cancel_requested.value)
        self.store.append_event(job_id, "job.cancel_requested", {"state": JobState.cancel_requested.value})
        if self._execution_enabled and self._executor is not None:
            await self._executor.cancel(job_id)
        return True

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
        rows, total = self.store.list_jobs(
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
            items=[self._to_status_response(row) for row in rows],
            total=total,
            page=max(1, page),
            page_size=max(1, page_size),
        )

    def upsert_artifact(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        artifact_id = hashlib.md5(
            request.artifact_uri.encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()
        row = self.store.upsert_artifact(
            {
                **request.model_dump(mode="python"),
                "artifact_id": artifact_id,
                "artifact_type": request.artifact_type.value,
                "provider": request.provider.value if request.provider else None,
            }
        )
        return ArtifactRecord.model_validate(row)

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
        rows, total = self.store.list_artifacts(
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
            items=[ArtifactRecord.model_validate(row) for row in rows],
            total=total,
            page=max(1, page),
            page_size=max(1, page_size),
        )

    async def stream_events(self, *, job_id: str | None, since: int | None):
        async for item in stream_persisted_events(
            self.store,
            job_id=job_id,
            since_id=since,
            heartbeat_seconds=10.0,
            poll_interval=0.4,
        ):
            yield item

    async def _monitor_queued_jobs_loop(self) -> None:
        while True:
            self.store.requeue_stale_running_jobs(self.settings.nimbus_stale_job_seconds)
            await self._enqueue_queued_jobs()
            await anyio.sleep(float(self.settings.nimbus_queue_poll_seconds))

    async def _worker_heartbeat_loop(self) -> None:
        while True:
            self._publish_worker_heartbeat()
            await anyio.sleep(float(self.settings.nimbus_worker_heartbeat_seconds))

    async def _enqueue_queued_jobs(self) -> None:
        if self._executor is None:
            return
        queued, _ = self.store.list_jobs(
            JobListFilters(state=JobState.queued.value, page=1, page_size=5000)
        )
        for row in queued:
            await self._executor.submit(str(row["job_id"]))

    def get_worker_status(self) -> dict[str, Any]:
        stale_after = max(5, int(self.settings.nimbus_worker_stale_seconds))
        pruned_workers = int(self.store.prune_stale_workers(stale_after))
        workers = list(self.store.list_workers())
        now = datetime.now(timezone.utc)
        alive_workers: list[dict[str, Any]] = []
        stale_workers: list[dict[str, Any]] = []

        running_rows, running_total = self.store.list_jobs(
            JobListFilters(
                states=(JobState.running.value, JobState.cancel_requested.value),
                page=1,
                page_size=max(1000, self.settings.nimbus_max_jobs * 200),
            )
        )
        queued_rows, queued_total = self.store.list_jobs(
            JobListFilters(
                state=JobState.queued.value,
                page=1,
                page_size=max(1000, self.settings.nimbus_max_jobs * 200),
            )
        )
        running_by_provider: dict[str, int] = {}
        queued_by_provider: dict[str, int] = {}
        configured_provider_limits = {
            str(name).strip().lower(): max(1, int(limit))
            for name, limit in self.settings.provider_limits_map.items()
        }

        running_by_worker: dict[str, int] = {}
        cancel_requested_by_worker: dict[str, int] = {}
        for row in running_rows:
            worker_id = str(row.get("worker_id") or "").strip()
            provider_name = str(row.get("provider") or "").strip().lower()
            if provider_name:
                running_by_provider[provider_name] = running_by_provider.get(provider_name, 0) + 1
            if not worker_id:
                continue
            state = str(row.get("state") or "").strip().lower()
            if state == JobState.cancel_requested.value:
                cancel_requested_by_worker[worker_id] = cancel_requested_by_worker.get(worker_id, 0) + 1
            else:
                running_by_worker[worker_id] = running_by_worker.get(worker_id, 0) + 1
        for row in queued_rows:
            provider_name = str(row.get("provider") or "").strip().lower()
            if provider_name:
                queued_by_provider[provider_name] = queued_by_provider.get(provider_name, 0) + 1

        worker_payloads: list[dict[str, Any]] = []
        capacity_total = 0
        capacity_used = 0
        provider_capacity_total: dict[str, int] = {}

        for worker in workers:
            last_seen = self._parse_iso(worker.get("last_seen_at"))
            age_seconds = None
            is_alive = False
            if last_seen is not None:
                age_seconds = max(0.0, (now - last_seen).total_seconds())
                is_alive = age_seconds <= stale_after
            worker_id = str(worker.get("worker_id") or "")
            running_count = int(running_by_worker.get(worker_id, worker.get("active_running_jobs", 0) or 0))
            cancel_requested_count = int(
                cancel_requested_by_worker.get(
                    worker_id,
                    worker.get("active_cancel_requested_jobs", 0) or 0,
                )
            )
            max_concurrent = max(1, int(worker.get("max_concurrent_jobs", 1) or 1))
            worker_capacity_used = running_count + cancel_requested_count
            capacity_total += max_concurrent
            capacity_used += min(max_concurrent, worker_capacity_used)
            item = {
                **worker,
                "status": "alive" if is_alive else "stale",
                "age_seconds": age_seconds,
                "active_running_jobs": running_count,
                "active_cancel_requested_jobs": cancel_requested_count,
                "available_slots": max(0, max_concurrent - worker_capacity_used),
            }
            worker_payloads.append(item)
            if is_alive:
                alive_workers.append(item)
                provider_limits = dict(item.get("provider_limits") or {})
                provider_names = set(configured_provider_limits) | set(
                    str(name).strip().lower() for name in provider_limits.keys() if str(name).strip()
                )
                for provider_name in provider_names:
                    limit = provider_limits.get(
                        provider_name,
                        configured_provider_limits.get(provider_name, 1),
                    )
                    provider_capacity_total[provider_name] = (
                        provider_capacity_total.get(provider_name, 0) + max(1, int(limit or 1))
                    )
            else:
                stale_workers.append(item)

        capacity_available = max(0, capacity_total - capacity_used)
        can_accept_work = bool(alive_workers) and capacity_available > 0
        ready = bool(alive_workers)
        status = "ready" if ready else "not_ready"
        if ready and not can_accept_work:
            status = "saturated"

        provider_names = (
            set(configured_provider_limits)
            | set(running_by_provider)
            | set(queued_by_provider)
            | set(provider_capacity_total)
        )
        provider_capacity: dict[str, dict[str, int | bool]] = {}
        for provider_name in sorted(provider_names):
            total_limit = max(
                0,
                int(
                    provider_capacity_total.get(
                        provider_name,
                        configured_provider_limits.get(provider_name, 0),
                    )
                ),
            )
            running_count = int(running_by_provider.get(provider_name, 0))
            queued_count = int(queued_by_provider.get(provider_name, 0))
            available = max(0, total_limit - running_count)
            provider_capacity[provider_name] = {
                "limit_total": total_limit,
                "running": running_count,
                "queued": queued_count,
                "available": available,
                "blocked_by_limit": queued_count > 0 and available <= 0,
            }

        return {
            "status": status,
            "ready": ready,
            "timestamp": now.isoformat(),
            "worker_stale_seconds": stale_after,
            "workers_pruned": pruned_workers,
            "workers_alive": len(alive_workers),
            "workers_stale": len(stale_workers),
            "workers_total": len(worker_payloads),
            "queued_jobs": int(queued_total),
            "running_jobs": int(running_total),
            "capacity_total": capacity_total,
            "capacity_used": capacity_used,
            "capacity_available": capacity_available,
            "can_accept_work": can_accept_work,
            "provider_capacity": provider_capacity,
            "workers": worker_payloads,
        }

    def _is_job_cancel_requested(self, job_id: str) -> bool:
        now = time.monotonic()
        cached = self._cancel_check_cache.get(job_id)
        if cached and now < cached[0]:
            return cached[1]

        row = self.store.get_job(job_id)
        is_cancelled = bool(
            row
            and row.get("state")
            in {JobState.cancel_requested.value, JobState.cancelled.value}
        )
        self._cancel_check_cache[job_id] = (now + 0.5, is_cancelled)
        return is_cancelled

    async def _execute_job(self, job_id: str, is_cancelled: Callable[[], bool]) -> None:
        row = self.store.get_job(job_id)
        if not row:
            return

        if not self.store.claim_job_for_execution(job_id, self._worker_id):
            return

        def is_cancelled_now() -> bool:
            return is_cancelled() or self._is_job_cancel_requested(job_id)

        if is_cancelled_now():
            self._mark_cancelled(job_id, "cancelled_before_start")
            return

        self.store.update_job(
            job_id,
            state=JobState.running.value,
            started_at=self._now_iso(),
            finished_at=None,
            progress=0.0,
            errors=[],
        )
        self.store.append_event(job_id, "job.started", {"state": JobState.running.value})

        request = self._request_adapter.validate_python(row["request"])
        output_dir = sanitize_output_dir(
            self.settings.nimbus_data_dir,
            getattr(request, "output_dir", None),
            fallback_name=job_id,
        )

        file_progress: dict[str, dict[str, int | None]] = {}
        aggregate = {
            "bytes_downloaded": 0,
            "bytes_total": 0,
            "last_emit": 0.0,
            "last_bytes": 0,
            "last_time": time.monotonic(),
        }

        def emit_progress(file_name: str, delta: int, downloaded: int, total: int | None) -> None:
            if is_cancelled_now():
                raise JobCancelledError("Job cancellation requested.")

            aggregate["bytes_downloaded"] += max(0, int(delta))
            file_progress[file_name] = {"downloaded": downloaded, "total": total}
            known_total = sum(
                int(item["total"])
                for item in file_progress.values()
                if item.get("total") is not None
            )
            aggregate["bytes_total"] = max(int(aggregate["bytes_total"]), known_total)

            now_mono = time.monotonic()
            elapsed = max(0.001, now_mono - float(aggregate["last_time"]))
            delta_bytes = int(aggregate["bytes_downloaded"]) - int(aggregate["last_bytes"])
            speed = max(0.0, delta_bytes / elapsed)

            progress_pct = 0.0
            if aggregate["bytes_total"] > 0:
                progress_pct = min(
                    99.0,
                    100.0 * int(aggregate["bytes_downloaded"]) / int(aggregate["bytes_total"]),
                )

            # Throttle DB writes and events.
            if now_mono - float(aggregate["last_emit"]) >= 0.25 or delta == 0:
                self.store.update_job(
                    job_id,
                    progress=progress_pct,
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=int(aggregate["bytes_total"]),
                )
                self.store.append_event(
                    job_id,
                    "job.progress",
                    {
                        "file": file_name,
                        "bytes": int(aggregate["bytes_downloaded"]),
                        "bytes_total": int(aggregate["bytes_total"]),
                        "speed": speed,
                        "status": JobState.running.value,
                    },
                )
                aggregate["last_emit"] = now_mono
                aggregate["last_time"] = now_mono
                aggregate["last_bytes"] = int(aggregate["bytes_downloaded"])

        try:
            result = await anyio.to_thread.run_sync(
                self._run_provider_job,
                job_id,
                request,
                output_dir,
                emit_progress,
                is_cancelled_now,
            )

            if is_cancelled_now():
                self._mark_cancelled(job_id, "cancelled_after_download")
                return

            paths = result["paths"]
            metadata = result["metadata"]

            checksums = checksums_for_paths(paths)
            manifest_entry = build_manifest_entry(
                job_id=job_id,
                provider=str(row["provider"]),
                collection=str(row["collection"]),
                metadata=metadata,
                paths=paths,
                checksums=checksums,
            )
            manifest_path = write_manifest(output_dir, manifest_entry)

            all_paths = [*paths, str(manifest_path)]
            checksums[str(manifest_path)] = checksums_for_paths([str(manifest_path)]).get(
                str(manifest_path), ""
            )

            self.store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "paths": all_paths,
                    "checksums": checksums,
                    "metadata": metadata,
                    "manifest_entry": manifest_entry,
                },
            )
            self.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                progress=100.0,
                finished_at=self._now_iso(),
                bytes_downloaded=int(aggregate["bytes_downloaded"]),
                bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
            )
            self.store.append_event(
                job_id,
                "job.succeeded",
                {
                    "status": JobState.succeeded.value,
                    "paths": all_paths,
                },
            )
        except (DownloadCancelled, JobCancelledError):
            self._mark_cancelled(job_id, "cancelled_during_download")
        except Exception as exc:
            self.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=self._now_iso(),
                errors=[str(exc)],
            )
            self.store.append_event(
                job_id,
                "job.failed",
                {"status": JobState.failed.value, "error": str(exc)},
            )
        finally:
            self._cancel_check_cache.pop(job_id, None)

    def _run_provider_job(
        self,
        job_id: str,
        request: JobCreateRequest,
        output_dir,
        progress_callback,
        is_cancelled,
    ) -> dict[str, Any]:
        provider_name = self._provider_name(request.provider)
        provider_limit = self.settings.provider_limits_map.get(provider_name, 1)

        download_manager = DownloadManager(
            max_concurrent=provider_limit,
            progress_callback=progress_callback,
            cancel_checker=is_cancelled,
        )
        provider = self._build_provider(provider_name, download_manager)

        if isinstance(request, SearchDownloadRequest):
            if is_cancelled():
                raise JobCancelledError("cancelled")
            geom = parse_aoi(request.aoi.model_dump())

            product_ids = provider.search_products(
                collection=request.collection,
                product_type=request.product_type,
                start_date=request.start_date.isoformat(),
                end_date=request.end_date.isoformat(),
                aoi=geom,
                tile_id=request.tile_id,
            )
            self.store.append_event(
                job_id,
                "job.products_found",
                {"count": len(product_ids)},
            )
            if is_cancelled():
                raise JobCancelledError("cancelled")

            if not product_ids:
                return {
                    "paths": [],
                    "metadata": {
                        "job_type": request.job_type,
                        "provider": provider_name,
                        "collection": request.collection,
                        "product_type": request.product_type,
                        "products_found": 0,
                        "products_downloaded": 0,
                        "output_dir": str(output_dir),
                    },
                }

            paths = provider.download_products(product_ids=product_ids, output_dir=str(output_dir))
            return {
                "paths": paths,
                "metadata": {
                    "job_type": request.job_type,
                    "provider": provider_name,
                    "collection": request.collection,
                    "product_type": request.product_type,
                    "products_found": len(product_ids),
                    "products_downloaded": len(paths),
                    "output_dir": str(output_dir),
                },
            }

        request = cast(DownloadProductsRequest, request)
        if hasattr(provider, "dataset"):
            setattr(provider, "dataset", request.collection)
        paths = provider.download_products(product_ids=request.product_ids, output_dir=str(output_dir))
        return {
            "paths": paths,
            "metadata": {
                "job_type": request.job_type,
                "provider": provider_name,
                "collection": request.collection,
                "products_requested": len(request.product_ids),
                "products_downloaded": len(paths),
                "output_dir": str(output_dir),
            },
        }

    @staticmethod
    def _provider_name(value: ProviderName | str) -> str:
        if isinstance(value, ProviderName):
            return value.value
        return str(value).strip().lower()

    def _build_provider(self, provider_name: str, download_manager: DownloadManager):
        provider_cls = self.provider_registry.get(provider_name)
        if not provider_cls:
            raise ValueError(f"Unsupported provider '{provider_name}'.")
        return provider_cls(self.settings, download_manager)

    def _mark_cancelled(self, job_id: str, reason: str) -> None:
        self.store.update_job(
            job_id,
            state=JobState.cancelled.value,
            finished_at=self._now_iso(),
        )
        self.store.append_event(
            job_id,
            "job.cancelled",
            {"status": JobState.cancelled.value, "reason": reason},
        )

    def _to_status_response(self, row: dict[str, Any]) -> JobStatusResponse:
        started_at = self._parse_iso(row.get("started_at"))
        finished_at = self._parse_iso(row.get("finished_at"))
        duration_seconds: float | None = None
        if started_at is not None:
            end_time = finished_at or datetime.now(timezone.utc)
            duration_seconds = max(0.0, (end_time - started_at).total_seconds())

        return JobStatusResponse(
            job_id=row["job_id"],
            state=JobState(row["state"]),
            progress=float(row["progress"]),
            bytes_downloaded=int(row["bytes_downloaded"]),
            bytes_total=int(row["bytes_total"]),
            product_type=row.get("product_type"),
            tile_id=row.get("tile_id"),
            created_at=self._parse_iso(row.get("created_at")),
            updated_at=self._parse_iso(row.get("updated_at")),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            errors=list(row.get("errors", [])),
            provider=ProviderName(row["provider"]),
            collection=str(row["collection"]),
        )

    @staticmethod
    def _parse_iso(value: str | datetime | None) -> datetime | None:
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

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _publish_worker_heartbeat(self) -> dict[str, Any] | None:
        if not self._execution_enabled:
            return None
        running_rows, running_total = self.store.list_jobs(
            JobListFilters(
                states=(JobState.running.value,),
                worker_id=self._worker_id,
                page=1,
                page_size=max(1, self.settings.nimbus_max_jobs * 2),
            )
        )
        cancel_rows, cancel_total = self.store.list_jobs(
            JobListFilters(
                states=(JobState.cancel_requested.value,),
                worker_id=self._worker_id,
                page=1,
                page_size=max(1, self.settings.nimbus_max_jobs * 2),
            )
        )
        queued_rows, queued_total = self.store.list_jobs(
            JobListFilters(
                state=JobState.queued.value,
                page=1,
                page_size=1,
            )
        )
        _ = running_rows, cancel_rows, queued_rows
        return self.store.upsert_worker_heartbeat(
            self._worker_id,
            {
                "runtime_role": self._runtime_role,
                "execution_enabled": self._execution_enabled,
                "max_concurrent_jobs": self.settings.nimbus_max_jobs,
                "queue_poll_seconds": self.settings.nimbus_queue_poll_seconds,
                "heartbeat_interval_seconds": self.settings.nimbus_worker_heartbeat_seconds,
                "provider_limits": self.settings.provider_limits_map,
                "hostname": self._worker_hostname,
                "pid": self._worker_pid,
                "active_running_jobs": running_total,
                "active_cancel_requested_jobs": cancel_total,
                "queue_backlog": queued_total,
                "started_at": self._worker_started_at,
                "last_seen_at": self._now_iso(),
                "metadata": {
                    "runtime_role": self._runtime_role,
                    "executor_present": self._executor is not None,
                },
            },
        )
