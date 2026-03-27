from __future__ import annotations

import asyncio
import hashlib
import re
import os
import socket
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
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
    ArtifactType,
    ArtifactUpsertRequest,
    BatchJobCreateRequest,
    DownloadProductsRequest,
    JobConvertRequest,
    JobCreateRequest,
    JobListResponse,
    JobResultResponse,
    JobState,
    JobStatusResponse,
    JobWaterMaskRequest,
    JobWaterMaskResponse,
    PipelineState,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.providers import CopernicusProvider, UsgsProvider
from nimbuschain_fetch.security.paths import sanitize_output_dir
from nimbuschain_fetch.jobs.store_factory import create_job_store
from nimbuschain_fetch.settings import Settings, get_settings
from nimbuschain_mask_service.service import MaskService
from nimbuschain_zarr_service.service import ZarrConversionService


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
        self._zarr_converter: ZarrConversionService | None = None
        self._mask_service: MaskService | None = None
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
        row = self._normalize_historical_job_row(row)
        row = self._normalize_backend_paths_in_job_row(row)
        return self._to_status_response(row)

    def get_result(self, job_id: str) -> JobResultResponse:
        job_row = self.store.get_job(job_id)
        if not job_row:
            raise JobNotFoundError(job_id)
        job_row = self._normalize_historical_job_row(job_row)
        job_row = self._normalize_backend_paths_in_job_row(job_row)
        result = self.store.get_result(job_id) or {}
        if not result and not any(
            [
                list(job_row.get("raw_outputs") or []),
                list(job_row.get("zarr_outputs") or []),
                list(job_row.get("watermask_outputs") or []),
            ]
        ):
            raise JobNotFoundError(job_id)

        result = self._normalize_backend_paths_in_result_payload(result)
        normalized_result = {
            "job_id": job_id,
            "paths": list(result.get("paths") or []),
            "raw_outputs": list(result.get("raw_outputs") or job_row.get("raw_outputs") or []),
            "zarr_outputs": list(result.get("zarr_outputs") or job_row.get("zarr_outputs") or []),
            "watermask_outputs": list(result.get("watermask_outputs") or job_row.get("watermask_outputs") or []),
            "checksums": dict(result.get("checksums") or {}),
            "metadata": dict(result.get("metadata") or {}),
            "manifest_entry": dict(result.get("manifest_entry") or {}),
            "pipeline_metadata": dict(result.get("pipeline_metadata") or job_row.get("pipeline_metadata") or {}),
            "conversion_metadata": dict(result.get("conversion_metadata") or job_row.get("conversion_metadata") or {}),
        }
        return JobResultResponse.model_validate(normalized_result)

    def apply_watermask_existing_job(self, job_id: str, request: JobWaterMaskRequest) -> JobWaterMaskResponse:
        row = self.store.get_job(job_id)
        if not row:
            raise JobNotFoundError(job_id)
        row = self._normalize_backend_paths_in_job_row(row)
        state = str(row.get("state") or "")
        if state in {JobState.queued.value, JobState.running.value, JobState.cancel_requested.value}:
            raise ValueError("Water mask is only allowed when the job is not actively running.")

        result = self._normalize_backend_paths_in_result_payload(self.store.get_result(job_id) or {})
        available_zarr_uris = self._job_related_zarr_uris(job_id=job_id, row=row, result=result)
        if not available_zarr_uris:
            raise ValueError("No Zarr output is attached to this job. Run the Zarr conversion first.")

        selected_zarr_uri = self._normalize_backend_path(str(request.zarr_uri or available_zarr_uris[0]).strip())
        if selected_zarr_uri not in available_zarr_uris:
            raise ValueError("The requested Zarr URI is not attached to this job lineage.")

        zarr_context = self._resolve_zarr_context(
            job_id=job_id,
            row=row,
            result=result,
            zarr_uri=selected_zarr_uri,
            scene_id_override=request.scene_id,
            product_type_override=request.product_type,
        )
        self.store.append_event(
            job_id,
            "job.water_masking",
            {
                "zarr_uri": selected_zarr_uri,
                "scene_id": zarr_context["scene_id"],
                "provider": zarr_context["provider"],
                "collection": zarr_context["collection"],
                "product_type": zarr_context["product_type"],
            },
        )
        water_mask = self._masker().apply_omniwater_to_zarr(
            job_id=job_id,
            zarr_uri=selected_zarr_uri,
            provider=zarr_context["provider"],
            collection=zarr_context["collection"],
            product_type=zarr_context["product_type"],
            scene_id=zarr_context["scene_id"],
            acquisition_datetime=zarr_context["acquisition_datetime"],
            dataset_summary=zarr_context["dataset_summary"],
            fail_on_error=bool(request.fail_on_error),
            stage_callback=None,
        )
        watermask_outputs = self._collect_watermask_outputs(result=result, water_mask=water_mask)
        conversion_metadata = dict(result.get("conversion_metadata") or row.get("conversion_metadata") or {})
        conversion_metadata["water_mask"] = water_mask
        conversion_metadata["water_mask_source_zarr_uri"] = selected_zarr_uri
        conversion_metadata["water_mask_output_zarr_uri"] = str(water_mask.get("output_zarr_uri") or "").strip()

        pipeline_metadata = dict(result.get("pipeline_metadata") or row.get("pipeline_metadata") or {})
        pipeline_metadata["water_mask_last_run_at"] = self._now_iso()
        pipeline_metadata["water_mask_source_zarr_uri"] = selected_zarr_uri
        pipeline_metadata["water_mask_output_zarr_uri"] = str(water_mask.get("output_zarr_uri") or "").strip()

        updated_result = {
            "job_id": job_id,
            "paths": self._merge_paths(list(result.get("paths") or []), watermask_outputs),
            "raw_outputs": list(result.get("raw_outputs") or row.get("raw_outputs") or []),
            "zarr_outputs": list(result.get("zarr_outputs") or row.get("zarr_outputs") or []),
            "watermask_outputs": watermask_outputs,
            "checksums": dict(result.get("checksums") or {}),
            "metadata": dict(result.get("metadata") or {}),
            "manifest_entry": dict(result.get("manifest_entry") or {}),
            "pipeline_metadata": pipeline_metadata,
            "conversion_metadata": conversion_metadata,
        }
        self.store.set_result(job_id, updated_result)
        self.store.update_job(
            job_id,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            raw_outputs=updated_result["raw_outputs"],
            zarr_outputs=updated_result["zarr_outputs"],
            watermask_outputs=watermask_outputs,
        )
        if water_mask.get("status") == "written":
            self._register_masked_zarr_artifact(
                job_id=job_id,
                provider_name=zarr_context["provider"],
                collection=zarr_context["collection"],
                scene_id=zarr_context["scene_id"],
                source_zarr_uri=selected_zarr_uri,
                water_mask=water_mask,
                dataset_summary=zarr_context["dataset_summary"],
            )
            self._register_watermask_artifact(
                job_id=job_id,
                provider_name=zarr_context["provider"],
                collection=zarr_context["collection"],
                scene_id=zarr_context["scene_id"],
                zarr_uri=selected_zarr_uri,
                water_mask=water_mask,
            )
            self.store.append_event(
                job_id,
                "job.water_mask_written",
                {
                    "zarr_uri": selected_zarr_uri,
                    "water_mask": water_mask,
                },
            )
        else:
            self.store.append_event(
                job_id,
                "job.water_mask_failed",
                {
                    "zarr_uri": selected_zarr_uri,
                    "water_mask": water_mask,
                },
            )
        return JobWaterMaskResponse(
            job_id=job_id,
            zarr_uri=selected_zarr_uri,
            masked_zarr_uri=str(water_mask.get("output_zarr_uri") or "").strip() or None,
            water_mask=water_mask,
            watermask_outputs=watermask_outputs,
            job=self.get_job(job_id),
        )

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
            items=[self._to_status_response(self._normalize_backend_paths_in_job_row(self._normalize_historical_job_row(row))) for row in rows],
            total=total,
            page=max(1, page),
            page_size=max(1, page_size),
        )

    def upsert_artifact(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        normalized_artifact_uri = self._normalize_backend_path(request.artifact_uri)
        normalized_source_uri = self._normalize_backend_path(request.source_uri) if request.source_uri else None
        artifact_id = hashlib.md5(
            normalized_artifact_uri.encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()
        row = self.store.upsert_artifact(
            {
                **request.model_dump(mode="python"),
                "artifact_id": artifact_id,
                "artifact_type": request.artifact_type.value,
                "provider": request.provider.value if request.provider else None,
                "artifact_uri": normalized_artifact_uri,
                "source_uri": normalized_source_uri,
            }
        )
        row = self._normalize_backend_paths_in_artifact_row(row)
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
            items=[ArtifactRecord.model_validate(self._normalize_backend_paths_in_artifact_row(row)) for row in rows],
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

    def _converter(self) -> ZarrConversionService:
        if self._zarr_converter is None:
            self._zarr_converter = ZarrConversionService()
        return self._zarr_converter

    def _update_pipeline(
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
        fields: dict[str, Any] = {
            "pipeline_state": pipeline_state.value,
            "pipeline_step": pipeline_step,
        }
        if pipeline_progress is not None:
            fields["pipeline_progress"] = max(0.0, min(100.0, float(pipeline_progress)))
        if pipeline_metadata is not None:
            fields["pipeline_metadata"] = pipeline_metadata
        if conversion_metadata is not None:
            fields["conversion_metadata"] = conversion_metadata
        if raw_outputs is not None:
            fields["raw_outputs"] = list(raw_outputs)
        if zarr_outputs is not None:
            fields["zarr_outputs"] = list(zarr_outputs)
        self.store.update_job(job_id, **fields)
        if event_type:
            self.store.append_event(
                job_id,
                event_type,
                {
                    "pipeline_state": pipeline_state.value,
                    "pipeline_step": pipeline_step,
                    **(event_payload or {}),
                },
            )

    @staticmethod
    def _filter_manifest_paths(paths: list[str]) -> list[str]:
        return [path for path in paths if Path(str(path)).name != "manifest.json"]

    @staticmethod
    def _scene_id_from_raw_uri(raw_uri: str) -> str:
        name = Path(str(raw_uri)).name
        for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
        return Path(name).stem or "scene"

    def _default_zarr_output_uri(self, scene_id: str) -> str:
        safe_scene = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (scene_id or "scene")).strip("._-")
        if not safe_scene:
            safe_scene = "scene"
        return str(self.settings.nimbus_data_dir / "zarr" / f"{safe_scene}.zarr")

    @staticmethod
    def _merge_paths(existing: list[str], additions: list[str]) -> list[str]:
        merged: list[str] = []
        for value in [*existing, *additions]:
            item = str(value).strip()
            if item and item not in merged:
                merged.append(item)
        return merged

    def _normalize_backend_path(self, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            return normalized
        root = str(self.settings.nimbus_data_dir).rstrip("/")
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
                suffix = normalized[len(legacy_prefix):]
                return f"{root}/{suffix}" if suffix else root
        return normalized

    def _normalize_backend_paths_payload(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._normalize_backend_paths_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize_backend_paths_payload(item) for item in value]
        if isinstance(value, tuple):
            return [self._normalize_backend_paths_payload(item) for item in value]
        return self._normalize_backend_path(value)

    def _normalize_backend_paths_in_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        normalized["request"] = dict(self._normalize_backend_paths_payload(dict(row.get("request") or {})))
        normalized["pipeline_metadata"] = dict(self._normalize_backend_paths_payload(dict(row.get("pipeline_metadata") or {})))
        normalized["conversion_metadata"] = dict(self._normalize_backend_paths_payload(dict(row.get("conversion_metadata") or {})))
        normalized["raw_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(row.get("raw_outputs") or []))))
        normalized["zarr_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(row.get("zarr_outputs") or []))))
        normalized["watermask_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(row.get("watermask_outputs") or []))))
        return normalized

    def _normalize_backend_paths_in_result_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result or {})
        normalized["paths"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("paths") or []))))
        normalized["raw_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("raw_outputs") or []))))
        normalized["zarr_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("zarr_outputs") or []))))
        normalized["watermask_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("watermask_outputs") or []))))
        normalized["metadata"] = dict(self._normalize_backend_paths_payload(dict(normalized.get("metadata") or {})))
        normalized["manifest_entry"] = dict(self._normalize_backend_paths_payload(dict(normalized.get("manifest_entry") or {})))
        normalized["pipeline_metadata"] = dict(self._normalize_backend_paths_payload(dict(normalized.get("pipeline_metadata") or {})))
        normalized["conversion_metadata"] = dict(self._normalize_backend_paths_payload(dict(normalized.get("conversion_metadata") or {})))
        return normalized

    def _normalize_backend_paths_in_artifact_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        normalized["artifact_uri"] = self._normalize_backend_path(row.get("artifact_uri"))
        normalized["source_uri"] = self._normalize_backend_path(row.get("source_uri"))
        normalized["metadata"] = dict(self._normalize_backend_paths_payload(dict(row.get("metadata") or {})))
        return normalized

    @staticmethod
    def _normalize_collection_for_zarr(provider_name: str, collection: str) -> str:
        return collection.strip().lower() if provider_name == "usgs" else collection.strip().upper()

    @staticmethod
    def _normalize_product_type_for_zarr(product_type: str | None) -> str | None:
        if product_type is None:
            return None
        normalized = str(product_type).strip()
        return normalized.upper() if normalized else None

    def _register_zarr_artifact(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        zarr_uri: str,
        data_family: str,
        conversion_summary: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> None:
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri=zarr_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_id,
            source_uri=raw_uri,
            created_by_job_id=job_id,
            source_job_id=job_id,
            data_family=data_family,
            band_names=list(dataset_summary.get("band_names") or []),
            dimensions=list(dataset_summary.get("dimensions") or []),
            shape=list(dataset_summary.get("shape") or []),
            metadata={
                "normalization_summary": conversion_summary,
                "zarr_summary": dataset_summary,
                "registered_via": "pipeline_job",
            },
        )
        self.upsert_artifact(artifact_request)

    def _register_watermask_artifact(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        zarr_uri: str,
        water_mask: dict[str, Any],
    ) -> None:
        artifact_uri = str(water_mask.get("artifact_uri") or "").strip()
        if not artifact_uri:
            return
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.watermask,
            artifact_uri=artifact_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_id,
            source_uri=zarr_uri,
            created_by_job_id=job_id,
            source_job_id=job_id,
            data_family="mask",
            dimensions=["time", "y", "x"],
            shape=list(water_mask.get("shape") or []),
            metadata={
                "water_mask": water_mask,
                "registered_via": "manual_watermask",
            },
        )
        self.upsert_artifact(artifact_request)

    def _register_masked_zarr_artifact(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        source_zarr_uri: str,
        water_mask: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> None:
        artifact_uri = str(water_mask.get("output_zarr_uri") or "").strip()
        if not artifact_uri:
            return
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr_masked,
            artifact_uri=artifact_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_id,
            source_uri=source_zarr_uri,
            created_by_job_id=job_id,
            source_job_id=job_id,
            data_family="optical",
            band_names=list(dataset_summary.get("band_names") or []),
            dimensions=list(dataset_summary.get("dimensions") or []),
            shape=list(dataset_summary.get("shape") or []),
            metadata={
                "water_mask": water_mask,
                "registered_via": "manual_watermask",
                "source_zarr_uri": source_zarr_uri,
            },
        )
        self.upsert_artifact(artifact_request)

    def _masker(self) -> MaskService:
        if self._mask_service is None:
            self._mask_service = MaskService()
        return self._mask_service

    def _job_related_zarr_uris(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result: dict[str, Any],
    ) -> list[str]:
        related: list[str] = []
        for value in list(result.get("zarr_outputs") or row.get("zarr_outputs") or []):
            uri = str(value).strip()
            if uri and uri not in related:
                related.append(uri)
        for artifact_type in (ArtifactType.zarr.value, ArtifactType.zarr_masked.value):
            artifacts = self.list_artifacts(
                artifact_type=artifact_type,
                provider=None,
                collection=None,
                scene_id=None,
                job_id=job_id,
                uri_query=None,
                date_from=None,
                date_to=None,
                page=1,
                page_size=500,
            )
            for item in artifacts.items:
                uri = str(item.artifact_uri).strip()
                if uri and uri not in related:
                    related.append(uri)
        return related

    def _resolve_zarr_context(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result: dict[str, Any],
        zarr_uri: str,
        scene_id_override: str | None,
        product_type_override: str | None,
    ) -> dict[str, Any]:
        conversion_metadata = dict(result.get("conversion_metadata") or row.get("conversion_metadata") or {})
        items = list(conversion_metadata.get("items") or [])
        matching_item = next(
            (
                item
                for item in items
                if str((item or {}).get("zarr_uri") or "").strip() == zarr_uri
            ),
            None,
        )
        dataset_summary = dict((matching_item or {}).get("dataset_summary") or {})
        summary = dict((matching_item or {}).get("summary") or {})
        grid_summary = dict(summary.get("grid") or {})
        if not dataset_summary.get("crs") and grid_summary.get("crs"):
            dataset_summary["crs"] = grid_summary.get("crs")
        if not dataset_summary.get("transform") and grid_summary.get("transform"):
            dataset_summary["transform"] = list(grid_summary.get("transform") or [])
        if not dataset_summary.get("dtype") and grid_summary.get("dtype"):
            dataset_summary["dtype"] = str(grid_summary.get("dtype") or "")
        if not dataset_summary.get("shape") and grid_summary.get("height") and grid_summary.get("width"):
            band_names = list(dataset_summary.get("band_names") or summary.get("normalized_band_order") or [])
            dataset_summary["shape"] = [1, len(band_names), int(grid_summary["height"]), int(grid_summary["width"])]
        if not dataset_summary.get("band_names") and summary.get("normalized_band_order"):
            dataset_summary["band_names"] = [str(item) for item in list(summary.get("normalized_band_order") or [])]
        if not dataset_summary.get("ancillary_layer_names") and summary.get("ancillary_layer_names"):
            dataset_summary["ancillary_layer_names"] = [
                str(item) for item in list(summary.get("ancillary_layer_names") or [])
            ]
        if not dataset_summary:
            dataset_summary = self._inspect_zarr_dataset(zarr_uri)
        provider_name = self._provider_name(row.get("provider"))
        collection = str(row.get("collection") or summary.get("collection") or "").strip()
        scene_id = (
            str(scene_id_override or "").strip()
            or str((matching_item or {}).get("scene_id") or "").strip()
            or str(summary.get("scene_id") or "").strip()
            or self._scene_id_from_raw_uri(zarr_uri)
        )
        product_type = (
            str(product_type_override or "").strip()
            or str(row.get("product_type") or "").strip()
            or str(summary.get("product_type") or "").strip()
            or None
        )
        acquisition_datetime = (
            str(dataset_summary.get("acquisition_datetime") or "").strip()
            or str(summary.get("acquisition_datetime") or "").strip()
            or None
        )
        if not collection:
            raise ValueError("Unable to infer collection for the selected Zarr output.")
        if not dataset_summary:
            raise ValueError("Unable to infer dataset summary for the selected Zarr output.")
        return {
            "provider": provider_name,
            "collection": collection,
            "scene_id": scene_id,
            "product_type": product_type,
            "acquisition_datetime": acquisition_datetime,
            "dataset_summary": dataset_summary,
        }

    def _inspect_zarr_dataset(self, zarr_uri: str) -> dict[str, Any]:
        try:
            import zarr
        except Exception as exc:
            raise ValueError(f"Unable to inspect existing Zarr output because zarr is unavailable ({exc}).") from exc

        from nimbuschain_zarr_service.core import _open_existing_output_store

        root = zarr.open_group(_open_existing_output_store(zarr_uri), mode="r")
        imagery = root.get("imagery")
        if imagery is None:
            raise ValueError("The selected Zarr output does not contain an imagery array.")
        band_names = list(root.attrs.get("band_names") or [])
        if not band_names and "band" in root:
            band_names = [str(item) for item in root["band"][:].tolist()]
        acquisition_datetime = None
        if "time" in root and len(root["time"]) > 0:
            raw_time = root["time"][0]
            acquisition_datetime = str(raw_time.item() if hasattr(raw_time, "item") else raw_time)
        attrs = dict(root.attrs)
        transform = list(attrs.get("transform") or [])
        if len(transform) < 6 and "x" in root and "y" in root:
            derived_transform = self._derive_transform_from_xy(
                x_values=root["x"][:].tolist(),
                y_values=root["y"][:].tolist(),
            )
            if derived_transform:
                transform = derived_transform
        crs = attrs.get("crs")
        if not crs:
            crs = self._infer_crs_from_scene_metadata(
                scene_id=str(attrs.get("scene_id") or ""),
                source_uri=str(attrs.get("source_uri") or zarr_uri),
            )
        return {
            "dimensions": ["time", "band", "y", "x"],
            "shape": list(imagery.shape),
            "band_names": [str(item) for item in band_names],
            "ancillary_layer_names": list(root.attrs.get("ancillary_layer_names") or []),
            "acquisition_datetime": acquisition_datetime,
            "crs": crs,
            "transform": transform,
            "dtype": str(attrs.get("dtype") or imagery.dtype),
        }

    @staticmethod
    def _derive_transform_from_xy(*, x_values: list[Any], y_values: list[Any]) -> list[float]:
        if len(x_values) < 2 or len(y_values) < 2:
            return []
        try:
            x0 = float(x_values[0])
            x1 = float(x_values[1])
            y0 = float(y_values[0])
            y1 = float(y_values[1])
        except (TypeError, ValueError):
            return []
        x_res = x1 - x0
        y_res = y1 - y0
        if x_res == 0.0 or y_res == 0.0:
            return []
        # Coordinates are pixel centers. Affine transform expects top-left corner.
        return [
            x_res,
            0.0,
            x0 - (x_res / 2.0),
            0.0,
            y_res,
            y0 - (y_res / 2.0),
        ]

    @staticmethod
    def _infer_crs_from_scene_metadata(*, scene_id: str, source_uri: str) -> str | None:
        text = f"{scene_id} {source_uri}"
        match = re.search(r"T(?P<zone>\d{2})(?P<band>[A-Z]{3})", text)
        if not match:
            return None
        zone = int(match.group("zone"))
        latitude_band = match.group("band")[0]
        epsg_base = 326 if latitude_band >= "N" else 327
        return f"EPSG:{epsg_base}{zone:02d}"

    @staticmethod
    def _collect_watermask_outputs(
        *,
        result: dict[str, Any],
        water_mask: dict[str, Any],
    ) -> list[str]:
        outputs: list[str] = []
        for value in list(result.get("watermask_outputs") or []):
            item = str(value).strip()
            if item and item not in outputs:
                outputs.append(item)
        for key in ("artifact_uri", "status_path"):
            item = str(water_mask.get(key) or "").strip()
            if item and item not in outputs:
                outputs.append(item)
        derived_zarr_uri = str(water_mask.get("output_zarr_uri") or "").strip()
        if derived_zarr_uri and derived_zarr_uri != str(water_mask.get("input_zarr_uri") or "").strip():
            if derived_zarr_uri not in outputs:
                outputs.append(derived_zarr_uri)
        return outputs

    def _convert_raw_outputs(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        product_type: str | None,
        raw_outputs: list[str],
        is_cancelled: Callable[[], bool],
        scene_id_override: str | None = None,
        output_uri_override: str | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        if not raw_outputs:
            return [], {"status": "skipped", "reason": "no_raw_outputs"}

        zarr_outputs: list[str] = []
        conversions: list[dict[str, Any]] = []
        total = max(1, len(raw_outputs))
        self._update_pipeline(
            job_id,
            pipeline_state=PipelineState.zarr_queued,
            pipeline_step="zarr_queued",
            pipeline_progress=0.0,
            raw_outputs=raw_outputs,
            conversion_metadata={
                "status": "queued",
                "stage": "zarr_queued",
                "current_index": 0,
                "total": total,
            },
            event_type="job.zarr_queued",
            event_payload={"raw_output_count": len(raw_outputs)},
        )
        for index, raw_uri in enumerate(raw_outputs, start=1):
            if is_cancelled():
                raise JobCancelledError("Job cancellation requested.")
            scene_id = (
                str(scene_id_override or "").strip()
                if index == 1 and scene_id_override
                else self._scene_id_from_raw_uri(raw_uri)
            )
            output_uri = (
                str(output_uri_override or "").strip()
                if index == 1 and output_uri_override
                else self._default_zarr_output_uri(scene_id)
            )
            per_item_progress = ((index - 1) / total) * 100.0
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="writing_chunks",
                pipeline_progress=per_item_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata={
                    "status": "running",
                    "stage": "writing_chunks",
                    "current_raw_uri": raw_uri,
                    "current_scene_id": scene_id,
                    "current_output_uri": output_uri,
                    "current_index": index,
                    "total": total,
                },
                event_type="job.zarr_converting",
                event_payload={
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": output_uri,
                    "index": index,
                    "total": total,
                    "stage": "writing_chunks",
                },
            )
            written_uri, data_family, conversion_summary, dataset_summary = self._converter().convert(
                provider=provider_name,
                collection=self._normalize_collection_for_zarr(provider_name, collection),
                scene_id=scene_id,
                raw_uri=raw_uri,
                output_uri=output_uri,
                product_type=self._normalize_product_type_for_zarr(product_type),
            )
            zarr_outputs.append(written_uri)
            conversions.append(
                {
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "zarr_uri": written_uri,
                    "data_family": data_family,
                    "summary": conversion_summary,
                    "dataset_summary": dataset_summary,
                }
            )
            register_progress = min(
                99.0,
                ((index - 1) / total) * 100.0 + (100.0 / total) * 0.85,
            )
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="registering_artifact",
                pipeline_progress=register_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata={
                    "status": "running",
                    "stage": "registering_artifact",
                    "current_raw_uri": raw_uri,
                    "current_scene_id": scene_id,
                    "current_output_uri": written_uri,
                    "current_index": index,
                    "total": total,
                },
                event_type="job.zarr_converting",
                event_payload={
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": written_uri,
                    "index": index,
                    "total": total,
                    "stage": "registering_artifact",
                },
            )
            self._register_zarr_artifact(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                scene_id=scene_id,
                raw_uri=raw_uri,
                zarr_uri=written_uri,
                data_family=data_family,
                conversion_summary=conversion_summary,
                dataset_summary=dataset_summary,
            )
        return zarr_outputs, {
            "status": "written",
            "count": len(zarr_outputs),
            "items": conversions,
        }

    def convert_existing_job(self, job_id: str, request: JobConvertRequest) -> JobStatusResponse:
        row = self.store.get_job(job_id)
        if not row:
            raise JobNotFoundError(job_id)
        state = str(row.get("state") or "")
        if state in {JobState.queued.value, JobState.running.value, JobState.cancel_requested.value}:
            raise ValueError("Manual conversion is only allowed when the job is not actively running.")

        result = self.store.get_result(job_id) or {}
        raw_outputs = list(row.get("raw_outputs") or result.get("raw_outputs") or self._filter_manifest_paths(list(result.get("paths") or [])))
        selected_raw_uri = str(request.raw_uri or (raw_outputs[0] if raw_outputs else "")).strip()
        if not selected_raw_uri:
            raise ValueError("No raw output is attached to this job. Provide raw_uri explicitly.")
        scene_id = str(request.scene_id or self._scene_id_from_raw_uri(selected_raw_uri)).strip()
        provider_name = self._provider_name(row.get("provider"))
        collection = str(row.get("collection") or "")
        product_type = str(request.product_type or row.get("product_type") or "").strip() or None
        output_uri = str(request.output_uri or self._default_zarr_output_uri(scene_id)).strip()

        self.store.update_job(job_id, state=JobState.running.value, finished_at=None, errors=[])
        zarr_outputs, conversion_metadata = self._convert_raw_outputs(
            job_id=job_id,
            provider_name=provider_name,
            collection=collection,
            product_type=product_type,
            raw_outputs=[selected_raw_uri],
            is_cancelled=lambda: self._is_job_cancel_requested(job_id),
            scene_id_override=scene_id,
            output_uri_override=output_uri,
        )
        pipeline_state = PipelineState.zarr_written
        pipeline_metadata = {
            **dict(row.get("pipeline_metadata") or {}),
            "manual_conversion": True,
            "raw_output_count": len(raw_outputs),
            "zarr_output_count": len(zarr_outputs),
        }
        result_payload = {
            "job_id": job_id,
            "paths": list(result.get("paths") or []),
            "raw_outputs": raw_outputs,
            "zarr_outputs": zarr_outputs,
            "checksums": dict(result.get("checksums") or {}),
            "metadata": dict(result.get("metadata") or {}),
            "manifest_entry": dict(result.get("manifest_entry") or {}),
            "pipeline_metadata": pipeline_metadata,
            "conversion_metadata": conversion_metadata,
        }
        self.store.set_result(job_id, result_payload)
        self.store.update_job(
            job_id,
            state=JobState.succeeded.value,
            finished_at=self._now_iso(),
            pipeline_state=pipeline_state.value,
            pipeline_step="zarr_written",
            pipeline_progress=100.0,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
        )
        self.store.append_event(
            job_id,
            "job.zarr_written",
            {
                "pipeline_state": pipeline_state.value,
                "manual_conversion": True,
                "zarr_outputs": zarr_outputs,
            },
        )
        return self.get_job(job_id)

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
        provider_name = self._provider_name(request.provider)
        base_pipeline_metadata: dict[str, Any] = {
            "provider": provider_name,
            "collection": request.collection,
            "product_type": getattr(request, "product_type", None),
            "output_dir": str(output_dir),
            "job_type": request.job_type,
        }
        if isinstance(request, SearchDownloadRequest):
            base_pipeline_metadata["tile_id"] = request.tile_id
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.searching,
                pipeline_step="searching",
                pipeline_progress=5.0,
                pipeline_metadata={**base_pipeline_metadata, "products_found": 0},
                event_type="job.searching",
                event_payload={"provider": provider_name, "collection": request.collection},
            )
        else:
            requested_ids = list(getattr(request, "product_ids", []) or [])
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.downloading,
                pipeline_step="downloading",
                pipeline_progress=5.0,
                pipeline_metadata={
                    **base_pipeline_metadata,
                    "products_requested": len(requested_ids),
                    "products_found": len(requested_ids),
                },
                event_type="job.downloading",
                event_payload={"products_requested": len(requested_ids)},
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
            pipeline_progress = min(69.0, 5.0 + (progress_pct * 0.64))

            # Throttle DB writes and events.
            if now_mono - float(aggregate["last_emit"]) >= 0.25 or delta == 0:
                self.store.update_job(
                    job_id,
                    progress=progress_pct,
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=int(aggregate["bytes_total"]),
                    pipeline_state=PipelineState.downloading.value,
                    pipeline_step="downloading",
                    pipeline_progress=pipeline_progress,
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

        def emit_retry(file_name: str, attempt: int, reason: str, retry_after: float | None) -> None:
            row_now = self.store.get_job(job_id) or {}
            retry_count = int(row_now.get("retry_count", 0) or 0) + 1
            last_retry_at = self._now_iso()
            self.store.update_job(
                job_id,
                retry_count=retry_count,
                last_retry_at=last_retry_at,
            )
            self.store.append_event(
                job_id,
                "job.retrying",
                {
                    "file": file_name,
                    "attempt": int(attempt),
                    "reason": reason,
                    "retry_after": retry_after,
                    "retry_count": retry_count,
                    "last_retry_at": last_retry_at,
                },
            )

        try:
            result = await anyio.to_thread.run_sync(
                self._run_provider_job,
                job_id,
                request,
                output_dir,
                emit_progress,
                emit_retry,
                is_cancelled_now,
            )

            if is_cancelled_now():
                self._mark_cancelled(job_id, "cancelled_after_download")
                return

            raw_outputs = self._filter_manifest_paths(list(result["paths"]))
            metadata = dict(result["metadata"])
            pipeline_metadata = {
                **base_pipeline_metadata,
                **metadata,
                "products_found": int(metadata.get("products_found", len(raw_outputs)) or len(raw_outputs)),
                "products_downloaded": int(metadata.get("products_downloaded", len(raw_outputs)) or len(raw_outputs)),
                "raw_output_count": len(raw_outputs),
            }
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.downloaded,
                pipeline_step="downloaded",
                pipeline_progress=70.0,
                pipeline_metadata=pipeline_metadata,
                raw_outputs=raw_outputs,
                event_type="job.downloaded",
                event_payload={"raw_outputs": raw_outputs, "raw_output_count": len(raw_outputs)},
            )

            checksums = checksums_for_paths(raw_outputs)
            manifest_entry = build_manifest_entry(
                job_id=job_id,
                provider=str(row["provider"]),
                collection=str(row["collection"]),
                metadata=metadata,
                paths=raw_outputs,
                checksums=checksums,
            )
            manifest_path = write_manifest(output_dir, manifest_entry)
            raw_result_paths = [*raw_outputs, str(manifest_path)]
            checksums[str(manifest_path)] = checksums_for_paths([str(manifest_path)]).get(
                str(manifest_path), ""
            )
            base_result_payload = {
                "job_id": job_id,
                "paths": raw_result_paths,
                "raw_outputs": raw_outputs,
                "zarr_outputs": [],
                "checksums": checksums,
                "metadata": metadata,
                "manifest_entry": manifest_entry,
                "pipeline_metadata": pipeline_metadata,
                "conversion_metadata": {},
            }
            self.store.set_result(job_id, base_result_payload)

            zarr_outputs, conversion_metadata = self._convert_raw_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=request.collection,
                product_type=getattr(request, "product_type", None),
                raw_outputs=raw_outputs,
                is_cancelled=is_cancelled_now,
            )
            final_paths = [*raw_result_paths, *zarr_outputs]
            conversion_status = str(conversion_metadata.get("status") or "")
            final_pipeline_state = (
                PipelineState.zarr_written
                if zarr_outputs or conversion_status == "written"
                else PipelineState.downloaded
            )
            final_pipeline_step = (
                "zarr_written" if final_pipeline_state == PipelineState.zarr_written else "downloaded"
            )
            final_pipeline_metadata = {
                **pipeline_metadata,
                "zarr_output_count": len(zarr_outputs),
                "manual_conversion": False,
            }

            self.store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "paths": final_paths,
                    "raw_outputs": raw_outputs,
                    "zarr_outputs": zarr_outputs,
                    "checksums": checksums,
                    "metadata": metadata,
                    "manifest_entry": manifest_entry,
                    "pipeline_metadata": final_pipeline_metadata,
                    "conversion_metadata": conversion_metadata,
                },
            )
            self.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                progress=100.0,
                finished_at=self._now_iso(),
                bytes_downloaded=int(aggregate["bytes_downloaded"]),
                bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                pipeline_state=final_pipeline_state.value,
                pipeline_step=final_pipeline_step,
                pipeline_progress=100.0,
                pipeline_metadata=final_pipeline_metadata,
                conversion_metadata=conversion_metadata,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
            )
            if final_pipeline_state == PipelineState.zarr_written:
                self.store.append_event(
                    job_id,
                    "job.zarr_written",
                    {
                        "pipeline_state": final_pipeline_state.value,
                        "zarr_outputs": zarr_outputs,
                    },
                )
            self.store.append_event(
                job_id,
                "job.succeeded",
                {
                    "status": JobState.succeeded.value,
                    "paths": final_paths,
                    "pipeline_state": final_pipeline_state.value,
                },
            )
        except (DownloadCancelled, JobCancelledError):
            self._mark_cancelled(job_id, "cancelled_during_download")
        except Exception as exc:
            current_row = self.store.get_job(job_id) or row
            existing_result = self.store.get_result(job_id) or {}
            raw_outputs = list(existing_result.get("raw_outputs") or current_row.get("raw_outputs") or [])
            current_pipeline_state = str(current_row.get("pipeline_state") or "")
            is_zarr_failure = current_pipeline_state in {
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.downloaded.value,
            }
            pipeline_state = PipelineState.zarr_failed if is_zarr_failure and raw_outputs else PipelineState.failed
            pipeline_step = "zarr_failed" if pipeline_state == PipelineState.zarr_failed else "failed"
            conversion_metadata = dict(existing_result.get("conversion_metadata") or {})
            if pipeline_state == PipelineState.zarr_failed:
                conversion_metadata = {
                    **conversion_metadata,
                    "status": "failed",
                    "error": str(exc),
                }
                if existing_result:
                    self.store.set_result(
                        job_id,
                        {
                            **existing_result,
                            "conversion_metadata": conversion_metadata,
                            "raw_outputs": raw_outputs,
                            "zarr_outputs": list(existing_result.get("zarr_outputs") or []),
                        },
                    )
            self.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=self._now_iso(),
                errors=[str(exc)],
                pipeline_state=pipeline_state.value,
                pipeline_step=pipeline_step,
                pipeline_metadata=dict(existing_result.get("pipeline_metadata") or current_row.get("pipeline_metadata") or {}),
                conversion_metadata=conversion_metadata,
                raw_outputs=raw_outputs,
                zarr_outputs=list(existing_result.get("zarr_outputs") or current_row.get("zarr_outputs") or []),
            )
            self.store.append_event(
                job_id,
                "job.failed",
                {
                    "status": JobState.failed.value,
                    "error": str(exc),
                    "pipeline_state": pipeline_state.value,
                },
            )
        finally:
            self._cancel_check_cache.pop(job_id, None)

    def _run_provider_job(
        self,
        job_id: str,
        request: JobCreateRequest,
        output_dir,
        progress_callback,
        retry_callback,
        is_cancelled,
    ) -> dict[str, Any]:
        provider_name = self._provider_name(request.provider)
        provider_limit = self.settings.provider_limits_map.get(provider_name, 1)

        download_manager_kwargs: dict[str, Any] = dict(
            max_concurrent=provider_limit,
            progress_callback=progress_callback,
            cancel_checker=is_cancelled,
            retry_callback=retry_callback,
        )
        if provider_name == "copernicus":
            # Copernicus download endpoints can intermittently return 504 before
            # the first byte. Use a longer retry window inspired by the legacy
            # downloader behavior.
            download_manager_kwargs.update(
                max_retries=6,
                initial_delay=3.0,
                backoff_factor=2.0,
                max_retry_delay=300.0,
                gateway_timeout_retries=4,
                gateway_timeout_floor_delay=10.0,
            )

        download_manager = DownloadManager(**download_manager_kwargs)
        provider = self._build_provider(provider_name, download_manager)

        if isinstance(request, SearchDownloadRequest):
            if is_cancelled():
                raise JobCancelledError("cancelled")
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.searching,
                pipeline_step="searching",
                pipeline_progress=5.0,
                pipeline_metadata={
                    "provider": provider_name,
                    "collection": request.collection,
                    "product_type": request.product_type,
                    "tile_id": request.tile_id,
                    "products_found": 0,
                    "output_dir": str(output_dir),
                    "job_type": request.job_type,
                },
                event_type="job.searching",
                event_payload={"provider": provider_name, "collection": request.collection},
            )
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
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.downloading,
                pipeline_step="downloading",
                pipeline_progress=10.0 if product_ids else 70.0,
                pipeline_metadata={
                    "provider": provider_name,
                    "collection": request.collection,
                    "product_type": request.product_type,
                    "tile_id": request.tile_id,
                    "products_found": len(product_ids),
                    "products_requested": len(product_ids),
                    "output_dir": str(output_dir),
                    "job_type": request.job_type,
                },
                event_type="job.downloading",
                event_payload={"products_found": len(product_ids)},
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
        self._update_pipeline(
            job_id,
            pipeline_state=PipelineState.downloading,
            pipeline_step="downloading",
            pipeline_progress=10.0,
            pipeline_metadata={
                "provider": provider_name,
                "collection": request.collection,
                "products_requested": len(request.product_ids),
                "products_found": len(request.product_ids),
                "output_dir": str(output_dir),
                "job_type": request.job_type,
            },
            event_type="job.downloading",
            event_payload={"products_requested": len(request.product_ids)},
        )
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
        current_row = self.store.get_job(job_id) or {}
        self.store.update_job(
            job_id,
            state=JobState.cancelled.value,
            finished_at=self._now_iso(),
            pipeline_state=PipelineState.cancelled.value,
            pipeline_step="cancelled",
            pipeline_progress=current_row.get("pipeline_progress"),
        )
        self.store.append_event(
            job_id,
            "job.cancelled",
            {
                "status": JobState.cancelled.value,
                "reason": reason,
                "pipeline_state": PipelineState.cancelled.value,
            },
        )

    def _normalize_historical_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
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

        self.store.update_job(row["job_id"], preserve_updated_at=True, **updates)
        normalized = dict(row)
        normalized.update(updates)
        return normalized

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
            pipeline_state=PipelineState(str(row.get("pipeline_state") or PipelineState.queued.value)),
            pipeline_step=row.get("pipeline_step"),
            pipeline_progress=(
                float(row["pipeline_progress"])
                if row.get("pipeline_progress") is not None
                else None
            ),
            pipeline_metadata=dict(row.get("pipeline_metadata") or {}),
            conversion_metadata=dict(row.get("conversion_metadata") or {}),
            raw_outputs=list(row.get("raw_outputs") or []),
            zarr_outputs=list(row.get("zarr_outputs") or []),
            watermask_outputs=list(row.get("watermask_outputs") or []),
            progress=float(row["progress"]),
            bytes_downloaded=int(row["bytes_downloaded"]),
            bytes_total=int(row["bytes_total"]),
            retry_count=int(row.get("retry_count", 0) or 0),
            last_retry_at=self._parse_iso(row.get("last_retry_at")),
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
