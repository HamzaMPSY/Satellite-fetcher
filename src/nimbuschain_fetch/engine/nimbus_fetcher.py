from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import inspect
import json
import re
import os
import shutil
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import anyio
from pydantic import TypeAdapter

from nimbuschain_fetch.download.coordinator import (
    DownloadBatchResult,
    DownloadCoordinator,
    DownloadCoordinatorStore,
)
from nimbuschain_fetch.download.download_manager import (
    CancelChecker,
    DownloadCancelled,
    DownloadManager,
    ProgressCallback,
    RetryCallback,
)
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
    JobCloudMaskRequest,
    JobCloudMaskResponse,
    JobConvertRequest,
    JobCreateRequest,
    JobListResponse,
    JobMaskRequest,
    JobMaskResponse,
    JobResultResponse,
    JobState,
    JobStatusResponse,
    JobWaterMaskRequest,
    JobWaterMaskResponse,
    PipelineState,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.pipeline_timeline import advance_pipeline_timeline
from nimbuschain_fetch.providers import CopernicusProvider, UsgsProvider
from nimbuschain_fetch.security.paths import sanitize_output_dir
from nimbuschain_fetch.jobs.store_factory import create_job_store
from nimbuschain_fetch.settings import Settings, get_settings
from nimbuschain_fetch.usgs_product_type import canonicalize_usgs_product_type
from nimbuschain_mask_service.client import MaskServiceClient
from nimbuschain_mask_service.runtime import normalize_device_name, resolve_inference_device
from nimbuschain_zarr_service.cube import build_grouped_time_cubes
from nimbuschain_zarr_service.service import ZarrConversionService


class JobNotFoundError(KeyError):
    pass


class JobCancelledError(RuntimeError):
    pass


class NimbusFetcher:
    """Core orchestrator for submission, execution and tracking of fetch jobs."""

    MASK_CONTRACT_VERSION = "v2"
    DOWNLOAD_PROGRESS_MIN_INTERVAL_SECONDS = 1.0
    DOWNLOAD_PROGRESS_MAX_INTERVAL_SECONDS = 3.0
    DOWNLOAD_PROGRESS_MIN_BYTES = 16 * 1024 * 1024
    DOWNLOAD_PROGRESS_MIN_PERCENT = 1.0
    ZARR_PROGRESS_MIN_INTERVAL_SECONDS = 0.5
    ZARR_PROGRESS_MIN_PERCENT = 1.0

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
                provider_limits=self.settings.provider_job_limits_map,
            )
            if self._execution_enabled
            else None
        )
        self._poller_task: asyncio.Task[None] | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop_event = threading.Event()
        self._worker_id = uuid.uuid4().hex
        self._worker_hostname = socket.gethostname()
        self._worker_pid = os.getpid()
        self._worker_started_at = datetime.now(timezone.utc).isoformat()
        self._cancel_check_cache: dict[str, tuple[float, bool]] = {}
        self._pipeline_update_lock = threading.RLock()
        self._zarr_converter: ZarrConversionService | None = None
        self._mask_service: Any | None = None
        self._download_coordinator: DownloadCoordinator | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self.settings.ensure_runtime_dirs()
        if self._execution_enabled and self._executor is not None:
            requeued_job_ids = self.store.requeue_incomplete_jobs()
            self._retire_legacy_mask_jobs()
            self._fail_interrupted_mask_jobs(
                job_ids=requeued_job_ids,
                reason="Mask job interrupted during service restart. Submit a new mask job from the Mask tab.",
                event_type="job.mask_failed_after_restart",
            )
            await self._executor.start()
            await self._enqueue_queued_jobs()
            self._publish_worker_heartbeat()
            self._start_worker_heartbeat_thread()
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
        self._stop_worker_heartbeat_thread()
        if self._executor is not None:
            await self._executor.stop()
        if self._mask_service is not None and hasattr(self._mask_service, "close"):
            self._mask_service.close()
            self._mask_service = None
        if self._download_coordinator is not None:
            self._download_coordinator.close()
            self._download_coordinator = None
        self._started = False

    def _retire_legacy_mask_jobs(self) -> None:
        rows, _total = self.store.list_jobs(
            JobListFilters(
                states=(
                    JobState.queued.value,
                    JobState.running.value,
                    JobState.cancel_requested.value,
                ),
                page=1,
                page_size=5000,
            )
        )
        now = self._now_iso()
        for row in rows:
            if str(row.get("job_type") or "").strip().lower() != "mask_existing_zarr":
                continue
            request_payload = dict(row.get("request") or {})
            if str(request_payload.get("mask_contract_version") or "").strip().lower() == self.MASK_CONTRACT_VERSION:
                continue
            job_id = str(row.get("job_id") or "").strip()
            if not job_id:
                continue
            self._cleanup_mask_job_outputs(job_id=job_id, row=row)
            self.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=now,
                progress=0.0,
                pipeline_state=PipelineState.failed.value,
                pipeline_step="failed",
                pipeline_progress=100.0,
                errors=[
                    "Legacy mask job retired during mask-service v2 cutover. Submit a new mask job from the Mask tab."
                ],
            )
            self.store.append_event(
                job_id,
                "job.mask_retired_after_cutover",
                {"reason": "legacy_mask_contract"},
            )

    def _cleanup_mask_job_outputs(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result_payload: dict[str, Any] | None = None,
        preserve_status_paths: bool = False,
        failure_reason: str | None = None,
    ) -> None:
        result_payload = self._normalize_backend_paths_in_result_payload(
            result_payload or self.store.get_result(job_id) or {}
        )
        normalized_row = self._normalize_backend_paths_in_job_row(self._normalize_historical_job_row(row))
        request_payload = dict(normalized_row.get("request") or {})
        pipeline_metadata = dict(
            result_payload.get("pipeline_metadata")
            or normalized_row.get("pipeline_metadata")
            or {}
        )
        conversion_metadata = dict(
            result_payload.get("conversion_metadata")
            or normalized_row.get("conversion_metadata")
            or {}
        )
        result_metadata = dict(result_payload.get("metadata") or {})
        water_mask = dict(
            conversion_metadata.get("water_mask")
            or pipeline_metadata.get("water_mask")
            or result_metadata.get("water_mask")
            or {}
        )
        cloud_mask = dict(
            conversion_metadata.get("cloud_mask")
            or pipeline_metadata.get("cloud_mask")
            or result_metadata.get("cloud_mask")
            or {}
        )
        source_zarr_uri = str(
            request_payload.get("source_zarr_uri")
            or pipeline_metadata.get("source_zarr_uri")
            or conversion_metadata.get("source_zarr_uri")
            or result_metadata.get("source_zarr_uri")
            or ""
        ).strip()

        masked_zarr_candidates: set[str] = set()
        artifact_candidates: set[str] = set()
        status_paths: list[tuple[str, dict[str, Any], str]] = []

        for field_name in ("zarr_outputs", "masked_zarr_outputs"):
            for value in list(normalized_row.get(field_name) or []):
                masked_zarr_candidates.add(str(value))
            for value in list(result_payload.get(field_name) or []):
                masked_zarr_candidates.add(str(value))

        for value in (
            result_payload.get("masked_zarr_uri"),
            result_metadata.get("masked_zarr_uri"),
            pipeline_metadata.get("masked_zarr_uri"),
            conversion_metadata.get("masked_zarr_uri"),
        ):
            text = str(value or "").strip()
            if text:
                masked_zarr_candidates.add(text)

        for label, payload in (("water", water_mask), ("cloud", cloud_mask)):
            for key in ("output_zarr_uri", "working_zarr_uri"):
                text = str(payload.get(key) or "").strip()
                if text:
                    masked_zarr_candidates.add(text)
            for key in ("artifact_uri",):
                text = str(payload.get(key) or "").strip()
                if text:
                    artifact_candidates.add(text)
            status_path = str(payload.get("status_path") or "").strip()
            if status_path:
                status_paths.append((status_path, payload, label))
                if not preserve_status_paths:
                    artifact_candidates.add(status_path)

        for field_name in ("watermask_outputs", "cloudmask_outputs", "paths"):
            for value in list(normalized_row.get(field_name) or []):
                text = str(value or "").strip()
                if text and not text.endswith(".zarr"):
                    artifact_candidates.add(text)
            for value in list(result_payload.get(field_name) or []):
                text = str(value or "").strip()
                if text and not text.endswith(".zarr"):
                    artifact_candidates.add(text)

        for raw_value in masked_zarr_candidates:
            normalized = str(self._normalize_backend_path(raw_value) or "").strip()
            if not normalized or normalized == source_zarr_uri:
                continue
            target = Path(normalized)
            self._remove_path_if_exists(target)
            if target.parent.exists():
                for pattern in (
                    f".{target.stem}.tmp-*{target.suffix}",
                    f".{target.stem}.backup-*{target.suffix}",
                ):
                    for sibling in target.parent.glob(pattern):
                        self._remove_path_if_exists(sibling)

        for raw_value in artifact_candidates:
            normalized = str(self._normalize_backend_path(raw_value) or "").strip()
            if not normalized:
                continue
            self._remove_path_if_exists(Path(normalized))

        if preserve_status_paths:
            for status_path, payload, label in status_paths:
                self._mark_mask_status_failed(
                    status_path=status_path,
                    payload=payload,
                    reason=failure_reason or f"{label} mask job interrupted.",
                )

    @staticmethod
    def _remove_path_if_exists(target: Path) -> None:
        try:
            if not target.exists():
                return
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        except OSError:
            return

    def _mark_mask_status_failed(
        self,
        *,
        status_path: str,
        payload: dict[str, Any],
        reason: str,
    ) -> None:
        normalized = str(self._normalize_backend_path(status_path) or "").strip()
        if not normalized:
            return
        target = Path(normalized)
        target.parent.mkdir(parents=True, exist_ok=True)
        failed_payload = dict(payload or {})
        failed_payload["status"] = "failed"
        failed_payload["reason"] = reason
        failed_payload["status_path"] = normalized
        failed_payload["updated_at"] = self._now_iso()
        target.write_text(json.dumps(failed_payload, indent=2, sort_keys=True), encoding="utf-8")

    def _fail_interrupted_mask_jobs(
        self,
        *,
        job_ids: list[str],
        reason: str,
        event_type: str,
    ) -> None:
        if not job_ids:
            return
        now = self._now_iso()
        for job_id in job_ids:
            row = self.store.get_job(job_id)
            if not row:
                continue
            if str(row.get("job_type") or "").strip().lower() != "mask_existing_zarr":
                continue
            request_payload = dict(row.get("request") or {})
            if str(request_payload.get("mask_contract_version") or "").strip().lower() != self.MASK_CONTRACT_VERSION:
                continue

            normalized_row = self._normalize_backend_paths_in_job_row(self._normalize_historical_job_row(row))
            result_payload = self._normalize_backend_paths_in_result_payload(self.store.get_result(job_id) or {})
            self._cleanup_mask_job_outputs(
                job_id=job_id,
                row=normalized_row,
                result_payload=result_payload,
                preserve_status_paths=True,
                failure_reason=reason,
            )

            pipeline_metadata = dict(
                result_payload.get("pipeline_metadata")
                or normalized_row.get("pipeline_metadata")
                or {}
            )
            conversion_metadata = dict(result_payload.get("conversion_metadata") or {})
            metadata = dict(result_payload.get("metadata") or {})
            for container in (pipeline_metadata, conversion_metadata, metadata):
                for key in ("water_mask", "cloud_mask"):
                    payload = dict(container.get(key) or {})
                    if not payload:
                        continue
                    payload["status"] = "failed"
                    payload["reason"] = reason
                    container[key] = payload
            for payload in (pipeline_metadata, conversion_metadata, metadata):
                payload["status"] = "failed"
                payload["interrupted_reason"] = reason
                payload.pop("masked_zarr_uri", None)

            self.store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "job_type": "mask_existing_zarr",
                    "job_kind": "mask",
                    "service_name": "mask_service",
                    "source_job_id": str(
                        metadata.get("source_job_id")
                        or pipeline_metadata.get("source_job_id")
                        or conversion_metadata.get("source_job_id")
                        or request_payload.get("source_job_id")
                        or ""
                    ).strip() or None,
                    "paths": [],
                    "raw_outputs": [],
                    "zarr_outputs": [],
                    "cube_outputs": [],
                    "masked_zarr_outputs": [],
                    "watermask_outputs": [],
                    "cloudmask_outputs": [],
                    "checksums": dict(result_payload.get("checksums") or {}),
                    "metadata": metadata,
                    "manifest_entry": dict(result_payload.get("manifest_entry") or {}),
                    "pipeline_metadata": pipeline_metadata,
                    "conversion_metadata": conversion_metadata,
                },
            )
            self.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=now,
                progress=0.0,
                pipeline_state=PipelineState.failed.value,
                pipeline_step="failed",
                pipeline_progress=95.0,
                pipeline_metadata=pipeline_metadata,
                conversion_metadata=conversion_metadata,
                zarr_outputs=[],
                watermask_outputs=[],
                cloudmask_outputs=[],
                errors=[reason],
            )
            self.store.append_event(
                job_id,
                event_type,
                {
                    "status": JobState.failed.value,
                    "reason": reason,
                    "pipeline_state": PipelineState.failed.value,
                },
            )

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

    @staticmethod
    def _job_kind_for_type(job_type: str | None) -> str:
        normalized = str(job_type or "").strip().lower()
        if normalized == "mask_existing_zarr":
            return "mask"
        return "fetch"

    @classmethod
    def _service_name_for_type(cls, job_type: str | None) -> str:
        return "mask_service" if cls._job_kind_for_type(job_type) == "mask" else "fetch_service"

    def _create_mask_job(
        self,
        *,
        source_job_id: str,
        provider_name: str,
        collection: str,
        request_payload: dict[str, Any],
    ) -> str:
        job_id = uuid.uuid4().hex
        self.store.create_job(
            job_id=job_id,
            job_type="mask_existing_zarr",
            provider=provider_name,
            collection=collection,
            request_payload=request_payload,
        )
        self.store.append_event(
            job_id,
            "job.queued",
            {
                "state": JobState.queued.value,
                "job_kind": "mask",
                "source_job_id": source_job_id,
                "mask_types": list(request_payload.get("mask_types") or []),
            },
        )
        return job_id

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
                list(job_row.get("cloudmask_outputs") or []),
            ]
        ):
            raise JobNotFoundError(job_id)

        result = self._normalize_backend_paths_in_result_payload(result)
        zarr_outputs = list(result.get("zarr_outputs") or job_row.get("zarr_outputs") or [])
        cube_outputs = self._cube_outputs_for_job(job_id=job_id, result=result, row=job_row)
        masked_zarr_outputs = self._masked_zarr_outputs_for_job(job_id=job_id, result=result, row=job_row)
        normalized_result = {
            "job_id": job_id,
            "job_type": job_row.get("job_type"),
            "job_kind": self._job_kind_for_type(job_row.get("job_type")),
            "service_name": self._service_name_for_type(job_row.get("job_type")),
            "source_job_id": str((result.get("metadata") or {}).get("source_job_id") or (result.get("pipeline_metadata") or {}).get("source_job_id") or (job_row.get("pipeline_metadata") or {}).get("source_job_id") or "").strip() or None,
            "paths": list(result.get("paths") or []),
            "raw_outputs": list(result.get("raw_outputs") or job_row.get("raw_outputs") or []),
            "zarr_outputs": zarr_outputs,
            "cube_outputs": cube_outputs,
            "masked_zarr_outputs": masked_zarr_outputs,
            "watermask_outputs": list(result.get("watermask_outputs") or job_row.get("watermask_outputs") or []),
            "cloudmask_outputs": list(result.get("cloudmask_outputs") or job_row.get("cloudmask_outputs") or []),
            "checksums": dict(result.get("checksums") or {}),
            "metadata": dict(result.get("metadata") or {}),
            "manifest_entry": dict(result.get("manifest_entry") or {}),
            "pipeline_metadata": dict(result.get("pipeline_metadata") or job_row.get("pipeline_metadata") or {}),
            "conversion_metadata": dict(result.get("conversion_metadata") or job_row.get("conversion_metadata") or {}),
        }
        return JobResultResponse.model_validate(normalized_result)

    def apply_mask_existing_job(self, job_id: str, request: JobMaskRequest) -> JobMaskResponse:
        row = self.store.get_job(job_id)
        if not row:
            raise JobNotFoundError(job_id)
        row = self._normalize_backend_paths_in_job_row(row)
        state = str(row.get("state") or "")
        if state in {JobState.queued.value, JobState.running.value, JobState.cancel_requested.value}:
            raise ValueError("Masking is only allowed when the source job is not actively running.")

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
        mask_types = [str(item).strip().lower() for item in list(request.mask_types or [])]
        mask_job_request = {
            "job_type": "mask_existing_zarr",
            "mask_contract_version": self.MASK_CONTRACT_VERSION,
            "provider": zarr_context["provider"],
            "collection": zarr_context["collection"],
            "product_type": zarr_context["product_type"],
            "source_job_id": job_id,
            "source_zarr_uri": selected_zarr_uri,
            "scene_id": zarr_context["scene_id"],
            "acquisition_datetime": zarr_context["acquisition_datetime"],
            "dataset_summary": dict(zarr_context["dataset_summary"] or {}),
            "mask_types": mask_types,
            "backend": str(request.backend or "auto").strip().lower() or "auto",
            "threshold": float(request.threshold if request.threshold is not None else 0.62),
            "overwrite": bool(request.overwrite),
            "inference_device": str(request.inference_device or "").strip() or None,
            "include_shadows": bool(request.include_shadows),
            "water_backend": str(request.water_backend or "auto").strip().lower() or "auto",
            "water_overwrite": bool(request.water_overwrite) if request.water_overwrite is not None else bool(request.overwrite),
            "water_inference_device": str(request.water_inference_device or "").strip() or None,
            "fail_on_error": bool(request.fail_on_error),
        }
        mask_job_id = self._create_mask_job(
            source_job_id=job_id,
            provider_name=zarr_context["provider"],
            collection=zarr_context["collection"],
            request_payload=mask_job_request,
        )
        self.store.update_job(
            mask_job_id,
            state=JobState.queued.value,
            pipeline_state=PipelineState.queued.value,
            pipeline_step="queued",
            pipeline_progress=0.0,
            pipeline_metadata={
                "source_job_id": job_id,
                "source_zarr_uri": selected_zarr_uri,
                "scene_id": zarr_context["scene_id"],
                "mask_types": mask_types,
                "backend": mask_job_request["backend"],
                "threshold": mask_job_request["threshold"],
                "include_shadows": mask_job_request["include_shadows"],
                "water_backend": mask_job_request["water_backend"],
                "job_kind": "mask",
            },
            progress=0.0,
        )
        self.store.append_event(
            mask_job_id,
            "job.mask_queued",
            {
                "source_job_id": job_id,
                "source_zarr_uri": selected_zarr_uri,
                "scene_id": zarr_context["scene_id"],
                "mask_types": mask_types,
                "backend": mask_job_request["backend"],
                "threshold": mask_job_request["threshold"],
                "include_shadows": mask_job_request["include_shadows"],
                "water_backend": mask_job_request["water_backend"],
            },
        )
        if self._execution_enabled and self._executor is not None:
            try:
                anyio.from_thread.run(self._executor.submit, mask_job_id)
            except RuntimeError:
                # Fall back to the queue poller when the request did not originate
                # from an anyio-managed thread.
                pass
        return JobMaskResponse(
            job_id=mask_job_id,
            source_job_id=job_id,
            source_zarr_uri=selected_zarr_uri,
            masked_zarr_uri=None,
            mask_types=mask_types,
            water_mask={},
            cloud_mask={},
            masked_zarr_outputs=[],
            watermask_outputs=[],
            cloudmask_outputs=[],
            job=self.get_job(mask_job_id),
        )

    @staticmethod
    def _normalized_mask_types(values: list[str] | tuple[str, ...] | None) -> list[str]:
        normalized: list[str] = []
        for value in list(values or []):
            candidate = str(value or "").strip().lower()
            if candidate not in {"water", "cloud"}:
                continue
            if candidate not in normalized:
                normalized.append(candidate)
        return normalized

    @staticmethod
    def _normalized_cube_mode(value: Any) -> str:
        candidate = str(value or "").strip().lower()
        if candidate in {"before_mask", "after_mask"}:
            return candidate
        return "none"

    def _timeline_cube_mode_for_row(
        self,
        row: dict[str, Any],
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> str:
        metadata = dict(row.get("pipeline_metadata") or {})
        if pipeline_metadata is not None:
            metadata.update(dict(pipeline_metadata))
        return self._normalized_cube_mode(
            metadata.get("cube_mode")
            or dict(row.get("request") or {}).get("cube_mode")
        )

    @staticmethod
    def _normalize_mask_failure_step(value: Any) -> str | None:
        candidate = str(value or "").strip().lower()
        if candidate in {"failed", "cloud_failed", "water_failed"}:
            return candidate
        return None

    @classmethod
    def _preferred_mask_failure_step(
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
            candidate = cls._normalize_mask_failure_step(raw_value)
            if candidate is None:
                continue
            candidate_priority = priority.get(candidate, -1)
            if candidate_priority > selected_priority:
                selected = candidate
                selected_priority = candidate_priority
        return selected

    @classmethod
    def _mask_failure_step_from_payloads(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        water_mask: dict[str, Any] | None,
        cloud_mask: dict[str, Any] | None,
    ) -> str | None:
        normalized_mask_types = cls._normalized_mask_types(list(mask_types or []))
        failure_steps: list[str] = []
        water_status = str((water_mask or {}).get("status") or "").strip().lower()
        cloud_status = str((cloud_mask or {}).get("status") or "").strip().lower()
        if "water" in normalized_mask_types and water_status == "failed":
            failure_steps.append("water_failed")
        if "cloud" in normalized_mask_types and cloud_status == "failed":
            failure_steps.append("cloud_failed")
        if not failure_steps:
            return None
        return cls._preferred_mask_failure_step(failure_steps)

    @classmethod
    def _mask_failure_step_from_items(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        items: list[dict[str, Any]] | None,
    ) -> str:
        failure_steps: list[str] = []
        for item in list(items or []):
            direct_step = cls._normalize_mask_failure_step(item.get("failed_step"))
            if direct_step is not None:
                failure_steps.append(direct_step)
                continue
            conversion_metadata = dict(item.get("conversion_metadata") or {})
            inferred_step = cls._mask_failure_step_from_payloads(
                mask_types=mask_types,
                water_mask=dict(conversion_metadata.get("water_mask") or {}),
                cloud_mask=dict(conversion_metadata.get("cloud_mask") or {}),
            )
            if inferred_step is not None:
                failure_steps.append(inferred_step)
        return cls._preferred_mask_failure_step(failure_steps)

    @staticmethod
    def _build_mask_progress_plan(
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

    def _run_in_place_mask_pipeline(
        self,
        *,
        job_id: str,
        source_job_id: str,
        selected_zarr_uri: str,
        zarr_context: dict[str, Any],
        mask_types: list[str],
        backend_name: str,
        threshold: float,
        inference_device: str | None,
        include_shadows: bool,
        overwrite: bool,
        water_backend_name: str,
        water_overwrite: bool,
        water_inference_device: str | None,
        fail_on_error: bool,
        mask_mode: str,
        include_resolve_stage: bool,
        resolve_progress: float | None,
        stage_start_progress: float,
        stage_end_progress: float,
        expose_masked_outputs: bool,
        register_masked_artifact: bool,
        pipeline_progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        progress_plan = self._build_mask_progress_plan(
            mask_types=mask_types,
            stage_start_progress=stage_start_progress,
            stage_end_progress=stage_end_progress,
        )
        base_mask_metadata = {
            "source_job_id": source_job_id,
            "source_zarr_uri": selected_zarr_uri,
            "scene_id": zarr_context["scene_id"],
            "mask_types": mask_types,
            "backend": backend_name,
            "threshold": threshold,
            "include_shadows": include_shadows,
            "water_backend": water_backend_name,
            "mask_mode": mask_mode,
        }

        def emit_pipeline_update(
            *,
            pipeline_state: PipelineState,
            pipeline_step: str,
            pipeline_progress: float,
            event_type: str,
            event_payload: dict[str, Any],
            pipeline_metadata: dict[str, Any],
        ) -> None:
            item_span = max(1e-6, float(stage_end_progress) - float(stage_start_progress))
            item_fraction = min(
                1.0,
                max(0.0, (float(pipeline_progress) - float(stage_start_progress)) / item_span),
            )
            payload = {
                "job_id": job_id,
                "pipeline_state": pipeline_state,
                "pipeline_step": pipeline_step,
                "pipeline_progress": float(pipeline_progress),
                "pipeline_metadata": dict(pipeline_metadata),
                "event_type": event_type,
                "event_payload": dict(event_payload),
                "item_fraction": item_fraction,
                "scene_id": zarr_context["scene_id"],
                "zarr_uri": selected_zarr_uri,
            }
            if pipeline_progress_callback is not None:
                pipeline_progress_callback(payload)
                return
            self._update_pipeline(
                job_id,
                pipeline_state=pipeline_state,
                pipeline_step=pipeline_step,
                pipeline_progress=pipeline_progress,
                pipeline_metadata=pipeline_metadata,
                event_type=event_type,
                event_payload=event_payload,
            )

        def stage_callback(stage_name: str, payload: dict[str, Any]) -> None:
            current_pipeline = dict((self.store.get_job(job_id) or {}).get("pipeline_metadata") or {})
            current_pipeline.update(base_mask_metadata)
            if stage_name == "cloud_masking_progress":
                fraction = float(payload.get("progress") or 0.0)
                fraction = max(0.0, min(fraction, 1.0))
                start = progress_plan["cloud_start"]
                end = progress_plan["cloud_end"]
                pipeline_progress = start + (fraction * max(0.0, end - start))
                emit_pipeline_update(
                    pipeline_state=PipelineState.running_cloud_inference,
                    pipeline_step="running_cloud_inference",
                    pipeline_progress=pipeline_progress,
                    event_type="job.cloud_masking_progress",
                    event_payload=payload,
                    pipeline_metadata=current_pipeline,
                )
                return
            if stage_name == "water_masking_progress":
                fraction = float(payload.get("progress") or 0.0)
                fraction = max(0.0, min(fraction, 1.0))
                start = progress_plan["water_start"]
                end = progress_plan["water_end"]
                pipeline_progress = start + (fraction * max(0.0, end - start))
                emit_pipeline_update(
                    pipeline_state=PipelineState.running_water_inference,
                    pipeline_step="running_water_inference",
                    pipeline_progress=pipeline_progress,
                    event_type="job.water_masking_progress",
                    event_payload=payload,
                    pipeline_metadata=current_pipeline,
                )
                return

            stage_map: dict[str, tuple[PipelineState, str, float]] = {
                "water_masking_started": (
                    PipelineState.running_water_inference,
                    "running_water_inference",
                    progress_plan["water_start"],
                ),
                "water_masking_finished": (
                    PipelineState.running_water_inference,
                    "running_water_inference",
                    progress_plan["water_end"],
                ),
                "water_masking_failed": (
                    PipelineState.failed,
                    "water_failed",
                    min(99.0, progress_plan["water_end"]),
                ),
                "cloud_masking_started": (
                    PipelineState.running_cloud_inference,
                    "running_cloud_inference",
                    progress_plan["cloud_start"],
                ),
                "cloud_masking_finished": (
                    PipelineState.running_cloud_inference,
                    "running_cloud_inference",
                    progress_plan["cloud_end"],
                ),
                "cloud_masking_failed": (
                    PipelineState.failed,
                    "cloud_failed",
                    min(99.0, progress_plan["cloud_end"]),
                ),
            }
            mapped = stage_map.get(stage_name)
            if not mapped:
                return
            pipeline_state, pipeline_step, pipeline_progress = mapped
            emit_pipeline_update(
                pipeline_state=pipeline_state,
                pipeline_step=pipeline_step,
                pipeline_progress=pipeline_progress,
                event_type=f"job.{stage_name}",
                event_payload=payload,
                pipeline_metadata=current_pipeline,
            )

        if include_resolve_stage:
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.resolving_source_zarr,
                pipeline_step="resolving_source_zarr",
                pipeline_progress=resolve_progress,
                pipeline_metadata=base_mask_metadata,
                event_type="job.mask_started",
                event_payload=base_mask_metadata,
            )

        masker = self._masker()
        if not bool(getattr(masker, "supports_stage_callbacks", True)):
            if "cloud" in mask_types:
                stage_callback(
                    "cloud_masking_started",
                    {
                        "zarr_uri": selected_zarr_uri,
                        "output_zarr_uri": selected_zarr_uri,
                        "scene_id": zarr_context["scene_id"],
                    },
                )
            elif "water" in mask_types:
                stage_callback(
                    "water_masking_started",
                    {
                        "zarr_uri": selected_zarr_uri,
                        "output_zarr_uri": selected_zarr_uri,
                        "scene_id": zarr_context["scene_id"],
                    },
                )

        if hasattr(masker, "apply_masks_to_zarr"):
            mask_response = self._invoke_mask_method(
                masker.apply_masks_to_zarr,
                job_id=job_id,
                zarr_uri=selected_zarr_uri,
                provider=zarr_context["provider"],
                collection=zarr_context["collection"],
                product_type=zarr_context["product_type"],
                scene_id=zarr_context["scene_id"],
                acquisition_datetime=zarr_context["acquisition_datetime"],
                dataset_summary=zarr_context["dataset_summary"],
                mask_types=mask_types,
                backend=backend_name,
                threshold=threshold,
                overwrite=overwrite,
                inference_device=inference_device,
                include_shadows=include_shadows,
                water_backend=water_backend_name,
                water_overwrite=water_overwrite,
                water_inference_device=water_inference_device,
                fail_on_error=fail_on_error,
                stage_callback=stage_callback,
            )
        elif mask_types == ["water"] and hasattr(masker, "apply_omniwater_to_zarr"):
            water_mask = self._invoke_mask_method(
                masker.apply_omniwater_to_zarr,
                job_id=job_id,
                zarr_uri=selected_zarr_uri,
                provider=zarr_context["provider"],
                collection=zarr_context["collection"],
                product_type=zarr_context["product_type"],
                scene_id=zarr_context["scene_id"],
                acquisition_datetime=zarr_context["acquisition_datetime"],
                dataset_summary=zarr_context["dataset_summary"],
                fail_on_error=fail_on_error,
                stage_callback=stage_callback,
            )
            masked_zarr_uri = str(water_mask.get("output_zarr_uri") or selected_zarr_uri).strip()
            mask_response = {
                "status": str(water_mask.get("status") or ""),
                "mask_types": ["water"],
                "input_zarr_uri": selected_zarr_uri,
                "output_zarr_uri": masked_zarr_uri,
                "masked_zarr_uri": masked_zarr_uri or None,
                "masked_zarr_outputs": [masked_zarr_uri] if masked_zarr_uri else [],
                "water_mask": water_mask,
                "cloud_mask": {},
                "watermask_outputs": [],
                "cloudmask_outputs": [],
            }
        else:
            raise RuntimeError("Configured mask service does not expose apply_masks_to_zarr.")

        masked_zarr_uri = str(
            mask_response.get("masked_zarr_uri")
            or mask_response.get("output_zarr_uri")
            or selected_zarr_uri
        ).strip()
        masked_zarr_outputs = [
            item for item in list(mask_response.get("masked_zarr_outputs") or []) if str(item).strip()
        ]
        water_mask = dict(mask_response.get("water_mask") or {})
        cloud_mask = dict(mask_response.get("cloud_mask") or {})
        final_status = str(mask_response.get("status") or "").strip().lower()
        mask_job_succeeded = final_status == "written"
        visible_masked_zarr_outputs = masked_zarr_outputs if mask_job_succeeded and expose_masked_outputs else []
        if (
            mask_job_succeeded
            and expose_masked_outputs
            and not visible_masked_zarr_outputs
            and masked_zarr_uri
        ):
            visible_masked_zarr_outputs = [masked_zarr_uri]
        quality_fields = self._mask_quality_fields(water_mask=water_mask, cloud_mask=cloud_mask)
        quality_scalars = {
            "water_fraction": float(quality_fields.get("water_fraction") or 0.0),
            "cloud_fraction": float(quality_fields.get("cloud_fraction") or 0.0),
            "cloud_only_fraction": float(quality_fields.get("cloud_only_fraction") or 0.0),
            "shadow_fraction": float(quality_fields.get("shadow_fraction") or 0.0),
        }
        pipeline_metadata = {
            "mask_contract_version": "v2",
            **base_mask_metadata,
            "masked_zarr_uri": masked_zarr_uri if mask_job_succeeded else "",
            "status": str(mask_response.get("status") or ""),
            "mask_quality": quality_fields,
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            **quality_scalars,
        }
        conversion_metadata = {
            "mask_contract_version": "v2",
            "mask_types": mask_types,
            "source_zarr_uri": selected_zarr_uri,
            "masked_zarr_uri": masked_zarr_uri if mask_job_succeeded else "",
            "backend": backend_name,
            "threshold": threshold,
            "include_shadows": include_shadows,
            "water_backend": water_backend_name,
            "status": str(mask_response.get("status") or ""),
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            "mask_quality": quality_fields,
            **quality_scalars,
        }
        if mask_job_succeeded and register_masked_artifact and masked_zarr_uri:
            self._register_masked_zarr_artifact(
                job_id=job_id,
                source_job_id=source_job_id,
                provider_name=zarr_context["provider"],
                collection=zarr_context["collection"],
                scene_id=zarr_context["scene_id"],
                source_zarr_uri=selected_zarr_uri,
                masked_zarr_uri=masked_zarr_uri,
                mask_payload=mask_response,
                dataset_summary=zarr_context["dataset_summary"],
            )

        errors: list[str] = []
        failed_step = None
        if not mask_job_succeeded:
            for payload in (water_mask, cloud_mask):
                reason = str(payload.get("reason") or "").strip()
                if reason and reason not in errors:
                    errors.append(reason)
            if not errors:
                errors.append(f"Mask execution failed with status '{final_status or 'unknown'}'.")
            failed_step = self._mask_failure_step_from_payloads(
                mask_types=mask_types,
                water_mask=water_mask,
                cloud_mask=cloud_mask,
            ) or "failed"
            pipeline_metadata["failed_step"] = failed_step
            conversion_metadata["failed_step"] = failed_step

        return {
            "status": final_status,
            "succeeded": mask_job_succeeded,
            "masked_zarr_uri": masked_zarr_uri,
            "masked_zarr_outputs": visible_masked_zarr_outputs,
            "watermask_outputs": [],
            "cloudmask_outputs": [],
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            "pipeline_metadata": pipeline_metadata,
            "conversion_metadata": conversion_metadata,
            "errors": errors,
            "failed_step": failed_step,
        }

    def _execute_mask_existing_zarr_job(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        is_cancelled_now: Callable[[], bool],
    ) -> None:
        request_payload = dict(row.get("request") or {})
        if str(request_payload.get("mask_contract_version") or "").strip().lower() != self.MASK_CONTRACT_VERSION:
            raise ValueError(
                "Legacy mask jobs are kept as read-only history and cannot be executed by the v2 mask service."
            )
        source_job_id = str(request_payload.get("source_job_id") or "").strip()
        selected_zarr_uri = str(request_payload.get("source_zarr_uri") or request_payload.get("zarr_uri") or "").strip()
        if not source_job_id or not selected_zarr_uri:
            raise ValueError("Mask job is missing source_job_id or source_zarr_uri.")

        mask_types = [str(item).strip().lower() for item in list(request_payload.get("mask_types") or [])]
        if not mask_types:
            raise ValueError("Mask job is missing mask_types.")

        zarr_context = {
            "provider": str(request_payload.get("provider") or row.get("provider") or "").strip().lower(),
            "collection": str(request_payload.get("collection") or row.get("collection") or "").strip(),
            "product_type": str(request_payload.get("product_type") or row.get("product_type") or "").strip() or None,
            "scene_id": str(request_payload.get("scene_id") or "").strip(),
            "acquisition_datetime": str(request_payload.get("acquisition_datetime") or "").strip() or None,
            "dataset_summary": dict(request_payload.get("dataset_summary") or {}),
        }
        if not zarr_context["collection"] or not zarr_context["scene_id"] or not zarr_context["dataset_summary"]:
            source_row = self.store.get_job(source_job_id) or {}
            source_result = self.store.get_result(source_job_id) or {}
            if source_row:
                source_row = self._normalize_backend_paths_in_job_row(self._normalize_historical_job_row(source_row))
            if source_result:
                source_result = self._normalize_backend_paths_in_result_payload(source_result)
            resolved = self._resolve_zarr_context(
                job_id=source_job_id,
                row=source_row,
                result=source_result,
                zarr_uri=selected_zarr_uri,
                scene_id_override=zarr_context["scene_id"] or None,
                product_type_override=zarr_context["product_type"],
            )
            zarr_context.update(resolved)

        backend_name = str(request_payload.get("backend") or "auto").strip().lower() or "auto"
        threshold = float(request_payload.get("threshold") if request_payload.get("threshold") is not None else 0.62)
        inference_device = str(request_payload.get("inference_device") or "").strip() or None
        include_shadows = bool(request_payload.get("include_shadows", True))
        overwrite = bool(request_payload.get("overwrite", True))
        water_backend_name = str(request_payload.get("water_backend") or "auto").strip().lower() or "auto"
        water_overwrite = bool(request_payload.get("water_overwrite", overwrite))
        water_inference_device = str(request_payload.get("water_inference_device") or "").strip() or None
        fail_on_error = bool(request_payload.get("fail_on_error", False))
        if is_cancelled_now():
            raise JobCancelledError("Mask job cancellation requested before execution.")

        mask_execution = self._run_in_place_mask_pipeline(
            job_id=job_id,
            source_job_id=source_job_id,
            selected_zarr_uri=selected_zarr_uri,
            zarr_context=zarr_context,
            mask_types=mask_types,
            backend_name=backend_name,
            threshold=threshold,
            inference_device=inference_device,
            include_shadows=include_shadows,
            overwrite=overwrite,
            water_backend_name=water_backend_name,
            water_overwrite=water_overwrite,
            water_inference_device=water_inference_device,
            fail_on_error=fail_on_error,
            mask_mode="standalone",
            include_resolve_stage=True,
            resolve_progress=5.0,
            stage_start_progress=35.0,
            stage_end_progress=88.0,
            expose_masked_outputs=True,
            register_masked_artifact=False,
        )
        mask_job_succeeded = bool(mask_execution["succeeded"])
        masked_zarr_uri = str(mask_execution["masked_zarr_uri"] or "").strip()
        visible_masked_zarr_outputs = list(mask_execution["masked_zarr_outputs"] or [])
        visible_watermask_outputs = list(mask_execution["watermask_outputs"] or [])
        visible_cloudmask_outputs = list(mask_execution["cloudmask_outputs"] or [])
        water_mask = dict(mask_execution["water_mask"] or {})
        cloud_mask = dict(mask_execution["cloud_mask"] or {})
        pipeline_metadata = dict(mask_execution["pipeline_metadata"] or {})
        conversion_metadata = dict(mask_execution["conversion_metadata"] or {})
        result_payload = {
            "job_id": job_id,
            "job_type": "mask_existing_zarr",
            "paths": self._merge_paths(visible_masked_zarr_outputs, []),
            "raw_outputs": [],
            "zarr_outputs": visible_masked_zarr_outputs,
            "cube_outputs": [],
            "masked_zarr_outputs": visible_masked_zarr_outputs,
            "watermask_outputs": visible_watermask_outputs,
            "cloudmask_outputs": visible_cloudmask_outputs,
            "checksums": {},
            "metadata": {
                "mask_contract_version": "v2",
                "source_job_id": source_job_id,
                "source_zarr_uri": selected_zarr_uri,
                "masked_zarr_uri": masked_zarr_uri if mask_job_succeeded else "",
                "scene_id": zarr_context["scene_id"],
                "mask_types": mask_types,
                "backend": backend_name,
                "threshold": threshold,
                "include_shadows": include_shadows,
                "water_backend": water_backend_name,
                "mask_quality": dict(conversion_metadata.get("mask_quality") or {}),
                "water_mask": water_mask,
                "cloud_mask": cloud_mask,
                "water_fraction": float(conversion_metadata.get("water_fraction") or 0.0),
                "cloud_fraction": float(conversion_metadata.get("cloud_fraction") or 0.0),
                "cloud_only_fraction": float(conversion_metadata.get("cloud_only_fraction") or 0.0),
                "shadow_fraction": float(conversion_metadata.get("shadow_fraction") or 0.0),
            },
            "manifest_entry": {},
            "pipeline_metadata": pipeline_metadata,
            "conversion_metadata": conversion_metadata,
        }
        terminal_pipeline_state = PipelineState.masked_zarr_written if mask_job_succeeded else PipelineState.failed
        terminal_pipeline_step = (
            PipelineState.masked_zarr_written.value
            if mask_job_succeeded
            else self._preferred_mask_failure_step([mask_execution.get("failed_step")])
        )
        terminal_state = JobState.succeeded if terminal_pipeline_state == PipelineState.masked_zarr_written else JobState.failed
        pipeline_metadata = self._merged_pipeline_metadata(job_id, pipeline_metadata)
        self._update_pipeline(
            job_id,
            pipeline_state=terminal_pipeline_state,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            zarr_outputs=visible_masked_zarr_outputs,
        )
        pipeline_metadata = self._merged_pipeline_metadata(job_id, pipeline_metadata)
        result_payload["pipeline_metadata"] = pipeline_metadata
        self.store.set_result(job_id, result_payload)
        errors = list(mask_execution.get("errors") or [])
        self.store.update_job(
            job_id,
            state=terminal_state.value,
            finished_at=self._now_iso(),
            progress=100.0 if terminal_state == JobState.succeeded else 0.0,
            pipeline_state=terminal_pipeline_state.value,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            zarr_outputs=visible_masked_zarr_outputs,
            watermask_outputs=visible_watermask_outputs,
            cloudmask_outputs=visible_cloudmask_outputs,
            errors=errors,
        )
        self.store.append_event(
            job_id,
            "job.mask_completed" if terminal_state == JobState.succeeded else "job.mask_failed",
            {
                "status": terminal_state.value,
                "source_job_id": source_job_id,
                "source_zarr_uri": selected_zarr_uri,
                "masked_zarr_uri": masked_zarr_uri,
                "mask_types": mask_types,
                "backend": backend_name,
                "threshold": threshold,
                "include_shadows": include_shadows,
                "water_backend": water_backend_name,
                "water_mask": water_mask,
                "cloud_mask": cloud_mask,
            },
        )

    def apply_watermask_existing_job(self, job_id: str, request: JobWaterMaskRequest) -> JobWaterMaskResponse:
        response = self.apply_mask_existing_job(job_id, JobMaskRequest.model_validate(request.model_dump(mode="python")))
        return JobWaterMaskResponse.model_validate(response.model_dump(mode="python"))

    def apply_cloud_mask_existing_job(self, job_id: str, request: JobCloudMaskRequest) -> JobCloudMaskResponse:
        payload = request.model_dump(mode="python")
        payload["mask_types"] = ["cloud"]
        response = self.apply_mask_existing_job(job_id, JobMaskRequest.model_validate(payload))
        return JobCloudMaskResponse.model_validate(response.model_dump(mode="python"))

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

    def _list_jobs_by_states(self, states: tuple[str, ...]) -> list[dict[str, Any]]:
        page = 1
        rows: list[dict[str, Any]] = []
        while True:
            batch, total = self.store.list_jobs(
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
        active_rows = self._list_jobs_by_states(
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
        if self._execution_enabled and self._executor is not None:
            for job_id in active_job_ids:
                try:
                    await self._executor.cancel(job_id)
                    executor_cancelled += 1
                except Exception:
                    continue

        for job_id in active_job_ids:
            self._mark_cancelled(job_id, "runtime_reset")
            self._cancel_check_cache[job_id] = (time.monotonic() + 60.0, True)

        coordinator_reason = "Download cancelled by runtime reset."
        if self._download_coordinator is not None:
            coordinator_reset = self._download_coordinator.reset_runtime_state(reason=coordinator_reason)
        else:
            coordinator_store = DownloadCoordinatorStore(self.settings.download_coordinator_db_path)
            try:
                task_ids = coordinator_store.cancel_all_non_terminal_tasks(reason=coordinator_reason)
            finally:
                coordinator_store.close()
            coordinator_reset = {
                "tasks_cancelled": len(task_ids),
                "task_ids": task_ids,
            }

        workers_cleared = int(self.store.clear_workers())

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
            stale_job_ids = self.store.requeue_stale_running_jobs(self.settings.nimbus_stale_job_seconds)
            self._fail_interrupted_mask_jobs(
                job_ids=stale_job_ids,
                reason="Mask job marked stale after exceeding the worker timeout. Submit a new mask job from the Mask tab.",
                event_type="job.mask_failed_stale_timeout",
            )
            await self._enqueue_queued_jobs()
            await anyio.sleep(float(self.settings.nimbus_queue_poll_seconds))

    def _start_worker_heartbeat_thread(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._worker_heartbeat_loop,
            name="nimbus-worker-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_worker_heartbeat_thread(self) -> None:
        thread = self._heartbeat_thread
        if thread is None:
            return
        self._heartbeat_stop_event.set()
        thread.join(timeout=2.0)
        self._heartbeat_thread = None

    def _worker_heartbeat_loop(self) -> None:
        interval_seconds = max(1.0, float(self.settings.nimbus_worker_heartbeat_seconds))
        while not self._heartbeat_stop_event.wait(interval_seconds):
            try:
                self._publish_worker_heartbeat()
            except Exception:
                # Heartbeats should retry on the next interval even if a single
                # storage update fails while the worker continues processing.
                continue

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
            for name, limit in self.settings.provider_job_limits_map.items()
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
            "provider_job_limits": configured_provider_limits,
            "provider_control_plane_limits": self.settings.provider_control_plane_limits_map,
            "provider_data_plane_limits": self.settings.provider_data_plane_limits_map,
            "download_guardrails": {
                "global_active_limit": int(self.settings.nimbus_download_global_limit),
                "min_free_bytes": int(self.settings.nimbus_download_min_free_bytes or 0),
                "global_max_bps": (
                    int(self.settings.nimbus_download_global_max_bps)
                    if self.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "provider_capacity": provider_capacity,
            "workers": worker_payloads,
        }

    def _download_coordinator_placeholder_status(self, *, status: str = "not_initialized") -> dict[str, Any]:
        configured_accounts = [
            {
                "account_label": str(item.get("label") or "primary").strip() or "primary",
                "active_downloads": 0,
                "cooldown_seconds": 0.0,
                "max_concurrent_downloads": int(self.settings.nimbus_copernicus_account_pool_concurrency),
            }
            for item in self.settings.copernicus_account_pool_accounts
        ]
        return {
            "status": status,
            "started": False,
            "closed": False,
            "timestamp": self._now_iso(),
            "db_path": str(self.settings.download_coordinator_db_path),
            "limits": {
                "job": dict(self.settings.provider_job_limits_map),
                "control_plane": dict(self.settings.provider_control_plane_limits_map),
                "data_plane": dict(self.settings.provider_data_plane_limits_map),
            },
            "machine": {
                "active_downloads": 0,
                "active_download_limit": int(self.settings.nimbus_download_global_limit),
                "disk_path": str(self.settings.nimbus_data_dir),
                "disk_free_bytes": None,
                "min_free_bytes": int(self.settings.nimbus_download_min_free_bytes or 0),
                "bandwidth_limit_bps": (
                    int(self.settings.nimbus_download_global_max_bps)
                    if self.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "providers": {
                "copernicus": {
                    "job_limit": int(self.settings.provider_job_limits_map.get("copernicus", 1)),
                    "control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("copernicus", 1)),
                    "data_plane_limit": int(self.settings.provider_data_plane_limits_map.get("copernicus", 1)),
                    "active_downloads": 0,
                    "pending_tasks": 0,
                    "counts": {
                        "queued": 0,
                        "preparing": 0,
                        "ready": 0,
                        "downloading": 0,
                        "done": 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                    "accounts_configured": len(configured_accounts),
                    "accounts": configured_accounts,
                },
                "usgs": {
                    "job_limit": int(self.settings.provider_job_limits_map.get("usgs", 1)),
                    "control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("usgs", 1)),
                    "data_plane_limit": int(self.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "active_prepares": 0,
                    "active_downloads": 0,
                    "adaptive_window_current": min(
                        2,
                        int(self.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_peak": min(
                        2,
                        int(self.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_max": int(self.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "success_streak": 0,
                    "cooldown_seconds": 0.0,
                    "pending_tasks": 0,
                    "counts": {
                        "queued": 0,
                        "preparing": 0,
                        "ready": 0,
                        "downloading": 0,
                        "done": 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                },
            },
            "jobs": {
                "pending_tasks_total": 0,
                "pending_jobs_total": 0,
                "pending_by_job": [],
            },
            "tasks": {
                "active": [],
                "recent_terminal": [],
            },
        }

    def _local_download_coordinator_report(self) -> dict[str, Any]:
        if self._download_coordinator is None:
            return self._download_coordinator_placeholder_status()
        return self._download_coordinator.snapshot()

    @staticmethod
    def _wrap_download_coordinator_reports(
        *,
        reports: list[dict[str, Any]],
        source: str,
        summary: dict[str, Any],
        timestamp: str,
    ) -> dict[str, Any]:
        return {
            "status": str(summary.get("status") or ("unavailable" if not reports else "unknown")),
            "source": source,
            "timestamp": timestamp,
            "workers_reporting": len(reports),
            "summary": summary,
            "workers": reports,
        }

    def get_download_coordinator_status(self) -> dict[str, Any]:
        if self._execution_enabled or self._download_coordinator is not None:
            local_report = {
                "worker_id": self._worker_id,
                "hostname": self._worker_hostname,
                "pid": self._worker_pid,
                "runtime_role": self._runtime_role,
                "execution_enabled": self._execution_enabled,
                "last_seen_at": self._now_iso(),
                "snapshot": self._local_download_coordinator_report(),
            }
            return self._wrap_download_coordinator_reports(
                reports=[local_report],
                source="local_worker",
                summary=dict(local_report["snapshot"]),
                timestamp=self._now_iso(),
            )

        stale_after = max(5, int(self.settings.nimbus_worker_stale_seconds))
        now = datetime.now(timezone.utc)
        worker_reports: list[dict[str, Any]] = []
        for worker in self.store.list_workers():
            last_seen = self._parse_iso(worker.get("last_seen_at"))
            if last_seen is None:
                continue
            age_seconds = max(0.0, (now - last_seen).total_seconds())
            if age_seconds > stale_after or not bool(worker.get("execution_enabled", False)):
                continue
            metadata = dict(worker.get("metadata") or {})
            snapshot = metadata.get("download_coordinator")
            if not isinstance(snapshot, dict):
                continue
            worker_reports.append(
                {
                    "worker_id": str(worker.get("worker_id") or "").strip(),
                    "hostname": str(worker.get("hostname") or "").strip(),
                    "pid": worker.get("pid"),
                    "runtime_role": str(worker.get("runtime_role") or "").strip(),
                    "execution_enabled": bool(worker.get("execution_enabled", False)),
                    "last_seen_at": worker.get("last_seen_at"),
                    "snapshot": snapshot,
                }
            )

        if worker_reports:
            return self._wrap_download_coordinator_reports(
                reports=worker_reports,
                source="worker_heartbeats",
                summary=dict(worker_reports[0]["snapshot"]),
                timestamp=self._now_iso(),
            )

        placeholder = self._download_coordinator_placeholder_status(status="unavailable")
        return self._wrap_download_coordinator_reports(
            reports=[],
            source="worker_heartbeats",
            summary=placeholder,
            timestamp=self._now_iso(),
        )

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
        with self._pipeline_update_lock:
            row_now = self.store.get_job(job_id) or {}
            existing_pipeline_metadata = dict(row_now.get("pipeline_metadata") or {})
            merged_pipeline_metadata = dict(
                pipeline_metadata if pipeline_metadata is not None else existing_pipeline_metadata
            )
            existing_timeline = existing_pipeline_metadata.get("timeline")
            merged_pipeline_metadata["timeline"] = advance_pipeline_timeline(
                dict(existing_timeline) if isinstance(existing_timeline, dict) else {},
                job_state=str(row_now.get("state") or ""),
                pipeline_state=pipeline_state.value,
                pipeline_step=pipeline_step,
                pipeline_progress=pipeline_progress,
                timestamp=self._now_iso(),
                job_kind=self._job_kind_for_type(row_now.get("job_type")),
                mask_types=self._normalized_mask_types(
                    merged_pipeline_metadata.get("mask_types")
                    or existing_pipeline_metadata.get("mask_types")
                    or dict(row_now.get("request") or {}).get("mask_types")
                    or []
                ),
                cube_mode=self._timeline_cube_mode_for_row(
                    row_now,
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

    def _merged_pipeline_metadata(
        self,
        job_id: str,
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row_now = self.store.get_job(job_id) or {}
        existing_pipeline_metadata = dict(row_now.get("pipeline_metadata") or {})
        merged = dict(existing_pipeline_metadata)
        if pipeline_metadata is not None:
            merged.update(pipeline_metadata)
        timeline = merged.get("timeline")
        if isinstance(timeline, dict):
            merged["timeline"] = dict(timeline)
        elif isinstance(existing_pipeline_metadata.get("timeline"), dict):
            merged["timeline"] = dict(existing_pipeline_metadata["timeline"])
        return merged

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

    def _default_cube_output_dir(self, job_id: str) -> str:
        safe_job_id = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (job_id or "job")).strip("._-")
        if not safe_job_id:
            safe_job_id = "job"
        return str(self.settings.nimbus_data_dir / "zarr" / "cubes" / safe_job_id)

    @staticmethod
    def _path_size_bytes(target_path: str | Path | None) -> int | None:
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
    def _cube_config_from_request(request: JobCreateRequest) -> dict[str, Any] | None:
        if not isinstance(request, SearchDownloadRequest):
            return None
        cube_mode = NimbusFetcher._normalized_cube_mode(
            getattr(request, "cube_mode", "none")
        )
        if cube_mode == "none":
            return None
        return {
            "mode": cube_mode,
            "start_date": getattr(request, "cube_start_date", None),
            "end_date": getattr(request, "cube_end_date", None),
        }

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
        normalized["cloudmask_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(row.get("cloudmask_outputs") or []))))
        return normalized

    def _normalize_backend_paths_in_result_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(result or {})
        normalized["paths"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("paths") or []))))
        normalized["raw_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("raw_outputs") or []))))
        normalized["zarr_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("zarr_outputs") or []))))
        normalized["cube_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("cube_outputs") or []))))
        normalized["masked_zarr_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("masked_zarr_outputs") or []))))
        normalized["watermask_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("watermask_outputs") or []))))
        normalized["cloudmask_outputs"] = self._merge_paths([], list(self._normalize_backend_paths_payload(list(normalized.get("cloudmask_outputs") or []))))
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
        if not normalized:
            return None
        return canonicalize_usgs_product_type(normalized)

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

    def _register_cube_artifact(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        cube_stage: str,
        cube_summary: dict[str, Any],
    ) -> None:
        artifact_uri = str(cube_summary.get("zarr_uri") or "").strip()
        if not artifact_uri:
            return
        scene_ids = [str(item) for item in list(cube_summary.get("scene_ids") or []) if str(item).strip()]
        source_uris = [str(item) for item in list(cube_summary.get("source_zarr_uris") or []) if str(item).strip()]
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr_cube,
            artifact_uri=artifact_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_ids[0] if len(scene_ids) == 1 else None,
            source_uri=source_uris[0] if len(source_uris) == 1 else None,
            created_by_job_id=job_id,
            source_job_id=job_id,
            data_family=str(cube_summary.get("data_family") or "").strip() or None,
            band_names=[str(item) for item in list(cube_summary.get("band_names") or [])],
            dimensions=[str(item) for item in list(cube_summary.get("dimensions") or [])],
            shape=[int(item) for item in list(cube_summary.get("shape") or [])],
            size_bytes=self._path_size_bytes(artifact_uri),
            metadata={
                "cube_summary": cube_summary,
                "cube_stage": cube_stage,
                "group_key": str(cube_summary.get("group_key") or "").strip() or None,
                "source_scene_ids": scene_ids,
                "source_zarr_uris": source_uris,
                "registered_via": "pipeline_job",
            },
        )
        self.upsert_artifact(artifact_request)

    def _register_watermask_artifact(
        self,
        *,
        job_id: str,
        source_job_id: str,
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
            source_job_id=source_job_id,
            data_family="mask",
            dimensions=["time", "y", "x"],
            shape=list(water_mask.get("shape") or []),
            metadata={
                "water_mask": water_mask,
                "quality": self._water_mask_quality_fields(water_mask),
                "mask_contract_version": "v2",
                "registered_via": "manual_mask_job",
                "source_job_id": source_job_id,
            },
        )
        self.upsert_artifact(artifact_request)

    def _register_masked_zarr_artifact(
        self,
        *,
        job_id: str,
        source_job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        source_zarr_uri: str,
        masked_zarr_uri: str,
        mask_payload: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> None:
        artifact_uri = str(masked_zarr_uri or "").strip()
        if not artifact_uri or artifact_uri == str(source_zarr_uri or "").strip():
            return
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr_masked,
            artifact_uri=artifact_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_id,
            source_uri=source_zarr_uri,
            created_by_job_id=job_id,
            source_job_id=source_job_id,
            data_family="optical",
            band_names=list(dataset_summary.get("band_names") or []),
            dimensions=list(dataset_summary.get("dimensions") or []),
            shape=list(dataset_summary.get("shape") or []),
            metadata={
                "mask": mask_payload,
                "quality": self._mask_quality_fields(
                    water_mask=dict(mask_payload.get("water_mask") or {}),
                    cloud_mask=dict(mask_payload.get("cloud_mask") or {}),
                ),
                "mask_contract_version": "v2",
                "registered_via": "manual_mask_job",
                "source_zarr_uri": source_zarr_uri,
                "source_job_id": source_job_id,
            },
        )
        self.upsert_artifact(artifact_request)

    def _register_cloudmask_artifact(
        self,
        *,
        job_id: str,
        source_job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        zarr_uri: str,
        cloud_mask: dict[str, Any],
    ) -> None:
        artifact_uri = str(cloud_mask.get("artifact_uri") or "").strip()
        if not artifact_uri:
            return
        artifact_request = ArtifactUpsertRequest(
            artifact_type=ArtifactType.cloudmask,
            artifact_uri=artifact_uri,
            provider=ProviderName(provider_name),
            collection=collection,
            scene_id=scene_id,
            source_uri=zarr_uri,
            created_by_job_id=job_id,
            source_job_id=source_job_id,
            data_family="mask",
            dimensions=["time", "y", "x"],
            shape=list(cloud_mask.get("shape") or []),
            metadata={
                "cloud_mask": cloud_mask,
                "quality": self._cloud_mask_quality_fields(cloud_mask),
                "mask_contract_version": "v2",
                "registered_via": "manual_mask_job",
                "source_job_id": source_job_id,
            },
        )
        self.upsert_artifact(artifact_request)

    @staticmethod
    def _water_mask_quality_fields(water_mask: dict[str, Any]) -> dict[str, Any]:
        if not water_mask:
            return {}
        return {
            "status": str(water_mask.get("status") or "").strip().lower(),
            "runtime_mode": str(water_mask.get("runtime_mode") or "").strip(),
            "threshold_used": water_mask.get("threshold_used"),
            "sensor_recipe": str(water_mask.get("sensor_recipe") or "").strip(),
            "probability_source": str(water_mask.get("probability_source") or "").strip(),
            "water_fraction": float(water_mask.get("water_fraction") or 0.0),
            "cloud_blocked_fraction": float(water_mask.get("cloud_blocked_fraction") or 0.0),
            "input_bands": [str(item) for item in list(water_mask.get("input_bands") or [])],
            "mask_path": str(water_mask.get("mask_path") or "").strip(),
            "probability_path": str(water_mask.get("probability_path") or "").strip(),
        }

    @staticmethod
    def _cloud_mask_quality_fields(cloud_mask: dict[str, Any]) -> dict[str, Any]:
        if not cloud_mask:
            return {}
        inference = dict(cloud_mask.get("inference") or {})
        return {
            "status": str(cloud_mask.get("status") or "").strip().lower(),
            "backend": str(cloud_mask.get("backend") or "").strip(),
            "threshold": cloud_mask.get("threshold"),
            "includes_shadows": bool(
                cloud_mask.get("include_shadows", inference.get("includes_shadows", False))
            ),
            "mask_source": str(cloud_mask.get("mask_source") or inference.get("mask_source") or "").strip(),
            "probability_source": str(
                cloud_mask.get("probability_source") or inference.get("probability_source") or ""
            ).strip(),
            "sensor_recipe": str(
                cloud_mask.get("sensor_recipe") or cloud_mask.get("sensor") or inference.get("sensor_recipe") or ""
            ).strip(),
            "cloud_fraction": float(cloud_mask.get("cloud_fraction") or inference.get("cloud_fraction") or 0.0),
            "cloud_only_fraction": float(
                cloud_mask.get("cloud_only_fraction") or inference.get("cloud_only_fraction") or 0.0
            ),
            "shadow_fraction": float(cloud_mask.get("shadow_fraction") or inference.get("shadow_fraction") or 0.0),
            "input_bands": [str(item) for item in list(cloud_mask.get("input_bands") or [])],
            "mask_path": str(cloud_mask.get("mask_path") or "").strip(),
            "probability_path": str(cloud_mask.get("probability_path") or "").strip(),
        }

    def _mask_quality_fields(self, *, water_mask: dict[str, Any], cloud_mask: dict[str, Any]) -> dict[str, Any]:
        return {
            "water_mask": self._water_mask_quality_fields(water_mask),
            "cloud_mask": self._cloud_mask_quality_fields(cloud_mask),
            "water_fraction": float(water_mask.get("water_fraction") or 0.0),
            "cloud_fraction": float(
                cloud_mask.get("cloud_fraction")
                or dict(cloud_mask.get("inference") or {}).get("cloud_fraction")
                or 0.0
            ),
            "cloud_only_fraction": float(
                cloud_mask.get("cloud_only_fraction")
                or dict(cloud_mask.get("inference") or {}).get("cloud_only_fraction")
                or 0.0
            ),
            "shadow_fraction": float(
                cloud_mask.get("shadow_fraction")
                or dict(cloud_mask.get("inference") or {}).get("shadow_fraction")
                or 0.0
            ),
        }

    def _masker(self) -> Any:
        if self._mask_service is None:
            self._mask_service = MaskServiceClient(
                service_url=self.settings.nimbus_mask_service_url,
            )
        return self._mask_service

    def _remote_mask_runtime(self) -> dict[str, Any]:
        service_url = str(self.settings.nimbus_mask_service_url or "").strip()
        if not service_url:
            return {}
        try:
            health = dict(self._masker().health() or {})
        except Exception:
            return {}
        return dict(health.get("runtime") or {})

    @staticmethod
    def _invoke_mask_method(method: Callable[..., Any], **kwargs: Any) -> Any:
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return method(**kwargs)
        if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
            return method(**kwargs)
        filtered_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in signature.parameters
        }
        return method(**filtered_kwargs)

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

    def _masked_zarr_outputs_for_job(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        row: dict[str, Any],
    ) -> list[str]:
        outputs = self._merge_paths([], list(result.get("masked_zarr_outputs") or []))
        if outputs:
            return outputs
        outputs = self._merge_paths([], list(row.get("masked_zarr_outputs") or []))
        if outputs:
            return outputs
        artifacts = self.list_artifacts(
            artifact_type=ArtifactType.zarr_masked.value,
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
        return [str(item.artifact_uri).strip() for item in artifacts.items if str(item.artifact_uri).strip()]

    def _cube_outputs_for_job(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        row: dict[str, Any],
    ) -> list[str]:
        outputs = self._merge_paths([], list(result.get("cube_outputs") or []))
        if outputs:
            return outputs
        pipeline_metadata = dict(result.get("pipeline_metadata") or row.get("pipeline_metadata") or {})
        outputs = self._merge_paths([], list(pipeline_metadata.get("cube_outputs") or []))
        if outputs:
            return outputs
        artifacts = self.list_artifacts(
            artifact_type=ArtifactType.zarr_cube.value,
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
        return [str(item.artifact_uri).strip() for item in artifacts.items if str(item.artifact_uri).strip()]

    def _build_cube_outputs(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        source_zarr_outputs: list[str],
        cube_mode: str,
        cube_start_date: Any,
        cube_end_date: Any,
        pipeline_metadata: dict[str, Any],
        stage_start_progress: float,
        stage_end_progress: float,
    ) -> dict[str, Any]:
        if not source_zarr_outputs:
            return {
                "status": "skipped",
                "reason": "no_source_zarrs",
                "cube_outputs": [],
                "items": [],
                "pipeline_metadata": {
                    **pipeline_metadata,
                    "cube_mode": cube_mode,
                    "cube_stage": cube_mode,
                    "cube_status": "skipped",
                    "cube_reason": "no_source_zarrs",
                    "cube_output_count": 0,
                    "cube_outputs": [],
                },
            }

        output_dir = self._default_cube_output_dir(job_id)
        requested_date_range = {
            "start_date": cube_start_date.isoformat() if hasattr(cube_start_date, "isoformat") else cube_start_date,
            "end_date": cube_end_date.isoformat() if hasattr(cube_end_date, "isoformat") else cube_end_date,
        }
        queued_metadata = {
            **pipeline_metadata,
            "cube_mode": cube_mode,
            "cube_stage": cube_mode,
            "cube_status": "queued",
            "cube_output_dir": output_dir,
            "cube_source_count": len(source_zarr_outputs),
            "cube_date_range": requested_date_range,
        }
        self._update_pipeline(
            job_id,
            pipeline_state=PipelineState.cube_queued,
            pipeline_step="cube_queued",
            pipeline_progress=stage_start_progress,
            pipeline_metadata=queued_metadata,
            event_type="job.cube_queued",
            event_payload={
                "cube_mode": cube_mode,
                "cube_source_count": len(source_zarr_outputs),
                "cube_output_dir": output_dir,
                "cube_date_range": requested_date_range,
            },
        )

        self._update_pipeline(
            job_id,
            pipeline_state=PipelineState.cube_building,
            pipeline_step="cube_building",
            pipeline_progress=min(99.0, stage_start_progress + max(0.5, (stage_end_progress - stage_start_progress) * 0.5)),
            pipeline_metadata={**queued_metadata, "cube_status": "running"},
            event_type="job.cube_building",
            event_payload={
                "cube_mode": cube_mode,
                "cube_source_count": len(source_zarr_outputs),
            },
        )

        last_cube_emit = {"mono": 0.0, "progress": float(stage_start_progress)}

        def _emit_cube_progress(payload: dict[str, Any]) -> None:
            group_total = max(1, int(payload.get("group_total") or 1))
            group_index = min(group_total, max(1, int(payload.get("group_index") or 1)))
            cube_fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
            aggregate_fraction = min(
                1.0,
                max(0.0, ((group_index - 1) + cube_fraction) / group_total),
            )
            stage_span = max(0.0, float(stage_end_progress) - float(stage_start_progress))
            stage_cap = (
                max(float(stage_start_progress), float(stage_end_progress) - 0.1)
                if stage_span > 0.1
                else float(stage_end_progress)
            )
            candidate_progress = (
                float(stage_start_progress) + stage_span * aggregate_fraction
                if stage_span > 0.0
                else float(stage_end_progress)
            )
            pipeline_progress = max(
                float(last_cube_emit["progress"]),
                min(stage_cap, candidate_progress),
            )
            now_mono = time.monotonic()
            if not self._should_emit_zarr_progress(
                now_mono=now_mono,
                last_emit=float(last_cube_emit["mono"]),
                progress_pct=pipeline_progress,
                last_progress=float(last_cube_emit["progress"]),
            ):
                return
            last_cube_emit["mono"] = now_mono
            last_cube_emit["progress"] = pipeline_progress
            progress_metadata = {
                **queued_metadata,
                "cube_status": "running",
                "cube_active_group": str(payload.get("group_key") or "").strip(),
                "cube_group_index": group_index,
                "cube_group_total": group_total,
                "cube_current_scene_id": str(payload.get("scene_id") or "").strip(),
                "cube_current_layer": str(payload.get("layer_name") or "").strip(),
                "cube_current_band": str(payload.get("band_name") or "").strip(),
                "cube_blocks_written": int(payload.get("blocks_written") or 0),
                "cube_total_blocks": int(payload.get("total_blocks") or 0),
                "cube_fraction": round(cube_fraction, 6),
                "cube_aggregate_fraction": round(aggregate_fraction, 6),
                "cube_output_uri": str(payload.get("group_output_uri") or "").strip(),
            }
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_building,
                pipeline_step="cube_building",
                pipeline_progress=pipeline_progress,
                pipeline_metadata=progress_metadata,
            )

        try:
            cube_summary = build_grouped_time_cubes(
                source_zarr_outputs,
                output_dir,
                start_date=cube_start_date,
                end_date=cube_end_date,
                stage_label=cube_mode,
                progress_callback=_emit_cube_progress,
            )
        except Exception as exc:
            failed_metadata = {
                **queued_metadata,
                "cube_status": "failed",
                "cube_reason": str(exc),
                "cube_output_count": 0,
                "cube_outputs": [],
            }
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_failed,
                pipeline_step="cube_failed",
                pipeline_progress=min(99.0, stage_end_progress),
                pipeline_metadata=failed_metadata,
                event_type="job.cube_failed",
                event_payload={
                    "cube_mode": cube_mode,
                    "error": str(exc),
                },
            )
            raise

        cube_outputs = self._merge_paths([], list(cube_summary.get("cube_outputs") or []))
        cube_items = [dict(item) for item in list(cube_summary.get("items") or [])]
        for item in cube_items:
            self._register_cube_artifact(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                cube_stage=cube_mode,
                cube_summary=item,
            )

        final_metadata = {
            **pipeline_metadata,
            "cube_mode": cube_mode,
            "cube_stage": cube_mode,
            "cube_status": str(cube_summary.get("status") or "").strip() or "skipped",
            "cube_reason": str(cube_summary.get("reason") or "").strip() or "",
            "cube_output_dir": output_dir,
            "cube_output_count": len(cube_outputs),
            "cube_outputs": cube_outputs,
            "cube_items": cube_items,
            "cube_tiles_built": list(cube_summary.get("tiles_built") or []),
            "cube_tiles_skipped": list(cube_summary.get("tiles_skipped") or []),
            "cube_date_range": dict(cube_summary.get("date_range") or requested_date_range),
        }
        if cube_outputs:
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_written,
                pipeline_step="cube_written",
                pipeline_progress=stage_end_progress,
                pipeline_metadata=final_metadata,
                event_type="job.cube_written",
                event_payload={
                    "cube_mode": cube_mode,
                    "cube_outputs": cube_outputs,
                    "cube_output_count": len(cube_outputs),
                },
            )
        else:
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_building,
                pipeline_step="cube_skipped",
                pipeline_progress=stage_end_progress,
                pipeline_metadata=final_metadata,
                event_type="job.cube_skipped",
                event_payload={
                    "cube_mode": cube_mode,
                    "reason": str(cube_summary.get("reason") or "").strip() or "no_groups_with_multiple_times",
                },
            )
        return {
            **cube_summary,
            "cube_outputs": cube_outputs,
            "items": cube_items,
            "pipeline_metadata": final_metadata,
        }

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
            "pixel_size": list(attrs.get("reference_pixel_size") or []),
            "reference_pixel_size": list(attrs.get("reference_pixel_size") or []),
            "band_metadata": dict(attrs.get("band_metadata") or {}),
            "ancillary_metadata": dict(attrs.get("ancillary_metadata") or {}),
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

    def _scene_parallelism_target_from_download(
        self,
        *,
        pipeline_metadata: dict[str, Any] | None,
        total: int,
    ) -> int:
        if total <= 1:
            return 1
        metadata = dict(pipeline_metadata or {})
        selected_accounts = int(metadata.get("account_pool_selected_accounts", 0) or 0)
        account_labels: set[str] = set()
        for item in list(metadata.get("account_pool_assignments") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        for item in list(dict(metadata.get("download_telemetry") or {}).get("accounts") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        if account_labels:
            selected_accounts = max(selected_accounts, len(account_labels))
        if selected_accounts <= 1:
            selected_accounts = min(total, 4)
        return max(
            1,
            min(
                int(selected_accounts or 1),
                max(1, total),
                max(1, int(self.settings.nimbus_max_jobs or 1)),
                4,
            ),
        )

    @staticmethod
    def _zarr_convert_max_workers(
        *,
        total: int,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
    ) -> int:
        raw = str(os.getenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        cpu_budget = max(1, min(4, max(1, int((os.cpu_count() or 2) / 2))))
        default_value = min(max(1, int(preferred_parallelism or 1)), cpu_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))

    @staticmethod
    def _integrated_mask_max_workers(
        *,
        total: int,
        inference_device: str | None,
        water_inference_device: str | None,
        remote_runtime: dict[str, Any] | None = None,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
    ) -> int:
        raw = str(os.getenv("NIMBUS_MASK_SCENE_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        resolved_cloud = resolve_inference_device(
            explicit=inference_device,
            env_var="NIMBUS_CLOUDMASK_DEVICE",
        )
        resolved_water = resolve_inference_device(
            explicit=water_inference_device,
            env_var="NIMBUS_WATERMASK_DEVICE",
        )
        runtime_payload = dict(remote_runtime or {})
        remote_cloud = normalize_device_name(
            dict(runtime_payload.get("cloud") or {}).get("resolved")
        )
        remote_water = normalize_device_name(
            dict(runtime_payload.get("water") or {}).get("resolved")
        )
        if remote_cloud not in {"", "auto"}:
            resolved_cloud = remote_cloud
        if remote_water not in {"", "auto"}:
            resolved_water = remote_water
        remote_service = remote_runtime is not None
        has_accelerator = any(device in {"cuda", "mps"} for device in {resolved_cloud, resolved_water})
        cpu_budget = max(1, min(4, max(1, int((os.cpu_count() or 2) / 2))))
        if remote_service:
            heuristic_budget = min(cpu_budget, 2 if has_accelerator else 1)
        else:
            heuristic_budget = min(cpu_budget, 3 if has_accelerator else 2)
        default_target = (
            max(1, int(preferred_parallelism or 1))
            if preferred_parallelism is not None
            else (2 if total > 1 and has_accelerator else 1)
        )
        default_value = min(default_target, heuristic_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))

    def _convert_single_raw_output(
        self,
        *,
        provider_name: str,
        collection: str,
        product_type: str | None,
        raw_uri: str,
        scene_id: str,
        output_uri: str,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        converter = self._converter()
        convert_kwargs: dict[str, Any] = {
            "provider": provider_name,
            "collection": self._normalize_collection_for_zarr(provider_name, collection),
            "scene_id": scene_id,
            "raw_uri": raw_uri,
            "output_uri": output_uri,
            "product_type": self._normalize_product_type_for_zarr(product_type),
        }
        if progress_callback is not None:
            try:
                signature = inspect.signature(converter.convert)
            except (TypeError, ValueError):
                signature = None
            if signature is not None and "progress_callback" in signature.parameters:
                convert_kwargs["progress_callback"] = progress_callback
        written_uri, data_family, conversion_summary, dataset_summary = converter.convert(**convert_kwargs)
        return {
            "raw_uri": raw_uri,
            "scene_id": scene_id,
            "zarr_uri": written_uri,
            "data_family": data_family,
            "summary": conversion_summary,
            "dataset_summary": dataset_summary,
        }

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
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        if not raw_outputs:
            return [], {"status": "skipped", "reason": "no_raw_outputs"}

        total = max(1, len(raw_outputs))
        zarr_outputs: list[str] = []
        conversions: list[dict[str, Any]] = []
        prepared_items: list[dict[str, Any]] = []
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
            prepared_items.append(
                {
                    "index": index,
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": output_uri,
                }
            )

        max_workers = self._zarr_convert_max_workers(
            total=total,
            preferred_parallelism=self._scene_parallelism_target_from_download(
                pipeline_metadata=pipeline_metadata,
                total=total,
            ),
            max_limit=min(4, max(1, int(self.settings.nimbus_max_jobs or 1))),
        )
        if max_workers <= 1:
            for item in prepared_items:
                if is_cancelled():
                    raise JobCancelledError("Job cancellation requested.")
                index = int(item["index"])
                raw_uri = str(item["raw_uri"])
                scene_id = str(item["scene_id"])
                output_uri = str(item["output_uri"])
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
                        "parallel_workers": 1,
                    },
                    event_type="job.zarr_converting",
                    event_payload={
                        "raw_uri": raw_uri,
                        "scene_id": scene_id,
                        "output_uri": output_uri,
                        "index": index,
                        "total": total,
                        "stage": "writing_chunks",
                        "parallel_workers": 1,
                    },
                )
                progress_callback = self._build_zarr_progress_callback(
                    job_id=job_id,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    raw_uri=raw_uri,
                    scene_id=scene_id,
                    output_uri=output_uri,
                    index=index,
                    total=total,
                    parallel_workers=max_workers,
                )
                converted = self._convert_single_raw_output(
                    provider_name=provider_name,
                    collection=collection,
                    product_type=product_type,
                    raw_uri=raw_uri,
                    scene_id=scene_id,
                    output_uri=output_uri,
                    progress_callback=progress_callback,
                )
                zarr_outputs.append(str(converted["zarr_uri"]))
                conversions.append(converted)
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
                        "current_output_uri": converted["zarr_uri"],
                        "current_index": index,
                        "total": total,
                        "parallel_workers": 1,
                    },
                    event_type="job.zarr_converting",
                    event_payload={
                        "raw_uri": raw_uri,
                        "scene_id": scene_id,
                        "output_uri": converted["zarr_uri"],
                        "index": index,
                        "total": total,
                        "stage": "registering_artifact",
                        "parallel_workers": 1,
                    },
                )
                self._register_zarr_artifact(
                    job_id=job_id,
                    provider_name=provider_name,
                    collection=collection,
                    scene_id=scene_id,
                    raw_uri=raw_uri,
                    zarr_uri=str(converted["zarr_uri"]),
                    data_family=str(converted["data_family"]),
                    conversion_summary=dict(converted["summary"]),
                    dataset_summary=dict(converted["dataset_summary"]),
                )
            return zarr_outputs, {
                "status": "written",
                "count": len(zarr_outputs),
                "items": conversions,
                "parallel_workers": 1,
            }

        self._update_pipeline(
            job_id,
            pipeline_state=PipelineState.zarr_converting,
            pipeline_step="writing_chunks",
            pipeline_progress=0.0,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            conversion_metadata={
                "status": "running",
                "stage": "writing_chunks",
                "current_index": 0,
                "total": total,
                "parallel_workers": max_workers,
            },
            event_type="job.zarr_converting",
            event_payload={
                "raw_output_count": len(raw_outputs),
                "stage": "writing_chunks",
                "parallel_workers": max_workers,
            },
        )
        completed_by_index: dict[int, dict[str, Any]] = {}
        progress_lock = threading.Lock()
        progress_by_index: dict[int, float] = {
            int(item["index"]): 0.0 for item in prepared_items
        }
        last_emit = {"mono": 0.0, "progress": -1.0}

        def _make_parallel_progress_callback(item: dict[str, Any]) -> Callable[[dict[str, Any]], None]:
            index = int(item["index"])
            raw_uri = str(item["raw_uri"])
            scene_id = str(item["scene_id"])
            output_uri = str(item["output_uri"])

            def _callback(payload: dict[str, Any]) -> None:
                fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
                blocks_written = int(payload.get("blocks_written") or 0)
                total_blocks = int(payload.get("total_blocks") or 0)
                source_array_name = str(
                    payload.get("source_array_name") or payload.get("array_name") or ""
                ).strip()
                band_name = str(payload.get("band_name") or "").strip()
                now_mono = time.monotonic()
                with progress_lock:
                    progress_by_index[index] = max(progress_by_index.get(index, 0.0), fraction)
                    aggregate_fraction = sum(progress_by_index.values()) / total
                    pipeline_progress = min(99.0, aggregate_fraction * 85.0)
                    if not self._should_emit_zarr_progress(
                        now_mono=now_mono,
                        last_emit=float(last_emit["mono"]),
                        progress_pct=pipeline_progress,
                        last_progress=float(last_emit["progress"]),
                    ):
                        return
                    last_emit["mono"] = now_mono
                    last_emit["progress"] = pipeline_progress
                    items_completed = sum(
                        1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                    )
                    items_active = sum(
                        1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                    )
                conversion_payload = {
                    "status": "running",
                    "stage": "writing_chunks",
                    "current_raw_uri": raw_uri,
                    "current_scene_id": scene_id,
                    "current_output_uri": output_uri,
                    "current_index": index,
                    "total": total,
                    "parallel_workers": max_workers,
                    "chunk_fraction": round(fraction, 6),
                    "aggregate_fraction": round(aggregate_fraction, 6),
                    "blocks_written": blocks_written,
                    "total_blocks": total_blocks,
                    "items_total": total,
                    "items_completed": items_completed,
                    "items_active": items_active,
                }
                if source_array_name:
                    conversion_payload["current_array"] = source_array_name
                if band_name:
                    conversion_payload["current_band"] = band_name
                event_payload = {
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": output_uri,
                    "index": index,
                    "total": total,
                    "stage": "writing_chunks",
                    "parallel_workers": max_workers,
                    "chunk_fraction": round(fraction, 6),
                    "aggregate_fraction": round(aggregate_fraction, 6),
                    "blocks_written": blocks_written,
                    "total_blocks": total_blocks,
                    "items_total": total,
                    "items_completed": items_completed,
                    "items_active": items_active,
                }
                if source_array_name:
                    event_payload["array_name"] = source_array_name
                if band_name:
                    event_payload["band_name"] = band_name
                self._update_pipeline(
                    job_id,
                    pipeline_state=PipelineState.zarr_converting,
                    pipeline_step="writing_chunks",
                    pipeline_progress=pipeline_progress,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    conversion_metadata=conversion_payload,
                    event_type="job.zarr_converting",
                    event_payload=event_payload,
                )

            return _callback

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="zarr-convert") as executor:
            future_to_item = {
                executor.submit(
                    self._convert_single_raw_output,
                    provider_name=provider_name,
                    collection=collection,
                    product_type=product_type,
                    raw_uri=str(item["raw_uri"]),
                    scene_id=str(item["scene_id"]),
                    output_uri=str(item["output_uri"]),
                    progress_callback=_make_parallel_progress_callback(item),
                ): item
                for item in prepared_items
            }
            for future in as_completed(future_to_item):
                if is_cancelled():
                    raise JobCancelledError("Job cancellation requested.")
                item = future_to_item[future]
                converted = future.result()
                index = int(item["index"])
                with progress_lock:
                    progress_by_index[index] = 1.0
                    items_completed = sum(
                        1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                    )
                    items_active = sum(
                        1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                    )
                completed_by_index[index] = converted
                ordered_indices = sorted(completed_by_index)
                zarr_outputs = [str(completed_by_index[current]["zarr_uri"]) for current in ordered_indices]
                conversions = [completed_by_index[current] for current in ordered_indices]
                register_progress = min(
                    99.0,
                    (len(completed_by_index) / total) * 85.0,
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
                        "current_raw_uri": item["raw_uri"],
                        "current_scene_id": item["scene_id"],
                        "current_output_uri": converted["zarr_uri"],
                        "current_index": len(completed_by_index),
                        "total": total,
                        "parallel_workers": max_workers,
                        "items_total": total,
                        "items_completed": items_completed,
                        "items_active": items_active,
                    },
                    event_type="job.zarr_converting",
                    event_payload={
                        "raw_uri": item["raw_uri"],
                        "scene_id": item["scene_id"],
                        "output_uri": converted["zarr_uri"],
                        "index": len(completed_by_index),
                        "total": total,
                        "stage": "registering_artifact",
                        "parallel_workers": max_workers,
                        "items_total": total,
                        "items_completed": items_completed,
                        "items_active": items_active,
                    },
                )
                self._register_zarr_artifact(
                    job_id=job_id,
                    provider_name=provider_name,
                    collection=collection,
                    scene_id=str(item["scene_id"]),
                    raw_uri=str(item["raw_uri"]),
                    zarr_uri=str(converted["zarr_uri"]),
                    data_family=str(converted["data_family"]),
                    conversion_summary=dict(converted["summary"]),
                    dataset_summary=dict(converted["dataset_summary"]),
                )
        return zarr_outputs, {
            "status": "written",
            "count": len(zarr_outputs),
            "items": conversions,
            "parallel_workers": max_workers,
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

        self.store.update_job(
            job_id,
            state=JobState.running.value,
            started_at=self._now_iso(),
            finished_at=None,
            errors=[],
        )
        zarr_outputs, conversion_metadata = self._convert_raw_outputs(
            job_id=job_id,
            provider_name=provider_name,
            collection=collection,
            product_type=product_type,
            raw_outputs=[selected_raw_uri],
            is_cancelled=lambda: self._is_job_cancel_requested(job_id),
            scene_id_override=scene_id,
            output_uri_override=output_uri,
            pipeline_metadata=dict(row.get("pipeline_metadata") or {}),
        )
        pipeline_state = PipelineState.zarr_written
        pipeline_metadata = self._merged_pipeline_metadata(
            job_id,
            {
                **dict(row.get("pipeline_metadata") or {}),
                "manual_conversion": True,
                "raw_output_count": len(raw_outputs),
                "zarr_output_count": len(zarr_outputs),
                "zarr_parallel_workers": int(conversion_metadata.get("parallel_workers", 1) or 1),
            },
        )
        self._update_pipeline(
            job_id,
            pipeline_state=pipeline_state,
            pipeline_step="zarr_written",
            pipeline_progress=100.0,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            event_type="job.zarr_written",
            event_payload={
                "manual_conversion": True,
                "zarr_outputs": zarr_outputs,
                "pipeline_progress": 100.0,
            },
        )
        pipeline_metadata = self._merged_pipeline_metadata(job_id, pipeline_metadata)
        result_payload = {
            "job_id": job_id,
            "paths": list(result.get("paths") or []),
            "raw_outputs": raw_outputs,
            "zarr_outputs": zarr_outputs,
            "cube_outputs": [],
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

        job_type = str(row.get("job_type") or "").strip().lower()
        if job_type == "mask_existing_zarr":
            try:
                self._execute_mask_existing_zarr_job(
                    job_id=job_id,
                    row=row,
                    is_cancelled_now=is_cancelled_now,
                )
            except (DownloadCancelled, JobCancelledError):
                self._mark_cancelled(job_id, "cancelled_during_mask")
            except Exception as exc:
                current_row = self.store.get_job(job_id) or row
                request_payload = dict(current_row.get("request") or {})
                if (
                    str(request_payload.get("mask_contract_version") or "").strip().lower()
                    == self.MASK_CONTRACT_VERSION
                ):
                    self._fail_interrupted_mask_jobs(
                        job_ids=[job_id],
                        reason=f"Mask job crashed before finalization: {exc}",
                        event_type="job.mask_failed",
                    )
                else:
                    self.store.update_job(
                        job_id,
                        state=JobState.failed.value,
                        finished_at=self._now_iso(),
                        progress=0.0,
                        pipeline_state=PipelineState.failed.value,
                        pipeline_step="failed",
                        pipeline_progress=95.0,
                        pipeline_metadata=dict(current_row.get("pipeline_metadata") or {}),
                        conversion_metadata=dict(current_row.get("conversion_metadata") or {}),
                        zarr_outputs=[],
                        watermask_outputs=[],
                        cloudmask_outputs=[],
                        errors=[str(exc)],
                    )
                    self.store.append_event(
                        job_id,
                        "job.mask_failed",
                        {
                            "status": JobState.failed.value,
                            "error": str(exc),
                            "pipeline_state": PipelineState.failed.value,
                        },
                    )
            finally:
                self._cancel_check_cache.pop(job_id, None)
            return

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
            "download_strategy": str(getattr(request, "download_strategy", "default") or "default"),
            "download_only": bool(getattr(request, "download_only", False)),
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

        file_progress: dict[str, dict[str, Any]] = {}
        account_retry_state: dict[str, dict[str, Any]] = {}
        progress_lock = threading.Lock()
        aggregate = {
            "bytes_downloaded": 0,
            "bytes_total": 0,
            "last_emit": 0.0,
            "last_bytes": 0,
            "last_progress": 0.0,
            "last_time": time.monotonic(),
            "last_speed_bps": 0.0,
            "last_file": "",
        }

        def _current_download_pipeline_metadata() -> dict[str, Any]:
            row_now = self.store.get_job(job_id) or {}
            return {
                **base_pipeline_metadata,
                **dict(row_now.get("pipeline_metadata") or {}),
            }

        def emit_progress(
            file_name: str,
            delta: int,
            downloaded: int,
            total: int | None,
            context: dict[str, Any] | None = None,
        ) -> None:
            if is_cancelled_now():
                raise JobCancelledError("Job cancellation requested.")
            with progress_lock:
                context_payload = dict(context or {})
                account_label = self._download_account_label(context_payload)
                file_key = (
                    str(context_payload.get("product_id") or "").strip()
                    or str(file_name).strip()
                    or f"file-{len(file_progress) + 1}"
                )
                aggregate["bytes_downloaded"] += max(0, int(delta))
                aggregate["last_file"] = str(file_name or "").strip()
                file_progress[file_key] = {
                    "file_name": str(file_name or "").strip(),
                    "product_id": str(context_payload.get("product_id") or "").strip(),
                    "account_label": account_label,
                    "downloaded": int(downloaded or 0),
                    "total": int(total) if total is not None else None,
                    "completed": bool(total is not None and int(downloaded or 0) >= int(total or 0) > 0),
                    "last_update_mono": time.monotonic(),
                }
                retry_entry = account_retry_state.setdefault(account_label, {})
                if int(delta or 0) > 0:
                    retry_entry["status"] = "running"
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
                aggregate["last_speed_bps"] = speed

                if self._should_emit_download_progress(
                    delta=int(delta),
                    now_mono=now_mono,
                    last_emit=float(aggregate["last_emit"]),
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    last_bytes=int(aggregate["last_bytes"]),
                    progress_pct=float(progress_pct),
                    last_progress=float(aggregate["last_progress"]),
                    bytes_total=int(aggregate["bytes_total"]),
                ):
                    current_pipeline_metadata = _current_download_pipeline_metadata()
                    download_telemetry = self._build_download_telemetry(
                        pipeline_metadata=current_pipeline_metadata,
                        file_progress=file_progress,
                        bytes_downloaded=int(aggregate["bytes_downloaded"]),
                        bytes_total=int(aggregate["bytes_total"]),
                        progress_pct=float(progress_pct),
                        speed_bps=float(speed),
                        retry_state=account_retry_state,
                        phase="running",
                        last_file=str(aggregate["last_file"] or file_name or "").strip() or None,
                    )
                    self.store.update_job(
                        job_id,
                        progress=progress_pct,
                        bytes_downloaded=int(aggregate["bytes_downloaded"]),
                        bytes_total=int(aggregate["bytes_total"]),
                        pipeline_state=PipelineState.downloading.value,
                        pipeline_step="downloading",
                        pipeline_progress=pipeline_progress,
                        pipeline_metadata={
                            **current_pipeline_metadata,
                            "download_telemetry": download_telemetry,
                        },
                    )
                    self.store.append_event(
                        job_id,
                        "job.progress",
                        {
                            "file": file_name,
                            "account_label": account_label,
                            "bytes": int(aggregate["bytes_downloaded"]),
                            "bytes_total": int(aggregate["bytes_total"]),
                            "speed": speed,
                            "status": JobState.running.value,
                        },
                    )
                    aggregate["last_emit"] = now_mono
                    aggregate["last_time"] = now_mono
                    aggregate["last_bytes"] = int(aggregate["bytes_downloaded"])
                    aggregate["last_progress"] = float(progress_pct)

        def emit_retry(
            file_name: str,
            attempt: int,
            reason: str,
            retry_after: float | None,
            context: dict[str, Any] | None = None,
        ) -> None:
            with progress_lock:
                context_payload = dict(context or {})
                account_label = self._download_account_label(context_payload)
                row_now = self.store.get_job(job_id) or {}
                retry_count = int(row_now.get("retry_count", 0) or 0) + 1
                last_retry_at = self._now_iso()
                retry_entry = account_retry_state.setdefault(account_label, {})
                retry_entry["retry_count"] = int(retry_entry.get("retry_count", 0) or 0) + 1
                retry_entry["last_retry_at"] = last_retry_at
                retry_entry["last_reason"] = str(reason or "").strip()
                retry_entry["status"] = (
                    "rate_limited"
                    if str(reason or "").strip().lower() == "http_429"
                    else "retrying"
                )
                current_pipeline_metadata = {
                    **base_pipeline_metadata,
                    **dict(row_now.get("pipeline_metadata") or {}),
                }
                progress_pct = 0.0
                if int(aggregate["bytes_total"]) > 0:
                    progress_pct = min(
                        99.0,
                        100.0
                        * int(aggregate["bytes_downloaded"])
                        / max(1, int(aggregate["bytes_total"])),
                    )
                download_telemetry = self._build_download_telemetry(
                    pipeline_metadata=current_pipeline_metadata,
                    file_progress=file_progress,
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=int(aggregate["bytes_total"]),
                    progress_pct=float(progress_pct),
                    speed_bps=float(aggregate["last_speed_bps"]),
                    retry_state=account_retry_state,
                    phase=(
                        "rate_limited"
                        if str(reason or "").strip().lower() == "http_429"
                        else "retrying"
                    ),
                    last_file=str(file_name or "").strip() or None,
                )
                self.store.update_job(
                    job_id,
                    retry_count=retry_count,
                    last_retry_at=last_retry_at,
                    pipeline_metadata={
                        **current_pipeline_metadata,
                        "download_telemetry": download_telemetry,
                    },
                )
                self.store.append_event(
                    job_id,
                    "job.retrying",
                    {
                        "file": file_name,
                        "account_label": account_label,
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
            current_pipeline_metadata = {
                **base_pipeline_metadata,
                **dict((self.store.get_job(job_id) or {}).get("pipeline_metadata") or {}),
            }
            final_download_progress_pct = 0.0
            if max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])) > 0:
                final_download_progress_pct = min(
                    100.0,
                    100.0
                    * int(aggregate["bytes_downloaded"])
                    / max(1, max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"]))),
                )
            final_download_telemetry = self._build_download_telemetry(
                pipeline_metadata={
                    **current_pipeline_metadata,
                    **metadata,
                },
                file_progress=file_progress,
                bytes_downloaded=int(aggregate["bytes_downloaded"]),
                bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                progress_pct=float(final_download_progress_pct),
                speed_bps=float(aggregate["last_speed_bps"]),
                retry_state=account_retry_state,
                phase="completed",
                last_file=str(aggregate["last_file"] or "").strip() or None,
            )
            pipeline_metadata = {
                **current_pipeline_metadata,
                **base_pipeline_metadata,
                **metadata,
                "products_found": int(metadata.get("products_found", len(raw_outputs)) or len(raw_outputs)),
                "products_downloaded": int(metadata.get("products_downloaded", len(raw_outputs)) or len(raw_outputs)),
                "raw_output_count": len(raw_outputs),
                "download_telemetry": final_download_telemetry,
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
                "cube_outputs": [],
                "checksums": checksums,
                "metadata": metadata,
                "manifest_entry": manifest_entry,
                "pipeline_metadata": self._merged_pipeline_metadata(job_id, pipeline_metadata),
                "conversion_metadata": {},
            }
            self.store.set_result(job_id, base_result_payload)

            if bool(getattr(request, "download_only", False)):
                final_pipeline_metadata = {
                    **pipeline_metadata,
                    "download_only": True,
                    "zarr_output_count": 0,
                    "manual_conversion": False,
                }
                self.store.set_result(
                    job_id,
                    {
                        **base_result_payload,
                        "pipeline_metadata": final_pipeline_metadata,
                    },
                )
                self.store.update_job(
                    job_id,
                    state=JobState.succeeded.value,
                    progress=100.0,
                    finished_at=self._now_iso(),
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                    pipeline_state=PipelineState.downloaded.value,
                    pipeline_step="downloaded",
                    pipeline_progress=100.0,
                    pipeline_metadata=final_pipeline_metadata,
                    conversion_metadata={},
                    raw_outputs=raw_outputs,
                    zarr_outputs=[],
                )
                self.store.append_event(
                    job_id,
                    "job.succeeded",
                    {
                        "status": JobState.succeeded.value,
                        "paths": raw_result_paths,
                        "pipeline_state": PipelineState.downloaded.value,
                    },
                )
                return

            zarr_outputs, conversion_metadata = self._convert_raw_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=request.collection,
                product_type=getattr(request, "product_type", None),
                raw_outputs=raw_outputs,
                is_cancelled=is_cancelled_now,
                pipeline_metadata=pipeline_metadata,
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
            requested_mask_types = self._normalized_mask_types(getattr(request, "mask_types", []))
            cube_config = self._cube_config_from_request(request)
            cube_outputs: list[str] = []
            final_pipeline_metadata = self._merged_pipeline_metadata(
                job_id,
                {
                    **pipeline_metadata,
                    "zarr_output_count": len(zarr_outputs),
                    "manual_conversion": False,
                    "zarr_parallel_workers": int(conversion_metadata.get("parallel_workers", 1) or 1),
                },
            )
            if requested_mask_types:
                final_pipeline_metadata["mask_types"] = requested_mask_types
                final_pipeline_metadata["mask_mode"] = "integrated"
            if cube_config is not None:
                final_pipeline_metadata["cube_mode"] = cube_config["mode"]
                final_pipeline_metadata["cube_date_range"] = {
                    "start_date": (
                        cube_config["start_date"].isoformat()
                        if hasattr(cube_config["start_date"], "isoformat")
                        else cube_config["start_date"]
                    ),
                    "end_date": (
                        cube_config["end_date"].isoformat()
                        if hasattr(cube_config["end_date"], "isoformat")
                        else cube_config["end_date"]
                    ),
                }

            if final_pipeline_state == PipelineState.zarr_written:
                self._update_pipeline(
                    job_id,
                    pipeline_state=final_pipeline_state,
                    pipeline_step=final_pipeline_step,
                    pipeline_progress=72.0 if requested_mask_types else 100.0,
                    pipeline_metadata=final_pipeline_metadata,
                    conversion_metadata=conversion_metadata,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    event_type="job.zarr_written",
                    event_payload={
                        "zarr_outputs": zarr_outputs,
                        "pipeline_progress": 72.0 if requested_mask_types else 100.0,
                    },
                )
            final_pipeline_metadata = self._merged_pipeline_metadata(job_id, final_pipeline_metadata)
            self.store.set_result(
                job_id,
                {
                    "job_id": job_id,
                    "paths": final_paths,
                    "raw_outputs": raw_outputs,
                    "zarr_outputs": zarr_outputs,
                    "cube_outputs": cube_outputs,
                    "checksums": checksums,
                    "metadata": metadata,
                    "manifest_entry": manifest_entry,
                    "pipeline_metadata": final_pipeline_metadata,
                    "conversion_metadata": conversion_metadata,
                },
            )
            if final_pipeline_state == PipelineState.zarr_written:
                if cube_config is not None and cube_config["mode"] == "before_mask":
                    cube_execution = self._build_cube_outputs(
                        job_id=job_id,
                        provider_name=provider_name,
                        collection=request.collection,
                        source_zarr_outputs=zarr_outputs,
                        cube_mode=str(cube_config["mode"]),
                        cube_start_date=cube_config["start_date"],
                        cube_end_date=cube_config["end_date"],
                        pipeline_metadata=final_pipeline_metadata,
                        stage_start_progress=73.0,
                        stage_end_progress=75.0 if requested_mask_types else 100.0,
                    )
                    cube_outputs = list(cube_execution.get("cube_outputs") or [])
                    final_pipeline_metadata = dict(cube_execution.get("pipeline_metadata") or final_pipeline_metadata)
                    final_paths = [*raw_result_paths, *zarr_outputs, *cube_outputs]
                    self.store.set_result(
                        job_id,
                        {
                            "job_id": job_id,
                            "paths": final_paths,
                            "raw_outputs": raw_outputs,
                            "zarr_outputs": zarr_outputs,
                            "cube_outputs": cube_outputs,
                            "checksums": checksums,
                            "metadata": metadata,
                            "manifest_entry": manifest_entry,
                            "pipeline_metadata": final_pipeline_metadata,
                            "conversion_metadata": conversion_metadata,
                        },
                    )
                    if cube_outputs:
                        final_pipeline_state = PipelineState.cube_written if not requested_mask_types else final_pipeline_state
                        final_pipeline_step = "cube_written" if not requested_mask_types else final_pipeline_step

                self.store.update_job(
                    job_id,
                    state=JobState.running.value if requested_mask_types else JobState.succeeded.value,
                    started_at=(
                        str((self.store.get_job(job_id) or {}).get("started_at") or "").strip()
                        or self._now_iso()
                    ),
                    progress=100.0,
                    finished_at=None if requested_mask_types else self._now_iso(),
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                    pipeline_state=final_pipeline_state.value,
                    pipeline_step=final_pipeline_step,
                    pipeline_progress=76.0 if requested_mask_types else 100.0,
                    pipeline_metadata=final_pipeline_metadata,
                    conversion_metadata=conversion_metadata,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                )
                self.store.append_event(
                    job_id,
                    "job.cube_written" if final_pipeline_state == PipelineState.cube_written else "job.zarr_written",
                    {
                        "pipeline_state": final_pipeline_state.value,
                        "zarr_outputs": zarr_outputs,
                        "cube_outputs": cube_outputs,
                    },
                )
                if requested_mask_types:
                    total_mask_outputs = max(1, len(zarr_outputs))
                    mask_inference_device = str(getattr(request, "inference_device", "") or "").strip() or None
                    water_inference_device = (
                        str(getattr(request, "water_inference_device", "") or "").strip() or None
                    )
                    remote_mask_runtime = self._remote_mask_runtime()
                    mask_workers = self._integrated_mask_max_workers(
                        total=len(zarr_outputs),
                        inference_device=mask_inference_device,
                        water_inference_device=water_inference_device,
                        remote_runtime=remote_mask_runtime,
                        preferred_parallelism=self._scene_parallelism_target_from_download(
                            pipeline_metadata=final_pipeline_metadata,
                            total=len(zarr_outputs),
                        ),
                        max_limit=min(4, max(1, int(self.settings.nimbus_max_jobs or 1))),
                    )
                    mask_items: list[dict[str, Any] | None] = [None] * len(zarr_outputs)
                    mask_errors: list[str] = []
                    last_mask_execution: dict[str, Any] | None = None

                    progress_lock = threading.Lock()
                    progress_by_index: dict[int, float] = {
                        item_index: 0.0 for item_index in range(len(zarr_outputs))
                    }
                    stage_by_index: dict[int, PipelineState] = {}
                    step_by_index: dict[int, str] = {}
                    scene_count = max(1, len(zarr_outputs))

                    def _normalize_pipeline_state(value: Any) -> PipelineState:
                        if isinstance(value, PipelineState):
                            return value
                        try:
                            return PipelineState(str(value or "").strip())
                        except Exception:
                            return PipelineState.running_cloud_inference

                    def _normalize_pipeline_step(value: Any) -> str:
                        candidate = str(value or "").strip().lower()
                        if candidate:
                            return candidate
                        return "running_cloud_inference"

                    def _aggregate_pipeline_state() -> tuple[PipelineState, str]:
                        states = set(stage_by_index.values())
                        if PipelineState.failed in states:
                            return (
                                PipelineState.failed,
                                self._preferred_mask_failure_step(step_by_index.values()),
                            )
                        if PipelineState.running_water_inference in states:
                            return (
                                PipelineState.running_water_inference,
                                "running_water_inference",
                            )
                        if PipelineState.running_cloud_inference in states:
                            return (
                                PipelineState.running_cloud_inference,
                                "running_cloud_inference",
                            )
                        if "water" in requested_mask_types:
                            return (
                                PipelineState.running_water_inference,
                                "running_water_inference",
                            )
                        return (
                            PipelineState.running_cloud_inference,
                            "running_cloud_inference",
                        )

                    def _make_item_progress_callback(item_index: int) -> Callable[[dict[str, Any]], None]:
                        def _callback(payload: dict[str, Any]) -> None:
                            item_fraction = min(
                                1.0,
                                max(0.0, float(payload.get("item_fraction") or 0.0)),
                            )
                            with progress_lock:
                                progress_by_index[item_index] = max(
                                    progress_by_index.get(item_index, 0.0),
                                    item_fraction,
                                )
                                stage_by_index[item_index] = _normalize_pipeline_state(
                                    payload.get("pipeline_state")
                                )
                                step_by_index[item_index] = _normalize_pipeline_step(
                                    payload.get("pipeline_step")
                                )
                                aggregate_fraction = sum(progress_by_index.values()) / scene_count
                                aggregate_progress = 76.0 + (aggregate_fraction * 22.0)
                                aggregate_state, aggregate_step = _aggregate_pipeline_state()
                                aggregate_metadata = {
                                    **final_pipeline_metadata,
                                    **dict(payload.get("pipeline_metadata") or {}),
                                    "mask_parallel_workers": mask_workers,
                                    "mask_total_scenes": scene_count,
                                    "mask_completed_scenes": sum(
                                        1
                                        for current_fraction in progress_by_index.values()
                                        if current_fraction >= 1.0
                                    ),
                                    "mask_active_scenes": sum(
                                        1
                                        for current_fraction in progress_by_index.values()
                                        if 0.0 < current_fraction < 1.0
                                    ),
                                }
                                aggregate_event_payload = {
                                    **dict(payload.get("event_payload") or {}),
                                    "scene_index": item_index + 1,
                                    "scene_total": scene_count,
                                    "item_fraction": item_fraction,
                                    "aggregate_fraction": aggregate_fraction,
                                }
                                self._update_pipeline(
                                    job_id,
                                    pipeline_state=aggregate_state,
                                    pipeline_step=aggregate_step,
                                    pipeline_progress=aggregate_progress,
                                    pipeline_metadata=aggregate_metadata,
                                    event_type=str(payload.get("event_type") or "").strip() or None,
                                    event_payload=aggregate_event_payload,
                                )

                        return _callback

                    def _mark_item_completion(
                        item_index: int,
                        *,
                        mask_execution: dict[str, Any],
                    ) -> None:
                        item_succeeded = bool(mask_execution.get("succeeded"))
                        with progress_lock:
                            progress_by_index[item_index] = max(progress_by_index.get(item_index, 0.0), 1.0)
                            if not item_succeeded:
                                stage_by_index[item_index] = PipelineState.failed
                                step_by_index[item_index] = self._preferred_mask_failure_step(
                                    [mask_execution.get("failed_step")]
                                )
                            else:
                                stage_by_index.pop(item_index, None)
                                step_by_index.pop(item_index, None)
                            aggregate_fraction = sum(progress_by_index.values()) / scene_count
                            aggregate_progress = 76.0 + (aggregate_fraction * 22.0)
                            aggregate_state, aggregate_step = _aggregate_pipeline_state()
                            self._update_pipeline(
                                job_id,
                                pipeline_state=aggregate_state,
                                pipeline_step=aggregate_step,
                                pipeline_progress=aggregate_progress,
                                pipeline_metadata={
                                    **final_pipeline_metadata,
                                    "mask_parallel_workers": mask_workers,
                                    "mask_total_scenes": scene_count,
                                    "mask_completed_scenes": sum(
                                        1
                                        for current_fraction in progress_by_index.values()
                                        if current_fraction >= 1.0
                                    ),
                                    "mask_active_scenes": sum(
                                        1
                                        for current_fraction in progress_by_index.values()
                                        if 0.0 < current_fraction < 1.0
                                    ),
                                },
                            )

                    def _run_mask_item(item_index: int, zarr_uri: str) -> tuple[int, str, dict[str, Any]]:
                        if is_cancelled_now():
                            raise JobCancelledError(
                                "Job cancellation requested during integrated masking."
                            )
                        zarr_context = self._resolve_zarr_context(
                            job_id=job_id,
                            row=row,
                            result={"conversion_metadata": conversion_metadata},
                            zarr_uri=zarr_uri,
                            scene_id_override=None,
                            product_type_override=getattr(request, "product_type", None),
                        )
                        item_start = 76.0 + (item_index * (22.0 / total_mask_outputs))
                        item_end = 76.0 + ((item_index + 1) * (22.0 / total_mask_outputs))
                        mask_execution = self._run_in_place_mask_pipeline(
                            job_id=job_id,
                            source_job_id=job_id,
                            selected_zarr_uri=zarr_uri,
                            zarr_context=zarr_context,
                            mask_types=requested_mask_types,
                            backend_name="auto",
                            threshold=0.62,
                            inference_device=mask_inference_device,
                            include_shadows=True,
                            overwrite=True,
                            water_backend_name="auto",
                            water_overwrite=True,
                            water_inference_device=water_inference_device,
                            fail_on_error=False,
                            mask_mode="integrated",
                            include_resolve_stage=False,
                            resolve_progress=None,
                            stage_start_progress=item_start,
                            stage_end_progress=item_end,
                            expose_masked_outputs=False,
                            register_masked_artifact=False,
                            pipeline_progress_callback=_make_item_progress_callback(item_index),
                        )
                        return item_index, zarr_uri, mask_execution

                    if mask_workers <= 1 or len(zarr_outputs) <= 1:
                        for item_index, zarr_uri in enumerate(zarr_outputs):
                            item_index, zarr_uri, mask_execution = _run_mask_item(item_index, zarr_uri)
                            item_pipeline = dict(mask_execution.get("pipeline_metadata") or {})
                            item_conversion = dict(mask_execution.get("conversion_metadata") or {})
                            mask_items[item_index] = {
                                "zarr_uri": zarr_uri,
                                "status": str(mask_execution.get("status") or ""),
                                "pipeline_metadata": item_pipeline,
                                "conversion_metadata": item_conversion,
                                "errors": list(mask_execution.get("errors") or []),
                                "failed_step": mask_execution.get("failed_step"),
                            }
                            _mark_item_completion(item_index, mask_execution=mask_execution)
                            last_mask_execution = mask_execution
                            mask_errors.extend(
                                error
                                for error in list(mask_execution.get("errors") or [])
                                if error not in mask_errors
                            )
                            if not bool(mask_execution.get("succeeded")):
                                break
                    else:
                        with ThreadPoolExecutor(
                            max_workers=mask_workers,
                            thread_name_prefix="mask-scene",
                        ) as executor:
                            future_map = {
                                executor.submit(_run_mask_item, item_index, zarr_uri): (item_index, zarr_uri)
                                for item_index, zarr_uri in enumerate(zarr_outputs)
                            }
                            for future in as_completed(future_map):
                                item_index, zarr_uri = future_map[future]
                                item_index, zarr_uri, mask_execution = future.result()
                                item_pipeline = dict(mask_execution.get("pipeline_metadata") or {})
                                item_conversion = dict(mask_execution.get("conversion_metadata") or {})
                                mask_items[item_index] = {
                                    "zarr_uri": zarr_uri,
                                    "status": str(mask_execution.get("status") or ""),
                                    "pipeline_metadata": item_pipeline,
                                    "conversion_metadata": item_conversion,
                                    "errors": list(mask_execution.get("errors") or []),
                                    "failed_step": mask_execution.get("failed_step"),
                                }
                                _mark_item_completion(item_index, mask_execution=mask_execution)
                                last_mask_execution = mask_execution
                                mask_errors.extend(
                                    error
                                    for error in list(mask_execution.get("errors") or [])
                                    if error not in mask_errors
                                )
                    mask_items = [item for item in mask_items if item is not None]
                    final_pipeline_metadata["mask_parallel_workers"] = mask_workers
                    final_pipeline_metadata["mask_total_scenes"] = scene_count
                    final_pipeline_metadata["mask_completed_scenes"] = len(mask_items)
                    final_pipeline_metadata["mask_active_scenes"] = 0

                    mask_item_statuses = [
                        str((item or {}).get("status") or "").strip().lower()
                        for item in mask_items
                    ]
                    mask_succeeded = (
                        len(mask_items) == len(zarr_outputs)
                        and bool(mask_items)
                        and not mask_errors
                        and all(status == "written" for status in mask_item_statuses)
                    )
                    mask_summary = {
                        "status": "written" if mask_succeeded else "failed",
                        "mask_types": requested_mask_types,
                        "mask_mode": "integrated",
                        "items": mask_items,
                    }
                    if last_mask_execution is not None:
                        item_pipeline = dict(last_mask_execution.get("pipeline_metadata") or {})
                        item_conversion = dict(last_mask_execution.get("conversion_metadata") or {})
                        if len(mask_items) == 1:
                            mask_summary.update(
                                {
                                    "masked_zarr_uri": item_pipeline.get("masked_zarr_uri") or zarr_outputs[0],
                                    "water_mask": item_conversion.get("water_mask") or {},
                                    "cloud_mask": item_conversion.get("cloud_mask") or {},
                                    "mask_quality": item_conversion.get("mask_quality") or {},
                                }
                            )
                    final_pipeline_metadata = {
                        **final_pipeline_metadata,
                        "mask_status": mask_summary["status"],
                        "mask_items": mask_items,
                        "mask_types": requested_mask_types,
                        "mask_mode": "integrated",
                    }
                    if len(mask_items) == 1 and last_mask_execution is not None:
                        item_pipeline = dict(last_mask_execution.get("pipeline_metadata") or {})
                        item_conversion = dict(last_mask_execution.get("conversion_metadata") or {})
                        final_pipeline_metadata.update(
                            {
                                "masked_zarr_uri": item_pipeline.get("masked_zarr_uri") or zarr_outputs[0],
                                "water_mask": item_conversion.get("water_mask") or {},
                                "cloud_mask": item_conversion.get("cloud_mask") or {},
                                "mask_quality": item_conversion.get("mask_quality") or {},
                            }
                        )
                    combined_conversion_metadata = {
                        **conversion_metadata,
                        "mask": mask_summary,
                    }
                    combined_metadata = {
                        **metadata,
                        "mask": mask_summary,
                    }
                    combined_cube_outputs = list(cube_outputs)
                    if cube_config is not None and cube_config["mode"] == "after_mask" and mask_succeeded:
                        cube_execution = self._build_cube_outputs(
                            job_id=job_id,
                            provider_name=provider_name,
                            collection=request.collection,
                            source_zarr_outputs=zarr_outputs,
                            cube_mode=str(cube_config["mode"]),
                            cube_start_date=cube_config["start_date"],
                            cube_end_date=cube_config["end_date"],
                            pipeline_metadata=final_pipeline_metadata,
                            stage_start_progress=98.0,
                            stage_end_progress=100.0,
                        )
                        combined_cube_outputs = list(cube_execution.get("cube_outputs") or [])
                        final_pipeline_metadata = dict(cube_execution.get("pipeline_metadata") or final_pipeline_metadata)
                    final_paths = [*raw_result_paths, *zarr_outputs, *combined_cube_outputs]
                    terminal_pipeline_step = (
                        PipelineState.cube_written.value
                        if mask_succeeded
                        and cube_config is not None
                        and cube_config["mode"] == "after_mask"
                        and combined_cube_outputs
                        else PipelineState.masked_zarr_written.value
                    )
                    if not mask_succeeded:
                        terminal_pipeline_step = self._mask_failure_step_from_items(
                            mask_types=requested_mask_types,
                            items=cast(list[dict[str, Any]], mask_items),
                        )
                        mask_summary["failed_step"] = terminal_pipeline_step
                        final_pipeline_metadata["failed_step"] = terminal_pipeline_step
                        combined_conversion_metadata["failed_step"] = terminal_pipeline_step
                    terminal_pipeline_state = (
                        PipelineState.cube_written
                        if mask_succeeded
                        and cube_config is not None
                        and cube_config["mode"] == "after_mask"
                        and combined_cube_outputs
                        else PipelineState.masked_zarr_written
                        if mask_succeeded
                        else PipelineState.failed
                    )
                    terminal_state = (
                        JobState.succeeded
                        if terminal_pipeline_state in {PipelineState.masked_zarr_written, PipelineState.cube_written}
                        else JobState.failed
                    )
                    final_pipeline_metadata = self._merged_pipeline_metadata(job_id, final_pipeline_metadata)
                    self._update_pipeline(
                        job_id,
                        pipeline_state=terminal_pipeline_state,
                        pipeline_step=terminal_pipeline_step,
                        pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
                        pipeline_metadata=final_pipeline_metadata,
                        conversion_metadata=combined_conversion_metadata,
                        raw_outputs=raw_outputs,
                        zarr_outputs=zarr_outputs,
                    )
                    final_pipeline_metadata = self._merged_pipeline_metadata(job_id, final_pipeline_metadata)
                    self.store.set_result(
                        job_id,
                        {
                            "job_id": job_id,
                            "paths": final_paths,
                            "raw_outputs": raw_outputs,
                            "zarr_outputs": zarr_outputs,
                            "cube_outputs": combined_cube_outputs,
                            "checksums": checksums,
                            "metadata": combined_metadata,
                            "manifest_entry": manifest_entry,
                            "pipeline_metadata": final_pipeline_metadata,
                            "conversion_metadata": combined_conversion_metadata,
                        },
                    )
                    self.store.update_job(
                        job_id,
                        state=terminal_state.value,
                        progress=100.0 if terminal_state == JobState.succeeded else 0.0,
                        finished_at=self._now_iso(),
                        bytes_downloaded=int(aggregate["bytes_downloaded"]),
                        bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                        pipeline_state=terminal_pipeline_state.value,
                        pipeline_step=terminal_pipeline_step,
                        pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
                        pipeline_metadata=final_pipeline_metadata,
                        conversion_metadata=combined_conversion_metadata,
                        raw_outputs=raw_outputs,
                        zarr_outputs=zarr_outputs,
                        errors=mask_errors,
                    )
                    self.store.append_event(
                        job_id,
                        "job.mask_completed" if terminal_state == JobState.succeeded else "job.mask_failed",
                        {
                            "status": terminal_state.value,
                            "source_job_id": job_id,
                            "mask_types": requested_mask_types,
                            "zarr_outputs": zarr_outputs,
                            "cube_outputs": combined_cube_outputs,
                            "pipeline_state": terminal_pipeline_state.value,
                            "errors": mask_errors,
                        },
                    )
                    self.store.append_event(
                        job_id,
                        "job.succeeded" if terminal_state == JobState.succeeded else "job.failed",
                        {
                            "status": terminal_state.value,
                            "paths": final_paths,
                            "pipeline_state": terminal_pipeline_state.value,
                            "error": mask_errors[0] if mask_errors else "",
                        },
                    )
                    return
            final_pipeline_metadata = self._merged_pipeline_metadata(job_id, final_pipeline_metadata)
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
            is_cube_failure = current_pipeline_state in {
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_failed.value,
            }
            pipeline_state = (
                PipelineState.zarr_failed
                if is_zarr_failure and raw_outputs
                else PipelineState.cube_failed
                if is_cube_failure
                else PipelineState.failed
            )
            pipeline_step = (
                "zarr_failed"
                if pipeline_state == PipelineState.zarr_failed
                else "cube_failed"
                if pipeline_state == PipelineState.cube_failed
                else "failed"
            )
            conversion_metadata = dict(existing_result.get("conversion_metadata") or {})
            if pipeline_state in {PipelineState.zarr_failed, PipelineState.cube_failed}:
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
                            "cube_outputs": list(existing_result.get("cube_outputs") or []),
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
        download_strategy = str(getattr(request, "download_strategy", "default") or "default").strip().lower() or "default"
        data_plane_limit = self.settings.provider_data_plane_limits_map.get(provider_name, 1)

        download_manager_kwargs: dict[str, Any] = dict(
            max_concurrent=data_plane_limit,
            progress_callback=progress_callback,
            cancel_checker=is_cancelled,
            retry_callback=retry_callback,
        )
        if provider_name == "copernicus":
            # Keep Copernicus aligned with the older downloader profile that
            # proved stable in the previous project version.
            download_manager_kwargs.update(
                max_concurrent=min(data_plane_limit, 2),
                max_retries=5,
                initial_delay=2.0,
                backoff_factor=1.5,
                connect_timeout=30.0,
                chunk_size=128 * 1024,
                max_connections=50,
                max_connections_per_host=2,
            )
        elif provider_name == "usgs":
            # Keep USGS closer to the older conservative downloader profile to
            # reduce incomplete request churn and per-host pressure.
            download_manager_kwargs.update(
                max_concurrent=min(data_plane_limit, 2),
                initial_delay=2.0,
                backoff_factor=1.5,
                connect_timeout=30.0,
                chunk_size=128 * 1024,
                max_connections=50,
                max_connections_per_host=2,
            )

        download_manager = DownloadManager(**download_manager_kwargs)
        provider = self._build_provider(provider_name, download_manager)
        if provider_name == "copernicus":
            setattr(provider, "download_strategy", download_strategy)

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
                    "download_strategy": download_strategy,
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
                    "download_strategy": download_strategy,
                    **(
                        dict(provider.plan_download_metadata(len(product_ids)))
                        if hasattr(provider, "plan_download_metadata")
                        else {}
                    ),
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

            if self._supports_download_coordinator(provider_name, provider):
                coordinator_result = self._download_with_coordinator(
                    job_id=job_id,
                    provider_name=provider_name,
                    provider=provider,
                    collection=request.collection,
                    product_ids=product_ids,
                    output_dir=Path(output_dir),
                    progress_callback=progress_callback,
                    retry_callback=retry_callback,
                    cancel_checker=is_cancelled,
                    download_strategy=download_strategy,
                )
                paths = list(coordinator_result.paths)
                provider_download_metadata = dict(coordinator_result.metadata or {})
            else:
                paths = provider.download_products(product_ids=product_ids, output_dir=str(output_dir))
                provider_download_metadata = dict(getattr(provider, "last_download_metadata", {}) or {})
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
                    "download_strategy": download_strategy,
                    **provider_download_metadata,
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
                "download_strategy": download_strategy,
                **(
                    dict(provider.plan_download_metadata(len(request.product_ids)))
                    if hasattr(provider, "plan_download_metadata")
                    else {}
                ),
            },
            event_type="job.downloading",
            event_payload={"products_requested": len(request.product_ids)},
        )
        if self._supports_download_coordinator(provider_name, provider):
            coordinator_result = self._download_with_coordinator(
                job_id=job_id,
                provider_name=provider_name,
                provider=provider,
                collection=request.collection,
                product_ids=list(request.product_ids),
                output_dir=Path(output_dir),
                progress_callback=progress_callback,
                retry_callback=retry_callback,
                cancel_checker=is_cancelled,
                download_strategy=download_strategy,
            )
            paths = list(coordinator_result.paths)
            provider_download_metadata = dict(coordinator_result.metadata or {})
        else:
            paths = provider.download_products(product_ids=request.product_ids, output_dir=str(output_dir))
            provider_download_metadata = dict(getattr(provider, "last_download_metadata", {}) or {})
        return {
            "paths": paths,
            "metadata": {
                "job_type": request.job_type,
                "provider": provider_name,
                "collection": request.collection,
                "products_requested": len(request.product_ids),
                "products_downloaded": len(paths),
                "output_dir": str(output_dir),
                "download_strategy": download_strategy,
                **provider_download_metadata,
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

    def _download_coordinator_instance(self) -> DownloadCoordinator:
        if self._download_coordinator is None:
            self._download_coordinator = DownloadCoordinator(self.settings)
        return self._download_coordinator

    @staticmethod
    def _supports_download_coordinator(
        provider_name: str,
        provider: Any,
    ) -> bool:
        if provider_name == "copernicus":
            return isinstance(provider, CopernicusProvider)
        if provider_name == "usgs":
            return isinstance(provider, UsgsProvider)
        return False

    def _download_with_coordinator(
        self,
        *,
        job_id: str,
        provider_name: str,
        provider: Any,
        collection: str,
        product_ids: list[str],
        output_dir: Path,
        progress_callback: ProgressCallback | None,
        retry_callback: RetryCallback | None,
        cancel_checker: CancelChecker | None,
        download_strategy: str,
    ) -> DownloadBatchResult:
        coordinator = self._download_coordinator_instance()
        result = coordinator.download_products(
            job_id=job_id,
            provider_name=provider_name,
            provider=provider,
            collection=collection,
            product_ids=product_ids,
            output_dir=str(output_dir),
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            cancel_checker=cancel_checker,
            download_strategy=download_strategy,
        )
        setattr(provider, "last_download_metadata", dict(result.metadata or {}))
        return result

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
        started_at = self._effective_started_at_for_row(row)
        finished_at = self._parse_iso(row.get("finished_at"))
        updated_at = self._parse_iso(row.get("updated_at"))
        duration_seconds = self._duration_seconds_for_row(
            state=str(row.get("state") or ""),
            started_at=started_at,
            finished_at=finished_at,
            updated_at=updated_at,
        )
        job_type = row.get("job_type")
        job_kind = self._job_kind_for_type(job_type)
        service_name = self._service_name_for_type(job_type)
        masked_zarr_outputs = self._masked_zarr_outputs_for_job(job_id=row["job_id"], result={}, row=row)
        pipeline_metadata = dict(row.get("pipeline_metadata") or {})
        cube_outputs = self._cube_outputs_for_job(job_id=row["job_id"], result={}, row=row)
        pipeline_progress = (
            float(row["pipeline_progress"])
            if row.get("pipeline_progress") is not None
            else None
        )
        timeline_cube_mode = self._timeline_cube_mode_for_row(row, pipeline_metadata)
        timeline_timestamp: str | datetime
        if str(row.get("state") or "").strip().lower() in {
            JobState.running.value,
            JobState.cancel_requested.value,
        }:
            timeline_timestamp = self._now_iso()
        else:
            timeline_timestamp = finished_at or updated_at or started_at or self._now_iso()
        existing_timeline = pipeline_metadata.get("timeline")
        pipeline_timeline = advance_pipeline_timeline(
            dict(existing_timeline) if isinstance(existing_timeline, dict) else {},
            job_state=str(row.get("state") or ""),
            pipeline_state=str(row.get("pipeline_state") or PipelineState.queued.value),
            pipeline_step=row.get("pipeline_step"),
            pipeline_progress=pipeline_progress,
            timestamp=timeline_timestamp,
            job_kind=job_kind,
            mask_types=self._normalized_mask_types(
                (row.get("request") or {}).get("mask_types")
                or pipeline_metadata.get("mask_types")
                or dict(row.get("conversion_metadata") or {}).get("mask_types")
                or []
            ),
            cube_mode=timeline_cube_mode,
        )
        timeline_mask_types = self._normalized_mask_types(
            (row.get("request") or {}).get("mask_types")
            or pipeline_metadata.get("mask_types")
            or dict(row.get("conversion_metadata") or {}).get("mask_types")
            or []
        )
        if self._pipeline_timeline_needs_rebuild(
            row=row,
            pipeline_timeline=pipeline_timeline,
            mask_types=timeline_mask_types,
            cube_mode=timeline_cube_mode,
        ):
            pipeline_timeline = self._rebuild_pipeline_timeline_from_events(
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
            last_retry_at=self._parse_iso(row.get("last_retry_at")),
            product_type=row.get("product_type"),
            tile_id=row.get("tile_id"),
            created_at=self._parse_iso(row.get("created_at")),
            updated_at=updated_at,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            errors=list(row.get("errors", [])),
            provider=ProviderName(row["provider"]),
            collection=str(row["collection"]),
        )

    def _rebuild_pipeline_timeline_from_events(
        self,
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
            events = self.store.list_events(str(row.get("job_id") or ""), None, 500)
        except Exception:
            events = []
        events = self._events_for_current_timeline_attempt(events)

        for event in events:
            payload = dict(event.get("payload") or {})
            event_pipeline_state = str(payload.get("pipeline_state") or "").strip().lower()
            event_pipeline_step = str(payload.get("pipeline_step") or event_pipeline_state).strip().lower()
            if not event_pipeline_state and not event_pipeline_step:
                continue
            event_progress = payload.get("pipeline_progress")
            event_cube_mode = self._normalized_cube_mode(payload.get("cube_mode") or cube_mode)
            rebuilt = advance_pipeline_timeline(
                rebuilt,
                job_state=str(row.get("state") or ""),
                pipeline_state=event_pipeline_state or str(row.get("pipeline_state") or ""),
                pipeline_step=event_pipeline_step or event_pipeline_state,
                pipeline_progress=(
                    float(event_progress)
                    if event_progress is not None
                    else None
                ),
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

    @staticmethod
    def _events_for_current_timeline_attempt(
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
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
    def _pipeline_timeline_needs_rebuild(
        *,
        row: dict[str, Any],
        pipeline_timeline: dict[str, Any],
        mask_types: list[str],
        cube_mode: str,
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
        normalized_cube_mode = NimbusFetcher._normalized_cube_mode(
            cube_mode or pipeline_timeline.get("cube_mode")
        )
        cube_related_states = {
            PipelineState.cube_queued.value,
            PipelineState.cube_building.value,
            PipelineState.cube_written.value,
            PipelineState.cube_failed.value,
        }

        if normalized_cube_mode != "none" and "cube" not in stage_statuses:
            return True
        if (
            pipeline_state in cube_related_states
            and stage_statuses.get("cube", "pending") == "pending"
        ):
            return True
        cube_stage_status = stage_statuses.get("cube", "pending")
        if (
            normalized_cube_mode == "before_mask"
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
            normalized_cube_mode == "after_mask"
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
        if pipeline_state in {PipelineState.downloading.value, PipelineState.downloaded.value} and stage_statuses.get("download", "pending") == "pending":
            return True
        if pipeline_state in {
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
        } and stage_statuses.get("convert", "pending") == "pending":
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
        if (
            pipeline_state == PipelineState.running_cloud_inference.value
            and current_stage not in {"cloud"}
        ):
            return True
        if (
            pipeline_state == PipelineState.running_water_inference.value
            and current_stage not in {"water"}
        ):
            return True
        if (
            pipeline_state == PipelineState.cube_building.value
            and current_stage not in {"cube"}
        ):
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
            normalized_cube_mode == "before_mask"
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
    def _duration_seconds_for_row(
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

    def _effective_started_at_for_row(self, row: dict[str, Any]) -> datetime | None:
        started_at = self._parse_iso(row.get("started_at"))
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
            events = self.store.list_events(str(row.get("job_id") or ""), None, 200)
        except Exception:
            events = []
        events = self._events_for_current_timeline_attempt(events)
        for event in events:
            event_type = str(event.get("type") or "").strip()
            event_ts = self._parse_iso(event.get("timestamp"))
            if event_ts is None:
                continue
            if event_type in preferred_types:
                return event_ts
            if fallback_event_ts is None and event_type not in ignored_types:
                fallback_event_ts = event_ts
        return fallback_event_ts or self._parse_iso(row.get("created_at"))

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

    @staticmethod
    def _download_account_label(context: dict[str, Any] | None) -> str:
        label = str((context or {}).get("account_label") or "primary").strip()
        return label or "primary"

    @classmethod
    def _build_download_telemetry(
        cls,
        *,
        pipeline_metadata: dict[str, Any],
        file_progress: dict[str, dict[str, Any]],
        bytes_downloaded: int,
        bytes_total: int,
        progress_pct: float,
        speed_bps: float,
        retry_state: dict[str, dict[str, Any]],
        phase: str,
        last_file: str | None = None,
    ) -> dict[str, Any]:
        metadata = dict(pipeline_metadata or {})
        assignments = list(metadata.get("account_pool_assignments") or [])
        planned_counts: dict[str, int] = {}
        ordered_labels: list[str] = []
        for entry in assignments:
            label = str((entry or {}).get("account_label") or "").strip()
            if not label:
                continue
            if label not in ordered_labels:
                ordered_labels.append(label)
            planned_counts[label] = max(0, int((entry or {}).get("product_count") or 0))

        for file_state in file_progress.values():
            label = cls._download_account_label(file_state)
            if label not in ordered_labels:
                ordered_labels.append(label)
            planned_counts.setdefault(label, 0)

        for label in retry_state:
            normalized = str(label or "").strip()
            if not normalized:
                continue
            if normalized not in ordered_labels:
                ordered_labels.append(normalized)
            planned_counts.setdefault(normalized, 0)

        products_found = int(metadata.get("products_found") or metadata.get("products_requested") or 0)
        products_downloaded = int(metadata.get("products_downloaded") or 0)
        total_known_files = len(file_progress)
        total_completed_files = sum(1 for entry in file_progress.values() if bool(entry.get("completed")))
        if str(phase).strip().lower() == "completed" and products_downloaded > 0:
            total_completed_files = max(total_completed_files, products_downloaded)

        account_rows: list[dict[str, Any]] = []
        for label in ordered_labels:
            observed = [
                dict(item)
                for item in file_progress.values()
                if cls._download_account_label(item) == label
            ]
            assigned = max(int(planned_counts.get(label, 0) or 0), len(observed))
            bytes_done = sum(max(0, int(item.get("downloaded") or 0)) for item in observed)
            bytes_known_total = sum(
                int(item.get("total") or 0)
                for item in observed
                if item.get("total") is not None
            )
            files_completed = sum(1 for item in observed if bool(item.get("completed")))
            if str(phase).strip().lower() == "completed" and assigned > 0:
                files_completed = max(files_completed, assigned)
            active_items = [item for item in observed if not bool(item.get("completed"))]
            latest_items = sorted(
                observed,
                key=lambda item: float(item.get("last_update_mono") or 0.0),
                reverse=True,
            )
            current_item = (active_items or latest_items or [None])[0]
            retry_info = dict(retry_state.get(label) or {})
            retry_status = str(retry_info.get("status") or "").strip().lower()
            rate_limited = str(retry_info.get("last_reason") or "").strip().lower() == "http_429"
            if str(phase).strip().lower() == "completed" and assigned > 0:
                account_status = "completed"
            elif retry_status == "rate_limited" and not active_items:
                account_status = "rate_limited"
            elif retry_status == "retrying" and not active_items:
                account_status = "retrying"
            elif files_completed >= assigned > 0:
                account_status = "completed"
            elif bytes_done > 0 or observed:
                account_status = "running"
            else:
                account_status = "queued"

            account_progress = 0.0
            if bytes_known_total > 0:
                account_progress = min(100.0, 100.0 * bytes_done / bytes_known_total)
            elif account_status == "completed" and assigned > 0:
                account_progress = 100.0

            account_rows.append(
                {
                    "account_label": label,
                    "status": account_status,
                    "product_count_assigned": assigned,
                    "files_observed": len(observed),
                    "files_completed": files_completed,
                    "active_file_count": len(active_items),
                    "bytes_downloaded": bytes_done,
                    "bytes_total": bytes_known_total,
                    "progress_pct": round(account_progress, 2),
                    "retry_count": int(retry_info.get("retry_count", 0) or 0),
                    "rate_limited": rate_limited,
                    "last_retry_at": retry_info.get("last_retry_at"),
                    "last_retry_reason": retry_info.get("last_reason"),
                    "current_file": (
                        str((current_item or {}).get("file_name") or "").strip() or None
                    ),
                }
            )

        total_bytes = max(int(bytes_total or 0), int(bytes_downloaded or 0))
        eta_seconds: float | None = None
        if speed_bps > 0 and total_bytes > int(bytes_downloaded or 0):
            eta_seconds = max(0.0, (total_bytes - int(bytes_downloaded or 0)) / max(speed_bps, 1.0))
        selected_accounts = int(metadata.get("account_pool_selected_accounts", 0) or 0)
        if selected_accounts <= 0 and account_rows:
            selected_accounts = len(
                [
                    row
                    for row in account_rows
                    if int(row.get("product_count_assigned", 0) or 0) > 0
                    or int(row.get("files_observed", 0) or 0) > 0
                    or int(row.get("bytes_downloaded", 0) or 0) > 0
                ]
            )
        try:
            download_window_seconds = (
                float(metadata.get("download_window_seconds"))
                if metadata.get("download_window_seconds") is not None
                else None
            )
        except (TypeError, ValueError):
            download_window_seconds = None

        return {
            "status": str(phase or "running").strip().lower() or "running",
            "strategy": str(metadata.get("download_strategy") or "default").strip().lower() or "default",
            "selected_accounts": selected_accounts,
            "pool_size": int(metadata.get("account_pool_size", 0) or 0),
            "per_account_concurrency": int(metadata.get("account_pool_per_account_concurrency", 0) or 0),
            "products_found": products_found,
            "products_downloaded": products_downloaded,
            "files_known": total_known_files,
            "files_completed": total_completed_files,
            "bytes_downloaded": int(bytes_downloaded or 0),
            "bytes_total": total_bytes,
            "progress_pct": round(float(progress_pct or 0.0), 2),
            "speed_bps": float(speed_bps or 0.0),
            "eta_seconds": eta_seconds,
            "started_at": str(metadata.get("download_started_at") or "").strip() or None,
            "finished_at": str(metadata.get("download_finished_at") or "").strip() or None,
            "duration_seconds": download_window_seconds,
            "last_file": str(last_file or "").strip() or None,
            "retry_count_total": sum(int((entry or {}).get("retry_count", 0) or 0) for entry in retry_state.values()),
            "rate_limited_accounts": sum(1 for row in account_rows if bool(row.get("rate_limited"))),
            "accounts": account_rows,
        }

    @classmethod
    def _should_emit_download_progress(
        cls,
        *,
        delta: int,
        now_mono: float,
        last_emit: float,
        bytes_downloaded: int,
        last_bytes: int,
        progress_pct: float,
        last_progress: float,
        bytes_total: int,
    ) -> bool:
        if delta == 0 or last_emit <= 0:
            return True

        elapsed = max(0.0, now_mono - last_emit)
        if bytes_total > 0 and bytes_downloaded >= bytes_total:
            return True
        if elapsed < cls.DOWNLOAD_PROGRESS_MIN_INTERVAL_SECONDS:
            return False
        if elapsed >= cls.DOWNLOAD_PROGRESS_MAX_INTERVAL_SECONDS:
            return True
        if max(0, bytes_downloaded - last_bytes) >= cls.DOWNLOAD_PROGRESS_MIN_BYTES:
            return True
        if max(0.0, progress_pct - last_progress) >= cls.DOWNLOAD_PROGRESS_MIN_PERCENT:
            return True
        return False

    @classmethod
    def _should_emit_zarr_progress(
        cls,
        *,
        now_mono: float,
        last_emit: float,
        progress_pct: float,
        last_progress: float,
    ) -> bool:
        if last_emit <= 0 or last_progress < 0.0:
            return True
        if progress_pct >= 100.0:
            return True
        if max(0.0, progress_pct - last_progress) >= cls.ZARR_PROGRESS_MIN_PERCENT:
            return True
        return max(0.0, now_mono - last_emit) >= cls.ZARR_PROGRESS_MIN_INTERVAL_SECONDS

    def _build_zarr_progress_callback(
        self,
        *,
        job_id: str,
        raw_outputs: list[str],
        zarr_outputs: list[str],
        raw_uri: str,
        scene_id: str,
        output_uri: str,
        index: int,
        total: int,
        parallel_workers: int = 1,
    ) -> Callable[[dict[str, Any]], None]:
        last_emit = {"mono": 0.0, "progress": -1.0}
        base_progress = ((index - 1) / total) * 100.0
        progress_span = (100.0 / total) * 0.85

        def _callback(payload: dict[str, Any]) -> None:
            fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
            pipeline_progress = min(99.0, base_progress + progress_span * fraction)
            now_mono = time.monotonic()
            if not self._should_emit_zarr_progress(
                now_mono=now_mono,
                last_emit=float(last_emit["mono"]),
                progress_pct=pipeline_progress,
                last_progress=float(last_emit["progress"]),
            ):
                return
            last_emit["mono"] = now_mono
            last_emit["progress"] = pipeline_progress
            blocks_written = int(payload.get("blocks_written") or 0)
            total_blocks = int(payload.get("total_blocks") or 0)
            source_array_name = str(payload.get("source_array_name") or payload.get("array_name") or "").strip()
            band_name = str(payload.get("band_name") or "").strip()
            metadata = {
                "status": "running",
                "stage": "writing_chunks",
                "current_raw_uri": raw_uri,
                "current_scene_id": scene_id,
                "current_output_uri": output_uri,
                "current_index": index,
                "total": total,
                "parallel_workers": parallel_workers,
                "chunk_fraction": round(fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": max(0, index - 1),
                "items_active": 1,
            }
            if source_array_name:
                metadata["current_array"] = source_array_name
            if band_name:
                metadata["current_band"] = band_name
            event_payload = {
                "raw_uri": raw_uri,
                "scene_id": scene_id,
                "output_uri": output_uri,
                "index": index,
                "total": total,
                "stage": "writing_chunks",
                "parallel_workers": parallel_workers,
                "chunk_fraction": round(fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": max(0, index - 1),
                "items_active": 1,
            }
            if source_array_name:
                event_payload["array_name"] = source_array_name
            if band_name:
                event_payload["band_name"] = band_name
            self._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="writing_chunks",
                pipeline_progress=pipeline_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata=metadata,
                event_type="job.zarr_converting",
                event_payload=event_payload,
            )

        return _callback

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
        download_coordinator_report = self._local_download_coordinator_report()
        return self.store.upsert_worker_heartbeat(
            self._worker_id,
            {
                "runtime_role": self._runtime_role,
                "execution_enabled": self._execution_enabled,
                "max_concurrent_jobs": self.settings.nimbus_max_jobs,
                "queue_poll_seconds": self.settings.nimbus_queue_poll_seconds,
                "heartbeat_interval_seconds": self.settings.nimbus_worker_heartbeat_seconds,
                "provider_limits": self.settings.provider_job_limits_map,
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
                    "provider_job_limits": self.settings.provider_job_limits_map,
                    "provider_control_plane_limits": self.settings.provider_control_plane_limits_map,
                    "provider_data_plane_limits": self.settings.provider_data_plane_limits_map,
                    "download_guardrails": {
                        "global_active_limit": int(self.settings.nimbus_download_global_limit),
                        "min_free_bytes": int(self.settings.nimbus_download_min_free_bytes or 0),
                        "global_max_bps": (
                            int(self.settings.nimbus_download_global_max_bps)
                            if self.settings.nimbus_download_global_max_bps
                            else None
                        ),
                    },
                    "download_coordinator": download_coordinator_report,
                },
            },
        )
