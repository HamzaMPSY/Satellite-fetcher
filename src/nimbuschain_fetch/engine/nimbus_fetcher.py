from __future__ import annotations

import asyncio
import os
import socket
import threading
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from nimbuschain_fetch.application.artifact_registry import ArtifactRegistryService
from nimbuschain_fetch.application.conversion import ManualConversionService
from nimbuschain_fetch.application.job_execution import (
    CallbackJobExecutionHandler,
    JobExecutionRegistry,
)
from nimbuschain_fetch.application.pipeline_execution import ModularPipelineJobExecutionHandler
from nimbuschain_fetch.application.pipeline_state import PipelineStateService
from nimbuschain_fetch.application.workflows import FetchJobWorkflowService, MaskJobWorkflowService
from nimbuschain_fetch.domain.records import JobResultRecord, JobRowRecord
from nimbuschain_fetch.download.coordinator import DownloadCoordinator
from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.engine.conversion_support import FetcherConversionSupport
from nimbuschain_fetch.engine.conversion_policy_support import FetcherConversionPolicySupport
from nimbuschain_fetch.engine.cube_support import FetcherCubeSupport
from nimbuschain_fetch.engine.artifact_support import FetcherArtifactSupport
from nimbuschain_fetch.engine.download_coordinator_support import FetcherDownloadCoordinatorSupport
from nimbuschain_fetch.engine.job_support import FetcherJobSupport
from nimbuschain_fetch.engine.lifecycle_support import FetcherLifecycleSupport
from nimbuschain_fetch.engine.mask_api_support import FetcherMaskApiSupport
from nimbuschain_fetch.engine.mask_runtime import run_in_place_mask_pipeline
from nimbuschain_fetch.engine.mask_policy_support import FetcherMaskPolicySupport
from nimbuschain_fetch.engine.normalization_support import FetcherNormalizationSupport
from nimbuschain_fetch.engine.operations_support import FetcherOperationsSupport
from nimbuschain_fetch.engine.path_support import FetcherPathSupport
from nimbuschain_fetch.engine.progress_support import FetcherProgressSupport
from nimbuschain_fetch.engine.provider_support import FetcherProviderSupport
from nimbuschain_fetch.engine.status_timeline_support import FetcherStatusTimelineSupport
from nimbuschain_fetch.engine.store_record_support import FetcherStoreRecordSupport
from nimbuschain_fetch.engine.worker_runtime_support import FetcherWorkerRuntimeSupport
from nimbuschain_fetch.engine.zarr_context_support import FetcherZarrContextSupport
from nimbuschain_fetch.ports import (
    ConverterPort,
    MaskPort,
    ProviderRegistryMapping,
)
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import (
    JobCreateRequest,
    JobStatusResponse,
    PipelineState,
    ProviderName,
)
from nimbuschain_fetch.registries import ExecutorRegistry, ProviderRegistry, StoreRegistry
from nimbuschain_fetch.settings import Settings, get_settings
from nimbuschain_shared.clients.mask import MaskServiceClient
from nimbuschain_shared.clients.zarr import ZarrServiceClient
from nimbuschain_shared.runtime import normalize_device_name, resolve_inference_device


class JobNotFoundError(KeyError):
    pass


class JobCancelledError(RuntimeError):
    pass


def _delegate_method(helper_attr: str, target_name: str):
    def _method(self, *args: Any, **kwargs: Any):
        helper = getattr(self, helper_attr)
        return getattr(helper, target_name)(*args, **kwargs)

    return _method


def _delegate_async_method(helper_attr: str, target_name: str):
    async def _method(self, *args: Any, **kwargs: Any):
        helper = getattr(self, helper_attr)
        return await getattr(helper, target_name)(*args, **kwargs)

    return _method


def _delegate_support_method(target: Callable[..., Any]):
    def _method(self, *args: Any, **kwargs: Any):
        return target(self, *args, **kwargs)

    return _method


def _run_provider_job_delegate(self, *args: Any, **kwargs: Any):
    return self._provider_support.run_provider_job(
        *args,
        **kwargs,
        download_manager_cls=DownloadManager,
    )


def _download_coordinator_instance_delegate(self, *args: Any, **kwargs: Any):
    return self._download_coordinator_support.instance(
        *args,
        **kwargs,
        coordinator_cls=DownloadCoordinator,
    )


def _download_with_coordinator_delegate(self, *args: Any, **kwargs: Any):
    return self._download_coordinator_support.download_with_coordinator(
        *args,
        **kwargs,
        coordinator_cls=DownloadCoordinator,
    )


def _build_provider_download_manager_delegate(self, *args: Any, **kwargs: Any):
    kwargs.setdefault("download_manager_cls", DownloadManager)
    return self._provider_support.build_provider_download_manager(
        *args,
        **kwargs,
    )


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
        provider_registry: ProviderRegistry | ProviderRegistryMapping | None = None,
        store_registry: StoreRegistry | None = None,
        executor_registry: ExecutorRegistry | None = None,
        job_execution_registry: JobExecutionRegistry | None = None,
    ):
        self.settings = settings or get_settings()
        self._store_registry = store_registry or StoreRegistry()
        self.store = store or self._store_registry.create(self.settings)
        if isinstance(provider_registry, ProviderRegistry):
            self._provider_registry = provider_registry
        else:
            self._provider_registry = ProviderRegistry(provider_registry)
        self._executor_registry = executor_registry or ExecutorRegistry()
        self._runtime_role = self.settings.runtime_role
        self._execution_enabled = self._runtime_role in {"all", "worker"}
        self._request_adapter = TypeAdapter(JobCreateRequest)
        self._executor = (
            self._executor_registry.create(
                self.settings.nimbus_executor_backend,
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
        self._path_support = FetcherPathSupport(self.settings)
        self._record_support = FetcherStoreRecordSupport(
            store=self.store,
            normalize_historical_job_row=self._normalize_historical_job_row,
            normalize_job_row=self._path_support.normalize_backend_paths_in_job_row,
            normalize_result_payload=self._path_support.normalize_backend_paths_in_result_payload,
        )
        self.asyncio = asyncio
        self._status_timeline_support = FetcherStatusTimelineSupport(
            store=self.store,
            now_iso=self._now_iso,
        )
        self._normalization_support = FetcherNormalizationSupport(self)
        self._cube_support = FetcherCubeSupport(self)
        self._artifact_support = FetcherArtifactSupport(self)
        self._job_support = FetcherJobSupport(self)
        self._provider_support = FetcherProviderSupport(self)
        self._conversion_support = FetcherConversionSupport(self)
        self._conversion_policy_support = FetcherConversionPolicySupport(self)
        self._download_coordinator_support = FetcherDownloadCoordinatorSupport(self)
        self._lifecycle_support = FetcherLifecycleSupport(self)
        self._mask_api_support = FetcherMaskApiSupport(self)
        self._operations_support = FetcherOperationsSupport(self)
        self._progress_support = FetcherProgressSupport(self)
        self._zarr_context_support = FetcherZarrContextSupport(
            converter=self._converter,
            provider_name=self._provider_name,
            scene_id_from_raw_uri=self._scene_id_from_raw_uri,
        )
        self._zarr_converter: ConverterPort | None = None
        self._mask_service: MaskPort | None = None
        self._download_coordinator: DownloadCoordinator | None = None
        self.job_not_found_error_cls = JobNotFoundError
        self.job_cancelled_error_cls = JobCancelledError
        self._pipeline_state = PipelineStateService(
            store=self.store,
            lock=self._pipeline_update_lock,
            now_iso=self._now_iso,
            job_kind_for_type=self._job_kind_for_type,
            normalized_mask_types=self._normalized_mask_types,
            timeline_cube_mode_for_row=self._timeline_cube_mode_for_row,
        )
        self._artifact_registry = ArtifactRegistryService(
            store=self.store,
            normalize_backend_path=self._normalize_backend_path,
            normalize_artifact_row=self._normalize_backend_paths_in_artifact_row,
            path_size_bytes=self._path_size_bytes,
            water_mask_quality_fields=self._water_mask_quality_fields,
            cloud_mask_quality_fields=self._cloud_mask_quality_fields,
            mask_quality_fields=self._mask_quality_fields,
        )
        self._manual_conversion_service = ManualConversionService(self)
        self._fetch_job_workflow = FetchJobWorkflowService(self)
        self._mask_job_workflow = MaskJobWorkflowService(self)
        fetch_pipeline_handler = ModularPipelineJobExecutionHandler(
            runtime=self,
            workflow=self._fetch_job_workflow,
        )
        self._job_execution_registry = job_execution_registry or JobExecutionRegistry(
            {
                "search_download": fetch_pipeline_handler,
                "download_products": fetch_pipeline_handler,
                "mask_existing_zarr": CallbackJobExecutionHandler(self._mask_job_workflow.execute_from_context),
            }
        )
        self._started = False

    async def start(self) -> None:
        await self._lifecycle_support.start()

    async def stop(self) -> None:
        await self._lifecycle_support.stop()

    @staticmethod
    def _job_kind_for_type(job_type: str | None) -> str:
        normalized = str(job_type or "").strip().lower()
        if normalized == "mask_existing_zarr":
            return "mask"
        return "fetch"

    @classmethod
    def _service_name_for_type(cls, job_type: str | None) -> str:
        return "mask_service" if cls._job_kind_for_type(job_type) == "mask" else "fetch_service"

    @staticmethod
    def _normalized_mask_types(values: list[str] | tuple[str, ...] | None) -> list[str]:
        return FetcherMaskPolicySupport.normalized_mask_types(values)

    @staticmethod
    def _normalized_cube_mode(value: Any) -> str:
        return FetcherMaskPolicySupport.normalized_cube_mode(value)

    def _timeline_cube_mode_for_row(
        self,
        row: dict[str, Any],
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> str:
        return FetcherMaskPolicySupport.timeline_cube_mode_for_row(
            row=row,
            pipeline_metadata=pipeline_metadata,
        )

    @staticmethod
    def _normalize_mask_failure_step(value: Any) -> str | None:
        return FetcherMaskPolicySupport.normalize_mask_failure_step(value)

    @classmethod
    def _preferred_mask_failure_step(
        cls,
        values: Iterable[Any],
    ) -> str:
        return FetcherMaskPolicySupport.preferred_mask_failure_step(values)

    @classmethod
    def _mask_failure_step_from_payloads(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        water_mask: dict[str, Any] | None,
        cloud_mask: dict[str, Any] | None,
    ) -> str | None:
        return FetcherMaskPolicySupport.mask_failure_step_from_payloads(
            mask_types=mask_types,
            water_mask=water_mask,
            cloud_mask=cloud_mask,
        )

    @classmethod
    def _mask_failure_step_from_items(
        cls,
        *,
        mask_types: list[str] | tuple[str, ...] | None,
        items: list[dict[str, Any]] | None,
    ) -> str:
        return FetcherMaskPolicySupport.mask_failure_step_from_items(
            mask_types=mask_types,
            items=items,
        )

    @staticmethod
    def _build_mask_progress_plan(
        *,
        mask_types: list[str],
        stage_start_progress: float,
        stage_end_progress: float,
    ) -> dict[str, float]:
        return FetcherMaskPolicySupport.build_mask_progress_plan(
            mask_types=mask_types,
            stage_start_progress=stage_start_progress,
            stage_end_progress=stage_end_progress,
        )

    async def stream_events(self, *, job_id: str | None, since: int | None):
        async for item in self._operations_support.stream_events(job_id=job_id, since=since):
            yield item

    @staticmethod
    def _wrap_download_coordinator_reports(
        *,
        reports: list[dict[str, Any]],
        source: str,
        summary: dict[str, Any],
        timestamp: str,
    ) -> dict[str, Any]:
        return FetcherDownloadCoordinatorSupport.wrap_reports(
            reports=reports,
            source=source,
            summary=summary,
            timestamp=timestamp,
        )

    def _converter(self) -> ConverterPort:
        if self._zarr_converter is None:
            service_url = str(self.settings.nimbus_zarr_service_url or "").strip()
            if not service_url:
                raise RuntimeError("Zarr service URL is not configured.")
            self._zarr_converter = ZarrServiceClient(service_url=service_url)
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
        self._pipeline_state.update(
            job_id,
            pipeline_state=pipeline_state,
            pipeline_step=pipeline_step,
            pipeline_progress=pipeline_progress,
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            event_type=event_type,
            event_payload=event_payload,
        )

    def _merged_pipeline_metadata(
        self,
        job_id: str,
        pipeline_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._pipeline_state.merged_metadata(job_id, pipeline_metadata)

    @staticmethod
    def _filter_manifest_paths(paths: list[str]) -> list[str]:
        return FetcherPathSupport.filter_manifest_paths(paths)

    @staticmethod
    def _scene_id_from_raw_uri(raw_uri: str) -> str:
        return FetcherPathSupport.scene_id_from_raw_uri(raw_uri)

    def _default_zarr_output_uri(self, scene_id: str) -> str:
        return self._path_support.default_zarr_output_uri(scene_id)

    def _default_cube_output_dir(self, job_id: str) -> str:
        return self._path_support.default_cube_output_dir(job_id)

    @staticmethod
    def _path_size_bytes(target_path: str | Path | None) -> int | None:
        return FetcherPathSupport.path_size_bytes(target_path)

    @staticmethod
    def _cube_config_from_request(request: JobCreateRequest) -> dict[str, Any] | None:
        return FetcherMaskPolicySupport.cube_config_from_request(request)

    @staticmethod
    def _merge_paths(existing: list[str], additions: list[str]) -> list[str]:
        return FetcherPathSupport.merge_paths(existing, additions)

    def _normalize_backend_path(self, value: Any) -> Any:
        return self._path_support.normalize_backend_path(value)

    def _normalize_backend_paths_payload(self, value: Any) -> Any:
        return self._path_support.normalize_backend_paths_payload(value)

    def _normalize_backend_paths_in_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return self._path_support.normalize_backend_paths_in_job_row(row)

    def _normalize_backend_paths_in_result_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        return self._path_support.normalize_backend_paths_in_result_payload(result)

    def _get_job_row_record(self, job_id: str) -> JobRowRecord | None:
        return self._record_support.get_job_row_record(job_id)

    def _get_job_row_payload(self, job_id: str) -> dict[str, Any]:
        return self._record_support.get_job_row_payload(job_id)

    def _get_result_record(self, job_id: str) -> JobResultRecord | None:
        return self._record_support.get_result_record(job_id)

    def _get_result_payload(self, job_id: str) -> dict[str, Any]:
        return self._record_support.get_result_payload(job_id)

    def _set_result_record(self, result: JobResultRecord) -> None:
        self._record_support.set_result_record(result)

    def _normalize_backend_paths_in_artifact_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return self._path_support.normalize_backend_paths_in_artifact_row(row)

    @staticmethod
    def _normalize_collection_for_zarr(provider_name: str, collection: str) -> str:
        return FetcherPathSupport.normalize_collection_for_zarr(provider_name, collection)

    @staticmethod
    def _normalize_product_type_for_zarr(product_type: str | None) -> str | None:
        return FetcherPathSupport.normalize_product_type_for_zarr(product_type)

    @staticmethod
    def _water_mask_quality_fields(water_mask: dict[str, Any]) -> dict[str, Any]:
        return FetcherArtifactSupport.water_mask_quality_fields(water_mask)

    @staticmethod
    def _cloud_mask_quality_fields(cloud_mask: dict[str, Any]) -> dict[str, Any]:
        return FetcherArtifactSupport.cloud_mask_quality_fields(cloud_mask)

    def _mask_quality_fields(self, *, water_mask: dict[str, Any], cloud_mask: dict[str, Any]) -> dict[str, Any]:
        return FetcherArtifactSupport.mask_quality_fields(
            water_mask=water_mask,
            cloud_mask=cloud_mask,
        )

    def _masker(self) -> MaskPort:
        if self._mask_service is None:
            service_url = str(self.settings.nimbus_mask_service_url or "").strip()
            if not service_url:
                raise RuntimeError("Mask service URL is not configured.")
            self._mask_service = MaskServiceClient(service_url=service_url)
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

    @classmethod
    def _cube_config_from_request_payload(cls, request_payload: dict[str, Any]) -> dict[str, Any] | None:
        return FetcherMaskPolicySupport.cube_config_from_request_payload(request_payload)

    @staticmethod
    def _derive_transform_from_xy(*, x_values: list[Any], y_values: list[Any]) -> list[float]:
        return FetcherZarrContextSupport.derive_transform_from_xy(
            x_values=x_values,
            y_values=y_values,
        )

    @staticmethod
    def _infer_crs_from_scene_metadata(*, scene_id: str, source_uri: str) -> str | None:
        return FetcherZarrContextSupport.infer_crs_from_scene_metadata(
            scene_id=scene_id,
            source_uri=source_uri,
        )

    @staticmethod
    def _collect_watermask_outputs(
        *,
        result: dict[str, Any],
        water_mask: dict[str, Any],
    ) -> list[str]:
        return FetcherArtifactSupport.collect_watermask_outputs(
            result=result,
            water_mask=water_mask,
        )

    def _scene_parallelism_target_from_download(
        self,
        *,
        pipeline_metadata: dict[str, Any] | None,
        total: int,
    ) -> int:
        return self._conversion_policy_support.scene_parallelism_target_from_download(
            pipeline_metadata=pipeline_metadata,
            total=total,
        )

    @staticmethod
    def _zarr_convert_max_workers(
        *,
        total: int,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
    ) -> int:
        return FetcherConversionPolicySupport.zarr_convert_max_workers(
            total=total,
            preferred_parallelism=preferred_parallelism,
            max_limit=max_limit,
            os_module=os,
        )

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
        return FetcherConversionPolicySupport.integrated_mask_max_workers(
            total=total,
            inference_device=inference_device,
            water_inference_device=water_inference_device,
            remote_runtime=remote_runtime,
            preferred_parallelism=preferred_parallelism,
            max_limit=max_limit,
            os_module=os,
            resolve_inference_device_fn=resolve_inference_device,
            normalize_device_name_fn=normalize_device_name,
        )

    @staticmethod
    def _provider_name(value: ProviderName | str) -> str:
        return FetcherProviderSupport.provider_name(value)

    @staticmethod
    def _supports_download_coordinator(
        provider: Any,
    ) -> bool:
        return FetcherDownloadCoordinatorSupport.supports(provider)

    def _normalize_historical_job_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return self._normalization_support.normalize_historical_job_row(row)

    def _to_status_response(self, row: dict[str, Any]) -> JobStatusResponse:
        return self._normalization_support.to_status_response(row)

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
        return self._normalization_support.rebuild_pipeline_timeline_from_events(
            row=row,
            job_kind=job_kind,
            mask_types=mask_types,
            cube_mode=cube_mode,
            pipeline_progress=pipeline_progress,
            timeline_timestamp=timeline_timestamp,
        )

    @staticmethod
    def _events_for_current_timeline_attempt(
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return FetcherNormalizationSupport.events_for_current_timeline_attempt(events)

    @staticmethod
    def _pipeline_timeline_needs_rebuild(
        *,
        row: dict[str, Any],
        pipeline_timeline: dict[str, Any],
        mask_types: list[str],
        cube_mode: str,
    ) -> bool:
        return FetcherNormalizationSupport.pipeline_timeline_needs_rebuild(
            row=row,
            pipeline_timeline=pipeline_timeline,
            mask_types=mask_types,
            cube_mode=cube_mode,
            normalized_cube_mode=NimbusFetcher._normalized_cube_mode,
        )

    @staticmethod
    def _duration_seconds_for_row(
        *,
        state: str,
        started_at: datetime | None,
        finished_at: datetime | None,
        updated_at: datetime | None,
    ) -> float | None:
        return FetcherNormalizationSupport.duration_seconds_for_row(
            state=state,
            started_at=started_at,
            finished_at=finished_at,
            updated_at=updated_at,
        )

    def _effective_started_at_for_row(self, row: dict[str, Any]) -> datetime | None:
        return self._normalization_support.effective_started_at_for_row(row)

    @staticmethod
    def _parse_iso(value: str | datetime | None) -> datetime | None:
        return FetcherNormalizationSupport.parse_iso(value)

    @staticmethod
    def _now_iso() -> str:
        return FetcherNormalizationSupport.now_iso()

    @staticmethod
    def _download_account_label(context: dict[str, Any] | None) -> str:
        return FetcherProgressSupport.download_account_label(context)

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
        return FetcherProgressSupport(cls).build_download_telemetry(
            pipeline_metadata=pipeline_metadata,
            file_progress=file_progress,
            bytes_downloaded=bytes_downloaded,
            bytes_total=bytes_total,
            progress_pct=progress_pct,
            speed_bps=speed_bps,
            retry_state=retry_state,
            phase=phase,
            last_file=last_file,
        )

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
        return FetcherProgressSupport(cls).should_emit_download_progress(
            delta=delta,
            now_mono=now_mono,
            last_emit=last_emit,
            bytes_downloaded=bytes_downloaded,
            last_bytes=last_bytes,
            progress_pct=progress_pct,
            last_progress=last_progress,
            bytes_total=bytes_total,
        )

    @classmethod
    def _should_emit_zarr_progress(
        cls,
        *,
        now_mono: float,
        last_emit: float,
        progress_pct: float,
        last_progress: float,
    ) -> bool:
        return FetcherProgressSupport(cls).should_emit_zarr_progress(
            now_mono=now_mono,
            last_emit=last_emit,
            progress_pct=progress_pct,
            last_progress=last_progress,
        )

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
        return self._progress_support.build_zarr_progress_callback(
            job_id=job_id,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            raw_uri=raw_uri,
            scene_id=scene_id,
            output_uri=output_uri,
            index=index,
            total=total,
            parallel_workers=parallel_workers,
        )

    def _publish_worker_heartbeat(self) -> dict[str, Any] | None:
        return FetcherWorkerRuntimeSupport.publish_worker_heartbeat(self)


NimbusFetcher._retire_legacy_mask_jobs = _delegate_method("_job_support", "retire_legacy_mask_jobs")
NimbusFetcher._cleanup_mask_job_outputs = _delegate_method("_job_support", "cleanup_mask_job_outputs")
NimbusFetcher._remove_path_if_exists = staticmethod(FetcherJobSupport.remove_path_if_exists)
NimbusFetcher._mark_mask_status_failed = _delegate_method("_job_support", "mark_mask_status_failed")
NimbusFetcher._fail_interrupted_mask_jobs = _delegate_method("_job_support", "fail_interrupted_mask_jobs")
NimbusFetcher.submit_job = _delegate_async_method("_job_support", "submit_job")
NimbusFetcher.submit_batch = _delegate_async_method("_job_support", "submit_batch")
NimbusFetcher._create_mask_job = _delegate_method("_job_support", "create_mask_job")
NimbusFetcher.get_job = _delegate_method("_job_support", "get_job")
NimbusFetcher._resume_metadata_for_row = _delegate_method("_job_support", "resume_metadata_for_row")
NimbusFetcher._resume_pipeline_from_mask_failure = _delegate_method("_job_support", "resume_pipeline_from_mask_failure")
NimbusFetcher._resume_pipeline_from_cube_failure = _delegate_method("_job_support", "resume_pipeline_from_cube_failure")
NimbusFetcher.resume_job = _delegate_method("_job_support", "resume_job")
NimbusFetcher.get_result = _delegate_method("_job_support", "get_result")
NimbusFetcher.apply_mask_existing_job = _delegate_method("_job_support", "apply_mask_existing_job")
NimbusFetcher._run_in_place_mask_pipeline = _delegate_support_method(run_in_place_mask_pipeline)
NimbusFetcher._execute_mask_existing_zarr_job = _delegate_method("_job_support", "execute_mask_existing_zarr_job")
NimbusFetcher.apply_watermask_existing_job = _delegate_method("_mask_api_support", "apply_watermask_existing_job")
NimbusFetcher.apply_cloud_mask_existing_job = _delegate_method("_mask_api_support", "apply_cloud_mask_existing_job")
NimbusFetcher.cancel_job = _delegate_async_method("_operations_support", "cancel_job")
NimbusFetcher._list_jobs_by_states = _delegate_method("_operations_support", "list_jobs_by_states")
NimbusFetcher.reset_runtime_state = _delegate_async_method("_operations_support", "reset_runtime_state")
NimbusFetcher.list_jobs = _delegate_method("_operations_support", "list_jobs")
NimbusFetcher.upsert_artifact = _delegate_method("_artifact_registry", "upsert")
NimbusFetcher.list_artifacts = _delegate_method("_operations_support", "list_artifacts")
NimbusFetcher._monitor_queued_jobs_loop = _delegate_async_method("_operations_support", "monitor_queued_jobs_loop")
NimbusFetcher._start_worker_heartbeat_thread = _delegate_support_method(FetcherWorkerRuntimeSupport.start_worker_heartbeat_thread)
NimbusFetcher._stop_worker_heartbeat_thread = _delegate_support_method(FetcherWorkerRuntimeSupport.stop_worker_heartbeat_thread)
NimbusFetcher._worker_heartbeat_loop = _delegate_support_method(FetcherWorkerRuntimeSupport.worker_heartbeat_loop)
NimbusFetcher._enqueue_queued_jobs = _delegate_async_method("_operations_support", "enqueue_queued_jobs")
NimbusFetcher.get_worker_status = _delegate_support_method(FetcherWorkerRuntimeSupport.get_worker_status)
NimbusFetcher._download_coordinator_placeholder_status = _delegate_method("_download_coordinator_support", "placeholder_status")
NimbusFetcher._local_download_coordinator_report = _delegate_method("_download_coordinator_support", "local_report")
NimbusFetcher.get_download_coordinator_status = _delegate_method("_download_coordinator_support", "get_status")
NimbusFetcher._is_job_cancel_requested = _delegate_method("_lifecycle_support", "is_job_cancel_requested")
NimbusFetcher._register_zarr_artifact = _delegate_method("_artifact_support", "register_zarr_artifact")
NimbusFetcher._register_cube_artifact = _delegate_method("_artifact_support", "register_cube_artifact")
NimbusFetcher._register_watermask_artifact = _delegate_method("_artifact_support", "register_watermask_artifact")
NimbusFetcher._register_masked_zarr_artifact = _delegate_method("_artifact_support", "register_masked_zarr_artifact")
NimbusFetcher._register_cloudmask_artifact = _delegate_method("_artifact_support", "register_cloudmask_artifact")
NimbusFetcher._job_related_zarr_uris = _delegate_method("_artifact_support", "job_related_zarr_uris")
NimbusFetcher._masked_zarr_outputs_for_job = _delegate_method("_artifact_support", "masked_zarr_outputs_for_job")
NimbusFetcher._cube_outputs_for_job = _delegate_method("_cube_support", "cube_outputs_for_job")
NimbusFetcher._build_cube_outputs = _delegate_method("_cube_support", "build_cube_outputs")
NimbusFetcher._resume_base_result_paths = _delegate_method("_cube_support", "resume_base_result_paths")
NimbusFetcher._continue_remaining_pipeline_after_zarr = _delegate_method("_fetch_job_workflow", "continue_remaining_pipeline_after_zarr")
NimbusFetcher._resolve_zarr_context = _delegate_method("_zarr_context_support", "resolve_zarr_context")
NimbusFetcher._inspect_zarr_dataset = _delegate_method("_zarr_context_support", "inspect_zarr_dataset")
NimbusFetcher._convert_single_raw_output = _delegate_method("_conversion_support", "convert_single_raw_output")
NimbusFetcher._convert_raw_outputs = _delegate_method("_conversion_support", "convert_raw_outputs")
NimbusFetcher.convert_existing_job = _delegate_method("_conversion_support", "convert_existing_job")
NimbusFetcher._execute_job = _delegate_async_method("_lifecycle_support", "execute_job")
NimbusFetcher._run_provider_job = _run_provider_job_delegate
NimbusFetcher._build_provider = _delegate_method("_provider_support", "build_provider")
NimbusFetcher._build_provider_download_manager = _build_provider_download_manager_delegate
NimbusFetcher._download_coordinator_instance = _download_coordinator_instance_delegate
NimbusFetcher._download_with_coordinator = _download_with_coordinator_delegate
NimbusFetcher._mark_cancelled = _delegate_method("_provider_support", "mark_cancelled")
