from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import anyio

from nimbuschain_fetch.application.job_execution import JobExecutionContext
from nimbuschain_fetch.application.sen2like_normalization import Sen2LikeNormalizationRouter
from nimbuschain_fetch.domain.metadata import (
    ConversionMetadataRecord,
    MaskStateRecord,
    PayloadRecord,
    PipelineMetadataRecord,
)
from nimbuschain_fetch.domain.records import JobResultRecord
from nimbuschain_fetch.domain.workflow_models import MaskWorkflowItem, MaskWorkflowSummary
from nimbuschain_fetch.download.download_manager import DownloadCancelled
from nimbuschain_fetch.manifest import build_manifest_entry, checksums_for_paths, write_manifest
from nimbuschain_fetch.models import (
    JobCreateRequest,
    JobState,
    JobStatusResponse,
    PipelineState,
    SearchDownloadRequest,
)
from nimbuschain_fetch.security.paths import sanitize_output_dir


class FetchJobWorkflowService:
    def __init__(self, runtime: Any):
        self._rt = runtime
        self._sen2like_router = Sen2LikeNormalizationRouter(runtime)

    def _store_result_record(self, result: JobResultRecord) -> None:
        store = self._rt.store
        if hasattr(store, "set_result_record"):
            store.set_result_record(result)
            return
        store.set_result(result.job_id, result.to_row())

    def _current_job_pipeline_metadata(
        self,
        job_id: str,
        *,
        base: dict[str, Any] | None = None,
    ) -> PipelineMetadataRecord:
        row_record = self._rt._get_job_row_record(job_id)
        current = PipelineMetadataRecord.from_mapping(
            row_record.pipeline_metadata if row_record is not None else {}
        )
        return current.merged_with(base)

    def _merged_pipeline_metadata_record(
        self,
        job_id: str,
        metadata: PipelineMetadataRecord | dict[str, Any],
    ) -> PipelineMetadataRecord:
        payload = metadata.to_dict() if isinstance(metadata, PipelineMetadataRecord) else dict(metadata)
        return PipelineMetadataRecord.from_mapping(
            self._rt._merged_pipeline_metadata(job_id, payload)
        )

    @staticmethod
    def _conversion_metadata_record(
        value: dict[str, Any] | ConversionMetadataRecord | None,
    ) -> ConversionMetadataRecord:
        if isinstance(value, ConversionMetadataRecord):
            return value
        return ConversionMetadataRecord.from_mapping(value)

    async def execute_from_context(self, context: JobExecutionContext) -> None:
        await self.execute(
            job_id=context.job_id,
            row=context.row,
            is_cancelled_now=context.is_cancelled_now,
        )

    async def execute(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        is_cancelled_now,
    ) -> None:
        rt = self._rt
        request = rt._request_adapter.validate_python(row["request"])
        output_dir = sanitize_output_dir(
            rt.settings.nimbus_data_dir,
            getattr(request, "output_dir", None),
            fallback_name=job_id,
        )
        provider_name = rt._provider_name(request.provider)
        base_pipeline_metadata = PipelineMetadataRecord.from_mapping({
            "provider": provider_name,
            "collection": request.collection,
            "product_type": getattr(request, "product_type", None),
            "output_dir": str(output_dir),
            "job_type": request.job_type,
            "download_strategy": str(getattr(request, "download_strategy", "default") or "default"),
            "download_only": bool(getattr(request, "download_only", False)),
        })
        if isinstance(request, SearchDownloadRequest):
            base_pipeline_metadata.payload["tile_id"] = request.tile_id
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.searching,
                pipeline_step="searching",
                pipeline_progress=5.0,
                pipeline_metadata=base_pipeline_metadata.merged_with({"products_found": 0}).to_dict(),
                event_type="job.searching",
                event_payload={"provider": provider_name, "collection": request.collection},
            )
        else:
            requested_ids = list(getattr(request, "product_ids", []) or [])
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.downloading,
                pipeline_step="downloading",
                pipeline_progress=5.0,
                pipeline_metadata=base_pipeline_metadata.merged_with(
                    {
                        "products_requested": len(requested_ids),
                        "products_found": len(requested_ids),
                    }
                ).to_dict(),
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

        def _current_download_pipeline_metadata() -> PipelineMetadataRecord:
            return self._current_job_pipeline_metadata(job_id, base=base_pipeline_metadata.to_dict())

        def emit_progress(
            file_name: str,
            delta: int,
            downloaded: int,
            total: int | None,
            context: dict[str, Any] | None = None,
        ) -> None:
            if is_cancelled_now():
                raise rt.job_cancelled_error_cls("Job cancellation requested.")
            with progress_lock:
                context_payload = dict(context or {})
                account_label = rt._download_account_label(context_payload)
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

                if rt._should_emit_download_progress(
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
                    download_telemetry = rt._build_download_telemetry(
                        pipeline_metadata=current_pipeline_metadata.to_dict(),
                        file_progress=file_progress,
                        bytes_downloaded=int(aggregate["bytes_downloaded"]),
                        bytes_total=int(aggregate["bytes_total"]),
                        progress_pct=float(progress_pct),
                        speed_bps=float(speed),
                        retry_state=account_retry_state,
                        phase="running",
                        last_file=str(aggregate["last_file"] or file_name or "").strip() or None,
                    )
                    rt.store.update_job(
                        job_id,
                        progress=progress_pct,
                        bytes_downloaded=int(aggregate["bytes_downloaded"]),
                        bytes_total=int(aggregate["bytes_total"]),
                        pipeline_state=PipelineState.downloading.value,
                        pipeline_step="downloading",
                        pipeline_progress=pipeline_progress,
                        pipeline_metadata=current_pipeline_metadata.merged_with(
                            {"download_telemetry": download_telemetry}
                        ).to_dict(),
                    )
                    rt.store.append_event(
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
                account_label = rt._download_account_label(context_payload)
                row_now = rt._get_job_row_record(job_id)
                retry_count = int(row_now.retry_count if row_now is not None else 0) + 1
                last_retry_at = rt._now_iso()
                retry_entry = account_retry_state.setdefault(account_label, {})
                retry_entry["retry_count"] = int(retry_entry.get("retry_count", 0) or 0) + 1
                retry_entry["last_retry_at"] = last_retry_at
                retry_entry["last_reason"] = str(reason or "").strip()
                retry_entry["status"] = (
                    "rate_limited"
                    if str(reason or "").strip().lower() == "http_429"
                    else "retrying"
                )
                current_pipeline_metadata = self._current_job_pipeline_metadata(
                    job_id,
                    base=base_pipeline_metadata.to_dict(),
                )
                progress_pct = 0.0
                if int(aggregate["bytes_total"]) > 0:
                    progress_pct = min(
                        99.0,
                        100.0
                        * int(aggregate["bytes_downloaded"])
                        / max(1, int(aggregate["bytes_total"])),
                    )
                download_telemetry = rt._build_download_telemetry(
                    pipeline_metadata=current_pipeline_metadata.to_dict(),
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
                rt.store.update_job(
                    job_id,
                    retry_count=retry_count,
                    last_retry_at=last_retry_at,
                    pipeline_metadata=current_pipeline_metadata.merged_with(
                        {"download_telemetry": download_telemetry}
                    ).to_dict(),
                )
                rt.store.append_event(
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
                rt._run_provider_job,
                job_id,
                request,
                output_dir,
                emit_progress,
                emit_retry,
                is_cancelled_now,
            )

            if is_cancelled_now():
                rt._mark_cancelled(job_id, "cancelled_after_download")
                return

            raw_outputs = rt._filter_manifest_paths(list(result["paths"]))
            metadata = dict(result["metadata"])
            current_pipeline_metadata = self._current_job_pipeline_metadata(
                job_id,
                base=base_pipeline_metadata.to_dict(),
            )
            final_download_progress_pct = 0.0
            if max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])) > 0:
                final_download_progress_pct = min(
                    100.0,
                    100.0
                    * int(aggregate["bytes_downloaded"])
                    / max(1, max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"]))),
                )
            final_download_telemetry = rt._build_download_telemetry(
                pipeline_metadata=current_pipeline_metadata.merged_with(metadata).to_dict(),
                file_progress=file_progress,
                bytes_downloaded=int(aggregate["bytes_downloaded"]),
                bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                progress_pct=float(final_download_progress_pct),
                speed_bps=float(aggregate["last_speed_bps"]),
                retry_state=account_retry_state,
                phase="completed",
                last_file=str(aggregate["last_file"] or "").strip() or None,
            )
            pipeline_metadata = current_pipeline_metadata.merged_with(
                {
                    **base_pipeline_metadata.to_dict(),
                    **metadata,
                    "products_found": int(metadata.get("products_found", len(raw_outputs)) or len(raw_outputs)),
                    "products_downloaded": int(metadata.get("products_downloaded", len(raw_outputs)) or len(raw_outputs)),
                    "raw_output_count": len(raw_outputs),
                    "download_telemetry": final_download_telemetry,
                }
            )
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.downloaded,
                pipeline_step="downloaded",
                pipeline_progress=70.0,
                pipeline_metadata=pipeline_metadata.to_dict(),
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
            base_result_record = JobResultRecord(
                job_id=job_id,
                paths=raw_result_paths,
                raw_outputs=raw_outputs,
                zarr_outputs=[],
                cube_outputs=[],
                checksums=checksums,
                metadata=metadata,
                manifest_entry=manifest_entry,
                pipeline_metadata=self._merged_pipeline_metadata_record(job_id, pipeline_metadata).to_dict(),
                conversion_metadata={},
            )
            self._store_result_record(base_result_record)

            if bool(getattr(request, "download_only", False)):
                final_pipeline_metadata = pipeline_metadata.merged_with(
                    {
                        "download_only": True,
                        "zarr_output_count": 0,
                        "manual_conversion": False,
                    }
                )
                self._store_result_record(
                    JobResultRecord(
                        job_id=base_result_record.job_id,
                        paths=list(base_result_record.paths),
                        raw_outputs=list(base_result_record.raw_outputs),
                        zarr_outputs=list(base_result_record.zarr_outputs),
                        cube_outputs=list(base_result_record.cube_outputs),
                        checksums=dict(base_result_record.checksums),
                        metadata=dict(base_result_record.metadata),
                        manifest_entry=dict(base_result_record.manifest_entry),
                        pipeline_metadata=final_pipeline_metadata.to_dict(),
                        conversion_metadata=dict(base_result_record.conversion_metadata),
                    )
                )
                rt.store.update_job(
                    job_id,
                    state=JobState.succeeded.value,
                    progress=100.0,
                    finished_at=rt._now_iso(),
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                    pipeline_state=PipelineState.downloaded.value,
                    pipeline_step="downloaded",
                    pipeline_progress=100.0,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata={},
                    raw_outputs=raw_outputs,
                    zarr_outputs=[],
                )
                rt.store.append_event(
                    job_id,
                    "job.succeeded",
                    {
                        "status": JobState.succeeded.value,
                        "paths": raw_result_paths,
                        "pipeline_state": PipelineState.downloaded.value,
                    },
                )
                return

            sen2like_routing = self._sen2like_router.normalize_if_required(
                job_id=job_id,
                provider=provider_name,
                collection=request.collection,
                product_type=getattr(request, "product_type", None),
                raw_outputs=raw_outputs,
                pipeline_metadata=pipeline_metadata,
                is_cancelled_now=is_cancelled_now,
            )
            zarr_input_outputs = list(sen2like_routing.conversion_inputs)
            pipeline_metadata = sen2like_routing.pipeline_metadata
            zarr_conversion_provider = "copernicus" if sen2like_routing.routed else None
            zarr_conversion_collection = "SENTINEL-2" if sen2like_routing.routed else None
            zarr_conversion_product_type = "S2MSI2A" if sen2like_routing.routed else None
            pre_zarr_result_paths = [
                *raw_result_paths,
                *(zarr_input_outputs if sen2like_routing.routed else []),
            ]

            zarr_outputs, conversion_metadata = rt._convert_raw_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=request.collection,
                product_type=getattr(request, "product_type", None),
                raw_outputs=zarr_input_outputs,
                is_cancelled=is_cancelled_now,
                pipeline_metadata=pipeline_metadata.to_dict(),
                conversion_provider_name=zarr_conversion_provider,
                conversion_collection=zarr_conversion_collection,
                conversion_product_type=zarr_conversion_product_type,
            )
            final_paths = [*pre_zarr_result_paths, *zarr_outputs]
            conversion_metadata_record = self._conversion_metadata_record(conversion_metadata)
            conversion_status = str(conversion_metadata_record.payload.get("status") or "")
            final_pipeline_state = (
                PipelineState.zarr_written
                if zarr_outputs or conversion_status == "written"
                else PipelineState.downloaded
            )
            final_pipeline_step = (
                "zarr_written" if final_pipeline_state == PipelineState.zarr_written else "downloaded"
            )
            requested_mask_types = rt._normalized_mask_types(getattr(request, "mask_types", []))
            cube_config = rt._cube_config_from_request(request)
            cube_outputs: list[str] = []
            final_pipeline_metadata = self._merged_pipeline_metadata_record(
                job_id,
                pipeline_metadata.merged_with(
                    {
                        "zarr_output_count": len(zarr_outputs),
                        "manual_conversion": False,
                        "zarr_parallel_workers": int(conversion_metadata_record.payload.get("parallel_workers", 1) or 1),
                    }
                ),
            )
            if requested_mask_types:
                final_pipeline_metadata.payload["mask_types"] = requested_mask_types
                final_pipeline_metadata.payload["mask_mode"] = "integrated"
            if cube_config is not None:
                final_pipeline_metadata.payload["cube_mode"] = cube_config["mode"]
                final_pipeline_metadata.payload["cube_layout"] = cube_config["layout"]
                final_pipeline_metadata.payload["cube_target_crs"] = cube_config["target_crs"]
                final_pipeline_metadata.payload["cube_target_resolution_m"] = cube_config["target_resolution_m"]
                final_pipeline_metadata.payload["cube_overlap_policy"] = cube_config["overlap_policy"]
                final_pipeline_metadata.payload["cube_date_range"] = {
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
                rt._update_pipeline(
                    job_id,
                    pipeline_state=final_pipeline_state,
                    pipeline_step=final_pipeline_step,
                    pipeline_progress=72.0 if requested_mask_types else 100.0,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata_record.to_dict(),
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    event_type="job.zarr_written",
                    event_payload={
                        "zarr_outputs": zarr_outputs,
                        "pipeline_progress": 72.0 if requested_mask_types else 100.0,
                    },
                )
            final_pipeline_metadata = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata)
            self._store_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=final_paths,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    cube_outputs=cube_outputs,
                    checksums=checksums,
                    metadata=metadata,
                    manifest_entry=manifest_entry,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata_record.to_dict(),
                )
            )
            if final_pipeline_state == PipelineState.zarr_written:
                if cube_config is not None and cube_config["mode"] == "before_mask":
                    cube_execution = rt._build_cube_outputs(
                        job_id=job_id,
                        provider_name=provider_name,
                        collection=request.collection,
                        source_zarr_outputs=zarr_outputs,
                        cube_mode=str(cube_config["mode"]),
                        cube_start_date=cube_config["start_date"],
                        cube_end_date=cube_config["end_date"],
                        pipeline_metadata=final_pipeline_metadata.to_dict(),
                        stage_start_progress=73.0,
                        stage_end_progress=75.0 if requested_mask_types else 100.0,
                    )
                    cube_outputs = list(cube_execution.get("cube_outputs") or [])
                    final_pipeline_metadata = PipelineMetadataRecord.from_mapping(
                        cube_execution.get("pipeline_metadata") or final_pipeline_metadata.to_dict()
                    )
                    final_paths = [*pre_zarr_result_paths, *zarr_outputs, *cube_outputs]
                    self._store_result_record(
                        JobResultRecord(
                            job_id=job_id,
                            paths=final_paths,
                            raw_outputs=raw_outputs,
                            zarr_outputs=zarr_outputs,
                            cube_outputs=cube_outputs,
                            checksums=checksums,
                            metadata=metadata,
                            manifest_entry=manifest_entry,
                            pipeline_metadata=final_pipeline_metadata.to_dict(),
                            conversion_metadata=conversion_metadata_record.to_dict(),
                        )
                    )
                    if cube_outputs:
                        final_pipeline_state = PipelineState.cube_written if not requested_mask_types else final_pipeline_state
                        final_pipeline_step = "cube_written" if not requested_mask_types else final_pipeline_step

                started_row = rt._get_job_row_record(job_id)
                rt.store.update_job(
                    job_id,
                    state=JobState.running.value if requested_mask_types else JobState.succeeded.value,
                    started_at=(
                        str((started_row.started_at if started_row is not None else "") or "").strip()
                        or rt._now_iso()
                    ),
                    progress=100.0,
                    finished_at=None if requested_mask_types else rt._now_iso(),
                    bytes_downloaded=int(aggregate["bytes_downloaded"]),
                    bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                    pipeline_state=final_pipeline_state.value,
                    pipeline_step=final_pipeline_step,
                    pipeline_progress=76.0 if requested_mask_types else 100.0,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata_record.to_dict(),
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                )
                rt.store.append_event(
                    job_id,
                    "job.cube_written" if final_pipeline_state == PipelineState.cube_written else "job.zarr_written",
                    {
                        "pipeline_state": final_pipeline_state.value,
                        "zarr_outputs": zarr_outputs,
                        "cube_outputs": cube_outputs,
                    },
                )
                if requested_mask_types:
                    self.execute_integrated_mask_stage(
                        job_id=job_id,
                        row=row,
                        request=request,
                        raw_result_paths=pre_zarr_result_paths,
                        raw_outputs=raw_outputs,
                        zarr_outputs=zarr_outputs,
                        cube_outputs=cube_outputs,
                        checksums=checksums,
                        metadata=metadata,
                        manifest_entry=manifest_entry,
                        provider_name=provider_name,
                        final_pipeline_metadata=final_pipeline_metadata.to_dict(),
                        conversion_metadata=conversion_metadata_record.to_dict(),
                        aggregate=aggregate,
                        is_cancelled_now=is_cancelled_now,
                        cube_config=cube_config,
                    )
                    return
            final_pipeline_metadata = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata)
            rt.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                progress=100.0,
                finished_at=rt._now_iso(),
                bytes_downloaded=int(aggregate["bytes_downloaded"]),
                bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
                pipeline_state=final_pipeline_state.value,
                pipeline_step=final_pipeline_step,
                pipeline_progress=100.0,
                pipeline_metadata=final_pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata_record.to_dict(),
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
            )
            rt.store.append_event(
                job_id,
                "job.succeeded",
                {
                    "status": JobState.succeeded.value,
                    "paths": final_paths,
                    "pipeline_state": final_pipeline_state.value,
                },
            )
        except (DownloadCancelled, rt.job_cancelled_error_cls):
            rt._mark_cancelled(job_id, "cancelled_during_download")
        except Exception as exc:
            current_row_record = rt._get_job_row_record(job_id)
            current_row = current_row_record.to_row() if current_row_record is not None else row
            existing_result = rt._get_result_payload(job_id)
            raw_outputs = list(existing_result.get("raw_outputs") or current_row.get("raw_outputs") or [])
            current_pipeline_state = str(current_row.get("pipeline_state") or "")
            is_sen2like_failure = current_pipeline_state in {
                PipelineState.sen2like_queued.value,
                PipelineState.sen2like_running.value,
                PipelineState.sen2like_failed.value,
            }
            is_zarr_failure = current_pipeline_state in {
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.downloaded.value,
                PipelineState.sen2like_written.value,
            }
            is_cube_failure = current_pipeline_state in {
                PipelineState.cube_queued.value,
                PipelineState.cube_building.value,
                PipelineState.cube_failed.value,
            }
            pipeline_state = (
                PipelineState.sen2like_failed
                if is_sen2like_failure
                else PipelineState.zarr_failed
                if is_zarr_failure and raw_outputs
                else PipelineState.cube_failed
                if is_cube_failure
                else PipelineState.failed
            )
            pipeline_step = (
                "sen2like_failed"
                if pipeline_state == PipelineState.sen2like_failed
                else "zarr_failed"
                if pipeline_state == PipelineState.zarr_failed
                else "cube_failed"
                if pipeline_state == PipelineState.cube_failed
                else "failed"
            )
            failure_pipeline_metadata = PipelineMetadataRecord.from_mapping(
                current_row.get("pipeline_metadata") or existing_result.get("pipeline_metadata")
            )
            conversion_metadata = self._conversion_metadata_record(existing_result.get("conversion_metadata"))
            if pipeline_state in {PipelineState.sen2like_failed, PipelineState.zarr_failed, PipelineState.cube_failed}:
                conversion_metadata = conversion_metadata.merged_with(
                    {
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                if existing_result:
                    self._store_result_record(
                        JobResultRecord(
                            job_id=job_id,
                            paths=list(existing_result.get("paths") or []),
                            raw_outputs=raw_outputs,
                            zarr_outputs=list(existing_result.get("zarr_outputs") or []),
                            cube_outputs=list(existing_result.get("cube_outputs") or []),
                            masked_zarr_outputs=list(existing_result.get("masked_zarr_outputs") or []),
                            watermask_outputs=list(existing_result.get("watermask_outputs") or []),
                            cloudmask_outputs=list(existing_result.get("cloudmask_outputs") or []),
                            checksums=dict(existing_result.get("checksums") or {}),
                            metadata=dict(existing_result.get("metadata") or {}),
                            manifest_entry=dict(existing_result.get("manifest_entry") or {}),
                            pipeline_metadata=failure_pipeline_metadata.to_dict(),
                            conversion_metadata=conversion_metadata.to_dict(),
                        )
                    )
            rt.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=rt._now_iso(),
                errors=[str(exc)],
                pipeline_state=pipeline_state.value,
                pipeline_step=pipeline_step,
                pipeline_metadata=failure_pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata.to_dict(),
                raw_outputs=raw_outputs,
                zarr_outputs=list(existing_result.get("zarr_outputs") or current_row.get("zarr_outputs") or []),
            )
            rt.store.append_event(
                job_id,
                "job.failed",
                {
                    "status": JobState.failed.value,
                    "error": str(exc),
                    "pipeline_state": pipeline_state.value,
                },
            )
        finally:
            rt._cancel_check_cache.pop(job_id, None)

    def execute_integrated_mask_stage(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        request: JobCreateRequest,
        raw_result_paths: list[str],
        raw_outputs: list[str],
        zarr_outputs: list[str],
        cube_outputs: list[str],
        checksums: dict[str, Any],
        metadata: dict[str, Any],
        manifest_entry: dict[str, Any],
        provider_name: str,
        final_pipeline_metadata: dict[str, Any],
        conversion_metadata: dict[str, Any],
        aggregate: dict[str, Any],
        is_cancelled_now,
        cube_config: dict[str, Any] | None,
        resumed_pipeline: bool = False,
    ) -> None:
        rt = self._rt
        requested_mask_types = rt._normalized_mask_types(getattr(request, "mask_types", []))
        final_pipeline_metadata_record = PipelineMetadataRecord.from_mapping(final_pipeline_metadata)
        conversion_metadata_record = self._conversion_metadata_record(conversion_metadata)
        total_mask_outputs = max(1, len(zarr_outputs))
        mask_inference_device = str(getattr(request, "inference_device", "") or "").strip() or None
        water_inference_device = str(getattr(request, "water_inference_device", "") or "").strip() or None
        remote_mask_runtime = rt._remote_mask_runtime()
        mask_workers = rt._integrated_mask_max_workers(
            total=len(zarr_outputs),
            inference_device=mask_inference_device,
            water_inference_device=water_inference_device,
            remote_runtime=remote_mask_runtime,
            preferred_parallelism=rt._scene_parallelism_target_from_download(
                pipeline_metadata=final_pipeline_metadata,
                total=len(zarr_outputs),
            ),
            max_limit=min(4, max(1, int(rt.settings.nimbus_max_jobs or 1))),
        )
        mask_items: list[MaskWorkflowItem | None] = [None] * len(zarr_outputs)
        mask_errors: list[str] = []
        last_mask_execution: dict[str, Any] | None = None

        progress_lock = threading.Lock()
        progress_by_index: dict[int, float] = {item_index: 0.0 for item_index in range(len(zarr_outputs))}
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
                    rt._preferred_mask_failure_step(step_by_index.values()),
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

        def _make_item_progress_callback(item_index: int):
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
                    stage_by_index[item_index] = _normalize_pipeline_state(payload.get("pipeline_state"))
                    step_by_index[item_index] = _normalize_pipeline_step(payload.get("pipeline_step"))
                    aggregate_fraction = sum(progress_by_index.values()) / scene_count
                    aggregate_progress = 76.0 + (aggregate_fraction * 22.0)
                    aggregate_state, aggregate_step = _aggregate_pipeline_state()
                    aggregate_metadata = final_pipeline_metadata_record.merged_with(
                        {
                            **dict(payload.get("pipeline_metadata") or {}),
                            "mask_parallel_workers": mask_workers,
                            "mask_total_scenes": scene_count,
                            "mask_completed_scenes": sum(
                                1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                            ),
                            "mask_active_scenes": sum(
                                1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                            ),
                        }
                    )
                    aggregate_event_payload = {
                        **dict(payload.get("event_payload") or {}),
                        "scene_index": item_index + 1,
                        "scene_total": scene_count,
                        "item_fraction": item_fraction,
                        "aggregate_fraction": aggregate_fraction,
                    }
                    rt._update_pipeline(
                        job_id,
                        pipeline_state=aggregate_state,
                        pipeline_step=aggregate_step,
                        pipeline_progress=aggregate_progress,
                        pipeline_metadata=aggregate_metadata.to_dict(),
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
                    step_by_index[item_index] = rt._preferred_mask_failure_step([mask_execution.get("failed_step")])
                else:
                    stage_by_index.pop(item_index, None)
                    step_by_index.pop(item_index, None)
                aggregate_fraction = sum(progress_by_index.values()) / scene_count
                aggregate_progress = 76.0 + (aggregate_fraction * 22.0)
                aggregate_state, aggregate_step = _aggregate_pipeline_state()
                rt._update_pipeline(
                    job_id,
                    pipeline_state=aggregate_state,
                    pipeline_step=aggregate_step,
                    pipeline_progress=aggregate_progress,
                    pipeline_metadata=final_pipeline_metadata_record.merged_with(
                        {
                            "mask_parallel_workers": mask_workers,
                            "mask_total_scenes": scene_count,
                            "mask_completed_scenes": sum(
                                1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                            ),
                            "mask_active_scenes": sum(
                                1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                            ),
                        }
                    ).to_dict(),
                )

        def _run_mask_item(item_index: int, zarr_uri: str) -> tuple[int, str, dict[str, Any]]:
            if is_cancelled_now():
                raise rt.job_cancelled_error_cls("Job cancellation requested during integrated masking.")
            zarr_context = rt._resolve_zarr_context(
                job_id=job_id,
                row=row,
                result={"conversion_metadata": conversion_metadata_record.to_dict()},
                zarr_uri=zarr_uri,
                scene_id_override=None,
                product_type_override=getattr(request, "product_type", None),
            )
            item_start = 76.0 + (item_index * (22.0 / total_mask_outputs))
            item_end = 76.0 + ((item_index + 1) * (22.0 / total_mask_outputs))
            mask_execution = rt._run_in_place_mask_pipeline(
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
                mask_items[item_index] = MaskWorkflowItem.from_execution(
                    zarr_uri=zarr_uri,
                    mask_execution=mask_execution,
                )
                _mark_item_completion(item_index, mask_execution=mask_execution)
                last_mask_execution = mask_execution
                mask_errors.extend(
                    error for error in list(mask_execution.get("errors") or []) if error not in mask_errors
                )
                if not bool(mask_execution.get("succeeded")):
                    break
        else:
            with ThreadPoolExecutor(max_workers=mask_workers, thread_name_prefix="mask-scene") as executor:
                future_map = {
                    executor.submit(_run_mask_item, item_index, zarr_uri): (item_index, zarr_uri)
                    for item_index, zarr_uri in enumerate(zarr_outputs)
                }
                for future in as_completed(future_map):
                    item_index, zarr_uri = future_map[future]
                    item_index, zarr_uri, mask_execution = future.result()
                    mask_items[item_index] = MaskWorkflowItem.from_execution(
                        zarr_uri=zarr_uri,
                        mask_execution=mask_execution,
                    )
                    _mark_item_completion(item_index, mask_execution=mask_execution)
                    last_mask_execution = mask_execution
                    mask_errors.extend(
                        error for error in list(mask_execution.get("errors") or []) if error not in mask_errors
                    )

        mask_items = [item for item in mask_items if item is not None]
        final_pipeline_metadata_record = final_pipeline_metadata_record.merged_with(
            {
                "mask_parallel_workers": mask_workers,
                "mask_total_scenes": scene_count,
                "mask_completed_scenes": len(mask_items),
                "mask_active_scenes": 0,
            }
        )

        mask_item_statuses = [item.status.strip().lower() for item in mask_items]
        mask_succeeded = (
            len(mask_items) == len(zarr_outputs)
            and bool(mask_items)
            and not mask_errors
            and all(status == "written" for status in mask_item_statuses)
        )
        mask_summary = MaskWorkflowSummary(
            status="written" if mask_succeeded else "failed",
            mask_types=requested_mask_types,
            mask_mode="integrated",
            items=list(mask_items),
        )
        if last_mask_execution is not None:
            item_pipeline = PipelineMetadataRecord.from_mapping(last_mask_execution.get("pipeline_metadata"))
            item_conversion = ConversionMetadataRecord.from_mapping(last_mask_execution.get("conversion_metadata"))
            if len(mask_items) == 1:
                mask_state = MaskStateRecord.from_sources(item_conversion.to_dict(), item_pipeline.to_dict())
                mask_summary.masked_zarr_uri = item_pipeline.masked_zarr_uri or zarr_outputs[0]
                mask_summary.water_mask = PayloadRecord.from_mapping(mask_state.water_mask)
                mask_summary.cloud_mask = PayloadRecord.from_mapping(mask_state.cloud_mask)
                mask_summary.mask_quality = PayloadRecord.from_mapping(mask_state.mask_quality)
        mask_summary_payload = mask_summary.to_payload()
        final_pipeline_metadata_record = final_pipeline_metadata_record.merged_with(
            {
                "mask_status": mask_summary.status,
                "mask_items": [item.to_payload() for item in mask_items],
                "mask_types": requested_mask_types,
                "mask_mode": "integrated",
            }
        )
        if len(mask_items) == 1 and last_mask_execution is not None:
            item_pipeline = PipelineMetadataRecord.from_mapping(last_mask_execution.get("pipeline_metadata"))
            item_conversion = ConversionMetadataRecord.from_mapping(last_mask_execution.get("conversion_metadata"))
            mask_state = MaskStateRecord.from_sources(item_conversion.to_dict(), item_pipeline.to_dict())
            final_pipeline_metadata_record = final_pipeline_metadata_record.merged_with(
                {
                    "masked_zarr_uri": item_pipeline.masked_zarr_uri or zarr_outputs[0],
                    **mask_state.to_metadata_fields(),
                }
            )
        combined_conversion_metadata = conversion_metadata_record.merged_with({"mask": mask_summary_payload})
        combined_metadata = {
            **metadata,
            "mask": mask_summary_payload,
        }
        combined_cube_outputs = list(cube_outputs)
        if cube_config is not None and cube_config["mode"] == "after_mask" and mask_succeeded:
            cube_execution = rt._build_cube_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=request.collection,
                source_zarr_outputs=zarr_outputs,
                cube_mode=str(cube_config["mode"]),
                cube_start_date=cube_config["start_date"],
                cube_end_date=cube_config["end_date"],
                pipeline_metadata=final_pipeline_metadata_record.to_dict(),
                stage_start_progress=98.0,
                stage_end_progress=100.0,
            )
            combined_cube_outputs = list(cube_execution.get("cube_outputs") or [])
            final_pipeline_metadata_record = PipelineMetadataRecord.from_mapping(
                cube_execution.get("pipeline_metadata") or final_pipeline_metadata_record.to_dict()
            )
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
            terminal_pipeline_step = rt._mask_failure_step_from_items(
                mask_types=requested_mask_types,
                items=[item.to_payload() for item in mask_items],
            )
            mask_summary.failed_step = terminal_pipeline_step
            mask_summary_payload = mask_summary.to_payload()
            final_pipeline_metadata_record.payload["failed_step"] = terminal_pipeline_step
            combined_conversion_metadata = combined_conversion_metadata.merged_with(
                {"mask": mask_summary_payload, "failed_step": terminal_pipeline_step}
            )
            combined_metadata["mask"] = mask_summary_payload
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
        final_pipeline_metadata_record = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata_record)
        rt._update_pipeline(
            job_id,
            pipeline_state=terminal_pipeline_state,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=final_pipeline_metadata_record.to_dict(),
            conversion_metadata=combined_conversion_metadata.to_dict(),
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
        )
        final_pipeline_metadata_record = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata_record)
        self._store_result_record(
            JobResultRecord(
                job_id=job_id,
                paths=final_paths,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                cube_outputs=combined_cube_outputs,
                checksums=checksums,
                metadata=combined_metadata,
                manifest_entry=manifest_entry,
                pipeline_metadata=final_pipeline_metadata_record.to_dict(),
                conversion_metadata=combined_conversion_metadata.to_dict(),
            )
        )
        rt.store.update_job(
            job_id,
            state=terminal_state.value,
            progress=100.0 if terminal_state == JobState.succeeded else 0.0,
            finished_at=rt._now_iso(),
            bytes_downloaded=int(aggregate["bytes_downloaded"]),
            bytes_total=max(int(aggregate["bytes_total"]), int(aggregate["bytes_downloaded"])),
            pipeline_state=terminal_pipeline_state.value,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=final_pipeline_metadata_record.to_dict(),
            conversion_metadata=combined_conversion_metadata.to_dict(),
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            errors=mask_errors,
        )
        rt.store.append_event(
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
                "resumed_pipeline": resumed_pipeline,
            },
        )
        rt.store.append_event(
            job_id,
            "job.succeeded" if terminal_state == JobState.succeeded else "job.failed",
            {
                "status": terminal_state.value,
                "paths": final_paths,
                "pipeline_state": terminal_pipeline_state.value,
                "error": mask_errors[0] if mask_errors else "",
            },
        )

    def continue_remaining_pipeline_after_zarr(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result: dict[str, Any],
        raw_outputs: list[str],
        zarr_outputs: list[str],
        conversion_metadata: dict[str, Any],
        rerun_before_mask_cube: bool = True,
        rerun_masks: bool = True,
        rerun_after_mask_cube: bool = True,
        existing_cube_outputs: list[str] | None = None,
        initial_pipeline_state: PipelineState = PipelineState.zarr_written,
        initial_pipeline_step: str = "zarr_written",
        initial_pipeline_progress: float | None = None,
    ) -> JobStatusResponse:
        rt = self._rt
        request_payload = dict(row.get("request") or {})
        provider_name = rt._provider_name(row.get("provider"))
        collection = str(row.get("collection") or "")
        product_type = str(request_payload.get("product_type") or row.get("product_type") or "").strip() or None
        requested_mask_types = rt._normalized_mask_types(request_payload.get("mask_types") or [])
        cube_config = rt._cube_config_from_request_payload(request_payload)

        checksums = dict(result.get("checksums") or {})
        metadata = dict(result.get("metadata") or {})
        manifest_entry = dict(result.get("manifest_entry") or {})
        base_paths = rt._resume_base_result_paths(result=result, raw_outputs=raw_outputs)
        bytes_downloaded = int(row.get("bytes_downloaded") or 0)
        bytes_total = max(int(row.get("bytes_total") or 0), bytes_downloaded)
        conversion_metadata_record = self._conversion_metadata_record(conversion_metadata)

        final_pipeline_metadata = self._merged_pipeline_metadata_record(
            job_id,
            PipelineMetadataRecord.from_mapping(
                result.get("pipeline_metadata") or row.get("pipeline_metadata")
            ).merged_with(
                {
                    "zarr_output_count": len(zarr_outputs),
                    "manual_conversion": True,
                    "zarr_parallel_workers": int(conversion_metadata_record.payload.get("parallel_workers", 1) or 1),
                }
            ),
        )
        if requested_mask_types:
            final_pipeline_metadata.payload["mask_types"] = requested_mask_types
            final_pipeline_metadata.payload["mask_mode"] = "integrated"
        if cube_config is not None:
            final_pipeline_metadata.payload["cube_mode"] = cube_config["mode"]
            final_pipeline_metadata.payload["cube_layout"] = cube_config["layout"]
            final_pipeline_metadata.payload["cube_target_crs"] = cube_config["target_crs"]
            final_pipeline_metadata.payload["cube_target_resolution_m"] = cube_config["target_resolution_m"]
            final_pipeline_metadata.payload["cube_overlap_policy"] = cube_config["overlap_policy"]
            final_pipeline_metadata.payload["cube_date_range"] = {
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

        if initial_pipeline_progress is None:
            if rerun_masks:
                initial_pipeline_progress = 72.0 if rerun_before_mask_cube else 76.0
            elif rerun_after_mask_cube:
                initial_pipeline_progress = 98.0
            else:
                initial_pipeline_progress = 100.0

        has_downstream_stages = any(
            (
                rerun_before_mask_cube and cube_config is not None and cube_config["mode"] == "before_mask",
                rerun_masks and bool(requested_mask_types),
                rerun_after_mask_cube and cube_config is not None and cube_config["mode"] == "after_mask",
            )
        )
        rt._update_pipeline(
            job_id,
            pipeline_state=initial_pipeline_state,
            pipeline_step=initial_pipeline_step,
            pipeline_progress=initial_pipeline_progress if has_downstream_stages else 100.0,
            pipeline_metadata=final_pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata_record.to_dict(),
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            event_type=None,
            event_payload={
                "zarr_outputs": zarr_outputs,
                "pipeline_progress": initial_pipeline_progress if has_downstream_stages else 100.0,
                "resumed_pipeline": True,
            },
        )
        final_pipeline_metadata = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata)
        cube_outputs: list[str] = rt._merge_paths([], list(existing_cube_outputs or []))
        final_paths = [*base_paths, *zarr_outputs]
        if cube_outputs:
            final_paths.extend(cube_outputs)
        self._store_result_record(
            JobResultRecord(
                job_id=job_id,
                paths=final_paths,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                cube_outputs=cube_outputs,
                checksums=checksums,
                metadata=metadata,
                manifest_entry=manifest_entry,
                pipeline_metadata=final_pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata_record.to_dict(),
            )
        )

        final_pipeline_state = initial_pipeline_state
        final_pipeline_step = initial_pipeline_step

        if rerun_before_mask_cube and cube_config is not None and cube_config["mode"] == "before_mask":
            cube_execution = rt._build_cube_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                source_zarr_outputs=zarr_outputs,
                cube_mode=str(cube_config["mode"]),
                cube_start_date=cube_config["start_date"],
                cube_end_date=cube_config["end_date"],
                pipeline_metadata=final_pipeline_metadata.to_dict(),
                stage_start_progress=73.0,
                stage_end_progress=75.0 if requested_mask_types else 100.0,
            )
            cube_outputs = list(cube_execution.get("cube_outputs") or [])
            final_pipeline_metadata = PipelineMetadataRecord.from_mapping(
                cube_execution.get("pipeline_metadata") or final_pipeline_metadata.to_dict()
            )
            final_paths = [*base_paths, *zarr_outputs, *cube_outputs]
            self._store_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=final_paths,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    cube_outputs=cube_outputs,
                    checksums=checksums,
                    metadata=metadata,
                    manifest_entry=manifest_entry,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata_record.to_dict(),
                )
            )
            if cube_outputs and not requested_mask_types:
                final_pipeline_state = PipelineState.cube_written
                final_pipeline_step = "cube_written"

        if not rerun_masks:
            combined_cube_outputs = list(cube_outputs)
            terminal_pipeline_state = final_pipeline_state
            terminal_pipeline_step = final_pipeline_step
            if rerun_after_mask_cube and cube_config is not None and cube_config["mode"] == "after_mask":
                cube_execution = rt._build_cube_outputs(
                    job_id=job_id,
                    provider_name=provider_name,
                    collection=collection,
                    source_zarr_outputs=zarr_outputs,
                    cube_mode=str(cube_config["mode"]),
                    cube_start_date=cube_config["start_date"],
                    cube_end_date=cube_config["end_date"],
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    stage_start_progress=98.0,
                    stage_end_progress=100.0,
                )
                combined_cube_outputs = list(cube_execution.get("cube_outputs") or [])
                final_pipeline_metadata = PipelineMetadataRecord.from_mapping(
                    cube_execution.get("pipeline_metadata") or final_pipeline_metadata.to_dict()
                )
                if combined_cube_outputs:
                    terminal_pipeline_state = PipelineState.cube_written
                    terminal_pipeline_step = "cube_written"
            final_paths = [*base_paths, *zarr_outputs, *combined_cube_outputs]
            final_pipeline_metadata = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata)
            self._store_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=final_paths,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    cube_outputs=combined_cube_outputs,
                    checksums=checksums,
                    metadata=metadata,
                    manifest_entry=manifest_entry,
                    pipeline_metadata=final_pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata_record.to_dict(),
                )
            )
            rt.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                progress=100.0,
                finished_at=rt._now_iso(),
                bytes_downloaded=bytes_downloaded,
                bytes_total=bytes_total,
                pipeline_state=terminal_pipeline_state.value,
                pipeline_step=terminal_pipeline_step,
                pipeline_progress=100.0,
                pipeline_metadata=final_pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata_record.to_dict(),
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                errors=[],
            )
            rt.store.append_event(
                job_id,
                "job.succeeded",
                {
                    "status": JobState.succeeded.value,
                    "paths": final_paths,
                    "pipeline_state": terminal_pipeline_state.value,
                },
            )
            return rt.get_job(job_id)

        if not requested_mask_types:
            final_pipeline_metadata = self._merged_pipeline_metadata_record(job_id, final_pipeline_metadata)
            rt.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                progress=100.0,
                finished_at=rt._now_iso(),
                bytes_downloaded=bytes_downloaded,
                bytes_total=bytes_total,
                pipeline_state=final_pipeline_state.value,
                pipeline_step=final_pipeline_step,
                pipeline_progress=100.0,
                pipeline_metadata=final_pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata_record.to_dict(),
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
            )
            rt.store.append_event(
                job_id,
                "job.succeeded",
                {
                    "status": JobState.succeeded.value,
                    "paths": final_paths,
                    "pipeline_state": final_pipeline_state.value,
                },
            )
            return rt.get_job(job_id)

        started_row = rt._get_job_row_record(job_id)
        rt.store.update_job(
            job_id,
            state=JobState.running.value,
            started_at=(
                str((started_row.started_at if started_row is not None else "") or "").strip()
                or rt._now_iso()
            ),
            progress=100.0,
            finished_at=None,
            bytes_downloaded=bytes_downloaded,
            bytes_total=bytes_total,
            pipeline_state=final_pipeline_state.value,
            pipeline_step=final_pipeline_step,
            pipeline_progress=76.0,
            pipeline_metadata=final_pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata_record.to_dict(),
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            errors=[],
        )
        rt.store.append_event(
            job_id,
            "job.cube_written" if final_pipeline_state == PipelineState.cube_written else "job.zarr_written",
            {
                "pipeline_state": final_pipeline_state.value,
                "zarr_outputs": zarr_outputs,
                "cube_outputs": cube_outputs,
                "resumed_pipeline": True,
            },
        )

        self.execute_integrated_mask_stage(
            job_id=job_id,
            row=row,
            request=rt._request_adapter.validate_python(request_payload),
            raw_result_paths=base_paths,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            cube_outputs=cube_outputs,
            checksums=checksums,
            metadata=metadata,
            manifest_entry=manifest_entry,
            provider_name=provider_name,
            final_pipeline_metadata=final_pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata_record.to_dict(),
            aggregate={
                "bytes_downloaded": bytes_downloaded,
                "bytes_total": bytes_total,
            },
            is_cancelled_now=lambda: rt._is_job_cancel_requested(job_id),
            cube_config=cube_config,
            resumed_pipeline=True,
        )
        return rt.get_job(job_id)


class MaskJobWorkflowService:
    def __init__(self, runtime: Any):
        self._rt = runtime

    def execute_from_context(self, context: JobExecutionContext) -> None:
        self.execute(
            job_id=context.job_id,
            row=context.row,
            is_cancelled_now=context.is_cancelled_now,
        )

    def execute(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        is_cancelled_now,
    ) -> None:
        rt = self._rt
        try:
            rt._execute_mask_existing_zarr_job(
                job_id=job_id,
                row=row,
                is_cancelled_now=is_cancelled_now,
            )
        except (DownloadCancelled, rt.job_cancelled_error_cls):
            rt._mark_cancelled(job_id, "cancelled_during_mask")
        except Exception as exc:
            current_row_record = rt._get_job_row_record(job_id)
            current_row = current_row_record.to_row() if current_row_record is not None else row
            request_payload = dict(current_row.get("request") or {})
            if (
                str(request_payload.get("mask_contract_version") or "").strip().lower()
                == rt.MASK_CONTRACT_VERSION
            ):
                rt._fail_interrupted_mask_jobs(
                    job_ids=[job_id],
                    reason=f"Mask job crashed before finalization: {exc}",
                    event_type="job.mask_failed",
                )
            else:
                rt.store.update_job(
                    job_id,
                    state=JobState.failed.value,
                    finished_at=rt._now_iso(),
                    progress=0.0,
                    pipeline_state=PipelineState.failed.value,
                    pipeline_step="failed",
                    pipeline_progress=95.0,
                    pipeline_metadata=PipelineMetadataRecord.from_mapping(
                        current_row.get("pipeline_metadata")
                    ).to_dict(),
                    conversion_metadata=ConversionMetadataRecord.from_mapping(
                        current_row.get("conversion_metadata")
                    ).to_dict(),
                    zarr_outputs=[],
                    watermask_outputs=[],
                    cloudmask_outputs=[],
                    errors=[str(exc)],
                )
                rt.store.append_event(
                    job_id,
                    "job.mask_failed",
                    {
                        "status": JobState.failed.value,
                        "error": str(exc),
                        "pipeline_state": PipelineState.failed.value,
                    },
                )
        finally:
            rt._cancel_check_cache.pop(job_id, None)
