from __future__ import annotations

from typing import Any

from nimbuschain_fetch.domain.metadata import ConversionMetadataRecord, PipelineMetadataRecord
from nimbuschain_fetch.domain.records import JobResultRecord
from nimbuschain_fetch.download.download_manager import DownloadCancelled
from nimbuschain_fetch.models import JobConvertRequest, JobState, JobStatusResponse, PipelineState


class ManualConversionService:
    def __init__(self, runtime: Any):
        self._rt = runtime

    def convert_existing_job(
        self,
        job_id: str,
        request: JobConvertRequest,
        *,
        continue_pipeline: bool = False,
    ) -> JobStatusResponse:
        rt = self._rt
        row_record = rt._get_job_row_record(job_id)
        if row_record is None:
            raise rt.job_not_found_error_cls(job_id)

        state = str(row_record.state or "")
        if state in {JobState.queued.value, JobState.running.value, JobState.cancel_requested.value}:
            raise ValueError("Manual conversion is only allowed when the job is not actively running.")

        result_record = rt._get_result_record(job_id)
        row = row_record.to_row()
        result = result_record.to_row() if result_record is not None else {}
        raw_outputs = list(
            row_record.raw_outputs
            or result.get("raw_outputs")
            or rt._filter_manifest_paths(list(result.get("paths") or []))
        )
        selected_raw_uri = str(request.raw_uri or (raw_outputs[0] if raw_outputs else "")).strip()
        if not selected_raw_uri:
            raise ValueError("No raw output is attached to this job. Provide raw_uri explicitly.")
        scene_id = str(request.scene_id or rt._scene_id_from_raw_uri(selected_raw_uri)).strip()
        provider_name = rt._provider_name(row_record.provider)
        collection = str(row_record.collection or "")
        product_type = str(request.product_type or row_record.product_type or "").strip() or None
        output_uri = str(request.output_uri or rt._default_zarr_output_uri(scene_id)).strip()

        rt.store.update_job(
            job_id,
            state=JobState.running.value,
            started_at=rt._now_iso(),
            finished_at=None,
            errors=[],
        )
        try:
            zarr_outputs, conversion_metadata = rt._convert_raw_outputs(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                product_type=product_type,
                raw_outputs=[selected_raw_uri],
                is_cancelled=lambda: rt._is_job_cancel_requested(job_id),
                scene_id_override=scene_id,
                output_uri_override=output_uri,
                pipeline_metadata=dict(row_record.pipeline_metadata),
            )
            if continue_pipeline:
                return rt._continue_remaining_pipeline_after_zarr(
                    job_id=job_id,
                    row=row,
                    result=result,
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    conversion_metadata=conversion_metadata,
                )

            pipeline_state = PipelineState.zarr_written
            pipeline_metadata = rt._merged_pipeline_metadata(
                job_id,
                {
                    **dict(row_record.pipeline_metadata),
                    "manual_conversion": True,
                    "raw_output_count": len(raw_outputs),
                    "zarr_output_count": len(zarr_outputs),
                    "zarr_parallel_workers": int(conversion_metadata.get("parallel_workers", 1) or 1),
                },
            )
            rt._update_pipeline(
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
            pipeline_metadata = rt._merged_pipeline_metadata(job_id, pipeline_metadata)
            rt._set_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=list(result_record.paths) if result_record is not None else list(result.get("paths") or []),
                    raw_outputs=raw_outputs,
                    zarr_outputs=zarr_outputs,
                    cube_outputs=[],
                    checksums=dict(result_record.checksums) if result_record is not None else dict(result.get("checksums") or {}),
                    metadata=dict(result_record.metadata) if result_record is not None else dict(result.get("metadata") or {}),
                    manifest_entry=(
                        dict(result_record.manifest_entry)
                        if result_record is not None
                        else dict(result.get("manifest_entry") or {})
                    ),
                    pipeline_metadata=pipeline_metadata,
                    conversion_metadata=conversion_metadata,
                )
            )
            rt.store.update_job(
                job_id,
                state=JobState.succeeded.value,
                finished_at=rt._now_iso(),
                pipeline_state=pipeline_state.value,
                pipeline_step="zarr_written",
                pipeline_progress=100.0,
                pipeline_metadata=pipeline_metadata,
                conversion_metadata=conversion_metadata,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
            )
            return rt.get_job(job_id)
        except (DownloadCancelled, rt.job_cancelled_error_cls):
            rt._mark_cancelled(job_id, "cancelled_during_zarr_resume")
            return rt.get_job(job_id)
        except Exception as exc:
            current_row_record = rt._get_job_row_record(job_id) or row_record
            current_row = current_row_record.to_row()
            existing_result = rt._get_result_payload(job_id) or result
            current_pipeline_state = str(
                current_row.get("pipeline_state")
                or row.get("pipeline_state")
                or row_record.pipeline_state
                or ""
            )
            is_zarr_failure = current_pipeline_state in {
                PipelineState.zarr_queued.value,
                PipelineState.zarr_converting.value,
                PipelineState.downloaded.value,
                PipelineState.zarr_failed.value,
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
            conversion_metadata = ConversionMetadataRecord.from_mapping(
                existing_result.get("conversion_metadata") or current_row.get("conversion_metadata")
            ).merged_with(
                {
                    "status": "failed",
                    "error": str(exc),
                }
            )
            pipeline_metadata = PipelineMetadataRecord.from_mapping(
                existing_result.get("pipeline_metadata") or current_row.get("pipeline_metadata")
            ).to_dict()
            rt._set_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=list(existing_result.get("paths") or []),
                    raw_outputs=raw_outputs,
                    zarr_outputs=list(existing_result.get("zarr_outputs") or current_row.get("zarr_outputs") or []),
                    cube_outputs=list(existing_result.get("cube_outputs") or []),
                    masked_zarr_outputs=list(existing_result.get("masked_zarr_outputs") or []),
                    watermask_outputs=list(existing_result.get("watermask_outputs") or []),
                    cloudmask_outputs=list(existing_result.get("cloudmask_outputs") or []),
                    checksums=dict(existing_result.get("checksums") or {}),
                    metadata=dict(existing_result.get("metadata") or {}),
                    manifest_entry=dict(existing_result.get("manifest_entry") or {}),
                    pipeline_metadata=pipeline_metadata,
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
                pipeline_metadata=pipeline_metadata,
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
            raise
        finally:
            rt._cancel_check_cache.pop(job_id, None)
