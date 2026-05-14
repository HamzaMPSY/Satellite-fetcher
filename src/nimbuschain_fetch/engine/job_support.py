from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Any, Callable, cast

import anyio

from nimbuschain_fetch.domain.metadata import (
    ConversionMetadataRecord,
    MaskStateRecord,
    PipelineMetadataRecord,
)
from nimbuschain_fetch.domain.records import JobResultRecord
from nimbuschain_fetch.jobs.store import JobListFilters
from nimbuschain_fetch.models import (
    BatchJobCreateRequest,
    JobConvertRequest,
    JobCreateRequest,
    JobMaskRequest,
    JobMaskResponse,
    JobResumeResponse,
    JobResultResponse,
    JobState,
    JobStatusResponse,
    PipelineState,
)


class FetcherJobSupport:
    """Submission, result, resume, and standalone mask-job helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def retire_legacy_mask_jobs(self) -> None:
        rows, _total = self._rt.store.list_jobs(
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
        now = self._rt._now_iso()
        for row in rows:
            if str(row.get("job_type") or "").strip().lower() != "mask_existing_zarr":
                continue
            request_payload = dict(row.get("request") or {})
            if str(request_payload.get("mask_contract_version") or "").strip().lower() == self._rt.MASK_CONTRACT_VERSION:
                continue
            job_id = str(row.get("job_id") or "").strip()
            if not job_id:
                continue
            self.cleanup_mask_job_outputs(job_id=job_id, row=row)
            self._rt.store.update_job(
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
            self._rt.store.append_event(
                job_id,
                "job.mask_retired_after_cutover",
                {"reason": "legacy_mask_contract"},
            )

    def cleanup_mask_job_outputs(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result_payload: dict[str, Any] | None = None,
        preserve_status_paths: bool = False,
        failure_reason: str | None = None,
    ) -> None:
        result_payload = self._rt._normalize_backend_paths_in_result_payload(
            result_payload or self._rt._get_result_payload(job_id)
        )
        normalized_row = self._rt._normalize_backend_paths_in_job_row(
            self._rt._normalize_historical_job_row(row)
        )
        request_payload = dict(normalized_row.get("request") or {})
        pipeline_metadata = PipelineMetadataRecord.from_mapping(
            result_payload.get("pipeline_metadata")
            or normalized_row.get("pipeline_metadata")
        )
        conversion_metadata = ConversionMetadataRecord.from_mapping(
            result_payload.get("conversion_metadata")
            or normalized_row.get("conversion_metadata")
        )
        result_metadata = dict(result_payload.get("metadata") or {})
        mask_state = MaskStateRecord.from_sources(
            conversion_metadata.to_dict(),
            pipeline_metadata.to_dict(),
            result_metadata,
        )
        source_zarr_uri = str(
            request_payload.get("source_zarr_uri")
            or pipeline_metadata.source_zarr_uri
            or conversion_metadata.source_zarr_uri
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
            pipeline_metadata.masked_zarr_uri,
            conversion_metadata.masked_zarr_uri,
        ):
            text = str(value or "").strip()
            if text:
                masked_zarr_candidates.add(text)

        for label, payload in (("water", mask_state.water_mask), ("cloud", mask_state.cloud_mask)):
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
            normalized = str(self._rt._normalize_backend_path(raw_value) or "").strip()
            if not normalized or normalized == source_zarr_uri:
                continue
            target = Path(normalized)
            self.remove_path_if_exists(target)
            if target.parent.exists():
                for pattern in (
                    f".{target.stem}.tmp-*{target.suffix}",
                    f".{target.stem}.backup-*{target.suffix}",
                ):
                    for sibling in target.parent.glob(pattern):
                        self.remove_path_if_exists(sibling)

        for raw_value in artifact_candidates:
            normalized = str(self._rt._normalize_backend_path(raw_value) or "").strip()
            if not normalized:
                continue
            self.remove_path_if_exists(Path(normalized))

        if preserve_status_paths:
            for status_path, payload, label in status_paths:
                self.mark_mask_status_failed(
                    status_path=status_path,
                    payload=payload,
                    reason=failure_reason or f"{label} mask job interrupted.",
                )

    @staticmethod
    def remove_path_if_exists(target: Path) -> None:
        try:
            if not target.exists():
                return
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        except OSError:
            return

    def mark_mask_status_failed(
        self,
        *,
        status_path: str,
        payload: dict[str, Any],
        reason: str,
    ) -> None:
        normalized = str(self._rt._normalize_backend_path(status_path) or "").strip()
        if not normalized:
            return
        target = Path(normalized)
        target.parent.mkdir(parents=True, exist_ok=True)
        failed_payload = dict(payload or {})
        failed_payload["status"] = "failed"
        failed_payload["reason"] = reason
        failed_payload["status_path"] = normalized
        failed_payload["updated_at"] = self._rt._now_iso()
        target.write_text(json.dumps(failed_payload, indent=2, sort_keys=True), encoding="utf-8")

    def fail_interrupted_mask_jobs(
        self,
        *,
        job_ids: list[str],
        reason: str,
        event_type: str,
    ) -> None:
        if not job_ids:
            return
        now = self._rt._now_iso()
        for job_id in job_ids:
            row_record = self._rt._get_job_row_record(job_id)
            if row_record is None:
                continue
            if str(row_record.job_type or "").strip().lower() != "mask_existing_zarr":
                continue
            request_payload = dict(row_record.request)
            if str(request_payload.get("mask_contract_version") or "").strip().lower() != self._rt.MASK_CONTRACT_VERSION:
                continue

            normalized_row = row_record.to_row()
            result_payload = self._rt._get_result_payload(job_id)
            self.cleanup_mask_job_outputs(
                job_id=job_id,
                row=normalized_row,
                result_payload=result_payload,
                preserve_status_paths=True,
                failure_reason=reason,
            )

            pipeline_metadata = PipelineMetadataRecord.from_mapping(
                result_payload.get("pipeline_metadata")
                or normalized_row.get("pipeline_metadata")
            )
            conversion_metadata = ConversionMetadataRecord.from_mapping(
                result_payload.get("conversion_metadata")
            )
            metadata = dict(result_payload.get("metadata") or {})
            mask_state = MaskStateRecord.from_sources(
                conversion_metadata.to_dict(),
                pipeline_metadata.to_dict(),
                metadata,
            )
            mask_state.apply_failure(reason)
            for payload in (pipeline_metadata.payload, conversion_metadata.payload, metadata):
                payload.update(mask_state.to_metadata_fields())
                payload["status"] = "failed"
                payload["interrupted_reason"] = reason
                payload.pop("masked_zarr_uri", None)

            self._rt._set_result_record(
                JobResultRecord(
                    job_id=job_id,
                    paths=[],
                    raw_outputs=[],
                    zarr_outputs=[],
                    cube_outputs=[],
                    masked_zarr_outputs=[],
                    watermask_outputs=[],
                    cloudmask_outputs=[],
                    checksums=dict(result_payload.get("checksums") or {}),
                    metadata={
                        **metadata,
                        "job_type": "mask_existing_zarr",
                        "job_kind": "mask",
                        "service_name": "mask_service",
                        "source_job_id": str(
                            metadata.get("source_job_id")
                            or pipeline_metadata.source_job_id
                            or conversion_metadata.source_job_id
                            or request_payload.get("source_job_id")
                            or ""
                        ).strip() or None,
                    },
                    manifest_entry=dict(result_payload.get("manifest_entry") or {}),
                    pipeline_metadata=pipeline_metadata.to_dict(),
                    conversion_metadata=conversion_metadata.to_dict(),
                )
            )
            self._rt.store.update_job(
                job_id,
                state=JobState.failed.value,
                finished_at=now,
                progress=0.0,
                pipeline_state=PipelineState.failed.value,
                pipeline_step="failed",
                pipeline_progress=95.0,
                pipeline_metadata=pipeline_metadata.to_dict(),
                conversion_metadata=conversion_metadata.to_dict(),
                zarr_outputs=[],
                watermask_outputs=[],
                cloudmask_outputs=[],
                errors=[reason],
            )
            self._rt.store.append_event(
                job_id,
                event_type,
                {
                    "status": JobState.failed.value,
                    "reason": reason,
                    "pipeline_state": PipelineState.failed.value,
                },
            )

    async def submit_job(self, request: JobCreateRequest) -> str:
        if not self._rt._started:
            await self._rt.start()

        request_payload = cast(dict[str, Any], request.model_dump(mode="json"))
        job_id = uuid.uuid4().hex
        self._rt.store.create_job(
            job_id=job_id,
            job_type=request_payload["job_type"],
            provider=request_payload["provider"],
            collection=request_payload["collection"],
            request_payload=request_payload,
        )
        self._rt.store.append_event(job_id, "job.queued", {"state": JobState.queued.value})
        if self._rt._execution_enabled and self._rt._executor is not None:
            await self._rt._executor.submit(job_id)
        return job_id

    async def submit_batch(self, request: BatchJobCreateRequest) -> list[str]:
        job_ids: list[str] = []
        for job in request.jobs:
            job_ids.append(await self.submit_job(job))
        return job_ids

    def create_mask_job(
        self,
        *,
        source_job_id: str,
        provider_name: str,
        collection: str,
        request_payload: dict[str, Any],
    ) -> str:
        job_id = uuid.uuid4().hex
        self._rt.store.create_job(
            job_id=job_id,
            job_type="mask_existing_zarr",
            provider=provider_name,
            collection=collection,
            request_payload=request_payload,
        )
        self._rt.store.append_event(
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
        row_record = self._rt.store.get_job_record(job_id)
        if row_record is None:
            raise self._rt.job_not_found_error_cls(job_id)
        row = row_record.to_row()
        row = self._rt._normalize_historical_job_row(row)
        row = self._rt._normalize_backend_paths_in_job_row(row)
        return self._rt._to_status_response(row)

    def resume_metadata_for_row(self, row: dict[str, Any]) -> dict[str, Any]:
        state = str(row.get("state") or "").strip().lower()
        pipeline_state = str(row.get("pipeline_state") or "").strip().lower()
        pipeline_step = str(row.get("pipeline_step") or "").strip().lower()
        raw_outputs = [str(item).strip() for item in list(row.get("raw_outputs") or []) if str(item).strip()]
        zarr_outputs = [str(item).strip() for item in list(row.get("zarr_outputs") or []) if str(item).strip()]
        request_payload = dict(row.get("request") or {})
        pipeline_metadata = PipelineMetadataRecord.from_mapping(row.get("pipeline_metadata"))
        conversion_metadata = ConversionMetadataRecord.from_mapping(row.get("conversion_metadata"))
        requested_mask_types = self._rt._normalized_mask_types(
            request_payload.get("mask_types") or pipeline_metadata.mask_types or []
        )
        cube_mode = self._rt._normalized_cube_mode(
            request_payload.get("cube_mode") or pipeline_metadata.cube_mode
        )
        mask_status = str(
            pipeline_metadata.mask_status
            or conversion_metadata.mask_status
            or ""
        ).strip().lower()

        if state != JobState.failed.value:
            return {
                "can_resume": False,
                "resume_action": None,
                "resume_label": None,
                "resume_reason": None,
            }

        zarr_resume_states = {
            PipelineState.downloaded.value,
            PipelineState.zarr_queued.value,
            PipelineState.zarr_converting.value,
            PipelineState.zarr_failed.value,
        }
        sen2like_resume_states = {
            PipelineState.sen2like_queued.value,
            PipelineState.sen2like_running.value,
            PipelineState.sen2like_failed.value,
        }
        cube_resume_states = {
            PipelineState.cube_queued.value,
            PipelineState.cube_building.value,
            PipelineState.cube_failed.value,
        }
        mask_resume_states = {
            PipelineState.resolving_source_zarr.value,
            PipelineState.copying_source_zarr.value,
            PipelineState.running_cloud_inference.value,
            PipelineState.running_water_inference.value,
            PipelineState.writing_mask_artifacts.value,
            PipelineState.writing_masked_zarr.value,
            PipelineState.registering_artifacts.value,
            PipelineState.failed.value,
        }

        if pipeline_state in sen2like_resume_states and raw_outputs:
            return {
                "can_resume": True,
                "resume_action": "resume_pipeline_from_sen2like",
                "resume_label": "Resume Pipeline",
                "resume_reason": (
                    "Downloaded raw outputs are already available, so Sen2Like can be retried "
                    "and the remaining pipeline stages can continue."
                ),
            }

        if pipeline_state in sen2like_resume_states:
            return {
                "can_resume": False,
                "resume_action": None,
                "resume_label": "Can't Resume",
                "resume_reason": (
                    "This Sen2Like step cannot be resumed because the required downloaded raw outputs "
                    "are not available."
                ),
            }

        if pipeline_state in zarr_resume_states and raw_outputs:
            return {
                "can_resume": True,
                "resume_action": "resume_pipeline_from_zarr",
                "resume_label": "Resume Pipeline",
                "resume_reason": (
                    "Downloaded raw outputs are already available, so conversion can continue "
                    "from the interrupted Zarr step and continue the remaining pipeline stages."
                ),
            }

        if pipeline_state in zarr_resume_states:
            return {
                "can_resume": False,
                "resume_action": None,
                "resume_label": "Can't Resume",
                "resume_reason": (
                    "This job does not have reusable raw outputs, so the interrupted Zarr step cannot be resumed yet."
                ),
            }

        if pipeline_state in cube_resume_states and zarr_outputs and cube_mode != "none":
            return {
                "can_resume": True,
                "resume_action": "resume_pipeline_from_cube",
                "resume_label": "Resume Pipeline",
                "resume_reason": (
                    "Zarr outputs are already available, so the pipeline can continue from the interrupted cube step."
                ),
            }

        if pipeline_state in cube_resume_states:
            return {
                "can_resume": False,
                "resume_action": None,
                "resume_label": "Can't Resume",
                "resume_reason": (
                    "This cube step cannot be resumed because the required Zarr outputs are not available."
                ),
            }

        if (
            mask_status == "failed"
            or pipeline_step in {"cloud_failed", "water_failed"}
            or pipeline_state in mask_resume_states
        ) and requested_mask_types and zarr_outputs:
            return {
                "can_resume": True,
                "resume_action": "resume_pipeline_from_mask",
                "resume_label": "Resume Pipeline",
                "resume_reason": (
                    "Zarr outputs are already available, so the pipeline can continue from the interrupted mask step."
                ),
            }

        if (
            mask_status == "failed"
            or pipeline_step in {"cloud_failed", "water_failed"}
            or pipeline_state in mask_resume_states
        ):
            return {
                "can_resume": False,
                "resume_action": None,
                "resume_label": "Can't Resume",
                "resume_reason": (
                    "This mask step cannot be resumed because the required Zarr outputs are not available."
                ),
            }

        return {
            "can_resume": False,
            "resume_action": None,
            "resume_label": "Can't Resume",
            "resume_reason": "Resume is not implemented for this failure type yet.",
        }

    def resume_pipeline_from_mask_failure(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
    ) -> JobStatusResponse:
        result = self._rt._get_result_payload(job_id)
        raw_outputs = self._rt._merge_paths(
            list(row.get("raw_outputs") or []),
            list(result.get("raw_outputs") or []),
        )
        zarr_outputs = self._rt._merge_paths(
            list(row.get("zarr_outputs") or []),
            list(result.get("zarr_outputs") or []),
        )
        if not zarr_outputs:
            raise ValueError("This job cannot be resumed because the required Zarr outputs are not available.")
        existing_cube_outputs = self._rt._merge_paths(
            list(row.get("cube_outputs") or []),
            list(result.get("cube_outputs") or []),
        )
        initial_state = PipelineState.cube_written if existing_cube_outputs else PipelineState.zarr_written
        initial_step = "cube_written" if existing_cube_outputs else "zarr_written"
        return self._rt._continue_remaining_pipeline_after_zarr(
            job_id=job_id,
            row=row,
            result=result,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            conversion_metadata=ConversionMetadataRecord.from_mapping(
                result.get("conversion_metadata") or row.get("conversion_metadata")
            ).to_dict(),
            rerun_before_mask_cube=False,
            rerun_masks=True,
            rerun_after_mask_cube=True,
            existing_cube_outputs=existing_cube_outputs,
            initial_pipeline_state=initial_state,
            initial_pipeline_step=initial_step,
            initial_pipeline_progress=76.0,
        )

    def resume_pipeline_from_cube_failure(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
    ) -> JobStatusResponse:
        result = self._rt._get_result_payload(job_id)
        raw_outputs = self._rt._merge_paths(
            list(row.get("raw_outputs") or []),
            list(result.get("raw_outputs") or []),
        )
        zarr_outputs = self._rt._merge_paths(
            list(row.get("zarr_outputs") or []),
            list(result.get("zarr_outputs") or []),
        )
        if not zarr_outputs:
            raise ValueError("This job cannot be resumed because the required Zarr outputs are not available.")
        cube_config = self._rt._cube_config_from_request_payload(dict(row.get("request") or {}))
        if cube_config is None:
            raise ValueError("This job cannot be resumed because its cube configuration is missing.")
        if str(cube_config.get("mode") or "") == "after_mask":
            return self._rt._continue_remaining_pipeline_after_zarr(
                job_id=job_id,
                row=row,
                result=result,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata=ConversionMetadataRecord.from_mapping(
                    result.get("conversion_metadata") or row.get("conversion_metadata")
                ).to_dict(),
                rerun_before_mask_cube=False,
                rerun_masks=False,
                rerun_after_mask_cube=True,
                existing_cube_outputs=[],
                initial_pipeline_state=PipelineState.masked_zarr_written,
                initial_pipeline_step="masked_zarr_written",
                initial_pipeline_progress=98.0,
            )
        return self._rt._continue_remaining_pipeline_after_zarr(
            job_id=job_id,
            row=row,
            result=result,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            conversion_metadata=ConversionMetadataRecord.from_mapping(
                result.get("conversion_metadata") or row.get("conversion_metadata")
            ).to_dict(),
            rerun_before_mask_cube=True,
            rerun_masks=bool(
                self._rt._normalized_mask_types(dict(row.get("request") or {}).get("mask_types") or [])
            ),
            rerun_after_mask_cube=False,
            existing_cube_outputs=[],
            initial_pipeline_state=PipelineState.zarr_written,
            initial_pipeline_step="zarr_written",
            initial_pipeline_progress=72.0,
        )

    def resume_job(self, job_id: str) -> JobResumeResponse:
        row_record = self._rt._get_job_row_record(job_id)
        if row_record is None:
            raise self._rt.job_not_found_error_cls(job_id)
        normalized_row = row_record.to_row()
        resume_metadata = self.resume_metadata_for_row(normalized_row)
        if not bool(resume_metadata.get("can_resume")):
            raise ValueError("This job cannot be resumed from its current pipeline state.")

        resume_action = str(resume_metadata.get("resume_action") or "").strip()
        resume_label = str(resume_metadata.get("resume_label") or "Resume").strip()
        self._rt.store.append_event(
            job_id,
            "job.resume_requested",
            {
                "resume_action": resume_action,
                "resume_label": resume_label,
                "pipeline_state": str(normalized_row.get("pipeline_state") or ""),
                "pipeline_step": str(normalized_row.get("pipeline_step") or ""),
            },
        )

        if resume_action == "resume_pipeline_from_sen2like":
            resumed_job = self._rt._resume_pipeline_from_sen2like_failure(
                job_id=job_id,
                row=normalized_row,
            )
        elif resume_action == "resume_pipeline_from_zarr":
            resumed_job = self._rt.convert_existing_job(
                job_id,
                JobConvertRequest(),
                continue_pipeline=True,
            )
        elif resume_action == "resume_pipeline_from_mask":
            resumed_job = self._rt._resume_pipeline_from_mask_failure(
                job_id=job_id,
                row=normalized_row,
            )
        elif resume_action == "resume_pipeline_from_cube":
            resumed_job = self._rt._resume_pipeline_from_cube_failure(
                job_id=job_id,
                row=normalized_row,
            )
        else:
            raise ValueError(f"Unsupported resume action '{resume_action}'.")

        self._rt.store.append_event(
            job_id,
            "job.resumed",
            {
                "resume_action": resume_action,
                "resume_label": resume_label,
                "resumed_job_id": job_id,
                "spawned_new_job": False,
            },
        )
        return JobResumeResponse(
            source_job_id=job_id,
            resumed_job_id=job_id,
            resume_action=resume_action,
            resume_label=resume_label,
            spawned_new_job=False,
            message="Resumed the existing job and continued the remaining pipeline stages.",
            job=resumed_job,
        )

    def get_result(self, job_id: str) -> JobResultResponse:
        job_row_record = self._rt._get_job_row_record(job_id)
        if job_row_record is None:
            raise self._rt.job_not_found_error_cls(job_id)
        job_row = job_row_record.to_row()
        result = self._rt._get_result_payload(job_id)
        if not result and not any(
            [
                list(job_row.get("raw_outputs") or []),
                list(job_row.get("zarr_outputs") or []),
                list(job_row.get("watermask_outputs") or []),
                list(job_row.get("cloudmask_outputs") or []),
            ]
        ):
            raise self._rt.job_not_found_error_cls(job_id)

        result = self._rt._normalize_backend_paths_in_result_payload(result)
        zarr_outputs = list(result.get("zarr_outputs") or job_row.get("zarr_outputs") or [])
        cube_outputs = self._rt._cube_outputs_for_job(job_id=job_id, result=result, row=job_row)
        masked_zarr_outputs = self._rt._masked_zarr_outputs_for_job(job_id=job_id, result=result, row=job_row)
        normalized_result = {
            "job_id": job_id,
            "job_type": job_row.get("job_type"),
            "job_kind": self._rt._job_kind_for_type(job_row.get("job_type")),
            "service_name": self._rt._service_name_for_type(job_row.get("job_type")),
            "source_job_id": str(
                (result.get("metadata") or {}).get("source_job_id")
                or PipelineMetadataRecord.from_mapping(result.get("pipeline_metadata")).source_job_id
                or PipelineMetadataRecord.from_mapping(job_row.get("pipeline_metadata")).source_job_id
                or ""
            ).strip() or None,
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
            "pipeline_metadata": PipelineMetadataRecord.from_mapping(
                result.get("pipeline_metadata") or job_row.get("pipeline_metadata")
            ).to_dict(),
            "conversion_metadata": ConversionMetadataRecord.from_mapping(
                result.get("conversion_metadata") or job_row.get("conversion_metadata")
            ).to_dict(),
        }
        return JobResultResponse.model_validate(normalized_result)

    def apply_mask_existing_job(self, job_id: str, request: JobMaskRequest) -> JobMaskResponse:
        row_record = self._rt._get_job_row_record(job_id)
        if row_record is None:
            raise self._rt.job_not_found_error_cls(job_id)
        row = row_record.to_row()
        state = str(row.get("state") or "")
        if state in {JobState.queued.value, JobState.running.value, JobState.cancel_requested.value}:
            raise ValueError("Masking is only allowed when the source job is not actively running.")

        result = self._rt._get_result_payload(job_id)
        available_zarr_uris = self._rt._job_related_zarr_uris(job_id=job_id, row=row, result=result)
        if not available_zarr_uris:
            raise ValueError("No Zarr output is attached to this job. Run the Zarr conversion first.")

        selected_zarr_uri = self._rt._normalize_backend_path(str(request.zarr_uri or available_zarr_uris[0]).strip())
        if selected_zarr_uri not in available_zarr_uris:
            raise ValueError("The requested Zarr URI is not attached to this job lineage.")

        zarr_context = self._rt._resolve_zarr_context(
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
            "mask_contract_version": self._rt.MASK_CONTRACT_VERSION,
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
            "water_overwrite": (
                bool(request.water_overwrite)
                if request.water_overwrite is not None
                else bool(request.overwrite)
            ),
            "water_inference_device": str(request.water_inference_device or "").strip() or None,
            "fail_on_error": bool(request.fail_on_error),
        }
        mask_job_id = self.create_mask_job(
            source_job_id=job_id,
            provider_name=zarr_context["provider"],
            collection=zarr_context["collection"],
            request_payload=mask_job_request,
        )
        self._rt.store.update_job(
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
        self._rt.store.append_event(
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
        if self._rt._execution_enabled and self._rt._executor is not None:
            try:
                anyio.from_thread.run(self._rt._executor.submit, mask_job_id)
            except RuntimeError:
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

    def execute_mask_existing_zarr_job(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        is_cancelled_now: Callable[[], bool],
    ) -> None:
        request_payload = dict(row.get("request") or {})
        if str(request_payload.get("mask_contract_version") or "").strip().lower() != self._rt.MASK_CONTRACT_VERSION:
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
            source_row_record = self._rt._get_job_row_record(source_job_id)
            source_row = source_row_record.to_row() if source_row_record is not None else {}
            source_result = self._rt._get_result_payload(source_job_id)
            resolved = self._rt._resolve_zarr_context(
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
            raise self._rt.job_cancelled_error_cls("Mask job cancellation requested before execution.")

        mask_execution = self._rt._run_in_place_mask_pipeline(
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
        pipeline_metadata = PipelineMetadataRecord.from_mapping(mask_execution["pipeline_metadata"] or {})
        conversion_metadata = ConversionMetadataRecord.from_mapping(mask_execution["conversion_metadata"] or {})
        mask_state = MaskStateRecord.from_sources(
            conversion_metadata.to_dict(),
            pipeline_metadata.to_dict(),
        )
        result_record = JobResultRecord(
            job_id=job_id,
            paths=self._rt._merge_paths(visible_masked_zarr_outputs, []),
            raw_outputs=[],
            zarr_outputs=visible_masked_zarr_outputs,
            cube_outputs=[],
            masked_zarr_outputs=visible_masked_zarr_outputs,
            watermask_outputs=visible_watermask_outputs,
            cloudmask_outputs=visible_cloudmask_outputs,
            checksums={},
            metadata={
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
                **mask_state.to_metadata_fields(),
            },
            manifest_entry={},
            pipeline_metadata=pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata.to_dict(),
        )
        terminal_pipeline_state = PipelineState.masked_zarr_written if mask_job_succeeded else PipelineState.failed
        terminal_pipeline_step = (
            PipelineState.masked_zarr_written.value
            if mask_job_succeeded
            else self._rt._preferred_mask_failure_step([mask_execution.get("failed_step")])
        )
        terminal_state = JobState.succeeded if terminal_pipeline_state == PipelineState.masked_zarr_written else JobState.failed
        pipeline_metadata = PipelineMetadataRecord.from_mapping(
            self._rt._merged_pipeline_metadata(job_id, pipeline_metadata.to_dict())
        )
        self._rt._update_pipeline(
            job_id,
            pipeline_state=terminal_pipeline_state,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata.to_dict(),
            zarr_outputs=visible_masked_zarr_outputs,
        )
        pipeline_metadata = PipelineMetadataRecord.from_mapping(
            self._rt._merged_pipeline_metadata(job_id, pipeline_metadata.to_dict())
        )
        result_record.pipeline_metadata = pipeline_metadata.to_dict()
        self._rt._set_result_record(result_record)
        errors = list(mask_execution.get("errors") or [])
        self._rt.store.update_job(
            job_id,
            state=terminal_state.value,
            finished_at=self._rt._now_iso(),
            progress=100.0 if terminal_state == JobState.succeeded else 0.0,
            pipeline_state=terminal_pipeline_state.value,
            pipeline_step=terminal_pipeline_step,
            pipeline_progress=100.0 if terminal_state == JobState.succeeded else 95.0,
            pipeline_metadata=pipeline_metadata.to_dict(),
            conversion_metadata=conversion_metadata.to_dict(),
            zarr_outputs=visible_masked_zarr_outputs,
            watermask_outputs=visible_watermask_outputs,
            cloudmask_outputs=visible_cloudmask_outputs,
            errors=errors,
        )
        self._rt.store.append_event(
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
                "water_mask": dict(mask_state.water_mask),
                "cloud_mask": dict(mask_state.cloud_mask),
            },
        )
