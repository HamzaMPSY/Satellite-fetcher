from __future__ import annotations

import inspect
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from nimbuschain_fetch.application.job_execution import JobExecutionContext, JobExecutionHandler
from nimbuschain_fetch.domain.metadata import PipelineMetadataRecord
from nimbuschain_fetch.models import JobState
from nimbuschain_fetch.pipeline import (
    PipelineOptions,
    PipelineOrchestrator,
    StageResult,
    StageStatus,
    build_default_pipeline_stages,
)


ORCHESTRATOR_METADATA_VERSION = 1


class ModularPipelineJobExecutionHandler(JobExecutionHandler):
    """Run API jobs through the modular pipeline planning boundary.

    The physical fetch/Zarr/mask/cube runners are still the existing production
    workflow service in this first migration slice. This handler makes the DAG
    explicit for API jobs and persists stage-level results so later commits can
    swap individual stage runners without changing public routes.
    """

    def __init__(self, *, runtime: Any, workflow: Any):
        self._rt = runtime
        self._workflow = workflow

    def execute(self, context: JobExecutionContext) -> Any:
        return self._execute(context)

    async def _execute(self, context: JobExecutionContext) -> None:
        plan = self._stage_plan(context.row)
        self._write_orchestrator_metadata(
            context.job_id,
            plan=plan,
            status="running",
            stage_results=[],
            error=None,
            finished_at=None,
        )
        try:
            result = self._workflow.execute_from_context(context)
            if inspect.isawaitable(result):
                await result
        finally:
            self._finalize_orchestrator_metadata(context.job_id, plan=plan)

    def _stage_plan(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        request_payload = dict(row.get("request") or {})
        provider = str(row.get("provider") or request_payload.get("provider") or "").strip().lower()
        collection = str(row.get("collection") or request_payload.get("collection") or "").strip()
        product_type = str(
            row.get("product_type") or request_payload.get("product_type") or ""
        ).strip() or None
        mask_types = tuple(self._rt._normalized_mask_types(request_payload.get("mask_types") or []))
        cube_mode = self._rt._normalized_cube_mode(request_payload.get("cube_mode"))
        options = PipelineOptions(
            provider=provider,
            collection=collection,
            product_type=product_type,
            mask_types=mask_types,
            cube_mode=cube_mode,
            sen2like_service_url=getattr(
                getattr(self._rt, "settings", None),
                "nimbus_sen2like_service_url",
                None,
            ),
        )
        orchestrator = PipelineOrchestrator(build_default_pipeline_stages(options))
        target_stage = "fetch" if bool(request_payload.get("download_only")) else None
        return orchestrator.describe_plan(target_stage=target_stage)

    def _write_orchestrator_metadata(
        self,
        job_id: str,
        *,
        plan: list[dict[str, Any]],
        status: str,
        stage_results: list[dict[str, Any]],
        error: str | None,
        finished_at: str | None,
    ) -> None:
        row_record = self._rt.store.get_job_record(job_id)
        existing_metadata = (
            row_record.pipeline_metadata.to_dict()
            if row_record is not None and hasattr(row_record.pipeline_metadata, "to_dict")
            else dict(getattr(row_record, "pipeline_metadata", {}) or {})
        )
        started_at = str(
            dict(existing_metadata.get("orchestrator") or {}).get("started_at")
            or _now_iso()
        )
        payload = {
            **existing_metadata,
            "stage_plan": list(plan),
            "stage_results": list(stage_results),
            "orchestrator": {
                "version": ORCHESTRATOR_METADATA_VERSION,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "stage_count": len(plan),
                "plan": list(plan),
                "stage_results": list(stage_results),
                "error": error,
            },
        }
        self._rt.store.update_job(job_id, pipeline_metadata=payload)

        result_payload = self._rt.store.get_result(job_id)
        if result_payload:
            updated_result = dict(result_payload)
            updated_result["pipeline_metadata"] = payload
            self._rt.store.set_result(job_id, updated_result)

    def _finalize_orchestrator_metadata(
        self,
        job_id: str,
        *,
        plan: list[dict[str, Any]],
    ) -> None:
        row_record = self._rt.store.get_job_record(job_id)
        if row_record is None:
            return
        row = row_record.to_row()
        result_payload = self._rt.store.get_result(job_id) or {}
        stage_results = self._stage_results_from_job(
            row=row,
            result=result_payload,
            plan=plan,
        )
        final_status = _orchestrator_status(
            job_state=str(row.get("state") or ""),
            stage_results=stage_results,
        )
        first_error = next(
            (
                str(item.get("error") or "").strip()
                for item in stage_results
                if str(item.get("error") or "").strip()
            ),
            None,
        )
        self._write_orchestrator_metadata(
            job_id,
            plan=plan,
            status=final_status,
            stage_results=stage_results,
            error=first_error,
            finished_at=_now_iso(),
        )
        self._rt.store.append_event(
            job_id,
            "job.pipeline_orchestrated",
            {
                "status": final_status,
                "stage_count": len(stage_results),
                "stage_results": stage_results,
            },
        )

    def _stage_results_from_job(
        self,
        *,
        row: dict[str, Any],
        result: dict[str, Any],
        plan: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result_payload = result
        result_pipeline_metadata = PipelineMetadataRecord.from_mapping(
            result_payload.get("pipeline_metadata")
        )
        row_pipeline_metadata = PipelineMetadataRecord.from_mapping(
            row.get("pipeline_metadata")
        )
        pipeline_metadata = result_pipeline_metadata.merged_with(
            row_pipeline_metadata.to_dict()
        ).to_dict()
        timeline = dict(pipeline_metadata.get("timeline") or {})
        timeline_stages = [
            dict(item)
            for item in list(timeline.get("stages") or [])
            if isinstance(item, dict)
        ]
        raw_outputs = _string_list(result_payload.get("raw_outputs") or row.get("raw_outputs"))
        zarr_outputs = _string_list(result_payload.get("zarr_outputs") or row.get("zarr_outputs"))
        cube_outputs = _string_list(result_payload.get("cube_outputs") or row.get("cube_outputs"))
        masked_outputs = _string_list(
            result_payload.get("masked_zarr_outputs") or row.get("masked_zarr_outputs")
        )
        water_outputs = _string_list(result_payload.get("watermask_outputs") or row.get("watermask_outputs"))
        cloud_outputs = _string_list(result_payload.get("cloudmask_outputs") or row.get("cloudmask_outputs"))
        job_state = str(row.get("state") or "").strip().lower()
        pipeline_state = str(row.get("pipeline_state") or "").strip().lower()
        errors = _string_list(row.get("errors"))
        results: list[dict[str, Any]] = []
        results_by_name: dict[str, dict[str, Any]] = {}
        for plan_item in plan:
            stage_name = str(plan_item.get("name") or "").strip()
            if not stage_name:
                continue
            blocked_by = [
                str(dependency)
                for dependency in list(plan_item.get("depends_on") or [])
                if _stage_blocks_downstream(results_by_name.get(str(dependency)))
            ]
            if blocked_by:
                stage_result = _stage_result_dict(
                    stage_name,
                    status=StageStatus.skipped.value,
                    outputs=[],
                    timeline_stages=[],
                    metadata={
                        "reason": "dependency_not_succeeded",
                        "blocked_by": blocked_by,
                    },
                )
                results.append(stage_result)
                results_by_name[stage_name] = stage_result
                continue
            if stage_name == "fetch":
                stage_result = _stage_result_dict(
                    "fetch",
                    status=_status_from_outputs(
                        raw_outputs,
                        job_state=job_state,
                        fallback=_combined_timeline_status(timeline_stages, ["search", "download"]),
                    ),
                    outputs=raw_outputs,
                    timeline_stages=_select_timeline_stages(timeline_stages, ["search", "download"]),
                    metadata={"runner": "provider_download"},
                    error=_stage_error(job_state, errors, has_outputs=bool(raw_outputs)),
                )
                results.append(stage_result)
                results_by_name[stage_name] = stage_result
            elif stage_name == "sen2like":
                sen2like_outputs = _string_list(
                    pipeline_metadata.get("sen2like_outputs")
                    or result_payload.get("sen2like_outputs")
                )
                sen2like_timeline_stages = _select_timeline_stages(timeline_stages, ["sen2like"])
                sen2like_status = str(pipeline_metadata.get("sen2like_status") or "").strip().lower()
                sen2like_error = str(pipeline_metadata.get("sen2like_error") or "").strip()
                if sen2like_outputs:
                    stage_result = _stage_result_dict(
                        "sen2like",
                        status=StageStatus.succeeded.value,
                        outputs=sen2like_outputs,
                        timeline_stages=sen2like_timeline_stages,
                        metadata={
                            "runner": "sen2like_service",
                            "service_url": pipeline_metadata.get("sen2like_service_url"),
                        },
                    )
                    results.append(stage_result)
                    results_by_name[stage_name] = stage_result
                elif sen2like_status == "failed" or pipeline_state == "sen2like_failed":
                    stage_result = _stage_result_dict(
                        "sen2like",
                        status=StageStatus.failed.value,
                        outputs=[],
                        timeline_stages=sen2like_timeline_stages,
                        metadata={
                            "runner": "sen2like_service",
                            "service_url": pipeline_metadata.get("sen2like_service_url"),
                        },
                        error=sen2like_error or _stage_error(
                            job_state,
                            errors,
                            has_outputs=False,
                        ),
                    )
                    results.append(stage_result)
                    results_by_name[stage_name] = stage_result
                else:
                    stage_result = _stage_result_dict(
                        "sen2like",
                        status=StageStatus.skipped.value,
                        outputs=[],
                        timeline_stages=[],
                        metadata={
                            "reason": "sen2like_runtime_not_routed_yet",
                            "runner": "pending_service_routing",
                        },
                    )
                    results.append(stage_result)
                    results_by_name[stage_name] = stage_result
            elif stage_name == "zarr":
                if pipeline_state == "sen2like_failed":
                    stage_result = _stage_result_dict(
                        "zarr",
                        status=StageStatus.skipped.value,
                        outputs=[],
                        timeline_stages=[],
                        metadata={
                            "runner": "zarr_service",
                            "reason": "dependency_not_succeeded",
                            "blocked_by": ["sen2like"],
                        },
                    )
                    results.append(stage_result)
                    results_by_name[stage_name] = stage_result
                    continue
                stage_result = _stage_result_dict(
                    "zarr",
                    status=_status_from_outputs(
                        zarr_outputs,
                        job_state=job_state,
                        fallback=_combined_timeline_status(timeline_stages, ["convert", "ready"]),
                    ),
                    outputs=zarr_outputs,
                    timeline_stages=_select_timeline_stages(timeline_stages, ["convert", "ready"]),
                    metadata={"runner": "zarr_service"},
                    error=_stage_error(job_state, errors, has_outputs=bool(zarr_outputs)),
                )
                results.append(stage_result)
                results_by_name[stage_name] = stage_result
            elif stage_name == "mask":
                mask_outputs = [*masked_outputs, *water_outputs, *cloud_outputs]
                mask_status = str(pipeline_metadata.get("mask_status") or "").strip().lower()
                status = (
                    StageStatus.succeeded.value
                    if mask_status == "written" or pipeline_state == "masked_zarr_written"
                    else StageStatus.failed.value
                    if job_state == JobState.failed.value
                    else _stage_status_from_timeline(
                        _combined_timeline_status(timeline_stages, ["cloud", "water"])
                    )
                )
                results.append(
                    _stage_result_dict(
                        "mask",
                        status=status,
                        outputs=mask_outputs,
                        timeline_stages=_select_timeline_stages(timeline_stages, ["cloud", "water"]),
                        metadata={
                            "runner": "mask_service",
                            "mask_types": _string_list(pipeline_metadata.get("mask_types")),
                            "mask_status": mask_status or None,
                        },
                        error=_stage_error(job_state, errors, has_outputs=status == StageStatus.succeeded.value),
                    )
                )
                results_by_name[stage_name] = results[-1]
            elif stage_name == "cube":
                status = (
                    StageStatus.succeeded.value
                    if cube_outputs or pipeline_state == "cube_written"
                    else StageStatus.failed.value
                    if pipeline_state == "cube_failed"
                    else _stage_status_from_timeline(
                        _combined_timeline_status(timeline_stages, ["cube"])
                    )
                )
                results.append(
                    _stage_result_dict(
                        "cube",
                        status=status,
                        outputs=cube_outputs,
                        timeline_stages=_select_timeline_stages(timeline_stages, ["cube"]),
                        metadata={
                            "runner": "cube_builder",
                            "cube_mode": str(pipeline_metadata.get("cube_mode") or "none"),
                        },
                        error=_stage_error(job_state, errors, has_outputs=status == StageStatus.succeeded.value),
                    )
                )
                results_by_name[stage_name] = results[-1]
        return results


def _stage_result_dict(
    name: str,
    *,
    status: str,
    outputs: Sequence[str],
    timeline_stages: list[dict[str, Any]],
    metadata: dict[str, Any],
    error: str | None = None,
) -> dict[str, Any]:
    stage_status = StageStatus(status)
    started_at = _first_value(timeline_stages, "started_at")
    finished_at = _last_value(timeline_stages, "finished_at")
    duration_seconds = sum(
        float(item.get("duration_seconds") or 0.0)
        for item in timeline_stages
        if item.get("duration_seconds") is not None
    )
    result = StageResult(
        name=name,
        status=stage_status,
        outputs=list(outputs),
        metadata={key: value for key, value in metadata.items() if value is not None},
        error=error,
        started_at=str(started_at) if started_at else None,
        finished_at=str(finished_at) if finished_at else None,
        duration_seconds=duration_seconds,
    )
    return result.to_dict()


def _select_timeline_stages(
    timeline_stages: list[dict[str, Any]],
    keys: Sequence[str],
) -> list[dict[str, Any]]:
    wanted = {str(key).strip().lower() for key in keys}
    return [
        dict(stage)
        for stage in timeline_stages
        if str(stage.get("key") or "").strip().lower() in wanted
    ]


def _combined_timeline_status(
    timeline_stages: list[dict[str, Any]],
    keys: Sequence[str],
) -> str:
    selected = _select_timeline_stages(timeline_stages, keys)
    statuses = [str(stage.get("status") or "").strip().lower() for stage in selected]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "cancelled" for status in statuses):
        return "cancelled"
    if any(status in {"running", "queued"} for status in statuses):
        return "running"
    if selected and any(status == "done" for status in statuses):
        return "done"
    return "pending"


def _stage_status_from_timeline(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "done":
        return StageStatus.succeeded.value
    if normalized in {"failed", "cancelled"}:
        return StageStatus.failed.value
    return StageStatus.skipped.value


def _stage_blocks_downstream(stage_result: dict[str, Any] | None) -> bool:
    if not stage_result:
        return False
    status = str(stage_result.get("status") or "").strip().lower()
    if status in {StageStatus.failed.value, "cancelled"}:
        return True
    if status != StageStatus.skipped.value:
        return False
    metadata = dict(stage_result.get("metadata") or {})
    return str(metadata.get("reason") or "").strip().lower() == "dependency_not_succeeded"


def _status_from_outputs(
    outputs: Sequence[str],
    *,
    job_state: str,
    fallback: str,
) -> str:
    if outputs:
        return StageStatus.succeeded.value
    if str(job_state or "").strip().lower() == JobState.failed.value:
        return StageStatus.failed.value
    return _stage_status_from_timeline(fallback)


def _stage_error(job_state: str, errors: Sequence[str], *, has_outputs: bool) -> str | None:
    if has_outputs:
        return None
    if str(job_state or "").strip().lower() != JobState.failed.value:
        return None
    return str(errors[0]) if errors else "stage_failed"


def _orchestrator_status(
    *,
    job_state: str,
    stage_results: Sequence[dict[str, Any]],
) -> str:
    normalized_job_state = str(job_state or "").strip().lower()
    if normalized_job_state == JobState.failed.value:
        return "failed"
    if normalized_job_state == JobState.cancelled.value:
        return "cancelled"
    if any(str(item.get("status") or "") == StageStatus.failed.value for item in stage_results):
        return "failed"
    if normalized_job_state == JobState.succeeded.value:
        return "succeeded"
    return normalized_job_state or "unknown"


def _first_value(items: Sequence[dict[str, Any]], key: str) -> Any:
    return next((item.get(key) for item in items if item.get(key)), None)


def _last_value(items: Sequence[dict[str, Any]], key: str) -> Any:
    return next((item.get(key) for item in reversed(items) if item.get(key)), None)


def _string_list(value: Any) -> list[str]:
    return [str(item) for item in list(value or []) if str(item).strip()]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
