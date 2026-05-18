from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from nimbuschain_fetch.domain.metadata import ConversionMetadataRecord, PipelineMetadataRecord
from nimbuschain_fetch.models import PipelineState
from nimbuschain_shared.dto import MaskExecutionRequest


def _call_mask_method(method: Any, **kwargs: Any) -> Any:
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(**kwargs)
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()):
        return method(**kwargs)
    accepted_kwargs = {
        name: value
        for name, value in kwargs.items()
        if name in signature.parameters
    }
    return method(**accepted_kwargs)


def _run_masker(
    masker: Any,
    *,
    request: MaskExecutionRequest,
    job_id: str,
    stage_callback: Callable[[str, dict[str, Any]], None],
) -> dict[str, Any]:
    if hasattr(masker, "apply_mask_request"):
        return _call_mask_method(
            masker.apply_mask_request,
            request=request,
            job_id=job_id,
            stage_callback=stage_callback,
        )
    if hasattr(masker, "apply_masks_to_zarr"):
        return _call_mask_method(
            masker.apply_masks_to_zarr,
            job_id=job_id,
            zarr_uri=request.source_zarr_uri,
            provider=request.provider,
            collection=request.collection,
            product_type=request.product_type,
            scene_id=request.scene_id,
            acquisition_datetime=request.acquisition_datetime,
            dataset_summary=request.dataset_summary,
            mask_types=request.mask_types,
            backend=request.backend,
            threshold=request.threshold,
            overwrite=request.overwrite,
            inference_device=request.inference_device,
            include_shadows=request.include_shadows,
            water_backend=request.water_backend,
            water_overwrite=request.water_overwrite,
            water_inference_device=request.water_inference_device,
            fail_on_error=request.fail_on_error,
            stage_callback=stage_callback,
        )
    if request.mask_types == ["water"] and hasattr(masker, "apply_omniwater_to_zarr"):
        water_mask = _call_mask_method(
            masker.apply_omniwater_to_zarr,
            job_id=job_id,
            zarr_uri=request.source_zarr_uri,
            provider=request.provider,
            collection=request.collection,
            product_type=request.product_type,
            scene_id=request.scene_id,
            acquisition_datetime=request.acquisition_datetime,
            dataset_summary=request.dataset_summary,
            fail_on_error=request.fail_on_error,
            stage_callback=stage_callback,
        )
        output_zarr_uri = str(
            water_mask.get("output_zarr_uri")
            or water_mask.get("input_zarr_uri")
            or request.source_zarr_uri
        ).strip()
        return {
            "status": str(water_mask.get("status") or "").strip().lower(),
            "mask_types": list(request.mask_types),
            "input_zarr_uri": request.source_zarr_uri,
            "output_zarr_uri": output_zarr_uri,
            "masked_zarr_uri": output_zarr_uri,
            "masked_zarr_outputs": [output_zarr_uri] if output_zarr_uri else [],
            "water_mask": dict(water_mask),
            "cloud_mask": {},
            "watermask_outputs": [],
            "cloudmask_outputs": [],
        }
    raise AttributeError("Mask service does not expose a supported mask execution entrypoint.")


def run_in_place_mask_pipeline(
    rt: Any,
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
    progress_plan = rt._build_mask_progress_plan(
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
        rt._update_pipeline(
            job_id,
            pipeline_state=pipeline_state,
            pipeline_step=pipeline_step,
            pipeline_progress=pipeline_progress,
            pipeline_metadata=pipeline_metadata,
            event_type=event_type,
            event_payload=event_payload,
        )

    def stage_callback(stage_name: str, payload: dict[str, Any]) -> None:
        current_row_record = rt._get_job_row_record(job_id)
        current_pipeline = dict(current_row_record.pipeline_metadata if current_row_record is not None else {})
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
        rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.resolving_source_zarr,
            pipeline_step="resolving_source_zarr",
            pipeline_progress=resolve_progress,
            pipeline_metadata=base_mask_metadata,
            event_type="job.mask_started",
            event_payload=base_mask_metadata,
        )

    masker = rt._masker()
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

    mask_request = MaskExecutionRequest(
        source_zarr_uri=selected_zarr_uri,
        provider=zarr_context["provider"],
        collection=zarr_context["collection"],
        product_type=zarr_context["product_type"],
        scene_id=zarr_context["scene_id"],
        acquisition_datetime=zarr_context["acquisition_datetime"],
        dataset_summary=dict(zarr_context["dataset_summary"] or {}),
        mask_types=list(mask_types),
        backend=backend_name,
        threshold=threshold,
        overwrite=overwrite,
        inference_device=inference_device,
        include_shadows=include_shadows,
        water_backend=water_backend_name,
        water_overwrite=water_overwrite,
        water_inference_device=water_inference_device,
        fail_on_error=fail_on_error,
    )
    mask_response = _run_masker(
        masker,
        request=mask_request,
        job_id=job_id,
        stage_callback=stage_callback,
    )

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
    quality_fields = rt._mask_quality_fields(water_mask=water_mask, cloud_mask=cloud_mask)
    quality_scalars = {
        "water_fraction": float(quality_fields.get("water_fraction") or 0.0),
        "cloud_fraction": float(quality_fields.get("cloud_fraction") or 0.0),
        "cloud_only_fraction": float(quality_fields.get("cloud_only_fraction") or 0.0),
        "shadow_fraction": float(quality_fields.get("shadow_fraction") or 0.0),
    }
    pipeline_metadata = PipelineMetadataRecord.from_mapping(
        {
            "mask_contract_version": "v2",
            **base_mask_metadata,
            "masked_zarr_uri": masked_zarr_uri if mask_job_succeeded else "",
            "status": str(mask_response.get("status") or ""),
            "mask_quality": quality_fields,
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            **quality_scalars,
        }
    )
    conversion_metadata = ConversionMetadataRecord.from_mapping(
        {
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
    )
    if mask_job_succeeded and register_masked_artifact and masked_zarr_uri:
        rt._register_masked_zarr_artifact(
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
        failed_step = rt._mask_failure_step_from_payloads(
            mask_types=mask_types,
            water_mask=water_mask,
            cloud_mask=cloud_mask,
        ) or "failed"
        pipeline_metadata.payload["failed_step"] = failed_step
        conversion_metadata.payload["failed_step"] = failed_step

    return {
        "status": final_status,
        "succeeded": mask_job_succeeded,
        "masked_zarr_uri": masked_zarr_uri,
        "masked_zarr_outputs": visible_masked_zarr_outputs,
        "watermask_outputs": [],
        "cloudmask_outputs": [],
        "water_mask": water_mask,
        "cloud_mask": cloud_mask,
        "pipeline_metadata": pipeline_metadata.to_dict(),
        "conversion_metadata": conversion_metadata.to_dict(),
        "errors": errors,
        "failed_step": failed_step,
    }
