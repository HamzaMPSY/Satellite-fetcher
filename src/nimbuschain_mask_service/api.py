from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from nimbuschain_mask_service.dependencies import get_mask_service
from nimbuschain_mask_service.models import StageEventPayload
from nimbuschain_mask_service.progress import get_progress, update_progress
from nimbuschain_mask_service.schema import default_mask_model
from nimbuschain_mask_service.service import MaskService, support_status
from nimbuschain_shared.contracts.mask import MaskApplyRequest


router = APIRouter()


@router.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "internal_only": True,
        **support_status(),
    }


@router.get("/schema")
def schema() -> dict[str, object]:
    return {
        "status": "ok",
        "internal_only": True,
        "mask_model": default_mask_model(),
    }


@router.get("/progress/{job_id}")
def progress(job_id: str) -> dict[str, object]:
    record = get_progress(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="No progress is available for this job.")
    return record


@router.post("/apply")
def apply(
    request: MaskApplyRequest,
    job_id: str | None = Query(default=None),
    service: MaskService = Depends(get_mask_service),
) -> dict[str, object]:
    normalized_job_id = str(job_id or "").strip()
    progress_callback = _build_progress_callback(normalized_job_id)

    if normalized_job_id:
        update_progress(
            normalized_job_id,
            stage_name="mask_request_received",
            payload=StageEventPayload.from_mapping({
                "source_zarr_uri": request.source_zarr_uri,
                "mask_types": list(request.mask_types),
            }),
            status="running",
        )
    try:
        result = service.apply_masks_to_zarr(
            job_id=normalized_job_id or None,
            zarr_uri=request.source_zarr_uri,
            provider=request.provider,
            collection=request.collection,
            product_type=request.product_type,
            scene_id=request.scene_id,
            acquisition_datetime=request.acquisition_datetime,
            dataset_summary=request.dataset_summary,
            mask_types=request.mask_types,
            output_zarr_uri=request.output_zarr_uri,
            fail_on_error=request.fail_on_error,
            backend=request.cloud.backend,
            threshold=request.cloud.threshold,
            overwrite=request.cloud.overwrite,
            inference_device=request.cloud.inference_device,
            include_shadows=request.cloud.include_shadows,
            water_backend=request.water.backend,
            water_overwrite=request.water.overwrite,
            water_inference_device=request.water.inference_device,
            stage_callback=progress_callback,
        )
    except Exception as exc:
        if normalized_job_id:
            update_progress(
                normalized_job_id,
                stage_name="mask_request_failed",
                payload=StageEventPayload(error=str(exc)),
                status="failed",
            )
        raise
    if normalized_job_id:
        status_value = str(result.get("status") or "").strip().lower()
        update_progress(
            normalized_job_id,
            stage_name="mask_request_finished",
            payload=StageEventPayload.from_mapping({
                "status": status_value,
                "masked_zarr_uri": result.get("masked_zarr_uri"),
                "output_zarr_uri": result.get("output_zarr_uri"),
                "mask_types": list(result.get("mask_types") or []),
            }),
            status="finished" if status_value == "written" else "failed",
        )
    return result


def _build_progress_callback(job_id: str):
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return None

    def progress_callback(stage_name: str, payload: StageEventPayload) -> None:
        update_progress(
            normalized_job_id,
            stage_name=stage_name,
            payload=payload,
            status="running",
        )

    return progress_callback


__all__ = ["router"]
