from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
import uvicorn

from nimbuschain_shared.contracts.mask import MaskApplyRequest
from nimbuschain_mask_service.progress import get_progress, update_progress
from nimbuschain_mask_service.schema import default_mask_model
from nimbuschain_mask_service.service import MaskService, support_status


app = FastAPI(
    title="NimbusChain Mask Service",
    version="0.1.0",
    description="Internal/dev-only mask runtime harness for Zarr-derived masks.",
)


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "internal_only": True,
        **support_status(),
    }


@app.get("/schema")
def schema() -> dict[str, object]:
    return {
        "status": "ok",
        "internal_only": True,
        "mask_model": default_mask_model(),
    }


@app.get("/progress/{job_id}")
def progress(job_id: str) -> dict[str, object]:
    record = get_progress(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="No progress is available for this job.")
    return record


@app.post("/apply")
def apply(request: MaskApplyRequest, job_id: str | None = Query(default=None)) -> dict[str, object]:
    normalized_job_id = str(job_id or "").strip()

    def progress_callback(stage_name: str, payload: dict[str, object]) -> None:
        if not normalized_job_id:
            return
        update_progress(
            normalized_job_id,
            stage_name=stage_name,
            payload=dict(payload or {}),
            status="running",
        )

    if normalized_job_id:
        update_progress(
            normalized_job_id,
            stage_name="mask_request_received",
            payload={
                "source_zarr_uri": request.source_zarr_uri,
                "mask_types": list(request.mask_types),
            },
            status="running",
        )
    service = MaskService()
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
            stage_callback=progress_callback if normalized_job_id else None,
        )
    except Exception as exc:
        if normalized_job_id:
            update_progress(
                normalized_job_id,
                stage_name="mask_request_failed",
                payload={"error": str(exc)},
                status="failed",
            )
        raise
    if normalized_job_id:
        update_progress(
            normalized_job_id,
            stage_name="mask_request_finished",
            payload={
                "status": str(result.get("status") or ""),
                "masked_zarr_uri": result.get("masked_zarr_uri"),
                "output_zarr_uri": result.get("output_zarr_uri"),
                "mask_types": list(result.get("mask_types") or []),
            },
            status="finished" if str(result.get("status") or "").strip().lower() == "written" else "failed",
        )
    return result


def run() -> None:
    uvicorn.run("nimbuschain_mask_service.main:app", host="0.0.0.0", port=8020, reload=False)
