from __future__ import annotations

from fastapi import FastAPI
import uvicorn

from nimbuschain_mask_service.contracts import MaskApplyRequest
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


@app.post("/apply")
def apply(request: MaskApplyRequest) -> dict[str, object]:
    service = MaskService()
    return service.apply_masks_to_zarr(
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
    )


def run() -> None:
    uvicorn.run("nimbuschain_mask_service.main:app", host="0.0.0.0", port=8020, reload=False)
