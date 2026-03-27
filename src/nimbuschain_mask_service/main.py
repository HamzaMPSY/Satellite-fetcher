from __future__ import annotations

import importlib.util

from fastapi import FastAPI
import uvicorn

from nimbuschain_mask_service.schema import default_mask_model


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
        "omniwatermask_available": importlib.util.find_spec("omniwatermask") is not None,
    }


@app.get("/schema")
def schema() -> dict[str, object]:
    return {
        "status": "ok",
        "internal_only": True,
        "mask_model": default_mask_model(),
    }


def run() -> None:
    uvicorn.run("nimbuschain_mask_service.main:app", host="0.0.0.0", port=8020, reload=False)
