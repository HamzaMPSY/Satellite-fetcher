from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field

from nimbuschain_zarr_service.schema import default_zarr_model


APP_VERSION = "0.1.0"
DEFAULT_PORT = 8010


class ConvertRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    pipeline_id: str = Field(..., min_length=1)
    trace_id: str = Field(..., min_length=1)
    provider: Literal["copernicus", "usgs"]
    collection: str = Field(..., min_length=1)
    scene_id: str = Field(..., min_length=1)
    raw_uri: str = Field(..., min_length=1)
    raw_format: str = Field(..., min_length=1)
    output_uri: str = Field(..., min_length=1)


class ConvertResponse(BaseModel):
    job_id: str
    pipeline_id: str
    status: Literal["accepted"]
    stage: Literal["zarr_converting"]
    service: Literal["zarr-converter-service"]
    message: str
    accepted_at: str


app = FastAPI(
    title="Nimbus Zarr Converter Service",
    version=APP_VERSION,
    description=(
        "Skeleton microservice for the future raw-scene to Zarr conversion stage. "
        "This version validates the contract and accepts conversion requests, "
        "but does not perform the actual conversion yet."
    ),
)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "zarr-converter-service",
        "status": "ok",
        "version": APP_VERSION,
        "docs": "/docs",
    }


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "service": "zarr-converter-service",
        "status": "ok",
        "version": APP_VERSION,
        "conversion_ready": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/schema")
def schema() -> dict[str, object]:
    return {
        "service": "zarr-converter-service",
        "status": "ok",
        "zarr_model": default_zarr_model(),
    }


@app.post("/convert", response_model=ConvertResponse)
def convert(payload: ConvertRequest) -> ConvertResponse:
    return ConvertResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status="accepted",
        stage="zarr_converting",
        service="zarr-converter-service",
        message=(
            "Request accepted by skeleton service. Conversion logic is not implemented yet."
        ),
        accepted_at=datetime.now(timezone.utc).isoformat(),
    )


def run() -> None:
    import uvicorn

    uvicorn.run(
        "nimbuschain_zarr_service.main:app",
        host="0.0.0.0",
        port=DEFAULT_PORT,
        reload=False,
    )


if __name__ == "__main__":
    run()
