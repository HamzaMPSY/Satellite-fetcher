from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from nimbuschain_zarr_service.schema import default_zarr_model


APP_VERSION = "0.1.0"
DEFAULT_PORT = 8010
COPERNICUS_ALLOWED_PREFIXES = ("SENTINEL-1", "SENTINEL-2")
USGS_ALLOWED_COLLECTIONS = {"landsat_ot_c2_l1", "landsat_ot_c2_l2"}


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
    status: Literal["accepted", "normalized", "written"]
    stage: Literal["zarr_converting"]
    service: Literal["zarr-converter-service"]
    message: str
    accepted_at: str
    zarr_uri: str | None = None
    data_family: str | None = None
    band_names: list[str] | None = None
    dimensions: list[str] | None = None
    normalization_summary: dict[str, object] | None = None


app = FastAPI(
    title="Nimbus Zarr Converter Service",
    version=APP_VERSION,
    description=(
        "Active microservice that reads supported raw products, normalizes them "
        "into time/band/y/x datasets, and writes local Zarr stores."
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
        "conversion_ready": True,
        "supported_families": ["optical", "sar"],
        "supported_collections": {
            "copernicus": list(COPERNICUS_ALLOWED_PREFIXES),
            "usgs": sorted(USGS_ALLOWED_COLLECTIONS),
        },
        "supported_product_types": {
            "SENTINEL-1": ["RAW", "GRD", "SLC", "IW_SLC__1S"],
            "SENTINEL-2": ["S2MSI1C", "S2MSI2A"],
            "landsat_ot_c2_l1": ["L1TP", "L1GT", "L1GS"],
            "landsat_ot_c2_l2": ["L2SP", "L2SR"],
        },
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
    from nimbuschain_zarr_service.core import (
        ConversionDependencyError,
        ConversionError,
        summarize_dataset,
        write_dataset_to_zarr,
    )
    from nimbuschain_zarr_service.copernicus import build_copernicus_dataset
    from nimbuschain_zarr_service.landsat import (
        LandsatDependencyError,
        LandsatNormalizationError,
        build_landsat_dataset,
    )

    try:
        if payload.provider == "usgs":
            normalized_collection = payload.collection.strip().lower()
            if normalized_collection not in USGS_ALLOWED_COLLECTIONS:
                raise ConversionError(
                    "Unsupported USGS collection for this project. "
                    f"Expected one of: {', '.join(sorted(USGS_ALLOWED_COLLECTIONS))}."
                )
            dataset, summary = build_landsat_dataset(
                raw_uri=payload.raw_uri,
                provider=payload.provider,
                collection=normalized_collection,
                scene_id=payload.scene_id,
            )
        elif payload.provider == "copernicus":
            normalized_collection = payload.collection.strip().upper()
            if not any(normalized_collection.startswith(prefix) for prefix in COPERNICUS_ALLOWED_PREFIXES):
                raise ConversionError(
                    "Unsupported Copernicus collection for this project. "
                    "Only SENTINEL-1 and SENTINEL-2 are supported."
                )
            dataset, summary = build_copernicus_dataset(
                raw_uri=payload.raw_uri,
                provider=payload.provider,
                collection=normalized_collection,
                scene_id=payload.scene_id,
            )
        else:
            raise ConversionError(
                "Unsupported conversion request. Expected a USGS Landsat or Copernicus product."
            )
        written_uri = write_dataset_to_zarr(dataset, payload.output_uri)
    except (LandsatNormalizationError, ConversionError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (LandsatDependencyError, ConversionDependencyError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    dataset_summary = summarize_dataset(
        dataset,
        data_family=str(summary.get("data_family", "unknown")),
        zarr_uri=written_uri,
    )

    return ConvertResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status="written",
        stage="zarr_converting",
        service="zarr-converter-service",
        message=(
            "Raw product converted into an x/y band time Zarr dataset and written successfully."
        ),
        accepted_at=datetime.now(timezone.utc).isoformat(),
        zarr_uri=written_uri,
        data_family=str(summary.get("data_family", "unknown")),
        band_names=list(dataset_summary["band_names"]),
        dimensions=list(dataset_summary["dimensions"]),
        normalization_summary={**summary, "zarr_summary": dataset_summary},
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
