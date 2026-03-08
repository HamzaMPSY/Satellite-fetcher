from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from nimbuschain_zarr_service.schema import default_zarr_model

# New architecture imports
from nimbuschain_zarr_service.converter import (
    ConfigLoader,
    CubeBuilder,
    LandsatReader,
    Sentinel2Reader,
    ZarrWriter,
)
from nimbuschain_zarr_service.converter.config import CollectionConfig


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
    # Map incoming collection/provider to config key
    config_path = Path(__file__).resolve().parent / "converter" / "config" / "bands.yml"
    loader = ConfigLoader(config_path)
    collections = loader.load()

    def _select_collection_key(provider: str, collection: str) -> str:
        norm = collection.strip().lower()
        if provider == "copernicus":
            if norm.startswith("sentinel-2") or norm.startswith("s2"):
                return "sentinel2_l2a"
        if provider == "usgs":
            if norm.endswith("_l2") or "_c2_l2" in norm:
                return "landsat_l2"
        return ""

    cfg_key = _select_collection_key(payload.provider, payload.collection)
    collection_cfg: CollectionConfig | None = collections.get(cfg_key)
    if collection_cfg is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unsupported provider/collection for this converter. "
                "Ensure bands.yml has a matching config and provider is copernicus/usgs."
            ),
        )

    reader = (
        Sentinel2Reader(collection_cfg, prefetch=True)
        if payload.provider == "copernicus"
        else LandsatReader(collection_cfg, prefetch=True)
    )

    try:
        dataset = reader.read(payload.raw_uri)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Normalization failed: {exc}") from exc

    # Optionally stack/appends could be added; here we write a single dataset
    writer = ZarrWriter()
    try:
        writer.write(dataset, payload.output_uri, mode="w")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Zarr write failed: {exc}") from exc

    # Build response summary
    bands = dataset["bands"]
    summary = {
        "provider": payload.provider,
        "collection": payload.collection,
        "scene_id": payload.scene_id,
        "product_id": dataset.attrs.get("product_id", payload.scene_id),
        "data_family": dataset.attrs.get("data_family", "unknown"),
        "original_path": dataset.attrs.get("original_path", payload.raw_uri),
        "band_order": [str(b) for b in bands.coords["band"].values.tolist()],
        "grid": {
            "height": int(bands.sizes.get("y", 0)),
            "width": int(bands.sizes.get("x", 0)),
            "crs": str(bands.rio.crs) if hasattr(bands, "rio") else None,
        },
    }

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
        zarr_uri=payload.output_uri,
        data_family=str(summary.get("data_family", "unknown")),
        band_names=[str(b) for b in bands.coords["band"].values.tolist()],
        dimensions=list(bands.dims),
        normalization_summary=summary,
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
