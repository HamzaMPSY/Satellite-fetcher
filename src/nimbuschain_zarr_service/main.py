from __future__ import annotations

import importlib.util
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from nimbuschain_zarr_service.config_loader import (
    _config_candidates,
    load_converter_config,
    supported_collections,
    supported_product_types,
)
from nimbuschain_zarr_service.schema import default_zarr_model
from nimbuschain_zarr_service.service import ZarrConversionService


APP_VERSION = "0.1.0"
DEFAULT_PORT = 8010
_CONVERSION_SERVICE = ZarrConversionService()
_REQUIRED_MODULES = ("numpy", "zarr", "xarray", "rasterio", "numcodecs")


class ConvertRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    pipeline_id: str = Field(..., min_length=1)
    trace_id: str = Field(..., min_length=1)
    provider: Literal["copernicus", "usgs"]
    collection: str = Field(..., min_length=1)
    product_type: str | None = None
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
def health() -> JSONResponse:
    checks = {
        "config": _check_config(),
        "dependencies": _check_dependencies(),
        "storage": _check_storage(),
        "service": _check_service(),
    }
    critical_failures = [
        name
        for name, check in checks.items()
        if not bool(check.get("ok")) and bool(check.get("critical", False))
    ]
    healthy = not critical_failures
    body = {
        "service": "zarr-converter-service",
        "status": "ok" if healthy else "degraded",
        "version": APP_VERSION,
        "conversion_ready": healthy,
        "supported_families": ["optical", "sar"],
        "supported_collections": supported_collections(),
        "supported_product_types": supported_product_types(),
        "checks": checks,
        "critical_failures": critical_failures,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(status_code=200 if healthy else 503, content=body)


@app.get("/readiness")
def readiness() -> JSONResponse:
    checks = {
        "config": _check_config(),
        "dependencies": _check_dependencies(),
        "storage": _check_storage(),
        "service": _check_service(),
        "smoke_zarr_write": _check_smoke_zarr_write(),
    }
    critical_failures = [
        name
        for name, check in checks.items()
        if not bool(check.get("ok")) and bool(check.get("critical", False))
    ]
    ready = not critical_failures
    body = {
        "service": "zarr-converter-service",
        "status": "ready" if ready else "not_ready",
        "version": APP_VERSION,
        "conversion_ready": ready,
        "supported_families": ["optical", "sar"],
        "supported_collections": supported_collections(),
        "supported_product_types": supported_product_types(),
        "checks": checks,
        "critical_failures": critical_failures,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(status_code=200 if ready else 503, content=body)


@app.get("/schema")
def schema() -> dict[str, object]:
    return {
        "service": "zarr-converter-service",
        "status": "ok",
        "zarr_model": default_zarr_model(),
        "converter_config": load_converter_config(),
    }


@app.post("/convert", response_model=ConvertResponse)
def convert(payload: ConvertRequest) -> ConvertResponse:
    from nimbuschain_zarr_service.core import (
        ConversionDependencyError,
        ConversionError,
    )
    from nimbuschain_zarr_service.landsat import (
        LandsatDependencyError,
        LandsatNormalizationError,
    )

    try:
        normalized_collection = payload.collection.strip().lower() if payload.provider == "usgs" else payload.collection.strip().upper()
        normalized_product_type = payload.product_type.strip().upper() if payload.product_type else None
        written_uri, data_family, summary, dataset_summary = _CONVERSION_SERVICE.convert(
            provider=payload.provider,
            collection=normalized_collection,
            scene_id=payload.scene_id,
            raw_uri=payload.raw_uri,
            output_uri=payload.output_uri,
            product_type=normalized_product_type,
        )
    except (LandsatNormalizationError, ConversionError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (LandsatDependencyError, ConversionDependencyError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

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
        data_family=data_family,
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


def _check_config() -> dict[str, object]:
    try:
        config = load_converter_config()
        return {
            "ok": bool(config),
            "critical": True,
            "path_candidates": [str(path) for path in _config_candidates()],
            "copernicus_collections": list((config.get("copernicus") or {}).keys()),
            "usgs_collections": list((config.get("usgs") or {}).keys()),
        }
    except Exception as exc:
        return {
            "ok": False,
            "critical": True,
            "error": str(exc),
            "path_candidates": [str(path) for path in _config_candidates()],
        }


def _check_dependencies() -> dict[str, object]:
    missing: list[str] = []
    for module_name in _REQUIRED_MODULES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return {
        "ok": not missing,
        "critical": True,
        "required_modules": list(_REQUIRED_MODULES),
        "missing_modules": missing,
    }


def _check_storage() -> dict[str, object]:
    downloads_root = Path(os.getenv("NIMBUS_DATA_DIR", "/data/downloads"))
    zarr_root = downloads_root / "zarr"
    try:
        zarr_root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="health_", suffix=".tmp", dir=zarr_root, delete=True) as handle:
            handle.write(b"ok")
            handle.flush()
        return {
            "ok": True,
            "critical": True,
            "downloads_root": str(downloads_root),
            "zarr_root": str(zarr_root),
            "zarr_root_exists": zarr_root.exists(),
            "zarr_root_writable": True,
        }
    except Exception as exc:
        return {
            "ok": False,
            "critical": True,
            "downloads_root": str(downloads_root),
            "zarr_root": str(zarr_root),
            "error": str(exc),
        }


def _check_service() -> dict[str, object]:
    return {
        "ok": hasattr(_CONVERSION_SERVICE, "convert"),
        "critical": True,
        "service_class": _CONVERSION_SERVICE.__class__.__name__,
    }


def _check_smoke_zarr_write() -> dict[str, object]:
    try:
        import numpy as np
        import zarr
    except Exception as exc:
        return {
            "ok": False,
            "critical": True,
            "error": str(exc),
        }

    zarr_root = Path(os.getenv("NIMBUS_DATA_DIR", "/data/downloads")) / "zarr"
    probe_dir = None
    try:
        zarr_root.mkdir(parents=True, exist_ok=True)
        probe_dir = Path(tempfile.mkdtemp(prefix="health_probe_", dir=zarr_root))
        store_path = probe_dir / "probe.zarr"
        group = zarr.open_group(store_path, mode="w", zarr_format=2)
        group.attrs.update({"probe": True})
        arr = group.create_array("imagery", shape=(1, 1, 2, 2), chunks=(1, 1, 2, 2), dtype="u2")
        arr[0, 0, :, :] = np.array([[1, 2], [3, 4]], dtype="u2")
        zarr.consolidate_metadata(store_path)
        return {
            "ok": True,
            "critical": True,
            "probe_store": str(store_path),
        }
    except Exception as exc:
        return {
            "ok": False,
            "critical": True,
            "error": str(exc),
            "probe_root": str(zarr_root),
        }
    finally:
        if probe_dir is not None:
            try:
                import shutil

                shutil.rmtree(probe_dir)
            except Exception:
                pass
