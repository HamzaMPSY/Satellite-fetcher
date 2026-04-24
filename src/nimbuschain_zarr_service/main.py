from __future__ import annotations

import importlib.util
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from nimbuschain_shared.contracts.zarr import (
    BuildCubeRequest,
    BuildCubeResponse,
    BuildGroupedCubesRequest,
    BuildGroupedCubesResponse,
    ConvertRequest,
    ConvertResponse,
    InspectDatasetRequest,
    InspectDatasetResponse,
)
from nimbuschain_zarr_service.config_loader import (
    _config_candidates,
    load_converter_config,
    supported_collections,
    supported_product_types,
)
from nimbuschain_zarr_service.logging_config import configure_logging
from nimbuschain_zarr_service.middleware import RequestTelemetryMiddleware
from nimbuschain_zarr_service.oci_storage import oci_support_status
from nimbuschain_zarr_service.sentinel1_raw import raw_support_status
from nimbuschain_zarr_service.snap_runtime import snap_support_status
from nimbuschain_zarr_service.cube import build_grouped_time_cubes, build_time_cube
from nimbuschain_zarr_service.schema import default_zarr_model
from nimbuschain_zarr_service.service import ZarrConversionService


APP_VERSION = "0.1.0"
DEFAULT_PORT = 8010
_CONVERSION_SERVICE = ZarrConversionService()
_REQUIRED_MODULES = ("numpy", "zarr", "xarray", "rasterio", "numcodecs")

configure_logging(
    level=str(os.getenv("NIMBUS_LOG_LEVEL") or "INFO"),
    json_logs=str(os.getenv("NIMBUS_LOG_JSON") or "").strip().lower() in {"1", "true", "yes", "on"},
)
logger = logging.getLogger("nimbus.zarr")

app = FastAPI(
    title="Nimbus Zarr Converter Service",
    version=APP_VERSION,
    description=(
        "Active microservice that reads supported raw products, normalizes them "
        "into time/band/y/x datasets, and writes local Zarr stores."
    ),
)
app.add_middleware(RequestTelemetryMiddleware)


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
        "remote_storage": _check_remote_storage(),
        "sentinel1": _check_sentinel1_support(),
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
        "remote_storage": _check_remote_storage(),
        "sentinel1": _check_sentinel1_support(),
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
        "runtime_capabilities": {
            "sentinel1": _check_sentinel1_support(),
        },
    }


@app.post("/convert", response_model=ConvertResponse)
def convert(payload: ConvertRequest, request: Request) -> ConvertResponse:
    from nimbuschain_zarr_service.core import (
        ConversionDependencyError,
        ConversionError,
    )
    from nimbuschain_zarr_service.landsat import (
        LandsatDependencyError,
        LandsatNormalizationError,
    )

    request_id = getattr(request.state, "request_id", None)

    logger.info(
        "conversion_requested job_id=%s pipeline_id=%s provider=%s collection=%s scene_id=%s output_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        payload.provider,
        payload.collection,
        payload.scene_id,
        payload.output_uri,
        extra={"request_id": request_id},
    )

    progress_state = {
        "fraction_bucket": -1,
        "stage": "",
        "array_name": "",
        "band_name": "",
    }

    def _progress_logger(progress: dict[str, Any]) -> None:
        stage = str(progress.get("stage") or "").strip() or "unknown"
        array_name = str(progress.get("array_name") or "").strip()
        band_name = str(progress.get("band_name") or "").strip()
        fraction = float(progress.get("fraction") or 0.0)
        bucket = int(min(100, max(0, fraction * 100)) // 5)

        should_log = (
            bucket > progress_state["fraction_bucket"]
            or stage != progress_state["stage"]
            or array_name != progress_state["array_name"]
            or band_name != progress_state["band_name"]
            or fraction >= 1.0
        )
        if not should_log:
            return

        progress_state["fraction_bucket"] = bucket
        progress_state["stage"] = stage
        progress_state["array_name"] = array_name
        progress_state["band_name"] = band_name

        logger.info(
            "conversion_progress job_id=%s scene_id=%s stage=%s array_name=%s band_name=%s fraction=%.4f blocks_written=%s total_blocks=%s",
            payload.job_id,
            payload.scene_id,
            stage,
            array_name or "-",
            band_name or "-",
            fraction,
            progress.get("blocks_written"),
            progress.get("total_blocks"),
            extra={"request_id": request_id},
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
            progress_callback=_progress_logger,
        )
    except (LandsatNormalizationError, ConversionError) as exc:
        logger.warning(
            "conversion_failed job_id=%s scene_id=%s provider=%s reason=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (LandsatDependencyError, ConversionDependencyError) as exc:
        logger.exception(
            "conversion_runtime_error job_id=%s scene_id=%s provider=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "conversion_unhandled_error job_id=%s scene_id=%s provider=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "conversion_completed job_id=%s scene_id=%s provider=%s data_family=%s zarr_uri=%s band_count=%s ancillary_count=%s",
        payload.job_id,
        payload.scene_id,
        payload.provider,
        data_family,
        written_uri,
        len(dataset_summary["band_names"]),
        len(dataset_summary.get("ancillary_layer_names") or []),
        extra={"request_id": request_id},
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
        data_family=data_family,
        band_names=list(dataset_summary["band_names"]),
        dimensions=list(dataset_summary["dimensions"]),
        ancillary_layer_names=list(dataset_summary.get("ancillary_layer_names") or []),
        ancillary_dimensions=list(dataset_summary.get("ancillary_dimensions") or []),
        normalization_summary={**summary, "zarr_summary": dataset_summary},
    )


@app.post("/cubes/grouped/build", response_model=BuildGroupedCubesResponse)
def build_grouped_cubes(payload: BuildGroupedCubesRequest, request: Request) -> BuildGroupedCubesResponse:
    from nimbuschain_zarr_service.core import (
        ConversionDependencyError,
        ConversionError,
    )

    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "grouped_cube_build_requested job_id=%s pipeline_id=%s source_count=%s output_dir=%s stage_label=%s",
        payload.job_id,
        payload.pipeline_id,
        len(payload.source_zarr_uris),
        payload.output_dir,
        payload.stage_label,
        extra={"request_id": request_id},
    )
    try:
        cube_summary = build_grouped_time_cubes(
            list(payload.source_zarr_uris),
            payload.output_dir,
            include_ancillary=bool(payload.include_ancillary),
            include_masks=payload.include_masks,
            start_date=payload.start_date,
            end_date=payload.end_date,
            stage_label=payload.stage_label,
        )
    except ConversionError as exc:
        logger.warning(
            "grouped_cube_build_rejected job_id=%s pipeline_id=%s reason=%s",
            payload.job_id,
            payload.pipeline_id,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConversionDependencyError as exc:
        logger.exception(
            "grouped_cube_build_runtime_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "grouped_cube_build_unhandled_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "grouped_cube_build_completed job_id=%s pipeline_id=%s status=%s outputs=%s",
        payload.job_id,
        payload.pipeline_id,
        cube_summary.get("status"),
        len(list(cube_summary.get("cube_outputs") or [])),
        extra={"request_id": request_id},
    )
    return BuildGroupedCubesResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status=str(cube_summary.get("status") or "skipped"),
        service="zarr-converter-service",
        cube_summary=cube_summary,
    )


@app.post("/cubes/build", response_model=BuildCubeResponse)
def build_cube(payload: BuildCubeRequest, request: Request) -> BuildCubeResponse:
    from nimbuschain_zarr_service.core import (
        ConversionDependencyError,
        ConversionError,
    )

    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "cube_build_requested job_id=%s pipeline_id=%s source_count=%s output_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        len(payload.source_zarr_uris),
        payload.output_uri,
        extra={"request_id": request_id},
    )
    try:
        cube_summary = build_time_cube(
            list(payload.source_zarr_uris),
            payload.output_uri,
            include_ancillary=bool(payload.include_ancillary),
            include_masks=bool(payload.include_masks),
        )
    except ConversionError as exc:
        logger.warning(
            "cube_build_rejected job_id=%s pipeline_id=%s reason=%s",
            payload.job_id,
            payload.pipeline_id,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConversionDependencyError as exc:
        logger.exception(
            "cube_build_runtime_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "cube_build_unhandled_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "cube_build_completed job_id=%s pipeline_id=%s zarr_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        cube_summary.get("zarr_uri"),
        extra={"request_id": request_id},
    )
    return BuildCubeResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status="written",
        service="zarr-converter-service",
        cube_summary=cube_summary,
    )


@app.post("/inspect-dataset", response_model=InspectDatasetResponse)
def inspect_dataset(payload: InspectDatasetRequest, request: Request) -> InspectDatasetResponse:
    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "dataset_inspection_requested zarr_uri=%s",
        payload.zarr_uri,
        extra={"request_id": request_id},
    )
    try:
        dataset_summary = _inspect_dataset_summary(payload.zarr_uri)
    except ValueError as exc:
        logger.warning(
            "dataset_inspection_rejected zarr_uri=%s reason=%s",
            payload.zarr_uri,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "dataset_inspection_unhandled_error zarr_uri=%s",
            payload.zarr_uri,
            extra={"request_id": request_id},
        )
        raise

    return InspectDatasetResponse(
        service="zarr-converter-service",
        zarr_uri=payload.zarr_uri,
        dataset_summary=dataset_summary,
    )


def run() -> None:
    import uvicorn

    logger.info(
        "zarr_service_starting host=%s port=%s log_level=%s json_logs=%s",
        "0.0.0.0",
        DEFAULT_PORT,
        str(os.getenv("NIMBUS_LOG_LEVEL") or "INFO"),
        str(os.getenv("NIMBUS_LOG_JSON") or "").strip().lower() in {"1", "true", "yes", "on"},
    )
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


def _check_remote_storage() -> dict[str, object]:
    status = oci_support_status()
    return {
        "ok": True,
        "critical": False,
        "oci_available": bool(status.get("available")),
        "config_path": status.get("config_path"),
        "profile": status.get("profile"),
        "namespace": status.get("namespace"),
    }


def _check_sentinel1_support() -> dict[str, object]:
    raw_status = raw_support_status()
    snap_status = snap_support_status()
    return {
        "ok": True,
        "critical": False,
        "raw_decoder_available": bool(raw_status.get("available")),
        "raw_decoder": raw_status,
        "snap": snap_status,
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


def _inspect_dataset_summary(zarr_uri: str) -> dict[str, Any]:
    try:
        import zarr
    except Exception as exc:
        raise ValueError(f"Unable to inspect existing Zarr output because zarr is unavailable ({exc}).") from exc

    from nimbuschain_zarr_service.core import _open_existing_output_store

    root = zarr.open_group(_open_existing_output_store(zarr_uri), mode="r")
    imagery = root.get("imagery")
    if imagery is None:
        raise ValueError("The selected Zarr output does not contain an imagery array.")
    band_names = list(root.attrs.get("band_names") or [])
    if not band_names and "band" in root:
        band_names = [str(item) for item in root["band"][:].tolist()]
    acquisition_datetime = None
    if "time" in root and len(root["time"]) > 0:
        raw_time = root["time"][0]
        acquisition_datetime = str(raw_time.item() if hasattr(raw_time, "item") else raw_time)
    attrs = dict(root.attrs)
    transform = list(attrs.get("transform") or [])
    if len(transform) < 6 and "x" in root and "y" in root:
        derived_transform = _derive_transform_from_xy(
            x_values=root["x"][:].tolist(),
            y_values=root["y"][:].tolist(),
        )
        if derived_transform:
            transform = derived_transform
    return {
        "dimensions": ["time", "band", "y", "x"],
        "shape": list(imagery.shape),
        "band_names": [str(item) for item in band_names],
        "ancillary_layer_names": list(root.attrs.get("ancillary_layer_names") or []),
        "acquisition_datetime": acquisition_datetime,
        "crs": attrs.get("crs"),
        "transform": transform,
        "dtype": str(attrs.get("dtype") or imagery.dtype),
        "pixel_size": list(attrs.get("reference_pixel_size") or []),
        "reference_pixel_size": list(attrs.get("reference_pixel_size") or []),
        "band_metadata": dict(attrs.get("band_metadata") or {}),
        "ancillary_metadata": dict(attrs.get("ancillary_metadata") or {}),
    }


def _derive_transform_from_xy(*, x_values: list[Any], y_values: list[Any]) -> list[float]:
    if len(x_values) < 2 or len(y_values) < 2:
        return []
    try:
        x0 = float(x_values[0])
        x1 = float(x_values[1])
        y0 = float(y_values[0])
        y1 = float(y_values[1])
    except (TypeError, ValueError):
        return []
    x_res = x1 - x0
    y_res = y1 - y0
    if x_res == 0.0 or y_res == 0.0:
        return []
    return [x_res, 0.0, x0 - (x_res / 2.0), 0.0, y_res, y0 - (y_res / 2.0)]
