from __future__ import annotations

import importlib.util
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi.responses import JSONResponse

from nimbuschain_zarr_service.config_loader import (
    _config_candidates,
    load_converter_config_record,
    supported_collections,
    supported_product_types,
)
from nimbuschain_zarr_service.constants import APP_VERSION, REQUIRED_MODULES, SERVICE_NAME
from nimbuschain_zarr_service.dependencies import get_conversion_service
from nimbuschain_zarr_service.oci_storage import oci_support_status
from nimbuschain_zarr_service.schema import default_zarr_model
from nimbuschain_zarr_service.sentinel1_raw import raw_support_status
from nimbuschain_zarr_service.snap_runtime import snap_support_status


def health_response() -> JSONResponse:
    checks = {
        "config": check_config(),
        "dependencies": check_dependencies(),
        "storage": check_storage(),
        "remote_storage": check_remote_storage(),
        "sentinel1": check_sentinel1_support(),
        "service": check_service(),
    }
    return _status_response(checks=checks, ready_label="ok", failure_label="degraded")


def readiness_response() -> JSONResponse:
    checks = {
        "config": check_config(),
        "dependencies": check_dependencies(),
        "storage": check_storage(),
        "remote_storage": check_remote_storage(),
        "sentinel1": check_sentinel1_support(),
        "service": check_service(),
        "smoke_zarr_write": check_smoke_zarr_write(),
    }
    return _status_response(checks=checks, ready_label="ready", failure_label="not_ready")


def schema_payload() -> dict[str, object]:
    return {
        "service": SERVICE_NAME,
        "status": "ok",
        "zarr_model": default_zarr_model(),
        "converter_config": load_converter_config_record().to_dict(),
        "runtime_capabilities": {
            "sentinel1": check_sentinel1_support(),
        },
    }


def check_config() -> dict[str, object]:
    try:
        config = load_converter_config_record()
        payload = config.to_dict()
        return {
            "ok": bool(payload),
            "critical": True,
            "path_candidates": [str(path) for path in _config_candidates()],
            "copernicus_collections": list(payload.get("copernicus", {}).keys()),
            "usgs_collections": list(payload.get("usgs", {}).keys()),
        }
    except Exception as exc:
        return {
            "ok": False,
            "critical": True,
            "error": str(exc),
            "path_candidates": [str(path) for path in _config_candidates()],
        }


def check_dependencies() -> dict[str, object]:
    missing: list[str] = []
    for module_name in REQUIRED_MODULES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return {
        "ok": not missing,
        "critical": True,
        "required_modules": list(REQUIRED_MODULES),
        "missing_modules": missing,
    }


def check_remote_storage() -> dict[str, object]:
    status = oci_support_status()
    return {
        "ok": True,
        "critical": False,
        "oci_available": bool(status.get("available")),
        "config_path": status.get("config_path"),
        "profile": status.get("profile"),
        "namespace": status.get("namespace"),
    }


def check_sentinel1_support() -> dict[str, object]:
    raw_status = raw_support_status()
    snap_status = snap_support_status()
    return {
        "ok": True,
        "critical": False,
        "raw_decoder_available": bool(raw_status.get("available")),
        "raw_decoder": raw_status,
        "snap": snap_status,
    }


def check_storage() -> dict[str, object]:
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


def check_service() -> dict[str, object]:
    service = get_conversion_service()
    return {
        "ok": hasattr(service, "convert"),
        "critical": True,
        "service_class": service.__class__.__name__,
    }


def check_smoke_zarr_write() -> dict[str, object]:
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


def _status_response(*, checks: dict[str, object], ready_label: str, failure_label: str) -> JSONResponse:
    critical_failures = [
        name
        for name, check in checks.items()
        if not bool(check.get("ok")) and bool(check.get("critical", False))
    ]
    healthy = not critical_failures
    body = {
        "service": SERVICE_NAME,
        "status": ready_label if healthy else failure_label,
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
