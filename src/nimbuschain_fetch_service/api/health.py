from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.jobs.mongodb_store import MongoJobStore
from nimbuschain_fetch.jobs.store import JobListFilters
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.dependencies import get_fetcher, get_runtime_settings

router = APIRouter(prefix="/v1", tags=["health"])


@router.get("/health")
def healthcheck(
    settings: Settings = Depends(get_runtime_settings),
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JSONResponse:
    checks = _build_checks(settings, fetcher, include_deep_store_check=False, include_executor_check=False)
    critical_failures = [name for name, item in checks.items() if item.get("critical") and not item.get("ok")]
    healthy = not critical_failures
    return JSONResponse(
        status_code=200 if healthy else 503,
        content=_health_payload(
            settings=settings,
            fetcher=fetcher,
            checks=checks,
            status="ok" if healthy else "degraded",
            ready=healthy,
            critical_failures=critical_failures,
            endpoint="health",
        ),
    )


@router.get("/readiness")
def readinesscheck(
    settings: Settings = Depends(get_runtime_settings),
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JSONResponse:
    checks = _build_checks(settings, fetcher, include_deep_store_check=True, include_executor_check=True)
    critical_failures = [name for name, item in checks.items() if item.get("critical") and not item.get("ok")]
    ready = not critical_failures
    return JSONResponse(
        status_code=200 if ready else 503,
        content=_health_payload(
            settings=settings,
            fetcher=fetcher,
            checks=checks,
            status="ready" if ready else "not_ready",
            ready=ready,
            critical_failures=critical_failures,
            endpoint="readiness",
        ),
    )


@router.get("/worker/status")
def worker_status(
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JSONResponse:
    payload = fetcher.get_worker_status()
    return JSONResponse(status_code=200, content=payload)


@router.get("/worker/download-coordinator")
def download_coordinator_status(
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> JSONResponse:
    payload = fetcher.get_download_coordinator_status()
    return JSONResponse(status_code=200, content=payload)


def _health_payload(
    *,
    settings: Settings,
    fetcher: NimbusFetcher,
    checks: dict[str, dict[str, Any]],
    status: str,
    ready: bool,
    critical_failures: list[str],
    endpoint: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "ready": ready,
        "endpoint": endpoint,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "runtime_role": settings.runtime_role,
        "db_backend": settings.nimbus_db_backend.strip().lower(),
        "metrics_enabled": bool(settings.nimbus_enable_metrics),
        "checks": checks,
        "critical_failures": critical_failures,
        "fetcher_started": bool(getattr(fetcher, "_started", False)),
    }


def _build_checks(
    settings: Settings,
    fetcher: NimbusFetcher,
    *,
    include_deep_store_check: bool,
    include_executor_check: bool,
) -> dict[str, dict[str, Any]]:
    checks = {
        "runtime_dirs": _check_runtime_dirs(settings),
        "fetcher": _check_fetcher(fetcher),
        "store": _check_store(fetcher, deep=include_deep_store_check),
    }
    if include_executor_check:
        checks["executor"] = _check_executor(fetcher)
    return checks


def _check_runtime_dirs(settings: Settings) -> dict[str, Any]:
    data_dir = Path(settings.nimbus_data_dir)
    db_path = Path(settings.nimbus_db_path)
    ok = data_dir.exists() and data_dir.is_dir()
    payload: dict[str, Any] = {
        "ok": ok,
        "critical": True,
        "data_dir": str(data_dir),
        "data_dir_exists": data_dir.exists(),
        "data_dir_is_dir": data_dir.is_dir(),
    }
    if settings.nimbus_db_backend.strip().lower() == "sqlite":
        payload["sqlite_db_path"] = str(db_path)
        payload["sqlite_parent_exists"] = db_path.parent.exists()
    return payload


def _check_fetcher(fetcher: NimbusFetcher) -> dict[str, Any]:
    return {
        "ok": bool(getattr(fetcher, "_started", False)),
        "critical": True,
        "runtime_role": getattr(fetcher, "_runtime_role", "unknown"),
        "execution_enabled": bool(getattr(fetcher, "_execution_enabled", False)),
    }


def _check_store(fetcher: NimbusFetcher, *, deep: bool) -> dict[str, Any]:
    store = getattr(fetcher, "store", None)
    if store is None:
        return {"ok": False, "critical": True, "store_type": None, "error": "store is not initialized"}

    store_type = store.__class__.__name__
    payload: dict[str, Any] = {"ok": True, "critical": True, "store_type": store_type}

    try:
        if isinstance(store, SQLiteJobStore):
            payload["backend"] = "sqlite"
            payload["db_path"] = str(store._db_path)
            if deep:
                row = store._conn.execute("SELECT 1 AS ok").fetchone()
                payload["ping"] = int(row["ok"]) if row is not None else None
        elif isinstance(store, MongoJobStore):
            payload["backend"] = "mongodb"
            payload["uri"] = store._uri
            payload["db_name"] = store._db.name
            if deep:
                result = store._client.admin.command("ping")
                payload["ping"] = int(result.get("ok", 0))
        else:
            payload["backend"] = "unknown"
            if deep and hasattr(store, "list_jobs"):
                store.list_jobs(JobListFilters(page=1, page_size=1))
    except (sqlite3.Error, Exception) as exc:
        payload["ok"] = False
        payload["error"] = str(exc)
    return payload


def _check_executor(fetcher: NimbusFetcher) -> dict[str, Any]:
    execution_enabled = bool(getattr(fetcher, "_execution_enabled", False))
    executor = getattr(fetcher, "_executor", None)
    if not execution_enabled:
        return {
            "ok": True,
            "critical": False,
            "execution_enabled": False,
            "executor_present": executor is not None,
        }
    return {
        "ok": executor is not None,
        "critical": True,
        "execution_enabled": True,
        "executor_present": executor is not None,
    }
