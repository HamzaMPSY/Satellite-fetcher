from __future__ import annotations

import asyncio
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.api.health import router as health_router


class _DummyFetcher:
    def get_worker_status(self) -> dict:
        return {"status": "ready"}

    def get_download_coordinator_status(self) -> dict:
        return {
            "status": "idle",
            "source": "local_worker",
            "timestamp": "2026-04-15T10:00:00+00:00",
            "workers_reporting": 1,
            "summary": {
                "status": "idle",
                "machine": {"active_downloads": 0, "active_download_limit": 8},
                "jobs": {"pending_tasks_total": 0},
                "providers": {},
                "tasks": {},
            },
            "workers": [],
        }


def _sqlite_settings(tmp_path, *, runtime_role: str) -> Settings:
    return Settings(
        NIMBUS_RUNTIME_ROLE=runtime_role,
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_COPERNICUS_USERNAME="copernicus@example.com",
        NIMBUS_COPERNICUS_PASSWORD="secret",
        NIMBUS_USGS_USERNAME="usgs@example.com",
        NIMBUS_USGS_TOKEN="token-value",
    )


def test_download_coordinator_endpoint_returns_fetcher_payload() -> None:
    app = FastAPI()
    app.state.fetcher = _DummyFetcher()
    app.state.settings = Settings()
    app.include_router(health_router)

    with TestClient(app) as client:
        response = client.get("/v1/worker/download-coordinator")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "idle"
    assert body["source"] == "local_worker"
    assert body["summary"]["machine"]["active_download_limit"] == 8


def test_fetcher_download_coordinator_status_uses_worker_heartbeat_reports(tmp_path) -> None:
    settings = _sqlite_settings(tmp_path, runtime_role="api")
    fetcher = NimbusFetcher(settings=settings)
    snapshot = {
        "status": "active",
        "machine": {
            "active_downloads": 2,
            "active_download_limit": 8,
        },
        "jobs": {
            "pending_tasks_total": 5,
            "pending_jobs_total": 2,
            "pending_by_job": [],
        },
        "providers": {
            "copernicus": {"accounts": [], "counts": {}},
            "usgs": {"counts": {}},
        },
        "tasks": {
            "active": [],
            "recent_terminal": [],
        },
    }
    fetcher.store.upsert_worker_heartbeat(
        "worker-123",
        {
            "runtime_role": "worker",
            "execution_enabled": True,
            "max_concurrent_jobs": 2,
            "queue_poll_seconds": 1.0,
            "heartbeat_interval_seconds": 5.0,
            "provider_limits": {"copernicus": 2, "usgs": 4},
            "hostname": "localhost",
            "pid": 4321,
            "active_running_jobs": 1,
            "active_cancel_requested_jobs": 0,
            "queue_backlog": 0,
            "started_at": fetcher._now_iso(),
            "last_seen_at": fetcher._now_iso(),
            "metadata": {
                "download_coordinator": snapshot,
            },
        },
    )

    payload = fetcher.get_download_coordinator_status()

    assert payload["source"] == "worker_heartbeats"
    assert payload["workers_reporting"] == 1
    assert payload["summary"]["status"] == "active"
    assert payload["summary"]["jobs"]["pending_tasks_total"] == 5
    assert payload["workers"][0]["worker_id"] == "worker-123"


def test_worker_heartbeat_persists_download_coordinator_metadata(tmp_path) -> None:
    settings = _sqlite_settings(tmp_path, runtime_role="worker")
    fetcher = NimbusFetcher(settings=settings)

    payload = fetcher._publish_worker_heartbeat()

    assert payload is not None
    workers = fetcher.store.list_workers()
    assert len(workers) == 1
    metadata = dict(workers[0].get("metadata") or {})
    coordinator = dict(metadata.get("download_coordinator") or {})
    assert coordinator["status"] == "not_initialized"
    assert coordinator["machine"]["active_download_limit"] == 8


def test_worker_heartbeat_thread_keeps_worker_alive_during_event_loop_block(monkeypatch, tmp_path) -> None:
    settings = _sqlite_settings(tmp_path, runtime_role="worker").model_copy(
        update={"nimbus_worker_heartbeat_seconds": 1.0}
    )
    fetcher = NimbusFetcher(settings=settings)
    heartbeat_calls: list[float] = []
    original_publish = fetcher._publish_worker_heartbeat

    def _wrapped_publish() -> dict | None:
        heartbeat_calls.append(time.monotonic())
        return original_publish()

    monkeypatch.setattr(fetcher, "_publish_worker_heartbeat", _wrapped_publish)

    async def _scenario() -> None:
        await fetcher.start()
        # Simulate a long synchronous conversion step that blocks the event loop.
        time.sleep(2.2)
        await fetcher.stop()

    asyncio.run(_scenario())

    assert len(heartbeat_calls) >= 2
