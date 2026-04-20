from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.download.coordinator import (
    DownloadCoordinatorStore,
    TASK_STATUS_DONE,
    TASK_STATUS_QUEUED,
)
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.api.jobs import router as jobs_router


def _sqlite_settings(tmp_path: Path, *, runtime_role: str = "api") -> Settings:
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


def _seed_job(fetcher: NimbusFetcher, job_id: str, *, state: str) -> None:
    fetcher.store.create_job(
        job_id=job_id,
        job_type="search_download",
        provider="copernicus",
        collection="SENTINEL-2",
        request_payload={
            "job_type": "search_download",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
        },
    )
    fetcher.store.update_job(
        job_id,
        state=state,
        pipeline_state="downloading",
        pipeline_step="downloading",
        pipeline_progress=42.0,
        worker_id="worker-a",
    )


def test_fetcher_reset_runtime_state_cancels_active_jobs_and_coordinator_tasks(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path)
    fetcher = NimbusFetcher(settings=settings)
    _seed_job(fetcher, "job-queued", state="queued")
    _seed_job(fetcher, "job-running", state="running")
    fetcher.store.upsert_worker_heartbeat(
        "worker-1",
        {
            "runtime_role": "worker",
            "execution_enabled": True,
            "max_concurrent_jobs": 2,
            "queue_poll_seconds": 1.0,
            "heartbeat_interval_seconds": 5.0,
            "provider_limits": {"copernicus": 2, "usgs": 4},
            "hostname": "localhost",
            "pid": 1234,
            "active_running_jobs": 1,
            "active_cancel_requested_jobs": 0,
            "queue_backlog": 0,
            "metadata": {},
        },
    )

    coordinator = DownloadCoordinatorStore(settings.download_coordinator_db_path)
    try:
        queued_task = coordinator.ensure_task(
            task_id="task-queued",
            provider="copernicus",
            job_id="job-running",
            collection="SENTINEL-2",
            product_id="scene-1",
            output_dir=str(tmp_path / "downloads"),
            metadata={},
        )
        done_task = coordinator.ensure_task(
            task_id="task-done",
            provider="copernicus",
            job_id="job-old",
            collection="SENTINEL-2",
            product_id="scene-2",
            output_dir=str(tmp_path / "downloads"),
            metadata={},
        )
        coordinator.update_task(str(queued_task["task_id"]), status=TASK_STATUS_QUEUED)
        coordinator.update_task(
            str(done_task["task_id"]),
            status=TASK_STATUS_DONE,
            finished_at="2026-04-17T11:00:00+00:00",
        )
    finally:
        coordinator.close()

    summary = asyncio.run(fetcher.reset_runtime_state())

    assert summary["status"] == "ok"
    assert summary["history_preserved"] is True
    assert summary["jobs_cancelled"] == 2
    assert summary["coordinator_tasks_cancelled"] == 1
    assert summary["worker_heartbeats_cleared"] == 1

    queued_row = fetcher.store.get_job("job-queued") or {}
    running_row = fetcher.store.get_job("job-running") or {}
    assert queued_row["state"] == "cancelled"
    assert queued_row["pipeline_state"] == "cancelled"
    assert queued_row["pipeline_step"] == "cancelled"
    assert queued_row["finished_at"] is not None
    assert running_row["state"] == "cancelled"
    assert running_row["pipeline_state"] == "cancelled"

    coordinator_after = DownloadCoordinatorStore(settings.download_coordinator_db_path)
    try:
        queued_task_row = coordinator_after.get_task("task-queued") or {}
        done_task_row = coordinator_after.get_task("task-done") or {}
        assert queued_task_row["status"] == "cancelled"
        assert done_task_row["status"] == TASK_STATUS_DONE
    finally:
        coordinator_after.close()

    assert fetcher.store.list_workers() == []


def test_reset_active_jobs_route_returns_fetcher_summary(tmp_path: Path) -> None:
    app = FastAPI()
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path))
    app.state.fetcher = fetcher
    app.state.settings = fetcher.settings
    app.include_router(jobs_router)

    with TestClient(app) as client:
        response = client.post("/v1/jobs/reset-active")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["history_preserved"] is True
