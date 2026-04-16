from __future__ import annotations

from collections import namedtuple
import json
from pathlib import Path

from nimbuschain_fetch.download.coordinator import (
    DownloadCoordinator,
    DownloadCoordinatorStore,
    TASK_STATUS_DOWNLOADING,
    TASK_STATUS_QUEUED,
    TASK_STATUS_READY,
)
from nimbuschain_fetch.settings import Settings


def _make_settings(tmp_path: Path, **overrides: str) -> Settings:
    values = {
        "NIMBUS_RUNTIME_ROLE": "api",
        "NIMBUS_DB_BACKEND": "sqlite",
        "NIMBUS_DB_PATH": str(tmp_path / "nimbus.db"),
        "NIMBUS_DATA_DIR": str(tmp_path / "downloads"),
        "NIMBUS_COPERNICUS_USERNAME": "copernicus@example.com",
        "NIMBUS_COPERNICUS_PASSWORD": "secret",
        "NIMBUS_USGS_USERNAME": "usgs@example.com",
        "NIMBUS_USGS_TOKEN": "token-value",
    }
    values.update(overrides)
    return Settings(**values)


def test_settings_provider_limit_split_defaults_and_legacy_fallback(tmp_path: Path) -> None:
    defaults = _make_settings(tmp_path)

    assert defaults.provider_job_limits_map == {"copernicus": 2, "usgs": 4}
    assert defaults.provider_control_plane_limits_map == {"copernicus": 2, "usgs": 1}
    assert defaults.provider_data_plane_limits_map == {"copernicus": 32, "usgs": 6}

    legacy = _make_settings(tmp_path, NIMBUS_PROVIDER_LIMITS="copernicus=5,usgs=7")
    assert legacy.provider_job_limits_map == {"copernicus": 5, "usgs": 7}
    assert legacy.provider_control_plane_limits_map == {"copernicus": 5, "usgs": 7}
    assert legacy.provider_data_plane_limits_map == {"copernicus": 32, "usgs": 6}

    explicit = _make_settings(
        tmp_path,
        NIMBUS_PROVIDER_LIMITS="copernicus=5,usgs=7",
        NIMBUS_PROVIDER_CONTROL_PLANE_LIMITS="copernicus=3,usgs=2",
        NIMBUS_PROVIDER_DATA_PLANE_LIMITS="copernicus=12,usgs=4",
    )
    assert explicit.provider_control_plane_limits_map == {"copernicus": 3, "usgs": 2}
    assert explicit.provider_data_plane_limits_map == {"copernicus": 12, "usgs": 4}


def test_download_coordinator_store_requeues_prepared_usgs_downloads_as_ready(tmp_path: Path) -> None:
    store = DownloadCoordinatorStore(tmp_path / "download-coordinator.db")
    task = store.ensure_task(
        task_id="task-1",
        provider="usgs",
        job_id="job-1",
        collection="landsat_ot_c2_l1",
        product_id="scene-1",
        output_dir=str(tmp_path / "downloads"),
        metadata={"download_strategy": "adaptive_local"},
    )

    store.update_task(
        str(task["task_id"]),
        status=TASK_STATUS_DOWNLOADING,
        source_url="https://example.test/scene-1.tar",
        file_name="scene-1.tar",
        account_label="primary",
        retry_after="2026-04-15T12:00:00+00:00",
    )

    store.reset_inflight_tasks()
    row = store.get_task(str(task["task_id"])) or {}

    assert row["status"] == TASK_STATUS_READY
    assert row["source_url"] == "https://example.test/scene-1.tar"
    assert row["file_name"] == "scene-1.tar"
    assert row["account_label"] is None
    assert row["retry_after"] is None

    store.close()


def test_download_coordinator_round_robin_interleaves_jobs(tmp_path: Path) -> None:
    coordinator = DownloadCoordinator(_make_settings(tmp_path))
    try:
        tasks = [
            {"task_id": "a-1", "job_id": "job-a", "created_at": "2026-04-15T10:00:00+00:00", "retry_after": None},
            {"task_id": "a-2", "job_id": "job-a", "created_at": "2026-04-15T10:00:01+00:00", "retry_after": None},
            {"task_id": "b-1", "job_id": "job-b", "created_at": "2026-04-15T10:00:02+00:00", "retry_after": None},
        ]

        first = coordinator._pick_round_robin_task("copernicus", tasks)
        remaining = [task for task in tasks if task["task_id"] != first["task_id"]]
        second = coordinator._pick_round_robin_task("copernicus", remaining)
        remaining = [task for task in remaining if task["task_id"] != second["task_id"]]
        third = coordinator._pick_round_robin_task("copernicus", remaining)

        assert [first["task_id"], second["task_id"], third["task_id"]] == ["a-1", "b-1", "a-2"]
    finally:
        coordinator.close()


def test_download_coordinator_copernicus_account_pool_rotates_one_product_per_account(
    monkeypatch,
    tmp_path: Path,
) -> None:
    started: list[tuple[object, tuple[object, ...], str | None]] = []

    class _DummyThread:
        def __init__(self, *, target, args=(), name=None, daemon=None):
            started.append((target, args, name))

        def start(self) -> None:
            return None

    monkeypatch.setattr("nimbuschain_fetch.download.coordinator.threading.Thread", _DummyThread)

    coordinator = DownloadCoordinator(
        _make_settings(
            tmp_path,
            NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON=json.dumps(
                [
                    {"label": "secondary-1", "username": "cop-2@example.com", "password": "pw-2"},
                    {"label": "secondary-2", "username": "cop-3@example.com", "password": "pw-3"},
                    {"label": "secondary-3", "username": "cop-4@example.com", "password": "pw-4"},
                ]
            ),
            NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE=str(tmp_path / "copernicus-pool.inline.json"),
            NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY="4",
        )
    )
    try:
        launched_task_ids: list[str] = []
        for index in range(1, 6):
            task = coordinator.store.ensure_task(
                task_id=f"cop-task-{index}",
                provider="copernicus",
                job_id="job-copernicus",
                collection="SENTINEL-2",
                product_id=f"scene-{index}",
                output_dir=str(tmp_path / "downloads"),
                metadata={"download_strategy": "copernicus_account_pool"},
            )
            launched_task_ids.append(str(task["task_id"]))

        for _ in range(5):
            assert coordinator._try_launch_copernicus_task() is True

        rows = [coordinator.store.get_task(task_id) or {} for task_id in launched_task_ids]
        labels = [str(row.get("account_label") or "") for row in rows]

        assert labels == [
            "primary",
            "secondary-1",
            "secondary-2",
            "secondary-3",
            "primary",
        ]
        assert len(started) == 5
    finally:
        coordinator.close()


def test_download_coordinator_usgs_download_scheduler_accepts_prepared_queued_task(
    monkeypatch,
    tmp_path: Path,
) -> None:
    started: list[tuple[object, tuple[object, ...], str | None]] = []

    class _DummyThread:
        def __init__(self, *, target, args=(), name=None, daemon=None):
            started.append((target, args, name))

        def start(self) -> None:
            return None

    monkeypatch.setattr("nimbuschain_fetch.download.coordinator.threading.Thread", _DummyThread)

    coordinator = DownloadCoordinator(_make_settings(tmp_path))
    try:
        task = coordinator.store.ensure_task(
            task_id="task-usgs-queued-ready",
            provider="usgs",
            job_id="job-usgs",
            collection="landsat_ot_c2_l1",
            product_id="scene-queued-ready",
            output_dir=str(tmp_path / "downloads"),
            metadata={"download_strategy": "adaptive_local"},
        )
        coordinator.store.update_task(
            str(task["task_id"]),
            status=TASK_STATUS_QUEUED,
            source_url="https://example.test/scene-queued-ready.tar",
            file_name="scene-queued-ready.tar",
        )

        assert coordinator._try_launch_usgs_download_task() is True
        row = coordinator.store.get_task(str(task["task_id"])) or {}

        assert row["status"] == TASK_STATUS_DOWNLOADING
        assert row["account_label"] == "primary"
        assert started and started[0][1] == (str(task["task_id"]),)
    finally:
        coordinator.close()


def test_download_coordinator_final_metadata_includes_download_window(tmp_path: Path) -> None:
    coordinator = DownloadCoordinator(_make_settings(tmp_path))
    try:
        metadata = coordinator._final_metadata_for_batch(
            provider_name="copernicus",
            provider=object(),  # type: ignore[arg-type]
            rows=[
                {
                    "status": "done",
                    "account_label": "primary",
                    "started_at": "2026-04-15T11:35:20+00:00",
                    "finished_at": "2026-04-15T11:38:00+00:00",
                    "bytes_total": 100,
                    "bytes_downloaded": 100,
                    "metadata": {
                        "download_strategy": "copernicus_account_pool",
                        "account_pool_size": 4,
                    },
                },
                {
                    "status": "done",
                    "account_label": "secondary-1",
                    "started_at": "2026-04-15T11:35:25+00:00",
                    "finished_at": "2026-04-15T11:39:05+00:00",
                    "bytes_total": 150,
                    "bytes_downloaded": 150,
                    "metadata": {
                        "download_strategy": "copernicus_account_pool",
                        "account_pool_size": 4,
                    },
                },
            ],
        )

        assert metadata["download_started_at"] == "2026-04-15T11:35:20+00:00"
        assert metadata["download_finished_at"] == "2026-04-15T11:39:05+00:00"
        assert metadata["download_window_seconds"] == 225.0
        assert metadata["download_bytes_total"] == 250
        assert metadata["download_bytes_downloaded"] == 250
        assert metadata["account_pool_selected_accounts"] == 2
    finally:
        coordinator.close()


def test_download_coordinator_disk_guard_blocks_launch(monkeypatch, tmp_path: Path) -> None:
    DiskUsage = namedtuple("usage", ["total", "used", "free"])
    started: list[object] = []

    class _DummyThread:
        def __init__(self, *, target, args=(), name=None, daemon=None):
            started.append((target, args, name))

        def start(self) -> None:
            return None

    monkeypatch.setattr("nimbuschain_fetch.download.coordinator.threading.Thread", _DummyThread)
    monkeypatch.setattr(
        "nimbuschain_fetch.download.coordinator.shutil.disk_usage",
        lambda path: DiskUsage(total=10_000, used=9_500, free=500),
    )

    coordinator = DownloadCoordinator(
        _make_settings(
            tmp_path,
            NIMBUS_DOWNLOAD_MIN_FREE_BYTES="1024",
        )
    )
    try:
        task = coordinator.store.ensure_task(
            task_id="task-copernicus-disk-guard",
            provider="copernicus",
            job_id="job-copernicus",
            collection="SENTINEL-2",
            product_id="scene-low-space",
            output_dir=str(tmp_path / "downloads"),
            metadata={"download_strategy": "default"},
        )

        assert coordinator._try_launch_copernicus_task() is False
        row = coordinator.store.get_task(str(task["task_id"])) or {}

        assert row["status"] == TASK_STATUS_QUEUED
        assert started == []
    finally:
        coordinator.close()
