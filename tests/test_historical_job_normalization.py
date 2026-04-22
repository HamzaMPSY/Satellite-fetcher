from __future__ import annotations

from pathlib import Path

from datetime import datetime, timedelta, timezone

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.models import ArtifactType, ArtifactUpsertRequest, JobState, PipelineState, ProviderName
from nimbuschain_fetch.pipeline_timeline import advance_pipeline_timeline
from nimbuschain_fetch.settings import get_settings


def _build_fetcher(tmp_path: Path) -> tuple[NimbusFetcher, SQLiteJobStore]:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "api",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "jobs.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    store = SQLiteJobStore(settings.nimbus_db_path)
    fetcher = NimbusFetcher(settings=settings, store=store)
    return fetcher, store


def _seed_terminal_job(store: SQLiteJobStore, *, job_id: str, state: str) -> None:
    store.create_job(
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
    store.update_job(
        job_id,
        state=state,
        pipeline_state="queued",
        pipeline_step=None,
        pipeline_progress=None,
        product_type="S2MSI2A",
        progress=100.0 if state == JobState.succeeded.value else 99.0,
        bytes_downloaded=100,
        bytes_total=100,
    )


def test_succeeded_historical_job_is_normalized_to_zarr_written(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)
    job_id = "legacy-succeeded"
    _seed_terminal_job(store, job_id=job_id, state=JobState.succeeded.value)
    updated_at_before = str(store.get_job(job_id)["updated_at"])

    store.update_job(
        job_id,
        preserve_updated_at=True,
        raw_outputs=["/download/raw/legacy/file.SAFE.zip"],
        zarr_outputs=["/download/zarr/legacy/file.zarr"],
        watermask_outputs=["/download/watermask/legacy/water_mask.tif"],
        conversion_metadata={"status": "written"},
        pipeline_metadata={},
    )

    status = fetcher.get_job(job_id)
    row = store.get_job(job_id)

    assert status.state == JobState.succeeded
    assert status.pipeline_state == PipelineState.zarr_written
    assert status.pipeline_step == "zarr_written"
    assert status.pipeline_progress == 100.0
    assert status.watermask_outputs == [f"{downloads_root}/watermask/legacy/water_mask.tif"]
    assert row["pipeline_state"] == PipelineState.zarr_written.value
    assert row["pipeline_step"] == "zarr_written"
    assert row["pipeline_progress"] == 100.0
    assert str(row["updated_at"]) == updated_at_before


def test_failed_historical_job_is_normalized_to_zarr_failed(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-failed"
    _seed_terminal_job(store, job_id=job_id, state=JobState.failed.value)
    updated_at_before = str(store.get_job(job_id)["updated_at"])

    store.update_job(
        job_id,
        preserve_updated_at=True,
        raw_outputs=["/download/raw/legacy/file.SAFE.zip"],
        conversion_metadata={"status": "failed", "error": "legacy conversion failed"},
        errors=["legacy conversion failed"],
        pipeline_metadata={"products_found": 1},
        progress=99.0,
    )

    status = fetcher.get_job(job_id)
    row = store.get_job(job_id)

    assert status.state == JobState.failed
    assert status.pipeline_state == PipelineState.zarr_failed
    assert status.pipeline_step == "zarr_failed"
    assert (status.pipeline_progress or 0.0) >= 72.0
    assert row["pipeline_state"] == PipelineState.zarr_failed.value
    assert row["pipeline_step"] == "zarr_failed"
    assert str(row["updated_at"]) == updated_at_before


def test_legacy_succeeded_job_without_outputs_is_exposed_as_downloaded(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-download-only"
    _seed_terminal_job(store, job_id=job_id, state=JobState.succeeded.value)

    status = fetcher.get_job(job_id)
    row = store.get_job(job_id)

    assert status.pipeline_state == PipelineState.downloaded
    assert status.pipeline_step == "downloaded"
    assert status.pipeline_progress == 100.0
    assert row["pipeline_state"] == PipelineState.downloaded.value
    assert row["pipeline_step"] == "downloaded"


def test_get_result_backfills_raw_and_watermask_outputs_from_normalized_job_row(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)
    job_id = "legacy-result"
    _seed_terminal_job(store, job_id=job_id, state=JobState.succeeded.value)

    store.update_job(
        job_id,
        preserve_updated_at=True,
        raw_outputs=["/download/raw/legacy/file.SAFE.zip"],
        zarr_outputs=["/download/zarr/legacy/file.zarr"],
        watermask_outputs=["/download/watermask/legacy/water_mask.tif"],
        pipeline_metadata={"products_found": 1},
        conversion_metadata={"status": "written"},
    )
    store.set_result(
        job_id,
        {
            "job_id": job_id,
            "paths": ["/download/raw/legacy/file.SAFE.zip"],
            "raw_outputs": ["/download/raw/legacy/file.SAFE.zip"],
        },
    )

    result = fetcher.get_result(job_id)

    assert result.raw_outputs == [f"{downloads_root}/raw/legacy/file.SAFE.zip"]
    assert result.paths == [f"{downloads_root}/raw/legacy/file.SAFE.zip"]


def test_list_jobs_normalizes_legacy_download_paths_in_status_payload(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)
    job_id = "legacy-list"
    _seed_terminal_job(store, job_id=job_id, state=JobState.succeeded.value)
    store.update_job(
        job_id,
        preserve_updated_at=True,
        raw_outputs=["/download/raw/legacy/file.SAFE.zip"],
        zarr_outputs=["/download/zarr/legacy/file.zarr"],
        watermask_outputs=["/download/watermask/legacy/water_mask.tif"],
        conversion_metadata={"status": "written"},
    )

    response = fetcher.list_jobs(
        state=None,
        provider=None,
        date_from=None,
        date_to=None,
        page=1,
        page_size=20,
    )
    row = next(item for item in response.items if item.job_id == job_id)

    assert row.raw_outputs == [f"{downloads_root}/raw/legacy/file.SAFE.zip"]
    assert row.zarr_outputs == [f"{downloads_root}/zarr/legacy/file.zarr"]
    assert row.watermask_outputs == [f"{downloads_root}/watermask/legacy/water_mask.tif"]


def test_artifact_api_normalizes_legacy_download_paths(tmp_path: Path) -> None:
    fetcher, _store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)

    record = fetcher.upsert_artifact(
        ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri="/download/zarr/legacy/file.zarr",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            scene_id="legacy-scene",
            source_uri="/download/raw/legacy/file.SAFE.zip",
            metadata={"current_output_uri": "/download/zarr/legacy/file.zarr"},
        )
    )

    assert record.artifact_uri == f"{downloads_root}/zarr/legacy/file.zarr"
    assert record.source_uri == f"{downloads_root}/raw/legacy/file.SAFE.zip"
    assert record.metadata["current_output_uri"] == f"{downloads_root}/zarr/legacy/file.zarr"

    response = fetcher.list_artifacts(
        artifact_type=ArtifactType.zarr.value,
        provider=None,
        collection=None,
        scene_id=None,
        job_id=None,
        uri_query="legacy/file.zarr",
        date_from=None,
        date_to=None,
        page=1,
        page_size=20,
    )
    listed = response.items[0]
    assert listed.artifact_uri == f"{downloads_root}/zarr/legacy/file.zarr"
    assert listed.source_uri == f"{downloads_root}/raw/legacy/file.SAFE.zip"


def test_artifact_api_normalizes_app_data_downloads_paths(tmp_path: Path) -> None:
    fetcher, _store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)

    record = fetcher.upsert_artifact(
        ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri="/app/data/downloads/zarr/legacy/file.zarr",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            scene_id="legacy-scene-app",
            source_uri="/app/data/downloads/raw/legacy/file.SAFE.zip",
            metadata={"current_output_uri": "/app/data/downloads/zarr/legacy/file.zarr"},
        )
    )

    assert record.artifact_uri == f"{downloads_root}/zarr/legacy/file.zarr"
    assert record.source_uri == f"{downloads_root}/raw/legacy/file.SAFE.zip"
    assert record.metadata["current_output_uri"] == f"{downloads_root}/zarr/legacy/file.zarr"


def test_queued_job_requeued_after_restart_has_no_execution_duration(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-requeued-no-duration"
    _seed_terminal_job(store, job_id=job_id, state=JobState.queued.value)
    store.update_job(
        job_id,
        state=JobState.queued.value,
        pipeline_state=PipelineState.downloading.value,
        pipeline_step="resume_after_restart",
        started_at="2026-04-06T00:00:00+00:00",
        finished_at=None,
        progress=83.57,
        bytes_downloaded=123,
        bytes_total=456,
    )

    status = fetcher.get_job(job_id)

    assert status.state == JobState.queued
    assert status.duration_seconds is None


def test_terminal_job_duration_uses_finished_at_when_available(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-terminal-duration"
    _seed_terminal_job(store, job_id=job_id, state=JobState.succeeded.value)
    store.update_job(
        job_id,
        state=JobState.succeeded.value,
        started_at="2026-04-07T09:00:00+00:00",
        finished_at="2026-04-07T09:02:30+00:00",
        updated_at="2026-04-07T09:05:00+00:00",
        preserve_updated_at=True,
    )

    status = fetcher.get_job(job_id)

    assert status.duration_seconds == 150.0


def test_running_job_duration_uses_current_time(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-running-duration"
    _seed_terminal_job(store, job_id=job_id, state=JobState.running.value)
    started_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=5)
    store.update_job(
        job_id,
        state=JobState.running.value,
        started_at=started_at.isoformat(),
        finished_at=None,
        updated_at=(started_at + timedelta(seconds=2)).isoformat(),
        preserve_updated_at=True,
    )

    status = fetcher.get_job(job_id)

    assert status.duration_seconds is not None
    assert status.duration_seconds >= 0.0


def test_running_job_missing_started_at_uses_first_execution_event(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-running-missing-started-at"
    _seed_terminal_job(store, job_id=job_id, state=JobState.running.value)
    store.update_job(
        job_id,
        state=JobState.running.value,
        started_at=None,
        finished_at=None,
        updated_at="2026-04-10T09:00:20+00:00",
        preserve_updated_at=True,
    )
    store.append_event(
        job_id,
        "job.started",
        {"state": JobState.running.value},
        timestamp=datetime(2026, 4, 10, 9, 0, 5, tzinfo=timezone.utc),
    )

    status = fetcher.get_job(job_id)

    assert status.started_at == datetime(2026, 4, 10, 9, 0, 5, tzinfo=timezone.utc)
    assert status.duration_seconds is not None


def test_get_job_rebuild_ignores_previous_attempt_cube_events_after_restart(tmp_path: Path) -> None:
    fetcher, store = _build_fetcher(tmp_path)
    job_id = "legacy-restarted-cube-timeline"
    store.create_job(
        job_id=job_id,
        job_type="search_download",
        provider="copernicus",
        collection="SENTINEL-2",
        request_payload={
            "job_type": "search_download",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "cube_mode": "before_mask",
        },
    )

    leaked_timeline: dict[str, object] = {}
    leaked_timeline = advance_pipeline_timeline(
        leaked_timeline,
        job_state="running",
        pipeline_state="searching",
        pipeline_step="searching",
        pipeline_progress=5.0,
        timestamp="2026-04-20T11:00:00+00:00",
        job_kind="fetch",
        mask_types=[],
        cube_mode="before_mask",
    )
    leaked_timeline = advance_pipeline_timeline(
        leaked_timeline,
        job_state="running",
        pipeline_state="downloaded",
        pipeline_step="downloaded",
        pipeline_progress=70.0,
        timestamp="2026-04-20T11:10:00+00:00",
        job_kind="fetch",
        mask_types=[],
        cube_mode="before_mask",
    )
    leaked_timeline = advance_pipeline_timeline(
        leaked_timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-04-20T11:20:00+00:00",
        job_kind="fetch",
        mask_types=[],
        cube_mode="before_mask",
    )
    leaked_timeline = advance_pipeline_timeline(
        leaked_timeline,
        job_state="running",
        pipeline_state="cube_written",
        pipeline_step="cube_written",
        pipeline_progress=100.0,
        timestamp="2026-04-20T11:30:00+00:00",
        job_kind="fetch",
        mask_types=[],
        cube_mode="before_mask",
    )
    leaked_timeline = advance_pipeline_timeline(
        leaked_timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=68.0,
        timestamp="2026-04-20T13:32:35+00:00",
        job_kind="fetch",
        mask_types=[],
        cube_mode="before_mask",
    )

    store.update_job(
        job_id,
        state=JobState.running.value,
        started_at="2026-04-20T13:32:17+00:00",
        pipeline_state=PipelineState.zarr_converting.value,
        pipeline_step="writing_chunks",
        pipeline_progress=68.0,
        pipeline_metadata={
            "cube_mode": "before_mask",
            "timeline": leaked_timeline,
        },
        product_type="S2MSI2A",
        progress=0.0,
        bytes_downloaded=0,
        bytes_total=0,
    )

    event_specs = [
        ("job.started", {"state": JobState.running.value}, "2026-04-20T11:00:00+00:00"),
        ("job.searching", {"pipeline_state": "searching", "pipeline_step": "searching"}, "2026-04-20T11:00:01+00:00"),
        ("job.downloaded", {"pipeline_state": "downloaded", "pipeline_step": "downloaded"}, "2026-04-20T11:10:00+00:00"),
        ("job.zarr_written", {"pipeline_state": "zarr_written", "pipeline_step": "zarr_written"}, "2026-04-20T11:20:00+00:00"),
        ("job.cube_written", {"pipeline_state": "cube_written", "pipeline_step": "cube_written", "cube_mode": "before_mask"}, "2026-04-20T11:30:00+00:00"),
        ("job.requeued_after_restart", {"reason": "service_restart"}, "2026-04-20T13:31:59+00:00"),
        ("job.started", {"state": JobState.running.value}, "2026-04-20T13:32:00+00:00"),
        ("job.searching", {"pipeline_state": "searching", "pipeline_step": "searching"}, "2026-04-20T13:32:01+00:00"),
        ("job.downloaded", {"pipeline_state": "downloaded", "pipeline_step": "downloaded"}, "2026-04-20T13:32:19+00:00"),
    ]
    for event_type, payload, timestamp in event_specs:
        store.append_event(
            job_id,
            event_type,
            payload,
            timestamp=datetime.fromisoformat(timestamp),
        )

    status = fetcher.get_job(job_id)
    timeline = status.pipeline_timeline
    cube_stage = next(
        stage
        for stage in timeline.get("stages", [])
        if isinstance(stage, dict) and stage.get("key") == "cube"
    )
    convert_stage = next(
        stage
        for stage in timeline.get("stages", [])
        if isinstance(stage, dict) and stage.get("key") == "convert"
    )

    assert convert_stage["status"] == "running"
    assert cube_stage["status"] == "pending"
    assert timeline["current_stage"] == "convert"


def test_artifact_api_normalizes_data_downloads_paths(tmp_path: Path) -> None:
    fetcher, _store = _build_fetcher(tmp_path)
    downloads_root = str(fetcher.settings.nimbus_data_dir)

    record = fetcher.upsert_artifact(
        ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri="/data/downloads/zarr/legacy/file.zarr",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            scene_id="legacy-scene-data",
            source_uri="/data/downloads/raw/legacy/file.SAFE.zip",
            metadata={"current_output_uri": "/data/downloads/zarr/legacy/file.zarr"},
        )
    )

    assert record.artifact_uri == f"{downloads_root}/zarr/legacy/file.zarr"
    assert record.source_uri == f"{downloads_root}/raw/legacy/file.SAFE.zip"
    assert record.metadata["current_output_uri"] == f"{downloads_root}/zarr/legacy/file.zarr"
