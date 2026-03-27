from __future__ import annotations

from pathlib import Path

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.models import ArtifactType, ArtifactUpsertRequest, JobState, PipelineState, ProviderName
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
