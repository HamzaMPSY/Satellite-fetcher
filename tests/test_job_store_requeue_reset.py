from __future__ import annotations

from pathlib import Path

from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore


def _seed_job(store: SQLiteJobStore, job_id: str) -> None:
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


def test_requeue_incomplete_jobs_preserves_completed_download_outputs(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-complete-download"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="zarr_converting",
        pipeline_step="water_masking",
        pipeline_progress=88.0,
        pipeline_metadata={"products_found": 2, "products_downloaded": 2},
        conversion_metadata={"stage": "water_masking", "current_output_uri": "/download/zarr/test.zarr"},
        raw_outputs=["/download/raw/a.SAFE.zip", "/download/raw/b.SAFE.zip"],
        zarr_outputs=["/download/zarr/test.zarr"],
        watermask_outputs=["/download/watermask/test/water_mask.tif"],
        progress=97.0,
        bytes_downloaded=100,
        bytes_total=100,
        retry_count=3,
        last_retry_at="2026-03-17T10:00:00+00:00",
        errors=["boom"],
    )

    requeued = store.requeue_incomplete_jobs()
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "zarr_converting"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 88.0
    assert row["pipeline_metadata"] == {"products_found": 2, "products_downloaded": 2}
    assert row["conversion_metadata"] == {"stage": "water_masking", "current_output_uri": "/download/zarr/test.zarr"}
    assert row["raw_outputs"] == ["/download/raw/a.SAFE.zip", "/download/raw/b.SAFE.zip"]
    assert row["zarr_outputs"] == ["/download/zarr/test.zarr"]
    assert row["watermask_outputs"] == ["/download/watermask/test/water_mask.tif"]
    assert row["progress"] == 97.0
    assert row["bytes_downloaded"] == 100
    assert row["bytes_total"] == 100
    assert row["retry_count"] == 3
    assert row["last_retry_at"] == "2026-03-17T10:00:00+00:00"
    assert row["errors"] == []
    assert row["worker_id"] is None


def test_requeue_incomplete_jobs_resumes_partial_download_without_clearing_raws(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-partial-download"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="downloading",
        pipeline_step="downloading",
        pipeline_progress=42.0,
        pipeline_metadata={"products_found": 5, "products_downloaded": 2},
        raw_outputs=["/download/raw/a.SAFE.zip", "/download/raw/b.SAFE.zip"],
        progress=37.0,
        bytes_downloaded=200,
        bytes_total=500,
        retry_count=1,
        errors=["transient"],
    )

    requeued = store.requeue_incomplete_jobs()
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "downloading"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 42.0
    assert row["pipeline_metadata"] == {"products_found": 5, "products_downloaded": 2}
    assert row["raw_outputs"] == ["/download/raw/a.SAFE.zip", "/download/raw/b.SAFE.zip"]
    assert row["zarr_outputs"] == []
    assert row["watermask_outputs"] == []
    assert row["progress"] == 37.0
    assert row["bytes_downloaded"] == 200
    assert row["bytes_total"] == 500
    assert row["retry_count"] == 1
    assert row["errors"] == []
    assert row["worker_id"] is None


def test_requeue_incomplete_jobs_preserves_conversion_outputs_when_stage_already_started(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-zarr-resume"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="zarr_converting",
        pipeline_step="registering_artifact",
        pipeline_progress=96.0,
        pipeline_metadata={"products_found": 1, "products_downloaded": 1},
        conversion_metadata={
            "stage": "registering_artifact",
            "current_raw_uri": "/download/raw/a.SAFE.zip",
            "current_output_uri": "/download/zarr/test.zarr",
            "items": [
                {
                    "raw_uri": "/download/raw/a.SAFE.zip",
                    "zarr_uri": "/download/zarr/test.zarr",
                    "summary": {"status": "written"},
                    "dataset_summary": {"shape": [1, 4, 32, 32]},
                }
            ],
        },
        raw_outputs=["/download/raw/a.SAFE.zip"],
        zarr_outputs=["/download/zarr/test.zarr"],
        watermask_outputs=["/download/watermask/test/water_mask_status.json"],
        progress=100.0,
        bytes_downloaded=100,
        bytes_total=100,
        errors=["transient conversion failure"],
    )

    requeued = store.requeue_incomplete_jobs()
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "zarr_converting"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 96.0
    assert row["conversion_metadata"]["stage"] == "registering_artifact"
    assert row["zarr_outputs"] == ["/download/zarr/test.zarr"]
    assert row["watermask_outputs"] == ["/download/watermask/test/water_mask_status.json"]


def test_claim_job_for_execution_keeps_resume_fields(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-claim-resume"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="queued",
        pipeline_state="downloaded",
        pipeline_step="resume_after_restart",
        pipeline_progress=70.0,
        pipeline_metadata={"products_found": 1, "products_downloaded": 1},
        raw_outputs=["/download/raw/b.SAFE.zip"],
        progress=100.0,
        bytes_downloaded=1000,
        bytes_total=1000,
        retry_count=2,
        errors=["old error"],
    )

    claimed = store.claim_job_for_execution(job_id, "worker-b")
    assert claimed is True

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "running"
    assert row["worker_id"] == "worker-b"
    assert row["pipeline_state"] == "downloaded"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 70.0
    assert row["pipeline_metadata"] == {"products_found": 1, "products_downloaded": 1}
    assert row["raw_outputs"] == ["/download/raw/b.SAFE.zip"]
    assert row["zarr_outputs"] == []
    assert row["watermask_outputs"] == []
    assert row["progress"] == 100.0
    assert row["bytes_downloaded"] == 1000
    assert row["bytes_total"] == 1000
    assert row["retry_count"] == 2
    assert row["errors"] == []


def test_requeue_incomplete_jobs_preserves_searching_stage_without_outputs(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-searching-no-outputs"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="searching",
        pipeline_step="searching",
        pipeline_progress=6.0,
        pipeline_metadata={"products_found": 0},
        progress=0.0,
        bytes_downloaded=0,
        bytes_total=0,
    )

    requeued = store.requeue_incomplete_jobs()
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "searching"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 6.0


def test_requeue_incomplete_jobs_preserves_waiting_first_byte_stage_without_outputs(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-waiting-first-byte"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="downloading",
        pipeline_step="waiting_first_byte",
        pipeline_progress=12.0,
        pipeline_metadata={"products_found": 6, "products_downloaded": 0},
        progress=0.0,
        bytes_downloaded=0,
        bytes_total=0,
    )

    requeued = store.requeue_incomplete_jobs()
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "downloading"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 12.0


def test_requeue_stale_running_jobs_preserves_conversion_outputs(tmp_path: Path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.db")
    job_id = "job-requeue-stale-zarr"
    _seed_job(store, job_id)
    store.update_job(
        job_id,
        state="running",
        worker_id="worker-a",
        pipeline_state="zarr_converting",
        pipeline_step="water_masking",
        pipeline_progress=88.0,
        pipeline_metadata={"products_found": 1, "products_downloaded": 1},
        conversion_metadata={
            "stage": "water_masking",
            "current_output_uri": "/download/zarr/test.zarr",
            "items": [
                {
                    "raw_uri": "/download/raw/a.SAFE.zip",
                    "scene_id": "SCENE_A",
                    "zarr_uri": "/download/zarr/test.zarr",
                    "status": "water_masking",
                    "mask_pending": True,
                    "data_family": "optical",
                    "summary": {"scene_id": "SCENE_A", "status": "written"},
                    "dataset_summary": {
                        "shape": [1, 4, 16, 16],
                        "dimensions": ["time", "band", "y", "x"],
                        "band_names": ["B02", "B03", "B04", "B08"],
                    },
                }
            ],
        },
        raw_outputs=["/download/raw/a.SAFE.zip"],
        zarr_outputs=["/download/zarr/test.zarr"],
        watermask_outputs=["/download/watermask/test/water_mask_status.json"],
        progress=100.0,
        bytes_downloaded=100,
        bytes_total=100,
    )
    store.update_job(job_id, updated_at="2026-03-01T00:00:00+00:00", preserve_updated_at=True)

    requeued = store.requeue_stale_running_jobs(30)
    assert requeued == [job_id]

    row = store.get_job(job_id)
    assert row is not None
    assert row["state"] == "queued"
    assert row["pipeline_state"] == "zarr_converting"
    assert row["pipeline_step"] == "resume_after_restart"
    assert row["pipeline_progress"] == 88.0
    assert row["raw_outputs"] == ["/download/raw/a.SAFE.zip"]
    assert row["zarr_outputs"] == ["/download/zarr/test.zarr"]
    assert row["watermask_outputs"] == ["/download/watermask/test/water_mask_status.json"]
    assert row["conversion_metadata"]["stage"] == "water_masking"
    assert row["worker_id"] is None
