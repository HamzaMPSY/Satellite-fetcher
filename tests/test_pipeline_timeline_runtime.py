from __future__ import annotations

from nimbuschain_fetch.pipeline_timeline import (
    advance_pipeline_timeline,
    refresh_pipeline_timeline,
)


def _stage_by_key(timeline: dict[str, object], key: str) -> dict[str, object]:
    for stage in timeline.get("stages", []):  # type: ignore[union-attr]
        if isinstance(stage, dict) and str(stage.get("key") or "") == key:
            return stage
    raise AssertionError(f"Stage '{key}' not found in timeline: {timeline}")


def test_pipeline_timeline_tracks_stage_durations_for_fetch_pipeline() -> None:
    timeline: dict[str, object] = {}

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="searching",
        pipeline_step="searching",
        pipeline_progress=5.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=[],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="downloading",
        pipeline_step="downloading",
        pipeline_progress=18.0,
        timestamp="2026-01-01T00:00:05+00:00",
        job_kind="fetch",
        mask_types=[],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=55.0,
        timestamp="2026-01-01T00:00:20+00:00",
        job_kind="fetch",
        mask_types=[],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="succeeded",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=100.0,
        timestamp="2026-01-01T00:00:35+00:00",
        job_kind="fetch",
        mask_types=[],
    )

    search_stage = _stage_by_key(timeline, "search")
    download_stage = _stage_by_key(timeline, "download")
    convert_stage = _stage_by_key(timeline, "convert")
    ready_stage = _stage_by_key(timeline, "ready")

    assert search_stage["status"] == "done"
    assert search_stage["duration_seconds"] == 5.0
    assert download_stage["status"] == "done"
    assert download_stage["duration_seconds"] == 15.0
    assert convert_stage["status"] == "done"
    assert convert_stage["duration_seconds"] == 15.0
    assert ready_stage["status"] == "done"
    assert ready_stage["duration_seconds"] == 0.0
    assert timeline["terminal"] is True
    assert timeline["current_stage"] == "ready"


def test_pipeline_timeline_marks_current_stage_failed_instead_of_spinning() -> None:
    timeline: dict[str, object] = {}

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="searching",
        pipeline_step="searching",
        pipeline_progress=5.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=[],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="downloading",
        pipeline_step="downloading",
        pipeline_progress=22.0,
        timestamp="2026-01-01T00:00:06+00:00",
        job_kind="fetch",
        mask_types=[],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="failed",
        pipeline_state="failed",
        pipeline_step="failed",
        pipeline_progress=22.0,
        timestamp="2026-01-01T00:00:19+00:00",
        job_kind="fetch",
        mask_types=[],
    )

    search_stage = _stage_by_key(timeline, "search")
    download_stage = _stage_by_key(timeline, "download")
    convert_stage = _stage_by_key(timeline, "convert")

    assert search_stage["status"] == "done"
    assert search_stage["duration_seconds"] == 6.0
    assert download_stage["status"] == "failed"
    assert download_stage["duration_seconds"] == 13.0
    assert convert_stage["status"] == "pending"
    assert timeline["terminal"] is True
    assert timeline["current_stage"] == "download"


def test_pipeline_timeline_keeps_mask_failure_on_water_stage() -> None:
    timeline: dict[str, object] = {}
    mask_types = ["cloud", "water"]

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_cloud_inference",
        pipeline_step="running_cloud_inference",
        pipeline_progress=82.0,
        timestamp="2026-01-01T00:00:05+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="running_water_inference",
        pipeline_progress=91.0,
        timestamp="2026-01-01T00:00:15+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="failed",
        pipeline_state="failed",
        pipeline_step="water_failed",
        pipeline_progress=95.0,
        timestamp="2026-01-01T00:00:27+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )

    cloud_stage = _stage_by_key(timeline, "cloud")
    water_stage = _stage_by_key(timeline, "water")
    ready_stage = _stage_by_key(timeline, "ready")

    assert cloud_stage["status"] == "done"
    assert cloud_stage["duration_seconds"] == 10.0
    assert water_stage["status"] == "failed"
    assert water_stage["duration_seconds"] == 12.0
    assert water_stage["detail_label"] == "Water Failed"
    assert ready_stage["status"] == "pending"
    assert timeline["terminal"] is True
    assert timeline["current_stage"] == "water"
    assert timeline["pipeline_step"] == "water_failed"


def test_pipeline_timeline_keeps_ready_pending_while_masks_are_still_running() -> None:
    timeline: dict[str, object] = {}
    mask_types = ["cloud", "water"]

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="searching",
        pipeline_step="searching",
        pipeline_progress=5.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="downloading",
        pipeline_step="downloading",
        pipeline_progress=20.0,
        timestamp="2026-01-01T00:00:04+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=55.0,
        timestamp="2026-01-01T00:00:18+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-01-01T00:00:24+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_cloud_inference",
        pipeline_step="running_cloud_inference",
        pipeline_progress=80.0,
        timestamp="2026-01-01T00:00:30+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="running_water_inference",
        pipeline_progress=92.0,
        timestamp="2026-01-01T00:00:39+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )

    search_stage = _stage_by_key(timeline, "search")
    download_stage = _stage_by_key(timeline, "download")
    convert_stage = _stage_by_key(timeline, "convert")
    cloud_stage = _stage_by_key(timeline, "cloud")
    water_stage = _stage_by_key(timeline, "water")
    ready_stage = _stage_by_key(timeline, "ready")

    assert search_stage["status"] == "done"
    assert download_stage["status"] == "done"
    assert convert_stage["status"] == "done"
    assert cloud_stage["status"] == "done"
    assert water_stage["status"] == "running"
    assert ready_stage["status"] == "pending"
    assert timeline["current_stage"] == "water"


def test_pipeline_timeline_reuses_latest_step_for_repeated_conversion_updates() -> None:
    timeline: dict[str, object] = {}

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=40.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="registering_artifact",
        pipeline_progress=55.0,
        timestamp="2026-01-01T00:00:10+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=63.0,
        timestamp="2026-01-01T00:00:15+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="writing_chunks",
        pipeline_progress=69.0,
        timestamp="2026-01-01T00:00:20+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-01-01T00:00:25+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="running_water_inference",
        pipeline_progress=84.0,
        timestamp="2026-01-01T00:00:30+00:00",
        job_kind="fetch",
        mask_types=["cloud", "water"],
    )

    steps = timeline.get("steps", [])
    assert isinstance(steps, list)
    writing_chunk_steps = [
        step
        for step in steps
        if isinstance(step, dict) and str(step.get("key") or "") == "writing_chunks"
    ]
    running_steps = [
        step
        for step in steps
        if isinstance(step, dict) and str(step.get("status") or "") == "running"
    ]
    convert_stage = _stage_by_key(timeline, "convert")
    water_stage = _stage_by_key(timeline, "water")

    assert len(writing_chunk_steps) == 2
    assert len(running_steps) == 1
    assert str(running_steps[0].get("key") or "") == "running_water_inference"
    assert convert_stage["status"] == "done"
    assert water_stage["status"] == "running"
    assert timeline["current_stage"] == "water"


def test_pipeline_timeline_ignores_stale_convert_step_once_water_is_running() -> None:
    mask_types = ["cloud", "water"]
    timeline: dict[str, object] = {}

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_converting",
        pipeline_step="registering_artifact",
        pipeline_progress=69.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-01-01T00:00:04+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_cloud_inference",
        pipeline_step="running_cloud_inference",
        pipeline_progress=82.0,
        timestamp="2026-01-01T00:00:08+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="registering_artifact",
        pipeline_progress=92.0,
        timestamp="2026-01-01T00:00:15+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )

    convert_stage = _stage_by_key(timeline, "convert")
    cloud_stage = _stage_by_key(timeline, "cloud")
    water_stage = _stage_by_key(timeline, "water")

    assert convert_stage["status"] == "done"
    assert cloud_stage["status"] == "done"
    assert water_stage["status"] == "running"
    assert timeline["current_stage"] == "water"
    assert timeline["current_step"] == "running_water_inference"
    assert timeline["pipeline_step"] == "running_water_inference"


def test_refresh_pipeline_timeline_preserves_backend_structure_despite_stale_row_step() -> None:
    mask_types = ["cloud", "water"]
    timeline: dict[str, object] = {}

    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="zarr_written",
        pipeline_step="zarr_written",
        pipeline_progress=72.0,
        timestamp="2026-01-01T00:00:00+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_cloud_inference",
        pipeline_step="running_cloud_inference",
        pipeline_progress=83.0,
        timestamp="2026-01-01T00:00:05+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )
    timeline = advance_pipeline_timeline(
        timeline,
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="running_water_inference",
        pipeline_progress=92.0,
        timestamp="2026-01-01T00:00:11+00:00",
        job_kind="fetch",
        mask_types=mask_types,
    )

    refreshed = refresh_pipeline_timeline(
        timeline,
        timestamp="2026-01-01T00:00:21+00:00",
        job_state="running",
        pipeline_state="running_water_inference",
        pipeline_step="registering_artifact",
        job_kind="fetch",
        mask_types=mask_types,
    )

    convert_stage = _stage_by_key(refreshed, "convert")
    cloud_stage = _stage_by_key(refreshed, "cloud")
    water_stage = _stage_by_key(refreshed, "water")

    assert convert_stage["status"] == "done"
    assert cloud_stage["status"] == "done"
    assert water_stage["status"] == "running"
    assert refreshed["current_stage"] == "water"
    assert refreshed["current_step"] == "running_water_inference"
    assert refreshed["pipeline_step"] == "running_water_inference"
    assert len(refreshed.get("steps", [])) == len(timeline.get("steps", []))
