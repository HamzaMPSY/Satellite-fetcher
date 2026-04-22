from __future__ import annotations

import nimbuschain_fetch_ui.app as app_module
from nimbuschain_fetch_ui.app import (
    _conversion_progress_snapshot,
    _cube_mode_from_payload,
    _mask_types_from_payload,
    _pipeline_timeline_snapshot,
    _timeline_breakdown_rows,
)


def test_mask_types_are_read_from_request_payload_for_pipeline_timeline() -> None:
    payload = {
        "request": {
            "mask_types": ["water", "cloud"],
        },
        "metadata": {},
        "pipeline_metadata": {},
        "conversion_metadata": {},
    }

    assert _mask_types_from_payload(payload) == ("water", "cloud")


def test_cube_mode_is_read_from_request_payload_for_pipeline_timeline() -> None:
    payload = {
        "request": {
            "cube_mode": "before_mask",
        },
        "metadata": {},
        "pipeline_metadata": {},
        "conversion_metadata": {},
    }

    assert _cube_mode_from_payload(payload) == "before_mask"


def test_conversion_progress_snapshot_prefers_runtime_counts() -> None:
    payload = {
        "raw_outputs": ["a", "b", "c", "d", "e", "f"],
        "zarr_outputs": ["z1"],
        "pipeline_metadata": {
            "raw_output_count": 6,
            "zarr_output_count": 2,
            "zarr_parallel_workers": 3,
        },
        "conversion_metadata": {
            "items_completed": 2,
            "items_active": 3,
            "items_total": 6,
            "parallel_workers": 3,
            "current_index": 1,
        },
    }

    assert _conversion_progress_snapshot(payload) == {
        "total": 6,
        "completed": 2,
        "active": 3,
        "workers": 3,
        "current_index": 3,
    }


def test_timeline_breakdown_rows_group_repeating_convert_steps() -> None:
    item = {
        "raw_outputs": ["a", "b", "c", "d", "e", "f"],
        "pipeline_metadata": {
            "raw_output_count": 6,
            "zarr_parallel_workers": 3,
        },
        "conversion_metadata": {
            "items_completed": 2,
            "items_active": 3,
            "items_total": 6,
            "parallel_workers": 3,
            "current_index": 1,
        },
    }
    timeline = {
        "updated_at": "2026-04-21T10:26:54+00:00",
        "steps": [
            {
                "key": "searching",
                "label": "Search Catalog",
                "status": "done",
                "started_at": "2026-04-21T10:25:25+00:00",
                "finished_at": "2026-04-21T10:25:28+00:00",
                "duration_seconds": 3.0,
                "group": "search",
                "group_label": "Search",
            },
            {
                "key": "writing_chunks",
                "label": "Write Zarr Chunks",
                "status": "done",
                "started_at": "2026-04-21T10:25:30+00:00",
                "finished_at": "2026-04-21T10:26:37+00:00",
                "duration_seconds": 67.0,
                "group": "convert",
                "group_label": "Convert",
            },
            {
                "key": "registering_artifact",
                "label": "Register Zarr Artifact",
                "status": "done",
                "started_at": "2026-04-21T10:26:37+00:00",
                "finished_at": "2026-04-21T10:26:42+00:00",
                "duration_seconds": 5.0,
                "group": "convert",
                "group_label": "Convert",
            },
            {
                "key": "writing_chunks",
                "label": "Write Zarr Chunks",
                "status": "running",
                "started_at": "2026-04-21T10:26:42+00:00",
                "finished_at": None,
                "duration_seconds": 12.0,
                "group": "convert",
                "group_label": "Convert",
            },
        ],
    }

    rows, grouped = _timeline_breakdown_rows(item, timeline)

    assert grouped is True
    assert len(rows) == 2
    assert rows[0]["step"] == "Search Catalog"
    assert rows[1]["step"] == "Convert Raw Scenes"
    assert rows[1]["status"] == "running"
    assert rows[1]["detail"] == (
        "Current action: Write Zarr Chunks · 2/6 completed · 3 active · 3 workers"
    )


def test_pipeline_timeline_snapshot_heals_terminal_display_gaps(monkeypatch) -> None:
    def _stale_refresh(*_args, **_kwargs):
        return {
            "job_kind": "fetch",
            "job_state": "succeeded",
            "pipeline_state": "masked_zarr_written",
            "current_stage": "ready",
            "current_stage_label": "Ready",
            "updated_at": "2026-04-22T10:25:00+00:00",
            "stages": [
                {"key": "search", "label": "Search", "badge": "SRCH", "status": "done"},
                {"key": "download", "label": "Download", "badge": "DL", "status": "done"},
                {"key": "convert", "label": "Convert", "badge": "CNV", "status": "done"},
                {"key": "cube", "label": "Cube", "badge": "CUBE", "status": "pending"},
                {"key": "cloud", "label": "Cloud", "badge": "CLD", "status": "pending"},
                {"key": "water", "label": "Water", "badge": "WTR", "status": "pending"},
                {
                    "key": "ready",
                    "label": "Ready",
                    "badge": "RDY",
                    "status": "done",
                    "detail_label": "Masked Zarr Ready",
                    "started_at": "2026-04-22T10:24:59+00:00",
                    "finished_at": "2026-04-22T10:25:00+00:00",
                    "duration_seconds": 1.0,
                },
            ],
        }

    monkeypatch.setattr(app_module, "refresh_pipeline_timeline", _stale_refresh)

    snapshot = _pipeline_timeline_snapshot(
        {
            "state": "succeeded",
            "pipeline_state": "masked_zarr_written",
            "request": {
                "mask_types": ["cloud", "water"],
                "cube_mode": "before_mask",
            },
            "pipeline_timeline": {
                "steps": [{"key": "masked_zarr_written", "status": "done"}],
            },
        }
    )

    assert snapshot["current_stage"] == "ready"
    assert snapshot["visual_normalized"] is True
    assert [stage["status"] for stage in snapshot["stages"]] == [
        "done",
        "done",
        "done",
        "done",
        "done",
        "done",
        "done",
    ]


def test_pipeline_timeline_snapshot_marks_ready_done_after_terminal_cube_stage(monkeypatch) -> None:
    def _stale_refresh(*_args, **_kwargs):
        return {
            "job_kind": "fetch",
            "job_state": "succeeded",
            "pipeline_state": "cube_written",
            "current_stage": "cube",
            "current_stage_label": "Cube",
            "updated_at": "2026-04-22T10:25:00+00:00",
            "stages": [
                {"key": "search", "label": "Search", "badge": "SRCH", "status": "done"},
                {"key": "download", "label": "Download", "badge": "DL", "status": "done"},
                {"key": "convert", "label": "Convert", "badge": "CNV", "status": "done"},
                {
                    "key": "cube",
                    "label": "Cube",
                    "badge": "CUBE",
                    "status": "done",
                    "started_at": "2026-04-22T10:24:50+00:00",
                    "finished_at": "2026-04-22T10:25:00+00:00",
                    "duration_seconds": 10.0,
                },
                {"key": "ready", "label": "Ready", "badge": "RDY", "status": "pending"},
            ],
        }

    monkeypatch.setattr(app_module, "refresh_pipeline_timeline", _stale_refresh)

    snapshot = _pipeline_timeline_snapshot(
        {
            "state": "succeeded",
            "pipeline_state": "cube_written",
            "request": {
                "cube_mode": "before_mask",
            },
            "pipeline_timeline": {
                "steps": [{"key": "cube_written", "status": "done"}],
            },
        }
    )

    assert snapshot["current_stage"] == "ready"
    assert [stage["status"] for stage in snapshot["stages"]] == [
        "done",
        "done",
        "done",
        "done",
        "done",
    ]
