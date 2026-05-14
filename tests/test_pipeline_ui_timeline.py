from __future__ import annotations

import nimbuschain_fetch_ui.app as app_module
from nimbuschain_fetch_ui.app import (
    _conversion_progress_snapshot,
    _cube_mode_from_payload,
    _current_stage_elapsed_label,
    _current_timeline_stage,
    _display_pipeline_stages,
    _format_runtime_duration,
    _job_pipeline_path_lines,
    _job_pipeline_style,
    _job_pipeline_summary,
    _job_pipeline_substate,
    _job_elapsed_seconds,
    _job_has_pipeline_warnings,
    _default_tile_system_for_provider,
    _mask_types_from_payload,
    _pipeline_timeline_snapshot,
    _stage_elapsed_seconds,
    _stage_status_label_for_display,
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
    assert rows[1]["step"] == "Zarr Conversion"
    assert rows[1]["stage"] == "Zarr"
    assert rows[1]["status"] == "running"
    assert rows[1]["detail"] == (
        "Current action: Write Zarr Chunks · 2/6 completed · 3 active · 3 workers"
    )


def test_timeline_breakdown_rows_fill_missing_modular_stages() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "raw_outputs": ["/data/raw/a.tar", "/data/raw/b.tar"],
        "zarr_outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
        "pipeline_metadata": {
            "sen2like_status": "raw_fallback",
            "sen2like_fallback_reason": "sen2like_resource_exhausted",
            "zarr_input_source": "raw",
            "cube_status": "skipped",
            "cube_reason": "no_groups_with_multiple_times",
            "mask_status": "written",
            "mask_types": ["water", "cloud"],
            "mask_total_scenes": 2,
            "mask_completed_scenes": 2,
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
                {"name": "cube", "depends_on": ["zarr"]},
                {"name": "mask", "depends_on": ["cube"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "outputs": ["/data/raw/a.tar", "/data/raw/b.tar"]},
                {
                    "name": "sen2like",
                    "status": "succeeded",
                    "outputs": ["/data/raw/a.tar", "/data/raw/b.tar"],
                    "metadata": {"fallback_to_raw": True, "sen2like_status": "raw_fallback"},
                },
                {"name": "zarr", "status": "succeeded", "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"]},
                {"name": "cube", "status": "skipped", "metadata": {"reason": "no_groups_with_multiple_times"}},
                {
                    "name": "mask",
                    "status": "succeeded",
                    "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
                    "metadata": {
                        "mask_status": "written",
                        "mask_types": ["water", "cloud"],
                        "mask_total_scenes": 2,
                        "mask_completed_scenes": 2,
                    },
                },
            ],
        },
    }
    timeline = {
        "steps": [
            {"key": "searching", "label": "Search Catalog", "status": "done", "group_label": "Search"},
            {"key": "downloading", "label": "Download Files", "status": "done", "group_label": "Download"},
            {"key": "masked_zarr_written", "label": "Pipeline Ready", "status": "done", "group_label": "Ready"},
        ],
    }

    rows, grouped = _timeline_breakdown_rows(item, timeline)

    assert grouped is False
    assert [row["stage"] for row in rows] == [
        "Search",
        "Download",
        "Ready",
        "Sen2Like",
        "Zarr",
        "Cube",
        "Mask",
    ]
    assert rows[3]["status"] == "fallback"
    assert rows[5]["status"] == "skipped"
    assert rows[6]["detail"] == "OK: Water + Cloud masks written in-place on 2/2 scenes"


def test_pipeline_display_stages_match_landsat_dag_vocabulary() -> None:
    item = {
        "provider": "usgs",
        "collection": "landsat_ot_c2_l1",
        "product_type": "L1TP",
    }
    stages = [
        {"key": "search", "label": "Search", "badge": "SRCH", "status": "done", "duration_seconds": 1.0},
        {"key": "download", "label": "Download", "badge": "DL", "status": "done", "duration_seconds": 2.0},
        {"key": "convert", "label": "Convert", "badge": "CNV", "status": "done", "duration_seconds": 3.0},
        {"key": "ready", "label": "Ready", "badge": "RDY", "status": "done", "duration_seconds": 0.0},
    ]

    display_stages = _display_pipeline_stages(item, {}, stages)

    assert [stage["key"] for stage in display_stages] == ["fetch", "sen2like", "zarr", "ready"]
    assert [stage["label"] for stage in display_stages] == ["Fetch", "Sen2Like", "Zarr", "Ready"]
    assert display_stages[0]["detail_label"] == "Search catalog + download files"
    assert display_stages[1]["detail_label"] == "Landsat normalized before Zarr"


def test_pipeline_display_prefers_modular_stage_results() -> None:
    item = {
        "pipeline_metadata": {
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
            ],
            "stage_results": [
                {
                    "name": "fetch",
                    "status": "succeeded",
                    "outputs": ["/data/raw/LC08"],
                    "duration_seconds": 2.5,
                },
                {
                    "name": "sen2like",
                    "status": "skipped",
                    "metadata": {"reason": "sen2like_runtime_not_routed_yet"},
                    "duration_seconds": 0.0,
                },
                {
                    "name": "zarr",
                    "status": "succeeded",
                    "outputs": ["/data/zarr/LC08.zarr"],
                    "duration_seconds": 5.0,
                },
            ],
        },
    }
    legacy_timeline_stages = [
        {"key": "search", "label": "Search", "status": "done"},
        {"key": "download", "label": "Download", "status": "done"},
        {"key": "convert", "label": "Convert", "status": "done"},
    ]

    display_stages = _display_pipeline_stages(item, {}, legacy_timeline_stages)

    assert [stage["key"] for stage in display_stages] == ["fetch", "sen2like", "zarr"]
    assert [stage["status"] for stage in display_stages] == ["done", "skipped", "done"]
    assert display_stages[0]["detail_label"] == "Provider fetch · 1 output"
    assert display_stages[1]["detail_label"] == "Skipped: Sen2Like runtime is not routed yet"
    assert display_stages[2]["duration_seconds"] == 5.0


def test_pipeline_display_heals_optional_cube_skip_after_resumed_masks() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "zarr_outputs": [
            "/data/zarr/a.zarr",
            "/data/zarr/b.zarr",
        ],
        "pipeline_metadata": {
            "cube_status": "skipped",
            "mask_status": "written",
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "zarr", "depends_on": ["fetch"]},
                {"name": "cube", "depends_on": ["zarr"]},
                {"name": "mask", "depends_on": ["cube"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "outputs": ["/data/raw/a.tar"]},
                {"name": "zarr", "status": "succeeded", "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"]},
                {
                    "name": "cube",
                    "status": "failed",
                    "error": (
                        "Zarr cube build request was rejected: Daily mosaic cube is currently "
                        "supported only for Sentinel-2 scene Zarr inputs."
                    ),
                },
                {
                    "name": "mask",
                    "status": "skipped",
                    "metadata": {"reason": "dependency_not_succeeded"},
                },
            ],
        },
    }

    display_stages = _display_pipeline_stages(item, {}, [])

    assert [stage["key"] for stage in display_stages] == ["fetch", "zarr", "cube", "mask"]
    assert [stage["status"] for stage in display_stages] == ["done", "done", "skipped", "done"]
    assert display_stages[2]["detail_label"] == (
        "Skipped: daily mosaic cubes currently require Sentinel-2 scene inputs"
    )
    assert display_stages[3]["detail_label"] == "OK: Masks written in-place on 2 scenes"


def test_pipeline_display_explains_landsat_raw_fallback_cube_skip_and_masks() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "zarr_outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
        "request": {
            "mask_types": ["water", "cloud"],
            "cube_mode": "before_mask",
        },
        "pipeline_metadata": {
            "sen2like_status": "raw_fallback",
            "sen2like_fallback_reason": "sen2like_resource_exhausted",
            "zarr_input_source": "raw",
            "cube_status": "skipped",
            "cube_reason": "no_groups_with_multiple_times",
            "cube_tiles_skipped": [
                {"group_key": "LC8198023", "reason": "fewer_than_two_unique_times"},
            ],
            "mask_status": "written",
            "mask_types": ["water", "cloud"],
            "mask_total_scenes": 2,
            "mask_completed_scenes": 2,
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
                {"name": "cube", "depends_on": ["zarr"]},
                {"name": "mask", "depends_on": ["cube"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "outputs": ["/data/raw/a.tar", "/data/raw/b.tar"]},
                {
                    "name": "sen2like",
                    "status": "succeeded",
                    "outputs": ["/data/raw/a.tar", "/data/raw/b.tar"],
                    "metadata": {
                        "fallback_to_raw": True,
                        "sen2like_status": "raw_fallback",
                        "fallback_reason": "sen2like_resource_exhausted",
                        "zarr_input_source": "raw",
                    },
                },
                {"name": "zarr", "status": "succeeded", "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"]},
                {
                    "name": "cube",
                    "status": "skipped",
                    "metadata": {
                        "reason": "no_groups_with_multiple_times",
                        "cube_tiles_skipped": [
                            {"group_key": "LC8198023", "reason": "fewer_than_two_unique_times"},
                        ],
                    },
                },
                {
                    "name": "mask",
                    "status": "succeeded",
                    "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
                    "metadata": {
                        "mask_status": "written",
                        "mask_types": ["water", "cloud"],
                        "mask_total_scenes": 2,
                        "mask_completed_scenes": 2,
                    },
                },
            ],
        },
    }

    display_stages = _display_pipeline_stages(item, {}, [])

    assert [stage["status"] for stage in display_stages] == [
        "done",
        "done",
        "done",
        "skipped",
        "done",
    ]
    assert display_stages[1]["detail_label"] == (
        "Sen2Like hit a memory limit; raw Landsat inputs were used for Zarr"
    )
    assert display_stages[1]["metadata"]["status_label"] == "fallback"
    assert _stage_status_label_for_display(display_stages[1], display_stages[1]["status"]) == "fallback"
    assert display_stages[2]["metadata"]["raw_fallback"] is True
    assert display_stages[2]["detail_label"] == (
        "Zarr conversion from raw Landsat fallback after Sen2Like memory kill · 2 outputs"
    )
    assert display_stages[3]["detail_label"] == (
        "Skipped: no cube built because each group has fewer than two acquisition times; "
        "select at least two dates for the same tile/path-row"
    )
    assert display_stages[4]["detail_label"] == "OK: Water + Cloud masks written in-place on 2/2 scenes"


def test_pipeline_ready_badge_warns_for_fallbacks_and_skips() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "zarr_outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
        "request": {
            "mask_types": ["water", "cloud"],
            "cube_mode": "before_mask",
        },
        "pipeline_metadata": {
            "sen2like_status": "raw_fallback",
            "zarr_input_source": "raw",
            "cube_status": "skipped",
            "mask_status": "written",
            "mask_mode": "integrated",
            "mask_types": ["water", "cloud"],
        },
    }

    assert _job_has_pipeline_warnings(item) is True
    assert _job_pipeline_style(item)[0] == "ready with caveats"
    assert app_module._terminal_pipeline_label(item) == "Ready with caveats"
    assert "ready with caveats" in str(_job_pipeline_summary(item))
    assert _job_pipeline_substate(item) == "Pipeline caveats: Sen2Like raw fallback, cube skipped."


def test_pipeline_display_heals_legacy_landsat_fallback_stage_results() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "raw_outputs": ["/data/raw/a.tar", "/data/raw/b.tar"],
        "zarr_outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"],
        "pipeline_metadata": {
            "sen2like_status": "raw_fallback",
            "sen2like_fallback_reason": "sen2like_resource_exhausted",
            "zarr_input_source": "raw",
            "zarr_input_outputs": ["/data/raw/a.tar", "/data/raw/b.tar"],
            "cube_status": "skipped",
            "cube_reason": "no_groups_with_multiple_times",
            "mask_status": "written",
            "mask_types": ["water", "cloud"],
            "mask_total_scenes": 2,
            "mask_completed_scenes": 2,
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
                {"name": "cube", "depends_on": ["zarr"]},
                {"name": "mask", "depends_on": ["cube"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "outputs": ["/data/raw/a.tar", "/data/raw/b.tar"]},
                {
                    "name": "sen2like",
                    "status": "skipped",
                    "outputs": [],
                    "metadata": {
                        "reason": "sen2like_runtime_not_routed_yet",
                        "runner": "pending_service_routing",
                    },
                },
                {"name": "zarr", "status": "succeeded", "outputs": ["/data/zarr/a.zarr", "/data/zarr/b.zarr"]},
                {
                    "name": "cube",
                    "status": "skipped",
                    "metadata": {"runner": "cube_builder", "cube_mode": "before_mask"},
                },
                {
                    "name": "mask",
                    "status": "succeeded",
                    "outputs": [],
                    "metadata": {"mask_status": "written"},
                },
            ],
        },
    }

    display_stages = _display_pipeline_stages(item, {}, [])

    assert [stage["status"] for stage in display_stages] == [
        "done",
        "done",
        "done",
        "skipped",
        "done",
    ]
    assert display_stages[1]["outputs"] == ["/data/raw/a.tar", "/data/raw/b.tar"]
    assert display_stages[1]["detail_label"] == (
        "Sen2Like hit a memory limit; raw Landsat inputs were used for Zarr"
    )
    assert display_stages[3]["detail_label"] == (
        "Skipped: no cube built because each group has fewer than two acquisition times; "
        "select at least two dates for the same tile/path-row"
    )
    assert display_stages[4]["outputs"] == ["/data/zarr/a.zarr", "/data/zarr/b.zarr"]
    assert display_stages[4]["detail_label"] == "OK: Water + Cloud masks written in-place on 2/2 scenes"


def test_pipeline_display_compacts_structured_sen2like_errors() -> None:
    item = {
        "pipeline_metadata": {
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
            ],
            "stage_results": [
                {
                    "name": "fetch",
                    "status": "succeeded",
                    "outputs": ["/data/raw/LC08"],
                    "duration_seconds": 2.5,
                },
                {
                    "name": "sen2like",
                    "status": "failed",
                    "error": (
                        "Sen2Like service failed: {&#x27;status&#x27;: &#x27;failed&#x27;, "
                        "&#x27;duration_seconds&#x27;: 2.49, &#x27;return_code&#x27;: 1, "
                        "&#x27;stderr_tail&#x27;: &#x27;java.io.IOException: Failed to create "
                        "a temp directory under /data/downloads/sen2like/spark&#x27;}"
                    ),
                    "duration_seconds": 2.49,
                },
            ],
        },
    }

    display_stages = _display_pipeline_stages(item, {}, [])

    assert display_stages[1]["detail_label"] == "Spark could not create its temporary directory."


def test_job_elapsed_prefers_stage_runtime_over_stale_row_duration() -> None:
    item = {
        "state": "failed",
        "pipeline_state": "sen2like_failed",
        "duration_seconds": 8 * 60 * 60,
        "pipeline_metadata": {
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "duration_seconds": 926.0},
                {"name": "sen2like", "status": "failed", "duration_seconds": 2.5, "error": "boom"},
                {
                    "name": "zarr",
                    "status": "skipped",
                    "duration_seconds": 0.0,
                    "metadata": {"reason": "dependency_not_succeeded"},
                },
            ],
        },
    }

    assert _job_elapsed_seconds(item) == 928.5
    assert _format_runtime_duration(_job_elapsed_seconds(item)) == "15m 28s"


def test_job_elapsed_uses_orchestrator_window_when_stage_results_are_partial() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "pipeline_metadata": {
            "orchestrator": {
                "started_at": "2026-05-14T10:00:00+00:00",
                "finished_at": "2026-05-14T10:02:47+00:00",
            },
            "stage_plan": [
                {"name": "fetch", "depends_on": []},
                {"name": "sen2like", "depends_on": ["fetch"]},
                {"name": "zarr", "depends_on": ["sen2like"]},
                {"name": "cube", "depends_on": ["zarr"]},
                {"name": "mask", "depends_on": ["cube"]},
            ],
            "stage_results": [
                {"name": "fetch", "status": "succeeded", "duration_seconds": 3.2},
                {"name": "sen2like", "status": "succeeded", "duration_seconds": 0.0},
                {"name": "zarr", "status": "succeeded", "duration_seconds": 0.0},
                {"name": "cube", "status": "skipped", "duration_seconds": 0.0},
                {"name": "mask", "status": "succeeded", "duration_seconds": 0.0},
            ],
        },
    }

    assert _job_elapsed_seconds(item) == 167.0
    assert _format_runtime_duration(_job_elapsed_seconds(item)) == "2m 47s"
    current_stage = _current_timeline_stage(item)
    assert (current_stage or {})["key"] == "ready"
    assert _current_stage_elapsed_label(
        item,
        current_stage=current_stage,
        job_duration=_job_elapsed_seconds(item),
    ) == "2m 47s"


def test_completed_stage_zero_duration_without_timing_is_hidden() -> None:
    assert _stage_elapsed_seconds({"status": "done", "duration_seconds": 0.0}) is None


def test_pipeline_path_lines_pair_multi_scene_outputs_by_index() -> None:
    item = {
        "state": "succeeded",
        "pipeline_state": "masked_zarr_written",
        "raw_outputs": [
            "/data/raw/LC81980232026133LGN00.tar",
            "/data/raw/LC82000232026131LGN01.tar",
        ],
        "zarr_outputs": [
            "/data/zarr/LC81980232026133LGN00.zarr",
            "/data/zarr/LC82000232026131LGN01.zarr",
        ],
    }

    assert _job_pipeline_path_lines(item) == [
        "Sources: 2 raw files",
        "Zarr stores: 2",
        "1. LC81980232026133LGN00.tar -> LC81980232026133LGN00.zarr",
        "2. LC82000232026131LGN01.tar -> LC82000232026131LGN01.zarr",
    ]


def test_default_tile_system_follows_provider_when_available() -> None:
    sat_tiles = {
        "sentinel-2": {"tiles": object()},
        "landsat": {"tiles": object()},
    }

    assert _default_tile_system_for_provider("USGS", sat_tiles) == "landsat"
    assert _default_tile_system_for_provider("Copernicus", sat_tiles) == "sentinel-2"


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
