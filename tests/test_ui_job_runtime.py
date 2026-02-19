from __future__ import annotations

import datetime as dt

from nimbuschain_fetch_ui.aoi_utils import parse_aoi_text
from nimbuschain_fetch_ui.job_api_runtime import (
    build_job_payload,
    filter_active_job_ids,
    merge_status_rows,
    parse_sse_lines,
    should_poll_fallback,
    summarize_statuses,
)


def test_parse_aoi_text_wkt_and_geojson() -> None:
    wkt_geom = parse_aoi_text("POLYGON((0 0,0 1,1 1,1 0,0 0))")
    assert wkt_geom is not None
    assert round(float(wkt_geom.area), 3) == 1.0

    geojson_geom = parse_aoi_text(
        '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[0,2],[2,2],[2,0],[0,0]]]}}]}'
    )
    assert geojson_geom is not None
    assert round(float(geojson_geom.area), 3) == 4.0


def test_build_job_payload_with_tile_id() -> None:
    payload = build_job_payload(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 3),
        aoi_wkt="POLYGON((0 0,0 1,1 1,1 0,0 0))",
        tile_id="31TFJ",
    )

    assert payload["job_type"] == "search_download"
    assert payload["provider"] == "copernicus"
    assert payload["tile_id"] == "31TFJ"
    assert payload["aoi"]["wkt"].startswith("POLYGON")


def test_parse_sse_lines_and_merge_rows() -> None:
    lines = [
        "id: 7",
        "event: job.progress",
        'data: {"job_id":"abc","payload":{"status":"running"}}',
        "",
        "id: 8",
        "event: job.succeeded",
        'data: {"job_id":"abc","payload":{"status":"succeeded"}}',
    ]
    events, max_id = parse_sse_lines(lines)

    assert len(events) == 2
    assert max_id == 8
    assert events[0]["id"] == 7
    assert events[1]["type"] == "job.succeeded"

    merged = merge_status_rows(
        [
            {"job_id": "abc", "state": "running"},
            {"job_id": "abc", "state": "succeeded"},
            {"job_id": "def", "state": "failed"},
        ]
    )
    assert merged["abc"]["state"] == "succeeded"
    assert merged["def"]["state"] == "failed"


def test_poll_fallback_and_summary() -> None:
    assert should_poll_fallback(last_sse_ok=100.0, now_ts=111.0, silence_seconds=8.0)
    assert not should_poll_fallback(last_sse_ok=100.0, now_ts=103.0, silence_seconds=8.0)

    statuses = [
        {"job_id": "1", "state": "running", "bytes_downloaded": 50, "bytes_total": 100},
        {"job_id": "2", "state": "succeeded", "bytes_downloaded": 100, "bytes_total": 100},
        {"job_id": "3", "state": "failed", "bytes_downloaded": 0, "bytes_total": 0},
    ]
    summary = summarize_statuses(statuses)
    assert summary["total_jobs"] == 3
    assert summary["active_jobs"] == 1
    assert summary["succeeded_jobs"] == 1
    assert summary["failed_jobs"] == 1
    assert summary["progress"] == 75.0

    active_ids = filter_active_job_ids(
        {
            "1": {"state": "queued"},
            "2": {"state": "running"},
            "3": {"state": "succeeded"},
            "4": {"state": "cancelled"},
        }
    )
    assert active_ids == ["1", "2"]
