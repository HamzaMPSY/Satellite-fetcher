from __future__ import annotations

import datetime as dt
import json
import time
from typing import Any

FINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}


def parse_sse_lines(lines: list[str]) -> tuple[list[dict[str, Any]], int]:
    events: list[dict[str, Any]] = []
    event_type = ""
    event_id = 0
    max_id = 0

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.startswith("id:"):
            try:
                event_id = int(line[3:].strip())
                if event_id > max_id:
                    max_id = event_id
            except Exception:
                event_id = 0
            continue

        if line.startswith("event:"):
            event_type = line[6:].strip()
            continue

        if not line.startswith("data:"):
            continue

        try:
            payload = json.loads(line[5:].strip())
        except Exception:
            continue

        if event_type and "type" not in payload:
            payload["type"] = event_type
        if event_id and "id" not in payload:
            payload["id"] = event_id

        events.append(payload)

    return events, max_id


def merge_status_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        job_id = str(row.get("job_id", "")).strip()
        if job_id:
            merged[job_id] = row
    return merged


def build_job_payload(
    *,
    provider: str,
    collection: str,
    product_type: str,
    start_date: dt.date,
    end_date: dt.date,
    aoi_wkt: str,
    tile_id: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_type": "search_download",
        "provider": provider,
        "collection": collection,
        "product_type": product_type,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "aoi": {"wkt": aoi_wkt},
    }
    if tile_id:
        payload["tile_id"] = tile_id
    if output_dir:
        payload["output_dir"] = output_dir
    return payload


def summarize_statuses(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    total_jobs = len(statuses)
    active_jobs = len(
        [
            item
            for item in statuses
            if str(item.get("state", "")) in {"queued", "running", "cancel_requested"}
        ]
    )
    succeeded_jobs = len([item for item in statuses if str(item.get("state", "")) == "succeeded"])
    failed_jobs = len([item for item in statuses if str(item.get("state", "")) == "failed"])
    cancelled_jobs = len([item for item in statuses if str(item.get("state", "")) == "cancelled"])

    bytes_downloaded = sum(int(item.get("bytes_downloaded", 0) or 0) for item in statuses)
    bytes_total = sum(int(item.get("bytes_total", 0) or 0) for item in statuses)
    progress = (100.0 * bytes_downloaded / bytes_total) if bytes_total > 0 else 0.0

    return {
        "total_jobs": total_jobs,
        "active_jobs": active_jobs,
        "succeeded_jobs": succeeded_jobs,
        "failed_jobs": failed_jobs,
        "cancelled_jobs": cancelled_jobs,
        "bytes_downloaded": bytes_downloaded,
        "bytes_total": bytes_total,
        "progress": progress,
    }


def filter_active_job_ids(cache: dict[str, dict[str, Any]]) -> list[str]:
    active_ids: list[str] = []
    for job_id, status in cache.items():
        state = str(status.get("state", ""))
        if state and state not in FINAL_JOB_STATES:
            active_ids.append(job_id)
    return active_ids


def should_poll_fallback(
    *,
    last_sse_ok: float,
    now_ts: float | None = None,
    silence_seconds: float = 8.0,
) -> bool:
    now_value = time.time() if now_ts is None else now_ts
    return (now_value - float(last_sse_ok or 0.0)) >= float(silence_seconds)
