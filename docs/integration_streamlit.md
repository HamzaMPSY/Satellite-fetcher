# Streamlit Integration Guide

This guide explains how to integrate Streamlit with this repository without managing shell subprocesses.

If you just need a ready UI container, use `src/nimbuschain_fetch_ui/app.py` with `podman-compose.yml` (`nimbus-ui` service).
The current UI keeps the legacy navigation style (`Map / Download / Results / Settings`) while using API jobs instead of subprocesses.

## Current UI contract (legacy UX, API execution)

The shipped UI reproduces the old behavior for:
- tile system and tile search/picker UX
- map AOI interaction
- download manager layout and controls
- results/settings structure

But runtime execution is now:
- submit single jobs with `POST /v1/jobs`
- submit Copernicus multi-tile runs with `POST /v1/jobs/batch`
- consume real-time events from `GET /v1/events` (SSE)
- fallback to polling `GET /v1/jobs/{job_id}`
- stop active jobs with `DELETE /v1/jobs/{job_id}`
- fetch output metadata with `GET /v1/jobs/{job_id}/result`

`Reset` and `Unlock` only reset in-memory UI tracking state.
They never remove data from `/data/downloads`.

## Local product preview in UI

The "Products Preview" panel is intentionally local in the UI container
(it does not call a dedicated backend preview endpoint):

- Copernicus: token + OData query
- USGS: login-token + scene-search

Required UI container env vars:
- `NIMBUS_COPERNICUS_BASE_URL`
- `NIMBUS_COPERNICUS_TOKEN_URL`
- `NIMBUS_COPERNICUS_DOWNLOAD_URL`
- `NIMBUS_COPERNICUS_USERNAME`
- `NIMBUS_COPERNICUS_PASSWORD`
- `NIMBUS_USGS_SERVICE_URL`
- `NIMBUS_USGS_USERNAME`
- `NIMBUS_USGS_TOKEN`

When credentials are missing/invalid, preview shows explicit warnings and the rest of UI remains usable.

## Why migrate from subprocess CLI calls

Legacy pattern (problematic):
- build shell command
- run via `subprocess`/`nohup`
- parse logs to estimate progress

Recommended pattern:
- call Python client methods (`NimbusFetcherClient`)
- read structured job state from DB-backed API/engine
- consume progress via SSE event stream

Benefits:
- deterministic states (`queued/running/succeeded/...`)
- typed responses with `progress`, `bytes_downloaded`, `duration_seconds`
- resilient restarts (jobs persisted in MongoDB or SQLite)

## Integration options

## A) Direct mode (same Python process)

Use when Streamlit and backend run on the same machine and you want lower overhead.

```python
from nimbuschain_fetch.client import NimbusFetcherClient

with NimbusFetcherClient(mode="direct") as client:
    job_id = client.submit_job(
        {
            "job_type": "search_download",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "start_date": "2026-01-01",
            "end_date": "2026-01-03",
            "aoi": {"wkt": "POLYGON((...))"},
            "output_dir": "streamlit/s2_run_001",
        }
    )
```

## B) Service mode (HTTP to FastAPI)

Use when Streamlit and backend are separated (local container or remote server).

```python
from nimbuschain_fetch.client import NimbusFetcherClient

with NimbusFetcherClient(
    mode="service",
    service_url="http://127.0.0.1:8000",
    api_key=None,  # provide key if NIMBUS_API_KEY is enabled
) as client:
    job_id = client.submit_job({...})
```

## End-to-end Streamlit flow

1. Collect user input:
   - provider, collection, date range, AOI, optional tile and output dir.
2. Build request payload.
3. Submit job using `client.submit_job(...)`.
4. Track progress using:
   - `client.stream_events(job_id=job_id)` (preferred), or
   - `client.get_job(job_id)` polling.
5. On terminal success:
   - call `client.get_result(job_id)` and display paths/checksums/manifest.
6. On cancel action:
   - call `client.cancel_job(job_id)`.

## Minimal reusable wrapper for Streamlit

```python
from nimbuschain_fetch.client import NimbusFetcherClient

FINAL_STATES = {"succeeded", "failed", "cancelled"}


def run_job_and_stream(payload: dict, mode: str = "service", service_url: str = "http://127.0.0.1:8000"):
    with NimbusFetcherClient(mode=mode, service_url=service_url) as client:
        job_id = client.submit_job(payload)
        yield {"kind": "job_created", "job_id": job_id}

        for event in client.stream_events(job_id=job_id, since=0):
            yield {"kind": "event", "event": event}
            if event.type in {"job.succeeded", "job.failed", "job.cancelled"}:
                break

        status = client.get_job(job_id)
        yield {"kind": "status", "status": status}
        if status.state.value == "succeeded":
            result = client.get_result(job_id)
            yield {"kind": "result", "result": result}
```

## Mapping from old app functions to new client calls

Replace old subprocess pieces in `/Users/mehdidinari/Desktop/nimbus/satellite-fetcher.py`:

- old: command builder (`_build_download_command`)
  - new: Python dict payload
- old: `nohup` process launch
  - new: `submit_job()`
- old: log parsing (`parse_download_logs`)
  - new: `stream_events()` + `get_job()`
- old: process PID cancellation
  - new: `cancel_job()`
- old: manual output path tracking
  - new: `get_result()` paths + checksums + manifest

## Event payloads useful for UI

Typical `job.progress` payload:
- `file`: current file name
- `bytes`: cumulative bytes downloaded by job
- `bytes_total`: estimated total bytes
- `speed`: bytes/sec estimate
- `status`: `"running"`

Terminal events:
- `job.succeeded`
- `job.failed`
- `job.cancelled`

## Deployment and CORS notes for Streamlit

If Streamlit runs separately from API:
- set `NIMBUS_CORS_ORIGINS` to include Streamlit origin, for example:
  - `http://localhost:8501`
- if API key is enabled:
  - include `X-API-Key` from Streamlit requests (service mode client supports this).

## Practical UX recommendations

- show `status.progress` as percentage bar.
- show bytes:
  - `bytes_downloaded` / `bytes_total`.
- show elapsed:
  - `duration_seconds`.
- show explicit terminal message from `errors` on failed jobs.
- persist submitted `job_id` in Streamlit session state to reconnect after rerun.

## Backend mode and persistence note

Runtime default store is MongoDB (`NIMBUS_DB_BACKEND=mongodb`), but the same client/API behavior works with SQLite (`NIMBUS_DB_BACKEND=sqlite`).
