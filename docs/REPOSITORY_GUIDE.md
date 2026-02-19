# NimbusChain Fetch: Detailed Repository Guide

This document explains the repository in detail so a new developer can understand how each part works, how data flows, and how to operate/debug the system.

## 1) What this project is

NimbusChain Fetch is a geospatial data fetch backend with:

1. **Core Engine package** (`nimbuschain_fetch`)
   - all business logic (providers, orchestration, retries, checksums, manifest, persistence).
   - reusable directly in Python (no HTTP).

2. **FastAPI service package** (`nimbuschain_fetch_service`)
   - thin HTTP control plane over the core engine.
   - exposes REST and SSE endpoints.
   - no duplicated business logic.

3. **UI package** (`nimbuschain_fetch_ui`)
   - Streamlit UI that calls the API.

In production-like runtime, these are deployed as independent containers:
- `nimbus-api` (control plane)
- `nimbus-worker` (download execution plane)
- `nimbus-ui` (frontend)
- `mongodb` (shared store)

## 2) High-level architecture

```text
Browser/CLI/Python
      |
      +--> Streamlit UI (nimbus-ui) ---> FastAPI API (nimbus-api)
                                         |
                                         v
                                      JobStore (MongoDB/SQLite)
                                         ^
                                         |
                                 Worker process (nimbus-worker)
                                         |
                                         v
                            NimbusFetcher + Providers + Downloader
```

## 3) Repository map (what each important file does)

## Root files
- `pyproject.toml`: packaging, dependencies, pytest/ruff config, CLI entrypoint.
- `README.md`: quick start and test commands.
- `.env.example`: environment template.
- `Dockerfile`, `Containerfile`: API/worker image build instructions.
- `ui/Containerfile`: UI image build instructions.
- `docker-compose.yml`, `podman-compose.yml`: multi-service deployment (`api + worker + ui + mongodb`).

## Core package: `src/nimbuschain_fetch/`
- `__init__.py`: exports `NimbusFetcher`, `NimbusFetcherClient`.
- `settings.py`: runtime settings from env (Pydantic settings), limits, directories.
- `models.py`: strict Pydantic request/response/event models.
- `manifest.py`: manifest generation and SHA256 checksum helpers.
- `cli.py`: CLI wrapper using unified client.
- `client.py`: unified client (`direct` or `service`).
- `worker.py`: worker entrypoint used by `nimbus-worker` container.

### Geometry and security
- `geometry/aoi.py`: WKT/GeoJSON AOI parsing + Polygon/MultiPolygon validation.
- `security/paths.py`: output path sanitization, traversal protection.

### Providers
- `providers/base.py`: provider interface contract.
- `providers/copernicus.py`: Copernicus search/download implementation.
- `providers/usgs.py`: USGS M2M search/download implementation.

### Download
- `download/download_manager.py`: async downloader with concurrency, retries, cancellation, progress callbacks.

### Jobs runtime and persistence
- `jobs/store.py`: `JobStore` protocol + filter dataclass + execution claim/requeue contract.
- `jobs/store_factory.py`: backend switch (`mongodb` or `sqlite`).
- `jobs/mongodb_store.py`: Mongo implementation + indexes + startup readiness ping + stale job requeue.
- `jobs/sqlite_store.py`: SQLite implementation + schema + stale job requeue.
- `jobs/executor_base.py`: executor interface.
- `jobs/executor_inprocess.py`: async in-process scheduler with global/provider semaphores.
- `jobs/executor_backends.py`: stubs for Celery/RQ/Arq.
- `jobs/events.py`: persisted-event polling stream with heartbeat.

### Orchestrator
- `engine/nimbus_fetcher.py`: main orchestration class.

## FastAPI package: `src/nimbuschain_fetch_service/`
- `main.py`: app creation, lifespan startup/shutdown, middleware, root page, router wiring.
- `dependencies.py`: DI getters for fetcher/settings.
- `middleware.py`: API key + payload limit + request telemetry middleware.
- `api/metrics.py`: `/v1/metrics` Prometheus endpoint.
- `observability.py`: counters/gauges/histograms for HTTP + jobs.
- `logging_config.py`: plain or JSON logs based on settings.
- `api/health.py`: `/v1/health`.
- `api/jobs.py`: jobs CRUD/list/result endpoints.
- `api/events.py`: SSE endpoint.

## UI package: `src/nimbuschain_fetch_ui/`
- `app.py`: Streamlit frontend for submitting, listing, and inspecting jobs.

## Tests: `tests/`
- `conftest.py`: fake providers + helper fixtures.
- `test_models.py`: schema validation tests.
- `test_engine.py`: engine lifecycle/cancel/result tests.

## Scripts: `scripts/`
- `00_check_podman.sh`: verify/start podman machine.
- `01_test_models.sh`: run model tests in podman.
- `02_test_engine.sh`: run engine tests in podman.
- `05_test_all.sh`: run full test suite.
- `test_in_podman.sh`: shared helper for running pytest in a python container.
- `10_up_stack.sh`: start full podman stack in background.
- `11_down_stack.sh`: stop stack and remove orphans.
- `12_scale_workers.sh`: scale `nimbus-worker` replicas.

## 4) Core domain model

Defined in `src/nimbuschain_fetch/models.py`.

## Job states
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancel_requested`
- `cancelled`

## Job request types (discriminated union)

1. `search_download`
   - required: `provider`, `collection`, `product_type`, `start_date`, `end_date`, `aoi`
   - optional: `tile_id`, `output_dir`

2. `download_products`
   - required: `provider`, `collection`, `product_ids[]`
   - optional: `output_dir`

## Validation rules
- provider is strict enum: `copernicus` or `usgs`.
- collection/product_type formats validated by regex.
- `end_date >= start_date`.
- AOI must contain exactly one of `wkt` or `geojson`.
- AOI geometry must be valid `Polygon` or `MultiPolygon`.
- `output_dir` must be relative and must not contain traversal (`..`).

## 5) End-to-end execution flow

This is the internal flow for `submit_job`:

1. `NimbusFetcher.submit_job()` via API
   - validates/adapts request using Pydantic adapter.
   - creates persisted job row with `queued` state.
   - appends `job.queued` event.
   - in `api` role, returns immediately (no heavy download work in API process).

2. Worker (`nimbus-worker`) polls queued jobs and claims them atomically.

3. `InProcessExecutor` worker picks claimed job.
   - applies global semaphore (`NIMBUS_MAX_JOBS`).
   - applies per-provider semaphore (`NIMBUS_PROVIDER_LIMITS`).

4. `NimbusFetcher._execute_job()`
   - updates state to `running`, stores `started_at`.
   - sanitizes output path into `NIMBUS_DATA_DIR`.
   - creates progress callback that updates DB/events (throttled).
   - runs provider/search/download inside `anyio.to_thread.run_sync(...)` to avoid event loop blocking.

5. On completion
   - computes SHA256 checksums for artifacts.
   - writes `manifest.json` into output directory.
   - persists result payload.
   - updates job state to `succeeded` and `finished_at`.
   - appends `job.succeeded` event.

6. On error/cancel
   - transitions to `failed` or `cancelled`.
   - stores error reason/event payload.

## Restart behavior
During startup in execution roles (`worker` or `all`):
- `store.requeue_incomplete_jobs()` requeues `running`/`cancel_requested` jobs as `queued`.
- `job.requeued_after_restart` event is added per requeued job.
- queued jobs are polled and submitted continuously.
- stale running jobs are auto-requeued after `NIMBUS_STALE_JOB_SECONDS`.

## 6) Providers behavior

## Copernicus provider (`providers/copernicus.py`)
- Auth: OAuth token fetched from `NIMBUS_COPERNICUS_TOKEN_URL`.
- Search: OData filter built from collection, product type, date range, tile, AOI intersection.
- Download: product content URLs built from product IDs, then downloaded by `DownloadManager` with Bearer auth.
- Token refresh path exists in downloader retry flow (401).

## USGS provider (`providers/usgs.py`)
- Auth: `login-token` endpoint with username/token.
- Search: `scene-search` using AOI geojson and acquisition filter.
- Filter: `product_type` is matched against `displayId`.
- Download:
  - `download-options`
  - picks available `Bundle` products
  - `download-request`
  - passes resulting URLs to `DownloadManager`

Important USGS note:
- `download_products` mode needs dataset context.
- Engine sets `provider.dataset = request.collection` before direct download by product IDs.

## 7) Download manager details

`src/nimbuschain_fetch/download/download_manager.py`

- Uses `aiohttp` session.
- Supports configurable:
  - max concurrency
  - retries/backoff
  - connect/read timeouts
  - chunk size
- Retry logic:
  - retries for transient HTTP/network errors.
  - can refresh token on 401 if callback is provided.
- Cancellation:
  - checked before and during stream writes.
- Progress callback signature:
  - `(file_name, delta_bytes, downloaded_bytes_for_file, file_total_or_none)`

## 8) Persistence layer

Backend selection is env-driven via `NIMBUS_DB_BACKEND`:
- `mongodb` (default)
- `sqlite`

`store_factory.py` handles instantiation.

## MongoDB schema
Collections:
- `jobs`
- `job_events`
- `job_results`
- `counters` (event sequence)

Notable indexes:
- unique `jobs.job_id`
- unique `job_events.event_id`
- unique `job_results.job_id`

Startup readiness:
- Mongo store waits up to 60 seconds for `ping` success.

## SQLite schema
Tables:
- `jobs`
- `job_events`
- `job_results`

SQLite settings:
- WAL mode
- foreign keys enabled

## 9) FastAPI service

## Lifecycle
In `main.py` lifespan:
- build settings
- create `NimbusFetcher`
- `await fetcher.start()`
- store `fetcher/settings` in app state
- shutdown: `await fetcher.stop()`

In 3-layer runtime:
- API container uses `NIMBUS_RUNTIME_ROLE=api` (control plane only).
- Worker container uses `NIMBUS_RUNTIME_ROLE=worker` (execution plane).

## Routes
- `GET /` basic HTML status page.
- `GET /v1/health` health + UTC timestamp + runtime role + backend info.
- `POST /v1/jobs` create one job.
- `POST /v1/jobs/batch` create many jobs.
- `GET /v1/jobs/{job_id}` status.
- `DELETE /v1/jobs/{job_id}` cancel request.
- `GET /v1/jobs/{job_id}/result` result payload.
- `GET /v1/jobs` list with filters/pagination.
- `GET /v1/events` SSE progress/events stream.
- `GET /v1/metrics` Prometheus metrics.

## SSE event format
Each event frame includes:
- `id:` (when persisted event has id)
- `event:` event type
- `data:` JSON serialized `JobEvent`

Also emits `heartbeat` events while idle.

## 10) Security and safety controls

Implemented controls:
- API key middleware (`X-API-Key`) if `NIMBUS_API_KEY` is set.
- Payload limit middleware (`NIMBUS_MAX_REQUEST_MB`) returns `413` on oversized body.
- Path sanitization to keep output under `NIMBUS_DATA_DIR`.
- Pydantic strict schemas (`extra="forbid"`) for request models.

Current behavior:
- `/` and `/v1/health` are public even when API key is enabled.

## 11) Configuration reference

Primary env vars in `settings.py` and `.env.example`:

- `NIMBUS_DB_BACKEND` = `mongodb|sqlite`
- `NIMBUS_DB_PATH` (SQLite path)
- `NIMBUS_MONGODB_URI`
- `NIMBUS_MONGODB_URI_INTERNAL` (compose internal host mapping)
- `NIMBUS_MONGODB_DB`
- `NIMBUS_DATA_DIR`
- `NIMBUS_RUNTIME_ROLE` = `all|api|worker`
- `NIMBUS_MAX_JOBS`
- `NIMBUS_QUEUE_POLL_SECONDS`
- `NIMBUS_STALE_JOB_SECONDS`
- `NIMBUS_PROVIDER_LIMITS` (e.g. `copernicus=2,usgs=4`)
- `NIMBUS_LOG_LEVEL`
- `NIMBUS_LOG_JSON`
- `NIMBUS_ENABLE_METRICS`
- `NIMBUS_API_KEY`
- `NIMBUS_CORS_ORIGINS` (comma-separated)
- `NIMBUS_MAX_REQUEST_MB`
- `NIMBUS_COPERNICUS_*`
- `NIMBUS_USGS_*`
- `NIMBUS_SERVICE_URL` (UI -> API)

Container networking tip:
- In compose, use `NIMBUS_MONGODB_URI_INTERNAL=mongodb://mongodb:27017`.
- `127.0.0.1` inside the app container points to itself, not the Mongo container.

## 12) How to run

## Full stack with Podman (`api + worker + ui + mongodb`)

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
podman machine start
./scripts/10_up_stack.sh
```

Check service:

```bash
curl http://127.0.0.1:8000/v1/health
open http://127.0.0.1:8000/docs
open http://127.0.0.1:8501
```

Scale workers:

```bash
./scripts/12_scale_workers.sh 3
```

## Direct Python mode example

```python
from nimbuschain_fetch.client import NimbusFetcherClient

with NimbusFetcherClient(mode="direct") as client:
    job_id = client.submit_job({
        "job_type": "search_download",
        "provider": "copernicus",
        "collection": "SENTINEL-2",
        "product_type": "S2MSI2A",
        "start_date": "2026-01-01",
        "end_date": "2026-01-02",
        "aoi": {"wkt": "POLYGON((0 0,0 1,1 1,1 0,0 0))"}
    })
    status = client.get_job(job_id)
```

## Service mode example

```python
from nimbuschain_fetch.client import NimbusFetcherClient

with NimbusFetcherClient(mode="service", service_url="http://127.0.0.1:8000") as client:
    job_id = client.submit_job({...})
```

## CLI example

```bash
nimbuschain-fetch \
  --mode service \
  --service-url http://127.0.0.1:8000 \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --start-date 2026-01-01 \
  --end-date 2026-01-02 \
  --aoi_file ./aoi.wkt
```

## 13) Testing strategy

## Test levels
- Schema validation: `tests/test_models.py`
- Engine orchestration/cancel/result: `tests/test_engine.py`

## Run with scripts

```bash
./scripts/00_check_podman.sh
./scripts/05_test_all.sh
```

## Why fake providers in unit tests
Most unit tests use fake providers in `tests/conftest.py` to avoid external API dependency and keep tests deterministic.

## 14) Troubleshooting guide

## "MongoDB not reachable at mongodb://127.0.0.1:27017"
Cause: app runs in container, `127.0.0.1` is wrong host.
Fix: use compose internal mapping (`NIMBUS_MONGODB_URI_INTERNAL=mongodb://mongodb:27017`).

## "Copernicus credentials are missing in environment variables"
Cause: missing `NIMBUS_COPERNICUS_USERNAME` or `NIMBUS_COPERNICUS_PASSWORD`.
Fix: set both in `.env`, then recreate service container.

## Job succeeded but `products_found=0` and bytes are 0
Cause: no matching products for AOI/date/product_type.
Fix: expand date range, verify AOI coordinates and product type.

## `curl ... | python -m json.tool` fails with "Expecting value"
Cause: often empty or non-JSON response due missing `JOB_ID` variable.
Fix: print `echo "$JOB_ID"`, then call full URL directly.

## `watch: command not found` on macOS
Use a shell loop:

```bash
while true; do
  curl -s "http://127.0.0.1:8000/v1/jobs/$JOB_ID"; echo; sleep 2;
done
```

## 15) Extension points

## Add a new provider
1. Implement `ProviderBase` methods.
2. Register provider class in `NimbusFetcher.provider_registry`.
3. Update validation enum/model if exposing publicly.
4. Add tests with fake provider path first.

## Replace executor backend
- Keep `ExecutorBackend` interface.
- Add concrete backend (Celery/RQ/Arq) and wire in fetcher constructor.
- Persist state transitions/events in same store contract.

## Add richer service capabilities
Potential future improvements:
- real rate limiter middleware
- auth scopes/roles
- websocket channel (currently SSE)
- artifact storage abstraction (S3/GCS)

## 16) Known limitations (current MVP)

- Queue polling is DB-based and simple (no external broker like Redis/Kafka).
- API streaming is SSE only (no WebSocket route).
- No built-in request-level rate limiter yet.
- Production hardening (worker model, monitoring, authz) is basic.
- Integration tests against live Copernicus/USGS are manual, not part of deterministic unit suite.

---

If you are onboarding a new engineer, start with these files in this order:
1. `src/nimbuschain_fetch/models.py`
2. `src/nimbuschain_fetch/engine/nimbus_fetcher.py`
3. `src/nimbuschain_fetch/jobs/executor_inprocess.py`
4. `src/nimbuschain_fetch/providers/copernicus.py`
5. `src/nimbuschain_fetch/providers/usgs.py`
6. `src/nimbuschain_fetch_service/api/jobs.py`
7. `tests/test_engine.py` and `tests/test_models.py`
