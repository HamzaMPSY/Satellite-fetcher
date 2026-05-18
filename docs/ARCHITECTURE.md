# Architecture

## 1. System overview

NimbusChain Fetch is split into five main runtime components with explicit boundaries, but the backend/orchestrator is the only public API surface.

```text
Browser
  -> nimbus-ui (Streamlit)
       -> nimbus-api (FastAPI orchestration layer)
            -> MongoDB or SQLite store
            -> nimbus-worker (download + Zarr execution)
            -> nimbus-zarr runtime/library (internal conversion code)
            -> nimbus-mask-service (cloud/water masking runtime)
```

The UI never executes downloads directly. It submits jobs to the API. The worker owns execution from search to download to Zarr conversion. The mask service owns cloud and water masking on existing Zarr outputs. The Zarr package is reused as worker-internal conversion logic, while the backend exposes converter and mask health/schema endpoints for the UI.

## 2. Runtime responsibilities

### `nimbus-ui`

Main role:
- AOI editing and tile selection
- product preview from provider APIs
- job creation, filtering, and operational monitoring
- manual Zarr conversion and artifact browsing

Key folders:
- `src/nimbuschain_fetch_ui/app.py`: Streamlit entrypoint
- `src/nimbuschain_fetch_ui/component_leaflet.py`: map component bridge
- `src/nimbuschain_fetch_ui/jobs_helpers.py`: API access and job filtering helpers
- `src/nimbuschain_fetch_ui/zarr_utils.py`: Zarr discovery and registration helpers
- `src/nimbuschain_fetch_ui/runtime_status.py`: UI runtime status collection and rendering

### `nimbus-api`

Main role:
- expose the public contract consumed by the UI
- create, list, inspect, cancel and filter jobs
- publish events and metrics
- register artifacts and expose health/readiness status
- expose worker capacity status
- expose converter health/readiness/schema and manual conversion routes under `/v1`
- expose mask health/schema and manual mask routes under `/v1`

Key folders:
- `src/nimbuschain_fetch_service/main.py`: FastAPI entrypoint
- `src/nimbuschain_fetch_service/api/`: routers
- `src/nimbuschain_fetch/engine/`: orchestration engine
- `src/nimbuschain_fetch/jobs/`: job store implementations

### `nimbus-worker`

Main role:
- poll the job store
- claim queued jobs
- execute provider-specific downloads
- trigger Zarr conversion automatically after successful download
- persist raw outputs, Zarr outputs, result paths and events under the same `job_id`
- heartbeat execution capacity back to the store

Key code:
- `src/nimbuschain_fetch/worker.py`
- `src/nimbuschain_fetch/providers/`

### `nimbus-zarr` runtime

Main role:
- read local raw scenes or archives
- preserve all native physical imagery layers from supported products using exact source layer names
- route QA, masks, classification, cloud, snow, angle, aerosol, and other support rasters into ancillary arrays
- write `imagery(time, band, y, x)` Zarr stores, plus `ancillary(time, ancillary_layer, y, x)` when needed
- provide reusable conversion code used by the worker and by backend-owned manual conversion routes

Key code:
- `src/nimbuschain_zarr_service/main.py`
- `src/nimbuschain_zarr_service/service.py`
- `src/nimbuschain_zarr_service/copernicus.py`
- `src/nimbuschain_zarr_service/landsat.py`
- `src/nimbuschain_zarr_service/core.py`

### `nimbus-mask-service`

Main role:
- apply cloud and water masks to an existing Zarr store
- support in-place masking and derived masked-store outputs
- expose a small internal FastAPI harness for health, schema, and remote apply calls
- support device-aware inference selection across `cpu`, `cuda`, and `mps`

Key code:
- `src/nimbuschain_mask_service/main.py`
- `src/nimbuschain_mask_service/service.py`
- `src/nimbuschain_mask_service/inference.py`
- `src/nimbuschain_mask_service/omniwater.py`
- `src/nimbuschain_mask_service/io.py`

## 3. Data flow

### Download flow

```text
UI form
  -> POST /v1/jobs
  -> job store persists job as queued
  -> worker claims job
  -> provider search/download executes
  -> raw outputs persisted
  -> worker runs Zarr conversion in-process
  -> raw + zarr outputs persisted on the same job
  -> optional mask job created for existing Zarr output
  -> events + pipeline status updates emitted
  -> UI reads jobs/events/results
```

### Zarr conversion flow

```text
raw scene folder/archive
  -> POST /v1/jobs/{job_id}/convert on nimbus-api (manual path)
  -> or automatic worker step after download success
  -> collection-specific normalization via nimbuschain_zarr_service library
  -> sensor-aware target resolution selection
  -> Zarr write under /data/downloads/zarr
  -> artifact registration through /v1/artifacts under the same job lineage
```

### Mask flow

```text
existing Zarr output
  -> POST /v1/jobs/{job_id}/mask on nimbus-api
  -> API creates a separate mask job linked to source_job_id
  -> worker resolves the selected source Zarr
  -> mask service applies cloud and/or water inference
  -> masked outputs and mask metadata written to Zarr
  -> result, artifacts, and mask-quality metadata persisted
```

## 4. Folder structure and intent

### Source code

- `src/nimbuschain_fetch/`
  Core domain logic, providers, worker logic, storage contracts.
- `src/nimbuschain_fetch_service/`
  Public API layer over the engine, stores, and converter-facing routes.
- `src/nimbuschain_fetch_ui/`
  Streamlit presentation layer and client-side orchestration.
- `src/nimbuschain_zarr_service/`
  Conversion logic reused by the worker and converter routes.
- `src/nimbuschain_mask_service/`
  Internal masking runtime and remote/client adapter.
### Infrastructure and operations

- `Containerfile`
  API and worker image.
- `ui/Containerfile`
  UI image, including tracked tile files for standalone fallback.
- `zarr-service/Containerfile`
  Zarr service image.
- `mask-service/Containerfile`
  Mask service image.
- `podman-compose.yml`
  Local Podman stack.
- `docker-compose.yml`
  Docker-compatible compose stack.
- `k8s/`
  Kubernetes manifests.
- `scripts/`
  Operational entrypoints for local run and Minikube.

## 5. Architecture strengths

Already good in the current project:
- service split is clear and justified
- UI, API, worker and converter-library boundaries are explicit
- API contracts are already stable enough for a real workflow
- conversion concerns are separated from provider download code
- the project is runnable locally with a small number of commands

## 6. Main technical debt still intentionally accepted

These points are known but not rewritten away because the project should stay faithful to its current scope:
- the Streamlit UI remains the main frontend shell instead of migrating to a JS SPA
- MongoDB remains the default operational store for simplicity
- Kubernetes manifests target a pragmatic local/minikube setup first
- not all Copernicus families are fully exercised by the available local data at all times
