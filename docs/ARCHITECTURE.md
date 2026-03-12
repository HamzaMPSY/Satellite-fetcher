# Architecture

## 1. System overview

NimbusChain Fetch is split into four runtime components with explicit boundaries.

```text
Browser
  -> nimbus-ui (Streamlit)
       -> nimbus-api (FastAPI orchestration layer)
            -> MongoDB or SQLite store
            -> nimbus-worker (download execution)
            -> nimbus-zarr (raw scene -> Zarr conversion)
```

The UI never executes downloads directly. It submits jobs to the API. The worker owns execution. The Zarr service owns normalization and writing of array stores.

## 2. Runtime responsibilities

### `nimbus-ui`

Main role:
- AOI editing and tile selection
- product preview from provider APIs
- job creation, filtering, and operational monitoring
- manual Zarr conversion and artifact browsing

Key folders:
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/app.py`: Streamlit entrypoint
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/component_leaflet.py`: map component bridge
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/jobs_helpers.py`: API access and job filtering helpers
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/zarr_utils.py`: Zarr discovery and registration helpers
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/runtime_status.py`: UI runtime status collection and rendering

### `nimbus-api`

Main role:
- expose the public contract consumed by the UI
- create, list, inspect, cancel and filter jobs
- publish events and metrics
- register artifacts and expose health/readiness status
- expose worker capacity status

Key folders:
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_service/main.py`: FastAPI entrypoint
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_service/api/`: routers
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch/engine/`: orchestration engine
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch/jobs/`: job store implementations

### `nimbus-worker`

Main role:
- poll the job store
- claim queued jobs
- execute provider-specific downloads
- persist result paths and events
- heartbeat execution capacity back to the store

Key code:
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch/worker.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch/providers/`

### `nimbus-zarr`

Main role:
- read local raw scenes or archives
- preserve all native physical imagery layers from supported products using exact source layer names
- route QA, masks, classification, cloud, snow, angle, aerosol, and other support rasters into ancillary arrays
- write `imagery(time, band, y, x)` Zarr stores, plus `ancillary(time, ancillary_layer, y, x)` when needed
- expose health, readiness and schema endpoints

Key code:
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/main.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/service.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/copernicus.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/landsat.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/core.py`

## 3. Data flow

### Download flow

```text
UI form
  -> POST /v1/jobs
  -> job store persists job as queued
  -> worker claims job
  -> provider download executes
  -> events + status updates emitted
  -> result paths persisted
  -> UI reads jobs/events/results
```

### Zarr conversion flow

```text
raw scene folder/archive
  -> POST /convert on nimbus-zarr
  -> collection-specific normalization
  -> sensor-aware target resolution selection
  -> Zarr write under /data/downloads/zarr
  -> artifact registration through /v1/artifacts
```

## 4. Folder structure and intent

### Source code

- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch/`
  Core domain logic, providers, worker logic, storage contracts.
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_service/`
  Thin API layer over the engine and stores.
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/`
  Streamlit presentation layer and client-side orchestration.
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/`
  Conversion logic and service API.

### Infrastructure and operations

- `/Users/mehdidinari/Desktop/backend nimbus/Containerfile`
  API and worker image.
- `/Users/mehdidinari/Desktop/backend nimbus/ui/Containerfile`
  UI image, including tracked tile files for standalone fallback.
- `/Users/mehdidinari/Desktop/backend nimbus/zarr-service/Containerfile`
  Zarr service image.
- `/Users/mehdidinari/Desktop/backend nimbus/podman-compose.yml`
  Local Podman stack.
- `/Users/mehdidinari/Desktop/backend nimbus/docker-compose.yml`
  Docker-compatible compose stack.
- `/Users/mehdidinari/Desktop/backend nimbus/k8s/`
  Kubernetes manifests.
- `/Users/mehdidinari/Desktop/backend nimbus/scripts/`
  Operational entrypoints for local run and Minikube.

## 5. Architecture strengths

Already good in the current project:
- service split is clear and justified
- UI, API, worker and converter boundaries are explicit
- API contracts are already stable enough for a real workflow
- conversion concerns are separated from the downloader
- the project is runnable locally with a small number of commands

## 6. Main technical debt still intentionally accepted

These points are known but not rewritten away because the project should stay faithful to its current scope:
- the Streamlit UI remains the main frontend shell instead of migrating to a JS SPA
- MongoDB remains the default operational store for simplicity
- Kubernetes manifests target a pragmatic local/minikube setup first
- not all Copernicus families are fully exercised by the available local data at all times
