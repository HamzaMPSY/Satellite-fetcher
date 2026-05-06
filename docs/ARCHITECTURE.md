# Architecture

## 1. System overview

NimbusChain Fetch is split into six main runtime components with explicit boundaries, but the backend/orchestrator is the only public API surface.

```text
Browser
  -> nimbus-ui (Streamlit)
       -> nimbus-api (FastAPI orchestration layer)
            -> MongoDB or SQLite store
            -> nimbus-worker (download + Zarr execution)
            -> nimbus-sen2like-service (Landsat -> Sentinel-2-like normalization)
            -> nimbus-zarr runtime/library (internal conversion code)
            -> nimbus-mask-service (cloud/water masking runtime)
```

The UI never executes downloads directly. It submits jobs to the API. The worker owns execution from search to download to Zarr conversion. The mask service owns cloud and water masking on existing Zarr outputs. The Zarr package is reused as worker-internal conversion logic, while the backend exposes converter and mask health/schema endpoints for the UI.

The production job runtime is still Nimbus-first: public routes keep using the existing API, worker, job store and `NimbusFetcher` behavior. The new `nimbuschain_fetch.pipeline` package is an incremental orchestration foundation for splitting the runtime into explicit stages without changing those public contracts.

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
- `src/nimbuschain_fetch_ui/orchestrator_tab.py`: visual wrapper around the modular stage CLI
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
- `src/nimbuschain_fetch/pipeline/`: modular stage/DAG foundation and local stage CLI support
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
- `src/nimbuschain_fetch/stage_cli.py`

### `nimbus-sen2like-service`

Main role:
- preserve the vendored PySpark Sen2Like implementation from `Pipeline.py`
- expose a small FastAPI wrapper for health, readiness, schema and normalization execution
- run Landsat scene normalization in a dedicated container on port `8030`
- return structured execution status, output paths, stdout/stderr tails and duration

Key code:
- `sen2like-service/vendor/Satellite-fetcher-feature-sen2like_reimplementation/`
- `sen2like-service/Containerfile`
- `src/nimbuschain_sen2like_service/`
- `src/nimbuschain_shared/clients/sen2like.py`

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

### Modular pipeline foundation

Main role:
- describe the future job flow as independent stages with dependencies
- record one `StageResult` per stage, including status, outputs, metadata, errors and duration
- provide a local CLI for inspecting a plan or running a target stage with its dependencies
- prepare the Landsat normalization hook and call the Sen2Like service when a raw Landsat path is provided

Current default stage graph:

```text
Copernicus/Sentinel:
fetch -> zarr -> mask? / cube? (ordered by cube_mode)

USGS/Landsat:
fetch -> sen2like -> zarr -> mask? / cube? (ordered by cube_mode)
```

`sen2like` is only present in the stage graph for USGS/Landsat selections. Sentinel/Copernicus jobs never plan or execute that stage. In the local CLI foundation, a missing or skipped Landsat Sen2Like stage blocks downstream target-stage execution because `zarr` depends on the normalized Landsat output. Public job routes still use the existing production runtime until the worker migration is completed.

Optional stages are omitted when disabled: no selected mask means no `mask` stage, and `cube_mode=none` means no `cube` stage. For example, a Sentinel run with no mask and no cube is simply `fetch -> zarr`.

For USGS/Landsat jobs, the stage is skipped with `sen2like_service_url_missing` when `NIMBUS_SEN2LIKE_SERVICE_URL` is not configured, or `sen2like_input_missing` when no raw Landsat path has been provided yet. When both are present, the stage calls `POST /normalize` on `nimbus-sen2like`.

Local CLI examples:

```bash
python -m nimbuschain_fetch.stage_cli plan \
  --provider usgs \
  --collection landsat_ot_c2_l2 \
  --product-type L2SP

python -m nimbuschain_fetch.stage_cli run-stage \
  --provider usgs \
  --collection landsat_ot_c2_l2 \
  --product-type L2SP \
  --raw-uri /data/downloads/raw/LC08_SCENE \
  --stage sen2like
```

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

### Target modular flow

```text
Sentinel/Copernicus:
fetcher -> Zarr conversion -> optional masking -> optional cube build

USGS/Landsat:
fetcher -> sen2like normalization -> Zarr conversion -> optional masking -> optional cube build

Both paths persist artifacts, events and per-stage timing under the job lineage.
```

This is the intended migration path, not a public API break. Each stage should eventually be executable from the worker or from the local CLI, and each stage should return a structured result with elapsed time.

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
- `src/nimbuschain_fetch/pipeline/`
  Stage protocol, DAG orchestration, default stage graph and optional Sen2Like hook.
- `src/nimbuschain_fetch_service/`
  Public API layer over the engine, stores, and converter-facing routes.
- `src/nimbuschain_fetch_ui/`
  Streamlit presentation layer and client-side orchestration.
- `src/nimbuschain_zarr_service/`
  Conversion logic reused by the worker and converter routes.
- `src/nimbuschain_mask_service/`
  Internal masking runtime and remote/client adapter.

### Import boundaries

- UI code may import UI helpers and API clients, but should not import provider download engines, Zarr writers or mask inference internals.
- API routers may import service/engine contracts and job stores, but should keep public DTOs stable.
- Worker code may orchestrate providers and internal clients, but stage implementations should keep provider, conversion, mask and cube concerns separated.
- Zarr code should own product-to-Zarr conversion only.
- Mask code should own cloud/water masking only.
- Shared code should be small and contract-like: middleware, DTOs, clients and pipeline stage primitives. A service should not import another service's FastAPI implementation.

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
