# NimbusChain Fetch

NimbusChain Fetch is a multi-service satellite data platform composed of:

- a FastAPI orchestrator for jobs, events, artifacts and health
- a worker that executes Copernicus and USGS jobs in the background
- a Streamlit UI for AOI selection, pipeline tracking and manual operations
- a standalone Zarr conversion service reached over HTTP
- a standalone mask service for cloud and water masking reached over HTTP

The repository is intentionally small in scope: it focuses on raw scene acquisition, operational monitoring, and conversion to a normalized `time, band, y, x` Zarr layout.

## Core capabilities

- submit and track satellite download jobs
- execute one pipeline job from search to download to Zarr conversion under a single `job_id`
- preview products locally from Copernicus and USGS credentials already configured in the environment
- persist job state, events, results and Zarr artifacts
- convert downloaded scenes to Zarr with sensor-aware band preservation
- apply cloud and water masks to existing Zarr outputs
- run locally with Podman or Docker-compatible compose
- deploy to Minikube with the provided Kubernetes manifests

## Architecture overview

NimbusChain Fetch is organized around one public backend and two internal compute services.

```text
Browser
  -> nimbus-ui
       -> nimbus-api
            -> MongoDB or SQLite job store
            -> nimbus-worker
            -> nimbus-zarr (HTTP)
            -> nimbus-mask (HTTP)
```

Runtime responsibilities:

- `nimbus-ui`
  User-facing Streamlit application for AOI selection, preview, job submission, monitoring, and manual actions.
- `nimbus-api`
  Public FastAPI entrypoint. It validates requests, exposes `/v1/...` endpoints, serves health/status, and orchestrates jobs through the shared fetch engine.
- `nimbus-worker`
  Background runtime role that polls the job store, claims queued jobs, executes provider downloads, updates pipeline state, and triggers remote conversion/masking work.
- `nimbus-zarr`
  Dedicated conversion microservice that reads raw products and writes normalized Zarr outputs.
- `nimbus-mask`
  Dedicated masking microservice that applies cloud and/or water inference to existing Zarr outputs.

One important implementation detail is that `nimbus-api` and `nimbus-worker` share the same core orchestration package and image, but they run with different runtime roles. They do not communicate with each other over HTTP; they coordinate through the persisted job store and worker heartbeats.

## Service communication

The current communication model is:

- UI -> API: HTTP
- API -> UI: HTTP responses plus server-sent events for live job/event updates
- API/worker -> job store: direct persistence calls through the configured store backend
- Fetch engine -> Zarr service: HTTP through `ZarrServiceClient`
- Fetch engine -> mask service: HTTP through `MaskServiceClient`

In practice this means:

- the UI only talks to the fetch API
- the fetch API is the only public control-plane surface
- the worker performs background execution by reading and updating shared job state
- conversion and masking are delegated to dedicated internal services over HTTP

## DTOs and contracts

There are several layers of models in the codebase:

- public API DTOs
  Used by the UI and external callers for `/v1/jobs`, `/v1/events`, `/v1/artifacts`, `/v1/jobs/{job_id}/convert`, and mask-related endpoints
- internal service-to-service HTTP contracts
  Used between the fetch engine and the standalone Zarr/mask services
- internal orchestration DTOs
  Used inside the fetch engine and shared clients before serializing to HTTP payloads
- persistence/domain records
  Used to store jobs, events, results, artifacts, and worker state in MongoDB or SQLite

This separation is intentional: public API models are not the same thing as the internal wire contracts used between services, and neither of those are the same as the stored domain records.

### Zarr service communication

The fetch side calls the Zarr service over HTTP for:

- health and readiness checks
- schema discovery
- raw-to-Zarr conversion
- grouped cube building
- single cube building
- dataset inspection

The request payloads carry identifiers and conversion context such as:

- `job_id`
- `pipeline_id`
- `trace_id`
- `provider`
- `collection`
- `product_type`
- `scene_id`
- `raw_uri`
- `output_uri`

The response returns structured conversion metadata such as:

- `zarr_uri`
- `data_family`
- `band_names`
- `dimensions`
- ancillary layer information
- normalization and dataset summaries

### Mask service communication

The fetch side calls the mask service over HTTP for:

- health checks
- schema discovery
- remote masking
- progress polling during masking

The masking request contains structured fields such as:

- `source_zarr_uri`
- `output_zarr_uri`
- `provider`
- `collection`
- `product_type`
- `scene_id`
- `acquisition_datetime`
- `dataset_summary`
- `mask_types`
- cloud options
- water options

The mask service returns structured masking results and exposes progress events that the fetch side can translate back into job-level pipeline updates.

## Repository map

```text
src/nimbuschain_fetch/            Core engine, workflows, providers, worker, stores
src/nimbuschain_fetch_service/    FastAPI API layer and public orchestration endpoints
src/nimbuschain_fetch_ui/         Streamlit frontend
src/nimbuschain_zarr_service/     Standalone Zarr conversion service
src/nimbuschain_mask_service/     Standalone cloud/water mask service
src/nimbuschain_shared/           Shared clients, DTOs, contracts, and runtime helpers

Containerfile                     API/worker image
ui/Containerfile                  UI image
zarr-service/Containerfile        Zarr service image
mask-service/Containerfile        Mask service image
podman-compose.yml                Local Podman stack
docker-compose.yml                Local Docker-compatible stack
k8s/                              Kubernetes base + overlay

data/Landsat-tiles/               Tracked Landsat tile index
data/Sentinel-2-tiles/            Tracked Sentinel-2 tile index
data/downloads/                   Local runtime data, ignored from git
```

## Local start

```bash
cd /path/to/Satellite-fetcher
cp .env.example .env
```

Then start the API, worker, UI, Zarr service, and mask service with the container or host-process commands appropriate for your environment.

## Health endpoints

- API health: [http://127.0.0.1:8000/v1/health](http://127.0.0.1:8000/v1/health)
- API readiness: [http://127.0.0.1:8000/v1/readiness](http://127.0.0.1:8000/v1/readiness)
- Worker status: [http://127.0.0.1:8000/v1/worker/status](http://127.0.0.1:8000/v1/worker/status)
- Converter health: [http://127.0.0.1:8000/v1/converter/health](http://127.0.0.1:8000/v1/converter/health)
- Converter readiness: [http://127.0.0.1:8000/v1/converter/readiness](http://127.0.0.1:8000/v1/converter/readiness)
- Mask health: [http://127.0.0.1:8000/v1/mask/health](http://127.0.0.1:8000/v1/mask/health)

## Data and git policy

Tracked in git:

- `data/Landsat-tiles/`
- `data/Sentinel-2-tiles/`

Ignored locally:

- `data/downloads/`
- generated Zarr stores
- local caches and logs
- port-forward runtime files

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/MASTER_DIAGRAM.md`
- `docs/RUNBOOK.md`
- `docs/API_REFERENCE.md`
- `docs/ZARR.md`
- `docs/PRODUCT_BANDS.md`

## Cube builder

The repo now includes a time-series cube builder for already-converted scene Zarrs, plus pipeline integration from the fetch job itself.

Standalone single-cube build:

```bash
nimbuschain-zarr-cube /path/to/scene_a.zarr /path/to/scene_b.zarr \
  --output-uri /path/to/cube.zarr
```

Standalone grouped build by tile/path-row:

```bash
nimbuschain-zarr-cube /data/downloads/zarr/*.zarr \
  --group-by-tile \
  --output-dir /data/downloads/zarr/cubes/manual \
  --start-date 2026-04-10 \
  --end-date 2026-04-13 \
  --stage-label before_mask
```

Fetch pipeline with cube before masking:

```bash
nimbuschain-fetch \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --start-date 2026-04-10 \
  --end-date 2026-04-13 \
  --aoi-file /path/to/aoi.geojson \
  --tile-id 37RDP \
  --mask-types water,cloud \
  --cube-mode before_mask \
  --cube-start-date 2026-04-10 \
  --cube-end-date 2026-04-13
```

Fetch pipeline with cube after masking:

```bash
nimbuschain-fetch \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --start-date 2026-04-10 \
  --end-date 2026-04-13 \
  --aoi-file /path/to/aoi.geojson \
  --tile-id 37RDP \
  --mask-types water,cloud \
  --cube-mode after_mask \
  --cube-start-date 2026-04-10 \
  --cube-end-date 2026-04-13
```

Current cube behavior:
- stacks compatible scene stores along real acquisition time
- groups by Sentinel tile ID or Landsat path/row in grouped mode
- requires the same `band`, `y`, `x`, CRS, transform, and pixel grid across inputs
- preserves `scene_id(time)` and `source_zarr_uri(time)` for provenance
- writes `ancillary(time, ancillary_layer, y, x)` only when all inputs share the same ancillary schema
- writes one root-level quadkey coverage block for the cube footprint
- does not stack `masks/...` arrays yet; the cube currently contains imagery plus compatible ancillary layers

In the UI, users can choose:
- no cube
- cube before masking
- cube after masking

When cube building is enabled in the UI, the date range is limited to the acquisition dates currently visible in the preview results.

## VM shell workflow

For VM performance testing, the repo now includes shell-first commands for each stage of the pipeline.

Start the services directly on the VM:

```bash
nimbuschain-api-service
nimbuschain-worker
nimbuschain-zarr-service
nimbuschain-mask-service
```

Inspect or download source data from OCI Object Storage:

```bash
nimbuschain-oci ls oci://my-bucket@my-namespace/raw/
nimbuschain-oci cp oci://my-bucket@my-namespace/raw/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.SAFE.zip /data/downloads/raw
```

Run each stage manually from the VM shell:

```bash
nimbuschain-zarr-convert \
  oci://my-bucket@my-namespace/raw/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.SAFE.zip \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --output-uri /data/downloads/zarr/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.zarr

nimbuschain-mask-apply \
  /data/downloads/zarr/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.zarr \
  --mask-types water,cloud

nimbuschain-zarr-cube /data/downloads/zarr/*.zarr \
  --group-by-tile \
  --output-dir /data/downloads/zarr/cubes/manual_vm
```

Run the whole VM-oriented flow in one command:

```bash
nimbuschain-vm-pipeline \
  oci://my-bucket@my-namespace/raw/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.SAFE.zip \
  oci://my-bucket@my-namespace/raw/S2B_MSIL2A_20260413T075609_N0512_R035_T37RDP_20260413T102805.SAFE.zip \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --mask-types water,cloud \
  --cube-mode after_mask \
  --group-by-tile \
  --include-masks-in-cube
```
