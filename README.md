# NimbusChain Fetch

NimbusChain Fetch is a multi-service satellite data platform composed of:

- a FastAPI orchestrator for jobs, events, artifacts and health
- a worker that executes Copernicus and USGS downloads
- a Streamlit UI for AOI selection, pipeline tracking and manual Zarr conversion
- an internal Zarr conversion runtime used by the worker and exposed through the backend API
- an internal mask service for cloud and water masking on existing Zarr outputs

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

## Repository map

```text
src/nimbuschain_fetch/            Core engine, providers, worker, stores
src/nimbuschain_fetch_service/    FastAPI API layer and public converter endpoints
src/nimbuschain_fetch_ui/         Streamlit frontend
src/nimbuschain_zarr_service/     Internal Zarr conversion runtime/library
src/nimbuschain_mask_service/     Cloud and water masking runtime/service
src/nimbuschain_zarr_viewer/      Local Zarr viewing/browser helpers

Containerfile                     API/worker image
ui/Containerfile                  UI image
zarr-service/Containerfile        Zarr service image
mask-service/Containerfile        Mask service image
podman-compose.yml                Local Podman stack
docker-compose.yml                Local Docker-compatible stack
k8s/                              Kubernetes base + overlay
scripts/                          Operational scripts

data/Landsat-tiles/               Tracked Landsat tile index
data/Sentinel-2-tiles/            Tracked Sentinel-2 tile index
data/downloads/                   Local runtime data, ignored from git
```

## Local start

```bash
cd /path/to/Satellite-fetcher
cp .env.example .env
./scripts/10_up_stack.sh
```

Then open:

- UI: [http://127.0.0.1:8501](http://127.0.0.1:8501)
- API docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- Converter schema: [http://127.0.0.1:8000/v1/converter/schema](http://127.0.0.1:8000/v1/converter/schema)
- Mask schema: [http://127.0.0.1:8000/v1/mask/schema](http://127.0.0.1:8000/v1/mask/schema)

Stop the stack:

```bash
./scripts/11_down_stack.sh
```

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
  --aoi_file /path/to/aoi.geojson \
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
  --aoi_file /path/to/aoi.geojson \
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
