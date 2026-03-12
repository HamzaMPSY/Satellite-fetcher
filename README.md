# NimbusChain Fetch

NimbusChain Fetch is a multi-service satellite data platform composed of:

- a FastAPI orchestrator for jobs, events, artifacts and health
- a worker that executes Copernicus and USGS downloads
- a Streamlit UI for AOI selection, job tracking and manual Zarr conversion
- a dedicated Zarr conversion service for raw scene normalization

The repository is intentionally small in scope: it focuses on raw scene acquisition, operational monitoring, and conversion to a normalized `time, band, y, x` Zarr layout.

## Core capabilities

- submit and track satellite download jobs
- preview products locally from Copernicus and USGS credentials already configured in the environment
- persist job state, events, results and Zarr artifacts
- convert downloaded scenes to Zarr with sensor-aware band preservation
- run locally with Podman or Docker-compatible compose
- deploy to Minikube with the provided Kubernetes manifests

## Repository map

```text
src/nimbuschain_fetch/            Core engine, providers, worker, stores
src/nimbuschain_fetch_service/    FastAPI API layer
src/nimbuschain_fetch_ui/         Streamlit frontend
src/nimbuschain_zarr_service/     Zarr conversion service

Containerfile                     API/worker image
ui/Containerfile                  UI image
zarr-service/Containerfile        Zarr service image
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
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
./scripts/10_up_stack.sh
```

Then open:

- UI: [http://127.0.0.1:8501](http://127.0.0.1:8501)
- API docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- Zarr schema: [http://127.0.0.1:8010/schema](http://127.0.0.1:8010/schema)

Stop the stack:

```bash
./scripts/11_down_stack.sh
```

## Health endpoints

- API health: [http://127.0.0.1:8000/v1/health](http://127.0.0.1:8000/v1/health)
- API readiness: [http://127.0.0.1:8000/v1/readiness](http://127.0.0.1:8000/v1/readiness)
- Worker status: [http://127.0.0.1:8000/v1/worker/status](http://127.0.0.1:8000/v1/worker/status)
- Zarr health: [http://127.0.0.1:8010/health](http://127.0.0.1:8010/health)
- Zarr readiness: [http://127.0.0.1:8010/readiness](http://127.0.0.1:8010/readiness)

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

- `/Users/mehdidinari/Desktop/backend nimbus/docs/ARCHITECTURE.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/RUNBOOK.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/API_REFERENCE.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/ZARR.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/PRODUCT_BANDS.md`
