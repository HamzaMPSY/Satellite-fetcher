# NimbusChain Fetch

NimbusChain Fetch is a satellite download orchestrator with four runtime components:

- `nimbus-api`: FastAPI control plane for jobs, events, artifacts, and metrics
- `nimbus-worker`: download execution worker for Copernicus and USGS
- `nimbus-ui`: Streamlit UI for AOI selection, job tracking, and Zarr conversion
- `nimbus-zarr`: conversion service from raw scenes to Zarr

## Repository layout

- `src/nimbuschain_fetch/`: core engine, providers, job store, worker entrypoint
- `src/nimbuschain_fetch_service/`: FastAPI API layer
- `src/nimbuschain_fetch_ui/`: Streamlit frontend
- `src/nimbuschain_zarr_service/`: Zarr conversion service
- `data/Landsat-tiles/`: tracked Landsat tile index
- `data/Sentinel-2-tiles/`: tracked Sentinel-2 tile index
- `k8s/`: Kubernetes base manifests and overlays
- `scripts/`: only the operational scripts kept in the repo

## Quick start with Podman

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
```

Fill at least:

- `NIMBUS_COPERNICUS_USERNAME`
- `NIMBUS_COPERNICUS_PASSWORD`
- `NIMBUS_USGS_USERNAME`
- `NIMBUS_USGS_TOKEN`

Start the stack:

```bash
./scripts/10_up_stack.sh
```

Check the services:

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
curl -s http://127.0.0.1:8010/health | python3 -m json.tool
```

Open:

- UI: `http://127.0.0.1:8501`
- API docs: `http://127.0.0.1:8000/docs`

Stop the stack:

```bash
./scripts/11_down_stack.sh
```

## Kubernetes

For Minikube + Podman:

```bash
./scripts/32_k8s_bootstrap_minikube.sh
./scripts/33_k8s_apply_minikube.sh
./scripts/35_k8s_expose_local.sh
```

Stop local port-forwards:

```bash
./scripts/36_k8s_unexpose_local.sh
```

## Data policy

Tracked in git:

- `data/Landsat-tiles/`
- `data/Sentinel-2-tiles/`

Ignored locally:

- downloads
- generated Zarr stores
- logs
- caches
- port-forward runtime files

## Documentation

- `docs/ARCHITECTURE.md`: runtime architecture and repository map
- `docs/RUNBOOK.md`: how to run, verify, and troubleshoot
- `docs/API_REFERENCE.md`: API endpoints and filters
- `docs/ZARR.md`: conversion scope, output model, and resolution policy
