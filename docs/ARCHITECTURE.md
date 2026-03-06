# Architecture

## Runtime components

```text
Browser
  -> nimbus-ui (Streamlit, port 8501)
  -> nimbus-api (FastAPI, port 8000)
  -> MongoDB (jobs, events, results, artifacts)
  -> nimbus-worker (downloads)
  -> nimbus-zarr (raw scene -> Zarr)
```

## Responsibility split

### `nimbus-ui`
- AOI selection and tile picking
- local product preview for Copernicus and USGS
- job submission and monitoring
- manual Zarr conversion requests

### `nimbus-api`
- create, list, cancel, and inspect jobs
- expose SSE events and metrics
- persist artifacts and job results
- serve as the contract between UI and workers

### `nimbus-worker`
- claim queued jobs from the store
- run provider downloads
- write manifests and result paths into `/data/downloads`

### `nimbus-zarr`
- read raw scene folders or archives
- normalize source bands into a canonical model
- write Zarr stores under `/data/downloads/zarr`

## Main repository paths

### Core code
- `src/nimbuschain_fetch/`: engine, providers, jobs, worker
- `src/nimbuschain_fetch_service/`: API routers and app wiring
- `src/nimbuschain_fetch_ui/`: Streamlit UI
- `src/nimbuschain_zarr_service/`: Zarr converters

### Operations
- `scripts/10_up_stack.sh`: start local Podman stack
- `scripts/11_down_stack.sh`: stop local Podman stack
- `scripts/32_k8s_bootstrap_minikube.sh`: create/start Minikube
- `scripts/33_k8s_apply_minikube.sh`: apply Kubernetes overlay
- `scripts/35_k8s_expose_local.sh`: local port-forward for API/UI
- `scripts/36_k8s_unexpose_local.sh`: stop port-forwards

### Infrastructure
- `Containerfile`: API and worker image
- `ui/Containerfile`: UI image
- `zarr-service/Containerfile`: Zarr image
- `podman-compose.yml`: local stack for Podman
- `docker-compose.yml`: local stack for Docker-compatible compose
- `k8s/`: base manifests and Minikube overlay

## Persistent data

- `/data/downloads`: raw downloads, manifests, and Zarr output
- MongoDB: jobs, events, job results, artifact registry

## Job lifecycle

```text
queued -> running -> succeeded
                -> failed
                -> cancel_requested -> cancelled
```

## Artifact flow

```text
raw scene/folder/archive
  -> download manifest
  -> Zarr conversion
  -> artifact registration
```
