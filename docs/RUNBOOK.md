# Runbook

## Prerequisites

- Python 3.11
- Podman + podman-compose
- optional: Minikube + kubectl for Kubernetes

## Local Podman workflow

### 1. Configure environment

```bash
cp .env.example .env
```

Fill credentials and adjust ports only if needed.

### 2. Start

```bash
./scripts/10_up_stack.sh
```

### 3. Verify

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
curl -s http://127.0.0.1:8010/health | python3 -m json.tool
podman ps
```

### 4. Logs

```bash
podman logs -f backendnimbus_nimbus-api_1
podman logs -f backendnimbus_nimbus-worker_1
podman logs -f backendnimbus_nimbus-ui_1
podman logs -f backendnimbus_nimbus-zarr_1
```

### 5. Stop

```bash
./scripts/11_down_stack.sh
```

## Smoke test

### API

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
```

### UI
Open `http://127.0.0.1:8501`

### Zarr service

```bash
curl -s http://127.0.0.1:8010/health | python3 -m json.tool
```

## Kubernetes with Minikube

### 1. Bootstrap cluster

```bash
./scripts/32_k8s_bootstrap_minikube.sh
```

### 2. Apply manifests

```bash
./scripts/33_k8s_apply_minikube.sh
```

### 3. Expose API and UI locally

```bash
./scripts/35_k8s_expose_local.sh
```

### 4. Stop local exposure

```bash
./scripts/36_k8s_unexpose_local.sh
```

## Common issues

### UI shows no live updates
- verify API health
- verify worker logs
- hard refresh the browser

### Zarr tab does not show raw sources
- confirm files exist under `data/downloads`
- check that the download job wrote `manifest.json`
- verify UI container has `/data` mounted

### Podman machine is down on macOS

```bash
podman machine start
```

### Port 8501 already used

```bash
lsof -nP -iTCP:8501 -sTCP:LISTEN
```
