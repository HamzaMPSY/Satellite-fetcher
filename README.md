# NimbusChain Fetch

NimbusChain Fetch is now organized as a **3-layer runtime architecture**:

1. `backend` layer: async download workers (job execution only)
2. `service` layer: FastAPI control plane + REST/SSE + CLI compatibility
3. `ui` layer: Streamlit frontend

All layers are isolated in separate containers and connected through MongoDB + HTTP.

## Legacy UI + Jobs API runtime

The Streamlit UI (`/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_fetch_ui/app.py`) keeps the legacy UX logic
(map, tile system/search/picker, download manager layout, results/settings tabs), but execution is now API-job based:

- `Start Download`: `POST /v1/jobs` or `POST /v1/jobs/batch`
- real-time tracking: `GET /v1/events` (SSE) with polling fallback on `/v1/jobs/{job_id}`
- `Stop`: `DELETE /v1/jobs/{job_id}` on active jobs
- `Reset` / `Unlock`: clear UI tracker state only (files on disk are preserved)

Product preview remains local in the UI container (direct Copernicus/USGS calls) and requires provider credentials in `nimbus-ui` environment.

## Repository packages

- `src/nimbuschain_fetch`: core engine package (providers, orchestration, downloader, manifest, security).
- `src/nimbuschain_fetch_service`: FastAPI API layer (thin wrapper around engine).
- `src/nimbuschain_fetch_ui`: Streamlit UI layer (legacy-style UX, API-job based).

## Runtime architecture

```text
Browser
  |
  v
Streamlit UI container (port 8501)
  |
  v
FastAPI API container (port 8000)  <------ CLI (service mode)
  |   create/list/cancel jobs
  v
MongoDB (job state + events + results)
  ^
  | claim queued jobs
Worker container(s) (downloads + providers + checksums + manifest)
```

## Main docs

- `/Users/mehdidinari/Desktop/backend nimbus/docs/README.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/DEPLOYMENT_3_LAYER.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/KUBERNETES_DEPLOYMENT.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/API_REFERENCE.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/REPOSITORY_GUIDE.md`
- `/Users/mehdidinari/Desktop/backend nimbus/docs/integration_streamlit.md`

## Podman quick start (recommended)

### 1) Prepare environment

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
```

Set credentials in `.env`:
- `NIMBUS_COPERNICUS_USERNAME`
- `NIMBUS_COPERNICUS_PASSWORD`
- `NIMBUS_USGS_USERNAME`
- `NIMBUS_USGS_TOKEN`

These credentials are used by:
- `nimbus-worker` for real downloads
- `nimbus-ui` for local product preview panel

### 2) Ensure Podman machine is running (macOS)

```bash
podman machine init
podman machine start
```

If already created/running, Podman will just report it.

### 3) Start all layers (api + worker + ui + mongodb)

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/10_up_stack.sh
```

### 4) Verify

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/metrics | head -n 30
```

Open:
- API docs: `http://127.0.0.1:8000/docs`
- UI: `http://127.0.0.1:8501`

### 5) Stop stack

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/11_down_stack.sh
```

## Docker Compose quick start (4 services)

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
# fill credentials in .env (Copernicus/USGS)
./scripts/13_up_stack_docker.sh
```

Services:
- `mongodb`
- `nimbus-worker`
- `nimbus-api`
- `nimbus-ui`

Open:
- API docs: `http://127.0.0.1:8000/docs`
- UI: `http://127.0.0.1:8501`

Stop:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/14_down_stack_docker.sh
```

## Kubernetes quick start

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
brew install minikube
MINIKUBE_MEMORY_MB=6144 ./scripts/32_k8s_bootstrap_minikube.sh
podman build -f Containerfile -t ghcr.io/nimbuschain/nimbus-api:latest .
podman build -f ui/Containerfile -t ghcr.io/nimbuschain/nimbus-ui:latest .
./scripts/34_k8s_load_images_minikube.sh
./scripts/33_k8s_apply_minikube.sh
kubectl -n nimbuschain get pods
```

Full guide:
- `/Users/mehdidinari/Desktop/backend nimbus/docs/KUBERNETES_DEPLOYMENT.md`

## Scale download throughput

Scale worker replicas:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/12_scale_workers.sh 3
```

Notes:
- each worker claims queued jobs atomically from DB (no duplicate execution).
- `NIMBUS_MAX_JOBS` controls per-worker concurrency.
- total max parallelism ~= `worker_replicas * NIMBUS_MAX_JOBS` (subject to provider limits and external APIs).

## Test commands

Unit tests kept in repo:
- `/Users/mehdidinari/Desktop/backend nimbus/tests/test_models.py`
- `/Users/mehdidinari/Desktop/backend nimbus/tests/test_engine.py`

Run with Podman:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/01_test_models.sh
./scripts/02_test_engine.sh
./scripts/05_test_all.sh
```

## CLI modes

Direct mode (single-process usage):

```bash
nimbuschain-fetch --mode direct ...
```

Service mode (recommended with separated stack):

```bash
nimbuschain-fetch --mode service --service-url http://127.0.0.1:8000 ...
```
