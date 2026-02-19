# 3-Layer Deployment Guide (Podman)

This guide explains how to run NimbusChain Fetch as independent but connected services:

- `nimbus-api`: FastAPI service (control plane)
- `nimbus-worker`: download execution plane
- `nimbus-ui`: Streamlit frontend
- `mongodb`: shared persistence

The objective is to separate responsibilities so each layer can scale independently.

For Kubernetes deployment, see:
- `/Users/mehdidinari/Desktop/backend nimbus/docs/KUBERNETES_DEPLOYMENT.md`

## 1) Service responsibilities

## API (`nimbus-api`)
- Exposes `/v1/jobs`, `/v1/events`, `/v1/health`, etc.
- Persists job requests to DB.
- Does not perform heavy downloads when `NIMBUS_RUNTIME_ROLE=api`.

## Worker (`nimbus-worker`)
- Polls DB for `queued` jobs.
- Claims jobs atomically before execution.
- Runs provider search/download, checksums, manifest generation.
- Updates status/events/results in DB.

## UI (`nimbus-ui`)
- Human interface for submitting and monitoring jobs.
- Calls API over HTTP.

## MongoDB
- Stores jobs, events, results.
- Enables decoupling between API and workers.

## 2) Key environment variables

Required core:
- `NIMBUS_DB_BACKEND` (`mongodb` recommended)
- `NIMBUS_MONGODB_URI` (local direct runs)
- `NIMBUS_MONGODB_URI_INTERNAL` (compose internal host, default `mongodb://mongodb:27017`)
- `NIMBUS_MONGODB_DB`
- `NIMBUS_DATA_DIR`
- `NIMBUS_MAX_JOBS`
- `NIMBUS_LOG_LEVEL`

Role/runtime:
- `NIMBUS_RUNTIME_ROLE`: `api`, `worker`, or `all`
- `NIMBUS_QUEUE_POLL_SECONDS`: queue polling interval for workers
- `NIMBUS_STALE_JOB_SECONDS`: stale running timeout before automatic requeue
- `NIMBUS_PROVIDER_LIMITS`: `copernicus=2,usgs=4`
- `NIMBUS_ENABLE_METRICS`: enable `/v1/metrics`

Security:
- `NIMBUS_API_KEY` (optional, enables `X-API-Key`)
- `NIMBUS_MAX_REQUEST_MB`
- `NIMBUS_CORS_ORIGINS`
- `NIMBUS_LOG_JSON` (structured logs)

Provider credentials:
- Copernicus: `NIMBUS_COPERNICUS_USERNAME`, `NIMBUS_COPERNICUS_PASSWORD`
- USGS: `NIMBUS_USGS_USERNAME`, `NIMBUS_USGS_TOKEN`

UI:
- `NIMBUS_SERVICE_URL` (UI -> API URL; default `http://nimbus-api:8000`)

## 3) Boot sequence

1. API starts with `NIMBUS_RUNTIME_ROLE=api`.
2. Worker starts with `NIMBUS_RUNTIME_ROLE=worker`.
3. User/UI submits jobs to API.
4. API inserts job as `queued`.
5. Worker claims job atomically (`queued -> running`).
6. Worker executes fetch/download and writes progress/events.
7. API and UI read status/result from DB through API endpoints.

## 4) Start/stop commands

From `/Users/mehdidinari/Desktop/backend nimbus`:

```bash
cp .env.example .env
podman machine init
podman machine start
./scripts/10_up_stack.sh
```

Stop:

```bash
./scripts/11_down_stack.sh
```

Scale workers:

```bash
./scripts/12_scale_workers.sh 3
```

## 5) Verification checklist

## API health

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
```

Expected:
- `"status": "ok"`
- `"runtime_role": "api"`

## UI reachability
- open `http://127.0.0.1:8501`

## Worker logs

```bash
podman compose -f podman-compose.yml logs -f nimbus-worker
```

## Prometheus metrics

```bash
curl -s http://127.0.0.1:8000/v1/metrics | head -n 40
```

## Submit a smoke job

```bash
curl -s -X POST "http://127.0.0.1:8000/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_type":"search_download",
    "provider":"copernicus",
    "collection":"SENTINEL-2",
    "product_type":"S2MSI2A",
    "start_date":"2025-01-01",
    "end_date":"2025-01-03",
    "aoi":{"wkt":"POLYGON((-6 33,-6 35,-4 35,-4 33,-6 33))"},
    "output_dir":"smoke/morocco_3d"
  }'
```

Then poll:

```bash
JOB_ID="<replace>"
while true; do
  curl -s "http://127.0.0.1:8000/v1/jobs/${JOB_ID}" | python3 -m json.tool
  sleep 2
done
```

## 6) Scalability model

Horizontal scaling:
- increase `nimbus-worker` replicas.
- each replica has independent async workers (`NIMBUS_MAX_JOBS`).

Concurrency controls:
- global per worker: `NIMBUS_MAX_JOBS`
- per provider: `NIMBUS_PROVIDER_LIMITS`

Important:
- external provider API quotas still apply (Copernicus/USGS rate limits).
- tune worker count gradually and monitor failures/retries.

## 6.1) CLI in service layer

The API image includes the legacy-compatible CLI (`nimbuschain-fetch`).

Example from host (using API endpoint):

```bash
nimbuschain-fetch \
  --mode service \
  --service-url http://127.0.0.1:8000 \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --start-date 2025-01-01 \
  --end-date 2025-01-03 \
  --aoi_file ./aoi.wkt
```

## 7) Troubleshooting

## API cannot reach Mongo in container
Symptom:
- `ServerSelectionTimeoutError` for `127.0.0.1:27017`

Cause:
- container using localhost instead of service hostname.

Fix:
- keep compose env `NIMBUS_MONGODB_URI` mapped from `NIMBUS_MONGODB_URI_INTERNAL` (`mongodb://mongodb:27017`).

## Jobs stay queued forever
Check:
- worker container running?
- worker logs healthy?
- `runtime_role` in worker set to `worker`?

## Cancellation behavior
- queued jobs are cancelled immediately.
- running jobs transition through `cancel_requested`, then `cancelled` once worker observes cancellation.

## 8) Extending later

To add new features/providers later:
- add provider implementation under `src/nimbuschain_fetch/providers/`.
- keep API thin; all business logic stays in `nimbuschain_fetch`.
- add new UI pages/widgets without coupling to worker internals.
- scale worker replicas independently from API/UI.
