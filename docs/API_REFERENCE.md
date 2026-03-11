# API Reference

Base URL: `http://127.0.0.1:8000`

If `NIMBUS_API_KEY` is configured, send:

```http
X-API-Key: <value>
```

## 1. Health and execution status

### `GET /v1/health`
Light health endpoint for the orchestrator.

### `GET /v1/readiness`
Strict readiness endpoint. Includes deeper checks on runtime store and execution wiring.

### `GET /v1/worker/status`
Worker heartbeat and execution-capacity view.

Typical response fields:
- `workers_alive`
- `workers_stale`
- `capacity_total`
- `capacity_used`
- `capacity_available`
- `queued_jobs`
- `running_jobs`
- `can_accept_work`

## 2. Jobs

### `POST /v1/jobs`
Create one download job.

### `POST /v1/jobs/batch`
Create multiple jobs in a single request.

### `GET /v1/jobs/{job_id}`
Get one job status.

### `DELETE /v1/jobs/{job_id}`
Request cancellation for one job.

### `GET /v1/jobs/{job_id}/result`
Read the persisted result payload for a finished job.

### `GET /v1/jobs`
List jobs with filters.

Supported query params:
- `state`
- `state_in`
- `provider`
- `collection`
- `product_type`
- `job_id_query`
- `date_from`
- `date_to`
- `updated_from`
- `updated_to`
- `sort_by`
- `sort_desc`
- `page`
- `page_size`

Examples:

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?state=running&page_size=20" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/v1/jobs?provider=copernicus&collection=SENTINEL-2" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/v1/jobs?product_type=L2SP&provider=usgs" | python3 -m json.tool
```

## 3. Events

### `GET /v1/events`
Server-sent events endpoint consumed by the UI.

Query params:
- `since`
- `job_id` when supported by the store implementation

## 4. Artifacts

### `POST /v1/artifacts`
Register an artifact, mainly used for Zarr stores.

### `GET /v1/artifacts`
List artifacts, optionally merged with locally discovered `.zarr` directories.

Supported query params:
- `artifact_type`
- `provider`
- `collection`
- `scene_id`
- `job_id`
- `uri_query`
- `date_from`
- `date_to`
- `include_local`
- `page`
- `page_size`

Example:

```bash
curl -s "http://127.0.0.1:8000/v1/artifacts?artifact_type=zarr&include_local=true" | python3 -m json.tool
```

## 5. Metrics

### `GET /v1/metrics`
Prometheus metrics endpoint.

## 6. Operational notes

- The API is the source of truth for job status.
- The UI should not infer job execution from local files only.
- Zarr stores become operationally visible through artifact registration or local discovery.
