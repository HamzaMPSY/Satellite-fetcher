# API Reference

Base URL: `http://127.0.0.1:8000`

## Authentication

If `NIMBUS_API_KEY` is set, send:

```http
X-API-Key: <value>
```

## Health and metrics

### `GET /v1/health`
Service health and runtime metadata.

### `GET /v1/metrics`
Prometheus metrics.

## Jobs

### `POST /v1/jobs`
Create one job.

### `POST /v1/jobs/batch`
Create multiple jobs in one request.

### `GET /v1/jobs/{job_id}`
Get live status for one job.

### `DELETE /v1/jobs/{job_id}`
Request cancellation for one job.

### `GET /v1/jobs/{job_id}/result`
Get result payload and paths for a finished job.

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

Typical examples:

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?state=running&page_size=20" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/v1/jobs?provider=copernicus&collection=SENTINEL-2" | python3 -m json.tool
curl -s "http://127.0.0.1:8000/v1/jobs?updated_from=2026-03-06T12:00:00Z" | python3 -m json.tool
```

## Events

### `GET /v1/events`
Server-sent events stream used by the UI.

Query params:
- `since`

## Artifacts

### `POST /v1/artifacts`
Register a produced artifact, mainly Zarr stores.

### `GET /v1/artifacts`
List registered artifacts.

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
