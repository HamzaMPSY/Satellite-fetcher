# NimbusChain Fetch API Reference

This reference describes every HTTP endpoint exposed by `nimbuschain_fetch_service`.

In 3-layer deployment:
- API handles control plane (job CRUD + events).
- Worker container executes downloads asynchronously.
- UI container consumes this API.

Base URL (local):
- `http://127.0.0.1:8000`

OpenAPI UI:
- `http://127.0.0.1:8000/docs`

## Authentication

If `NIMBUS_API_KEY` is configured, include:

```http
X-API-Key: <your-key>
```

Public routes even with API key enabled:
- `GET /`
- `GET /v1/health`

## Content types

- Requests: `application/json`
- SSE stream: `text/event-stream`

Response headers:
- `X-Request-ID` is added to responses for traceability.

## States and job semantics

Possible job states:
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancel_requested`
- `cancelled`

`progress` is a percentage in `[0, 100]`.

`duration_seconds`:
- `null` before start
- running elapsed seconds while in progress
- total elapsed seconds after finish

---

## GET /

Simple HTML status page.

### Response
- `200 OK`
- HTML content with links to docs and health.

---

## GET /v1/health

Healthcheck endpoint.

### Response example

```json
{
  "status": "ok",
  "timestamp": "2026-02-17T19:00:33.890098+00:00",
  "runtime_role": "api",
  "db_backend": "mongodb",
  "metrics_enabled": "true"
}
```

---

## POST /v1/jobs

Create one job.

### Request: search_download

```json
{
  "job_type": "search_download",
  "provider": "copernicus",
  "collection": "SENTINEL-2",
  "product_type": "S2MSI2A",
  "start_date": "2026-01-01",
  "end_date": "2026-01-02",
  "aoi": {
    "wkt": "POLYGON((0 0,0 1,1 1,1 0,0 0))"
  },
  "tile_id": "29SQT",
  "output_dir": "rabat_s2_l2a"
}
```

### Request: download_products

```json
{
  "job_type": "download_products",
  "provider": "usgs",
  "collection": "landsat_ot_c2_l2",
  "product_ids": ["LC82010362025087LGN00"],
  "output_dir": "usgs_rabat_test"
}
```

### Response
- `201 Created`

```json
{
  "job_id": "fe6a91a4ddc14a39ba2646db6cc20147"
}
```

### Validation errors
- `422 Unprocessable Entity` for invalid dates/provider/AOI/output_dir.
- `413 Payload too large` when body exceeds `NIMBUS_MAX_REQUEST_MB`.

---

## POST /v1/jobs/batch

Create multiple jobs in one request.

### Request

```json
{
  "jobs": [
    {
      "job_type": "search_download",
      "provider": "copernicus",
      "collection": "SENTINEL-2",
      "product_type": "S2MSI2A",
      "start_date": "2025-01-01",
      "end_date": "2026-01-31",
      "aoi": {
        "wkt": "POLYGON((7.409 43.723,7.409 43.751,7.439 43.751,7.439 43.723,7.409 43.723))"
      },
      "output_dir": "countries/monaco"
    },
    {
      "job_type": "search_download",
      "provider": "copernicus",
      "collection": "SENTINEL-2",
      "product_type": "S2MSI2A",
      "start_date": "2025-01-01",
      "end_date": "2026-01-31",
      "aoi": {
        "wkt": "POLYGON((28.86 -2.84,28.86 -1.04,30.90 -1.04,30.90 -2.84,28.86 -2.84))"
      },
      "output_dir": "countries/rwanda"
    }
  ]
}
```

### Response
- `201 Created`

```json
{
  "job_ids": [
    "a1...",
    "b2..."
  ]
}
```

---

## GET /v1/jobs/{job_id}

Get status for one job.

### Response example

```json
{
  "job_id": "4096e3dd875d473aa88f5f203b445b53",
  "state": "succeeded",
  "progress": 100.0,
  "bytes_downloaded": 1725791483,
  "bytes_total": 1725791483,
  "started_at": "2026-02-17T15:02:14.847904Z",
  "finished_at": "2026-02-17T15:11:04.501134Z",
  "duration_seconds": 529.65323,
  "errors": [],
  "provider": "copernicus",
  "collection": "SENTINEL-2"
}
```

### Errors
- `404` if unknown job ID.

---

## DELETE /v1/jobs/{job_id}

Best-effort cancellation request.

### Response example

```json
{
  "job_id": "fe6a91a4ddc14a39ba2646db6cc20147",
  "cancel_requested": true
}
```

`cancel_requested=false` means job already terminal (or cancellation not needed).

---

## GET /v1/jobs/{job_id}/result

Returns final artifacts + metadata for completed job.

### Response example

```json
{
  "job_id": "fe6a91a4ddc14a39ba2646db6cc20147",
  "paths": [
    "/data/downloads/usgs_rabat_test/usgs_landsat_ot_c2_l2_0.zip",
    "/data/downloads/usgs_rabat_test/manifest.json"
  ],
  "checksums": {
    "/data/downloads/usgs_rabat_test/usgs_landsat_ot_c2_l2_0.zip": "aacd...0192",
    "/data/downloads/usgs_rabat_test/manifest.json": "a826...21b8"
  },
  "metadata": {
    "job_type": "download_products",
    "provider": "usgs",
    "collection": "landsat_ot_c2_l2",
    "products_requested": 1,
    "products_downloaded": 1,
    "output_dir": "/data/downloads/usgs_rabat_test"
  },
  "manifest_entry": {
    "job_id": "fe6a91a4ddc14a39ba2646db6cc20147",
    "provider": "usgs",
    "collection": "landsat_ot_c2_l2",
    "created_at": "2026-02-17T19:04:19.997095+00:00",
    "paths": [
      "/data/downloads/usgs_rabat_test/usgs_landsat_ot_c2_l2_0.zip"
    ],
    "checksums": {
      "/data/downloads/usgs_rabat_test/usgs_landsat_ot_c2_l2_0.zip": "aacd...0192",
      "/data/downloads/usgs_rabat_test/manifest.json": "a826...21b8"
    },
    "metadata": {
      "job_type": "download_products",
      "provider": "usgs",
      "collection": "landsat_ot_c2_l2",
      "products_requested": 1,
      "products_downloaded": 1,
      "output_dir": "/data/downloads/usgs_rabat_test"
    }
  }
}
```

### Errors
- `404` if result is not found yet.

---

## GET /v1/jobs

List jobs with filters and pagination.

### Query params
- `state` (optional)
- `provider` (optional)
- `date_from` (optional ISO datetime)
- `date_to` (optional ISO datetime)
- `page` (default `1`)
- `page_size` (default `20`)

### Example

```bash
curl -s "http://127.0.0.1:8000/v1/jobs?state=succeeded&provider=copernicus&page=1&page_size=20"
```

### Response shape

```json
{
  "items": [
    {
      "job_id": "...",
      "state": "succeeded",
      "progress": 100.0,
      "bytes_downloaded": 123,
      "bytes_total": 123,
      "started_at": "...",
      "finished_at": "...",
      "duration_seconds": 1.23,
      "errors": [],
      "provider": "copernicus",
      "collection": "SENTINEL-2"
    }
  ],
  "total": 42,
  "page": 1,
  "page_size": 20
}
```

---

## GET /v1/metrics

Prometheus metrics endpoint for service monitoring.

### Behavior
- returns Prometheus exposition text
- includes HTTP metrics + job submission/cancellation counters + job state gauges
- if `NIMBUS_ENABLE_METRICS=false`, endpoint returns `404`

### Notes
- if `NIMBUS_API_KEY` is set, this route is protected like other `/v1/*` routes.

## GET /v1/events (SSE)

Stream persisted job events for UI progress.

### Query params
- `job_id` (optional): scope stream to one job
- `since` (optional int): replay from event id > since

### Curl example

```bash
curl -N "http://127.0.0.1:8000/v1/events?job_id=<JOB_ID>&since=0"
```

### Example event frame

```text
id: 3
event: job.progress
data: {"id":3,"job_id":"...","type":"job.progress","timestamp":"...","payload":{"file":"...","bytes":12345,"bytes_total":56789,"speed":345678.0,"status":"running"}}
```

### Common event types
- `job.queued`
- `job.started`
- `job.products_found`
- `job.progress`
- `job.cancel_requested`
- `job.cancelled`
- `job.failed`
- `job.succeeded`
- `job.requeued_after_restart`
- `heartbeat`

---

## Error model overview

Typical error payload from FastAPI:

```json
{
  "detail": "..."
}
```

Frequent codes:
- `401` invalid/missing API key (when enabled)
- `404` unknown job/result
- `413` payload too large
- `422` validation errors
- `500` unexpected runtime/provider failures

---

## Practical workflow example

1. Create job:

```bash
JOB_ID=$(curl -s -X POST "http://127.0.0.1:8000/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_type":"search_download",
    "provider":"copernicus",
    "collection":"SENTINEL-2",
    "product_type":"S2MSI2A",
    "start_date":"2026-01-01",
    "end_date":"2026-01-02",
    "aoi":{"wkt":"POLYGON((0 0,0 1,1 1,1 0,0 0))"}
  }' | python3 -c 'import sys,json; print(json.load(sys.stdin)["job_id"])')
```

2. Watch status:

```bash
while true; do
  curl -s "http://127.0.0.1:8000/v1/jobs/$JOB_ID" | python3 -m json.tool
  sleep 2
done
```

3. Get result after success:

```bash
curl -s "http://127.0.0.1:8000/v1/jobs/$JOB_ID/result" | python3 -m json.tool
```
