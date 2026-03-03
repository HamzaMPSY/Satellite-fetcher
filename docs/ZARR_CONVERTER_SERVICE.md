# Zarr Converter Service Skeleton

This document describes the skeleton created for task 2: the initial
`zarr-converter-service`.

## Purpose

The service is the future microservice responsible for:

- reading one raw scene downloaded by the fetcher
- validating the conversion contract
- converting the scene into a normalized Zarr store

The current implementation is intentionally minimal:

- it starts locally
- it exposes health/docs endpoints
- it accepts `POST /convert`
- it validates the request payload
- it does not execute the real conversion yet

## Code Location

- package: `src/nimbuschain_zarr_service`
- app entrypoint: `src/nimbuschain_zarr_service/main.py`
- container: `zarr-service/Containerfile`

## Endpoints

## `GET /`

Returns service metadata and docs location.

## `GET /health`

Returns a simple health payload with:

- `service`
- `status`
- `version`
- `conversion_ready`

`conversion_ready` is `false` in the skeleton because the actual conversion
logic is not implemented yet.

## `GET /schema`

Returns the current machine-readable Zarr model used as the baseline for the
conversion service.

This endpoint exposes:

- layout paths
- dimension order
- canonical bands
- required metadata
- default chunks
- default compression

## `POST /convert`

Accepts the stage contract payload and returns an `accepted` response.

Required request fields:

- `job_id`
- `pipeline_id`
- `trace_id`
- `provider`
- `collection`
- `scene_id`
- `raw_uri`
- `raw_format`
- `output_uri`

The endpoint currently:

- validates the payload
- returns `status=accepted`
- returns `stage=zarr_converting`

## Local Run

From the repository root:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
PYTHONPATH=src python3 -m uvicorn nimbuschain_zarr_service.main:app --host 0.0.0.0 --port 8010
```

Then open:

- `http://127.0.0.1:8010/health`
- `http://127.0.0.1:8010/docs`

## Console Script

The root `pyproject.toml` now exposes:

```bash
nimbuschain-zarr-service
```

This runs the same FastAPI service on port `8010`.

## Container Build

Build:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
podman build -f zarr-service/Containerfile -t ghcr.io/nimbuschain/nimbus-zarr-service:latest .
```

Run:

```bash
podman run --rm -p 8010:8010 ghcr.io/nimbuschain/nimbus-zarr-service:latest
```

## What Is Intentionally Missing

Not implemented yet:

- SAFE parsing
- Landsat parsing
- Zarr writing
- storage backends
- orchestration integration

Those belong to the next implementation tasks.

The Zarr data model itself is now frozen and documented separately in:

- `/Users/mehdidinari/Desktop/backend nimbus/docs/ZARR_DATA_MODEL.md`
