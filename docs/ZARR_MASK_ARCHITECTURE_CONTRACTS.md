# Zarr + Mask Architecture Contracts

This document freezes the target architecture and inter-service contracts for the
next pipeline extension:

`fetcher-service -> zarr-converter-service -> mask-service`

It is the source of truth for task 1:

- service responsibilities
- input/output payloads
- pipeline states

The goal is to let two contributors work independently without drifting on
interfaces.

## Scope

This contract covers:

- raw scene download (`fetcher-service`)
- conversion of raw scenes to Zarr (`zarr-converter-service`)
- generation of cloud/water masks on top of the Zarr (`mask-service`)
- orchestration/job state transitions in the existing backend

This contract does not yet cover:

- UI changes
- direct OCI write path
- multi-scene mosaics
- cross-scene temporal stacks

## Services And Responsibilities

## `fetcher-service`

Current role in the platform.

Responsibilities:

- accept a download request from the orchestrator
- download one scene from the provider
- validate that the raw artifact exists
- write raw files into the working storage area
- return the raw artifact location and source metadata

Outputs owned by this service:

- raw scene directory or archive (`raw_uri`)
- provider metadata needed downstream

It must not:

- convert to Zarr
- compute masks

## `zarr-converter-service`

New microservice.

Responsibilities:

- read one downloaded raw scene
- normalize provider-specific band layout into the internal band schema
- write a Zarr store with imagery + metadata
- return the Zarr location and technical metadata

Outputs owned by this service:

- Zarr store (`zarr_uri`)
- band inventory
- shape/chunk/dtype summary

It must not:

- download source data
- call OmniCloudMask / OmniWaterMask

## `mask-service`

New microservice.

Responsibilities:

- read one normalized Zarr store
- extract required bands for cloud/water inference
- run `OmniCloudMask` and `OmniWaterMask`
- write masks back into the Zarr store
- return mask locations and summary statistics

Outputs owned by this service:

- `/masks/cloud`
- `/masks/water`
- mask summary metrics

It must not:

- download provider data
- own Zarr conversion

## `orchestrator`

Existing backend orchestration layer.

Responsibilities:

- create and persist jobs
- dispatch stage-specific work
- enforce stage order
- track state transitions, retries, and failure reasons
- persist final artifact references

It owns:

- pipeline state machine
- stage chaining
- retries and visibility

## Artifact Model

The pipeline produces three artifact layers:

1. Raw artifact
2. Zarr artifact
3. Enriched Zarr artifact with masks

## Raw Artifact Contract

- `raw_uri`: local path or storage URI pointing to the downloaded scene root
- `raw_format`: one of `copernicus_safe`, `landsat_bundle`, `geotiff_bundle`
- `scene_id`: provider scene identifier
- `provider`: `copernicus` or `usgs`
- `collection`: provider collection / mission

## Zarr Artifact Contract

- `zarr_uri`: local path or storage URI of the written Zarr root
- `zarr_format_version`: internal schema version, starts at `1`
- `bands_written`: list of canonical band names
- `shape`: array shape summary
- `chunks`: chunk shape summary
- `dtype`: data type summary

## Mask Artifact Contract

Masks are written into the same Zarr store.

- cloud mask path: `masks/cloud`
- water mask path: `masks/water`

Returned summary:

- `cloud_coverage_pct`
- `water_coverage_pct`
- `nodata_pct`
- optional per-class counts when available

## Canonical Band Schema

Both providers must be normalized into the same internal schema before masking.

Initial canonical names:

- `blue`
- `green`
- `red`
- `nir`
- `swir1`
- `swir2`

Minimum required for masks:

- `OmniCloudMask`: `red`, `green`, `nir`
- `OmniWaterMask`: `red`, `green`, `blue`, `nir`

If required bands are missing:

- the stage must fail explicitly
- the orchestrator must record a non-retryable error unless the root cause is a transient read issue

## Target Zarr Layout

Initial Zarr layout:

```text
<zarr_root>/
  imagery/
  masks/
    cloud/
    water/
  metadata/
```

Minimum logical model:

- dimensions: `time`, `band`, `y`, `x`
- a single-scene job writes `time=1`
- metadata must preserve CRS and transform information

Required metadata fields:

- `provider`
- `collection`
- `scene_id`
- `acquisition_datetime`
- `crs`
- `transform`
- `band_names`
- `zarr_format_version`

## Inter-Service Request Contracts

All stage requests must include a common job envelope plus stage-specific fields.

## Common Job Envelope

```json
{
  "job_id": "string",
  "pipeline_id": "string",
  "trace_id": "string",
  "requested_by": "string",
  "created_at": "ISO-8601 timestamp"
}
```

Rules:

- `job_id` identifies the current stage job
- `pipeline_id` identifies the full chained workflow
- `trace_id` is reused across all stages for logs/observability

## Contract: `fetcher-service` -> `zarr-converter-service`

Request payload:

```json
{
  "job_id": "zarr-job-123",
  "pipeline_id": "pipe-123",
  "trace_id": "trace-123",
  "provider": "copernicus",
  "collection": "SENTINEL-2",
  "scene_id": "S2A_...",
  "raw_uri": "/data/downloads/job-abc/scene.SAFE",
  "raw_format": "copernicus_safe",
  "output_uri": "/data/zarr/job-abc"
}
```

Response payload:

```json
{
  "job_id": "zarr-job-123",
  "pipeline_id": "pipe-123",
  "status": "succeeded",
  "zarr_uri": "/data/zarr/job-abc/scene.zarr",
  "zarr_format_version": 1,
  "bands_written": ["blue", "green", "red", "nir"],
  "shape": {"time": 1, "band": 4, "y": 10980, "x": 10980},
  "chunks": {"time": 1, "band": 1, "y": 1024, "x": 1024},
  "dtype": "uint16"
}
```

## Contract: `zarr-converter-service` -> `mask-service`

Request payload:

```json
{
  "job_id": "mask-job-123",
  "pipeline_id": "pipe-123",
  "trace_id": "trace-123",
  "provider": "copernicus",
  "collection": "SENTINEL-2",
  "scene_id": "S2A_...",
  "zarr_uri": "/data/zarr/job-abc/scene.zarr",
  "band_map": {
    "blue": "imagery/blue",
    "green": "imagery/green",
    "red": "imagery/red",
    "nir": "imagery/nir"
  }
}
```

Response payload:

```json
{
  "job_id": "mask-job-123",
  "pipeline_id": "pipe-123",
  "status": "succeeded",
  "zarr_uri": "/data/zarr/job-abc/scene.zarr",
  "cloud_mask_path": "masks/cloud",
  "water_mask_path": "masks/water",
  "mask_summary": {
    "cloud_coverage_pct": 18.2,
    "water_coverage_pct": 4.6,
    "nodata_pct": 0.0
  }
}
```

## Contract: Final Stage -> `orchestrator`

Persisted final artifact shape:

```json
{
  "pipeline_id": "pipe-123",
  "raw_uri": "/data/downloads/job-abc/scene.SAFE",
  "zarr_uri": "/data/zarr/job-abc/scene.zarr",
  "mask_summary": {
    "cloud_coverage_pct": 18.2,
    "water_coverage_pct": 4.6,
    "nodata_pct": 0.0
  }
}
```

## Pipeline State Machine

The orchestrator owns all stage states.

Initial state set:

- `queued`
- `fetching`
- `fetched`
- `zarr_converting`
- `zarr_ready`
- `masking`
- `completed`
- `failed`
- `cancel_requested`
- `cancelled`

## Required State Order

Happy path:

```text
queued
-> fetching
-> fetched
-> zarr_converting
-> zarr_ready
-> masking
-> completed
```

Failure path:

```text
<any active stage> -> failed
```

Cancellation path:

```text
<any active stage> -> cancel_requested -> cancelled
```

Rules:

- `zarr_converting` cannot start before `fetched`
- `masking` cannot start before `zarr_ready`
- `completed` requires both masks persisted (or an explicitly accepted partial-mode contract, not part of V1)

## Retry Rules

Retries must be stage-local.

- fetch retry must not rerun successful downstream stages because none exist yet
- Zarr conversion retry must not trigger a new download if `raw_uri` is valid
- masking retry must not trigger a new download or a new Zarr conversion if `zarr_uri` is valid

This is required to keep the pipeline cost and runtime predictable.

## Error Contract

Each service must return structured failures:

```json
{
  "job_id": "string",
  "pipeline_id": "string",
  "status": "failed",
  "error_code": "string",
  "error_message": "string",
  "retryable": false
}
```

Minimum error codes:

- `RAW_NOT_FOUND`
- `UNSUPPORTED_RAW_FORMAT`
- `MISSING_REQUIRED_BANDS`
- `ZARR_WRITE_FAILED`
- `MASK_RUNTIME_FAILED`
- `MASK_INPUT_INVALID`

## Observability Contract

Every stage must log with:

- `trace_id`
- `pipeline_id`
- `job_id`
- `stage`
- `provider`
- `scene_id`

Minimum metrics per stage:

- stage duration
- success/failure count
- retry count
- output size when applicable

## Definition Of Done For Task 1

Task 1 is complete when:

1. both contributors agree to this document as the contract baseline
2. service boundaries are explicit
3. request/response payloads are fixed enough to start coding task 2
4. the pipeline state machine is fixed enough to start orchestration changes later

## Immediate Follow-Up

The next implementation task can now start safely:

- task 2: create the `zarr-converter-service` skeleton using this contract as its interface baseline
