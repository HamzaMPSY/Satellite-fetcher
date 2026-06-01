# Manual CLI Test Matrix

This runbook covers manual end-to-end checks for the two local CLIs:

- `nimbuschain_fetch.pipeline_cli`: VM-friendly direct-source runner.
- `nimbuschain_fetch.stage_cli`: modular stage/DAG runner.

It is written for the local compose stack, with commands executed inside the
`nimbus-api` container so paths such as `/data/downloads/...` match the service
mounts.

## 1. Preflight

Start or verify the stack from the repository root:

```bash
podman machine start
scripts/run_all.sh --launch-mode mps --no-build
podman ps --format '{{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -fsS http://127.0.0.1:8000/v1/health
```

Expected:

- `nimbus-api`, `nimbus-zarr`, `nimbus-sen2like`, `nimbus-ui`, and `mongodb`
  are `Up`.
- In `mps` launch mode, `nimbus-mask` is intentionally not part of the compose
  path; masking goes to the host-native MPS service.
- API health returns `{"status":"ok","ready":true,...}`.

If VS Code Pod Manager cannot connect but `podman ps` works in the terminal,
the stack is still usable for these CLI tests.

Define a small helper and the raw input paths. Replace these paths with your
own real or synthetic products when needed.

```bash
export API_CONTAINER=${API_CONTAINER:-backendnimbus-nimbus-api-1}
api() { podman exec "$API_CONTAINER" "$@"; }

export RUN_ID=${RUN_ID:-manual-cli-$(date +%Y%m%d-%H%M%S)}
export BASE=/data/downloads/cli-e2e/run-465bced4
export OUT=/data/downloads/cli-manual/$RUN_ID

export S2_RAW=$BASE/raw/S2A_MSIL2A_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE
export L8_RAW_A=$BASE/raw/LC09_L1TP_199026_20260101_20260102_02_T1
export L8_RAW_B=$BASE/raw/LC09_L1TP_199026_20260103_20260104_02_T1

api test -e "$S2_RAW"
api test -e "$L8_RAW_A"
api test -e "$L8_RAW_B"
```

Service URLs used by the commands:

```bash
export ZARR_URL=http://nimbus-zarr:8010
export MASK_URL=http://host.containers.internal:18021
export SEN2LIKE_URL=http://nimbus-sen2like:8030
```

## 2. Case Matrix

Run the cases that match the change you are validating. For release smoke
testing, run all green-path cases and at least one edge case.

| Case | CLI | Sensor | Goal | Expected |
| --- | --- | --- | --- | --- |
| P1 | `pipeline_cli` | Sentinel | Zarr only | `status=completed`, one Zarr |
| P2 | `pipeline_cli` | Sentinel | Masks only | mask result `written` |
| P3 | `pipeline_cli` | Sentinel | Cube before mask | cube result `written` |
| P4 | `pipeline_cli` | Sentinel | Cube after mask daily mosaic | cube result `written` |
| P5 | `pipeline_cli` | Landsat | Zarr only | `status=completed`, one Zarr |
| P6 | `pipeline_cli` | Landsat | Cube before mask, two scenes | cube result `written` |
| P7 | `pipeline_cli` | Landsat | Cube after mask, two scenes | cube result `written` |
| P8 | `pipeline_cli` | Landsat | Daily mosaic edge | cube may be `skipped`, masks still run |
| S1 | `stage_cli` | Sentinel | Plan | no `sen2like` stage |
| S2 | `stage_cli` | Landsat | Plan | includes `sen2like` before `zarr` |
| S3 | `stage_cli` | Sentinel | Runtime after-mask cube | all stages `succeeded` |
| S4 | `stage_cli` | Landsat | Runtime before-mask cube | Sen2Like writes normalized outputs; Sen2Like failure fails the run |
| S5 | `stage_cli` | Any | Existing Zarr inputs | skips raw conversion and runs Mask/Cube |

## 3. `pipeline_cli` Cases

### P1. Sentinel Zarr Only

```bash
api python -m nimbuschain_fetch.pipeline_cli "$S2_RAW" \
  --launch-mode mps \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-sentinel-zarr"
```

Expected:

- top-level `status` is `completed`
- `converted_items[0].zarr_uri` points under `$OUT/zarr/pipeline-sentinel-zarr`
- `cube_result` is `null`

### P2. Sentinel Masks Only

```bash
api python -m nimbuschain_fetch.pipeline_cli "$S2_RAW" \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-sentinel-mask" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL"
```

Expected:

- top-level `status` is `completed`
- `converted_items[0].mask_result.status` is `written`
- water and cloud mask arrays are present in the output Zarr

### P3. Sentinel Cube Before Mask

```bash
api python -m nimbuschain_fetch.pipeline_cli "$S2_RAW" \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-sentinel-before" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL" \
  --cube-mode before_mask \
  --cube-output-dir "$OUT/cubes/pipeline-sentinel-before" \
  --cube-layout grouped_time \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 10 \
  --cube-overlap-policy least_cloud \
  --group-by-tile
```

Expected:

- `cube_result.status` is `written`
- `cube_result.items[0].source_scene_count` is `1`
- `converted_items[0].mask_result.status` is `written`

### P4. Sentinel Cube After Mask, Daily Mosaic

```bash
api python -m nimbuschain_fetch.pipeline_cli "$S2_RAW" \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-sentinel-after" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL" \
  --cube-mode after_mask \
  --cube-output-dir "$OUT/cubes/pipeline-sentinel-after" \
  --cube-layout daily_mosaic \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 10 \
  --cube-overlap-policy least_cloud \
  --group-by-tile \
  --include-masks-in-cube
```

Expected:

- `cube_result.status` is `written`
- cube output is named like `daily_mosaic_*_after_mask.zarr`
- `masks_written` is `true` when masks are included successfully

### P5. Landsat Zarr Only

```bash
api python -m nimbuschain_fetch.pipeline_cli "$L8_RAW_A" \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-landsat-zarr"
```

Expected:

- top-level `status` is `completed`
- `converted_items[0].summary.provider` is `usgs`
- `converted_items[0].summary.collection` is `landsat_ot_c2_l1`

### P6. Landsat Cube Before Mask, Two Scenes

```bash
api python -m nimbuschain_fetch.pipeline_cli "$L8_RAW_A" "$L8_RAW_B" \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-landsat-before" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL" \
  --cube-mode before_mask \
  --cube-output-dir "$OUT/cubes/pipeline-landsat-before" \
  --cube-layout grouped_time \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 30 \
  --cube-overlap-policy latest \
  --group-by-tile
```

Expected:

- `cube_result.status` is `written`
- `cube_result.items[0].source_scene_count` is `2`
- cube output is named like `cube_199026_*_before_mask.zarr`
- masks are written after the cube is built

### P7. Landsat Cube After Mask, Two Scenes

```bash
api python -m nimbuschain_fetch.pipeline_cli "$L8_RAW_A" "$L8_RAW_B" \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-landsat-after" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL" \
  --cube-mode after_mask \
  --cube-output-dir "$OUT/cubes/pipeline-landsat-after" \
  --cube-layout grouped_time \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 30 \
  --cube-overlap-policy latest \
  --group-by-tile \
  --include-masks-in-cube
```

Expected:

- `cube_result.status` is `written`
- `stage_label` is `after_mask`
- masks are available before cube construction

### P8. Landsat Daily Mosaic Edge

Daily mosaic is a Sentinel-oriented layout. This is an edge/regression check:
the run must not fail the whole pipeline when the cube is not applicable.

```bash
api python -m nimbuschain_fetch.pipeline_cli "$L8_RAW_A" \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --zarr-service-url "$ZARR_URL" \
  --zarr-dir "$OUT/zarr/pipeline-landsat-daily-edge" \
  --mask-types water,cloud \
  --mask-service-url "$MASK_URL" \
  --cube-mode after_mask \
  --cube-output-dir "$OUT/cubes/pipeline-landsat-daily-edge" \
  --cube-layout daily_mosaic \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 30 \
  --cube-overlap-policy least_cloud \
  --group-by-tile
```

Expected:

- top-level `status` is `completed`
- masks still write successfully
- `cube_result.status` may be `skipped` with a clear reason

## 4. `stage_cli` Cases

### S1. Sentinel Plan

```bash
api python -m nimbuschain_fetch.stage_cli plan \
  --launch-mode mps \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --target-stage zarr
```

Expected:

- `status` is `planned`
- stages are `fetch -> zarr`
- there is no `sen2like` stage

### S2. Landsat Plan

```bash
api python -m nimbuschain_fetch.stage_cli plan \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --target-stage zarr
```

Expected:

- `status` is `planned`
- stages are `fetch -> sen2like -> zarr`
- `zarr.depends_on` is `["sen2like"]`

For a full Landsat before-mask DAG:

```bash
api python -m nimbuschain_fetch.stage_cli plan \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --mask-types water,cloud \
  --cube-mode before_mask \
  --target-stage mask
```

Expected stages:

```text
fetch -> sen2like -> zarr -> cube -> mask
```

### S3. Sentinel Runtime After-Mask Cube

```bash
api python -m nimbuschain_fetch.stage_cli run-stage \
  --job-id "manual-stage-sentinel-$RUN_ID" \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --raw-uri "$S2_RAW" \
  --mask-types water,cloud \
  --zarr-service-url "$ZARR_URL" \
  --mask-service-url "$MASK_URL" \
  --zarr-output-dir "$OUT/zarr/stage-sentinel-after" \
  --cube-output-dir "$OUT/cubes/stage-sentinel-after" \
  --cube-mode after_mask \
  --cube-layout daily_mosaic \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 10 \
  --cube-overlap-policy least_cloud \
  --include-masks-in-cube \
  --stage cube \
  --execute
```

Expected:

- top-level `status` is `completed`
- `execution_mode` is `runtime`
- `fetch`, `zarr`, `mask`, and `cube` are all `succeeded`
- cube output is under `$OUT/cubes/stage-sentinel-after`

### S4. Landsat Runtime Before-Mask Cube, Then Mask

This is the main modular Landsat E2E check. Sen2Like must write normalized
Sentinel-like outputs. A Sen2Like failure is expected to fail the run before
Zarr conversion unless `--allow-sen2like-raw-fallback` is explicitly passed for
degraded fallback testing.

```bash
api python -m nimbuschain_fetch.stage_cli run-stage \
  --job-id "manual-stage-landsat-$RUN_ID" \
  --provider usgs \
  --collection landsat_ot_c2_l1 \
  --product-type L1TP \
  --raw-uris "$L8_RAW_A,$L8_RAW_B" \
  --sen2like-service-url "$SEN2LIKE_URL" \
  --mask-types water,cloud \
  --zarr-service-url "$ZARR_URL" \
  --mask-service-url "$MASK_URL" \
  --zarr-output-dir "$OUT/zarr/stage-landsat-before" \
  --cube-output-dir "$OUT/cubes/stage-landsat-before" \
  --cube-mode before_mask \
  --cube-layout grouped_time \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 30 \
  --cube-overlap-policy latest \
  --stage mask \
  --execute
```

Expected:

- top-level `status` is `completed`
- stages are `fetch`, `sen2like`, `zarr`, `cube`, `mask`
- every stage status is `succeeded`
- `sen2like.outputs` point to Sentinel-like outputs, not the raw Landsat inputs
- `zarr.metadata.conversion_provider` is `copernicus`
- cube output is named like `cube_199026_*_before_mask.zarr`

### S5. Existing Zarr Inputs

Use this when raw conversion has already been done and you only want to test
mask/cube stages.

```bash
export S2_ZARR=$OUT/zarr/stage-sentinel-after/S2A_MSIL2A_20260101T105501_N0511_R051_T31TDN_20260101T145209.zarr

api python -m nimbuschain_fetch.stage_cli run-stage \
  --job-id "manual-stage-existing-zarr-$RUN_ID" \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --source-zarr-uri "$S2_ZARR" \
  --mask-types water,cloud \
  --cube-mode after_mask \
  --cube-layout daily_mosaic \
  --cube-target-crs EPSG:32631 \
  --cube-target-resolution-m 10 \
  --cube-overlap-policy least_cloud \
  --zarr-service-url "$ZARR_URL" \
  --mask-service-url "$MASK_URL" \
  --cube-output-dir "$OUT/cubes/stage-existing-zarr" \
  --stage cube \
  --execute
```

Expected:

- `zarr` stage reports `runner=existing_zarr_inputs`
- `mask` and `cube` stages are `succeeded`

## 5. Quick Output Checks

List generated Zarr scene stores and cubes:

```bash
api find "$OUT" -maxdepth 4 -type d -name '*.zarr' | sort
```

Inspect one dataset through the Zarr service API if needed:

```bash
curl -fsS "http://127.0.0.1:8010/v1/health"
```

For CLI JSON responses, the most important fields are:

- top-level `status`
- `converted_items[*].zarr_uri`
- `converted_items[*].mask_result.status`
- `cube_result.status`
- `results[*].name`
- `results[*].status`
- `results[*].metadata.fallback_to_raw` (only when
  `--allow-sen2like-raw-fallback` is explicitly used for degraded tests)
- `results[*].metadata.conversion_provider`

## 6. Troubleshooting

### Podman socket errors in VS Code

Use the terminal as the source of truth:

```bash
podman machine start
podman ps
```

If `podman ps` works, manual CLI tests can run even if the VS Code Pod Manager
extension is stale.

### API health fails

```bash
podman compose ps
podman compose logs --tail=100 nimbus-api
podman compose logs --tail=100 nimbus-zarr
tail -n 100 data/downloads/mask-cache/host-mps-mask.log
```

### Raw path does not exist in the container

Remember that the host `data/downloads/...` directory is mounted as
`/data/downloads/...` in the containers.

```bash
api ls -lah /data/downloads
api find /data/downloads -maxdepth 4 -type d | head
```

### Sen2Like fails on Landsat

For normal API and `stage_cli` runtime tests, this is fatal. Expected behavior:

- `sen2like` stage status is `failed`
- downstream `zarr` is skipped because `sen2like` did not succeed
- the job/run exits failed, with the Sen2Like error in the stage metadata

For degraded fallback experiments only, pass
`--allow-sen2like-raw-fallback`. In that mode the expected fallback behavior is:

- `sen2like` stage status is `succeeded`
- `sen2like.metadata.fallback_to_raw` is `true`
- `zarr` stage still runs
- `zarr.metadata.conversion_provider` is `usgs`

Do not use degraded fallback for production Landsat validation.

If the error mentions `6S executable not found`, rebuild and recreate
`nimbus-sen2like`, then verify:

```bash
podman exec backendnimbus_nimbus-sen2like_1 which sixs
podman exec backendnimbus_nimbus-sen2like_1 python -m sixs_bin --test-wrapper
curl -fsS http://127.0.0.1:8030/readiness
```

The readiness payload must include `"sixs_executable_exists": true`.

### Water mask logs show `Permission denied: 'cache'`

This comes from OSMnx inside OmniWaterMask trying to write its default relative
`./cache` folder. Restart the host MPS mask service or recreate `nimbus-mask`
in `oci` mode with:

```bash
NIMBUS_WATERMASK_OSMNX_CACHE_DIR=/data/downloads/mask-cache/osmnx
NIMBUS_WATERMASK_OSMNX_USE_CACHE=true
NIMBUS_WATERMASK_BATCH_SIZE=2
NIMBUS_WATERMASK_TILE_SIZE=512
NIMBUS_WATERMASK_MODEL_PROGRESS_SECONDS=5
```

The water-mask request can keep running, but every tile may stay slow until the
mask service is restarted with the writable cache settings. The local MPS path
uses the host-native service; the `oci` profile uses the `nimbus-mask` container.

### Landsat daily mosaic

Daily mosaic is not the primary Landsat layout. Use `grouped_time` for the
green path. A Landsat daily-mosaic test is only an edge check to make sure the
whole pipeline does not fail when the cube is skipped.
