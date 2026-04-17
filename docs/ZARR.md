# Zarr Conversion

## 1. Purpose

The converter runtime transforms raw downloaded scenes into a normalized array store suitable for:
- downstream analytics
- spectral and temporal stacking
- artifact indexing
- later cloud/water masking workflows

The core output layout is:

```text
imagery(time, band, y, x)
```

There is also a cube-building step for already-normalized scene stores:

```text
imagery(time, band, y, x)
```

In that cube, `time` grows across scenes instead of remaining `1`.

Default output location:

```text
/data/downloads/zarr/<scene>.zarr
```

## 2. Supported inputs

### Copernicus
- `SENTINEL-1`: `RAW`, `GRD`, `SLC`, `IW_SLC__1S`
- `SENTINEL-2`: `S2MSI1C`, `S2MSI2A`

### USGS
- `landsat_ot_c2_l1`: `L1TP`, `L1GT`, `L1GS`
- `landsat_ot_c2_l2`: `L2SP`, `L2SR`
- supported satellites: Landsat 8 and Landsat 9

## 3. Resolution policy

The converter uses the best target grid per sensor family, not a fake one-size-fits-all grid.

### Sentinel-2
- target grid: `10 m`
- native `20 m` and `60 m` bands are reprojected to `10 m`
- every native spectral raster layer found in the source product is preserved

### Landsat 8/9
- target grid: `10 m`
- all native image bands are aligned to the collection target grid configured by the converter
- ancillary layers such as QA and angle rasters are stored separately when present

### Sentinel-1
- target grid: native measurement/reference raster grid
- no optical-style forced `10 m` normalization
- all detected rasterized polarizations are preserved on the `band` axis
- ancillary radar rasters are stored separately when present

## 4. Layer preservation policy

Detailed product-by-product reference:
- `docs/PRODUCT_BANDS.md`

The converter no longer maps imagery to a reduced RGB/NIR/SWIR subset.

Rules:
- preserve every native physical imagery raster layer found in the source product
- preserve exact source layer names on the `band` coordinate
- preserve QA, mask, classification, cloud, snow, aerosol, angle, thermal-support, or radar-support rasters in `ancillary(time, ancillary_layer, y, x)` when present
- keep `imagery` and `ancillary` separate instead of flattening everything into one array

Typical examples:
- Sentinel-2 L1C imagery layers: `B01` through `B12` plus `B8A`
- Sentinel-2 L2A imagery layers: `B01` through `B12` plus `B8A`, excluding `B10`
- Landsat L1 imagery layers: `B1` through `B11`
- Landsat L2 imagery layers: `SR_B1` through `SR_B7`, plus `ST_B10` when present
- Sentinel-1 imagery layers: exact source polarization names such as `VV`, `VH`, `HH`, `HV`

## 5. Runtime API

The backend/orchestrator is the public API. The standalone Zarr FastAPI can remain available as an internal compatibility harness, but the UI and normal pipeline should use the backend endpoints below.

### `GET /v1/converter/health`
Basic converter health exposed by the backend.

### `GET /v1/converter/readiness`
Strict converter readiness, including a write-path smoke test.

### `GET /v1/converter/schema`
Returns:
- Zarr model metadata
- converter configuration actually loaded by the runtime

### `POST /v1/jobs/{job_id}/convert`
Run a manual conversion for raw outputs already attached to an existing pipeline job. The conversion remains attached to the same `job_id` and artifact lineage.

Minimal payload:

```json
{
  "raw_uri": "/data/downloads/<job>/<scene>",
  "output_uri": "/data/downloads/zarr/<scene>.zarr",
  "scene_id": "LC08_L2SP_...",
  "product_type": "L2SP"
}
```

### Automatic conversion

The default path is automatic:

```text
POST /v1/jobs
  -> worker searches and downloads
  -> worker detects raw outputs
  -> worker invokes the converter library in-process
  -> the same job transitions to pipeline_state=zarr_written
```

## 6. Implementation notes

Important modules:
- `src/nimbuschain_zarr_service/service.py`
- `src/nimbuschain_zarr_service/core.py`
- `src/nimbuschain_zarr_service/copernicus.py`
- `src/nimbuschain_zarr_service/readers/landsat.py`
- `src/nimbuschain_zarr_service/readers/sentinel.py`
- `src/nimbuschain_zarr_service/writers/zarr.py`
- `src/nimbuschain_zarr_service/config/config.yaml`
- `src/nimbuschain_fetch/engine/nimbus_fetcher.py`
- `src/nimbuschain_fetch_service/api/converter.py`

Design choice:
- Landsat conversion uses a streaming write path to avoid building an oversized in-memory cube.
- Copernicus conversion preserves exact source imagery layer names and routes ancillary layers into separate arrays.
- Sentinel-1 RAW produces a sample-space Zarr, not a georeferenced map-grid product.

## 7. Operational rule

A Zarr store is considered current when:
- it was produced by the current converter flow
- it is either registered as an artifact or locally discovered and matches the current schema expectations

Legacy local stores may still exist on disk. The UI hides them by default unless explicitly requested.

## 8. Time-series cube builder

The project also supports building a time-series cube from existing scene-level Zarr stores.

CLI:

```bash
nimbuschain-zarr-cube /path/to/scene_1.zarr /path/to/scene_2.zarr \
  --output-uri /path/to/cube.zarr
```

Grouped CLI:

```bash
nimbuschain-zarr-cube /data/downloads/zarr/*.zarr \
  --group-by-tile \
  --output-dir /data/downloads/zarr/cubes/manual \
  --start-date 2026-04-10 \
  --end-date 2026-04-13 \
  --stage-label before_mask
```

Integrated fetch pipeline:

```text
search/download -> scene zarr conversion -> optional cube before masking -> optional in-place masking -> optional cube after masking
```

The fetch job request now accepts:
- `cube_mode`: `none`, `before_mask`, or `after_mask`
- `cube_start_date`
- `cube_end_date`

The Streamlit UI exposes the same choices and limits the selectable cube date range to dates that are actually present in the current preview results.

V1 rules:
- inputs must already share the same spatial grid and imagery schema
- grouped mode builds one cube per Sentinel tile or Landsat path/row
- `time` uses the real acquisition timestamps from the source scene Zarr metadata
- the cube preserves `scene_id(time)` and `source_zarr_uri(time)` arrays for provenance
- ancillary layers are stacked only when every source has the same ancillary layer names, shape, and dtype
- quadkeys are written once at the cube root because the full cube shares one spatial footprint
- cube outputs are registered as `zarr_cube` artifacts by the pipeline
- `masks/...` arrays are not stacked into the cube yet; cube contents are currently imagery plus compatible ancillary layers
- reprojection, mosaicking, and mixed-grid harmonization are out of scope for this first version
