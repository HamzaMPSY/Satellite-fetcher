# Zarr Conversion

## 1. Purpose

The Zarr service converts raw downloaded scenes into a normalized array store suitable for:
- downstream analytics
- spectral and temporal stacking
- artifact indexing
- later cloud/water masking workflows

The core output layout is:

```text
imagery(time, band, y, x)
```

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
- target grid: `30 m`
- all native image bands are aligned to the `30 m` grid
- Level-1 `B8` panchromatic is preserved as a band, but it does **not** force the whole cube to `15 m`
- ancillary layers such as QA and angle rasters are stored separately when present

### Sentinel-1
- target grid: native measurement/reference raster grid
- no optical-style forced `10 m` normalization
- all detected rasterized polarizations are preserved on the `band` axis
- ancillary radar rasters are stored separately when present

## 4. Layer preservation policy

Detailed product-by-product reference:
- `/Users/mehdidinari/Desktop/backend nimbus/docs/PRODUCT_BANDS.md`

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

### `GET /health`
Basic health for the Zarr service.

### `GET /readiness`
Strict readiness, including a write-path smoke test.

### `GET /schema`
Returns:
- Zarr model metadata
- converter configuration actually loaded by the service

### `POST /convert`
Convert one local raw scene/archive into Zarr.

Minimal payload:

```json
{
  "job_id": "job-1",
  "pipeline_id": "pipe-1",
  "trace_id": "trace-1",
  "provider": "usgs",
  "collection": "landsat_ot_c2_l2",
  "product_type": "L2SP",
  "scene_id": "LC08_L2SP_...",
  "raw_uri": "/data/downloads/<job>/<scene>",
  "raw_format": "directory",
  "output_uri": "/data/downloads/zarr/<scene>.zarr"
}
```

## 6. Implementation notes

Important modules:
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/service.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/core.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/copernicus.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/readers/landsat.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/readers/sentinel.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/writers/zarr.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/config/config.yaml`

Design choice:
- Landsat conversion uses a streaming write path to avoid building an oversized in-memory cube.
- Copernicus conversion preserves exact source imagery layer names and routes ancillary layers into separate arrays.
- Sentinel-1 RAW produces a sample-space Zarr, not a georeferenced map-grid product.

## 7. Operational rule

A Zarr store is considered current when:
- it was produced by the current converter flow
- it is either registered as an artifact or locally discovered and matches the current schema expectations

Legacy local stores may still exist on disk. The UI hides them by default unless explicitly requested.
