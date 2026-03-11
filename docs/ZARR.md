# Zarr Conversion

## 1. Purpose

The Zarr service converts raw downloaded scenes into a normalized array store suitable for:
- downstream analytics
- spectral and temporal stacking
- artifact indexing
- later cloud/water masking workflows

The output layout is:

```text
time, band, y, x
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

### Landsat 8/9
- target grid: `30 m`
- thermal bands are aligned to the `30 m` grid
- Level-1 `B8` panchromatic is preserved as a band, but it does **not** force the whole cube to `15 m`
- the converter does not upsample Landsat to `10 m`

### Sentinel-1
- target grid: native measurement/reference raster grid
- no optical-style forced `10 m` normalization

## 4. Band preservation policy

### Sentinel-2 canonical bands

The converter preserves the useful spectral bands of the product instead of collapsing everything into RGB/NIR/SWIR only.

Expected canonical set:
- `coastal`
- `blue`
- `green`
- `red`
- `rededge1`
- `rededge2`
- `rededge3`
- `nir`
- `nir_narrow`
- `water_vapor`
- `cirrus`
- `swir1`
- `swir2`
- `scene_classification` when `SCL` is available in `S2MSI2A`

### Landsat 8/9 canonical bands

#### Level 1
- `coastal`
- `blue`
- `green`
- `red`
- `pan`
- `nir`
- `cirrus`
- `swir1`
- `swir2`
- `thermal1`
- `thermal2`

#### Level 2
Bands depend on the product family actually present in the source. Typical `L2SP` output keeps:
- `coastal`
- `blue`
- `green`
- `red`
- `nir`
- `swir1`
- `swir2`
- `thermal1`

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
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/landsat.py`
- `/Users/mehdidinari/Desktop/backend nimbus/src/nimbuschain_zarr_service/config/config.yaml`

Design choice:
- Landsat conversion uses a streaming write path to avoid building an oversized in-memory cube.
- Copernicus conversion keeps a sensor-aware normalization model and supports a stream-to-Zarr path as well.

## 7. Operational rule

A Zarr store is considered current when:
- it was produced by the current converter flow
- it is either registered as an artifact or locally discovered and matches the current schema expectations

Legacy local stores may still exist on disk. The UI hides them by default unless explicitly requested.
