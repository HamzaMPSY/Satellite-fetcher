# Zarr Conversion

## Purpose

The Zarr service converts raw downloaded scenes into a normalized array store for downstream analytics and masking.

Output layout:

```text
time, band, y, x
```

Default output location:

```text
/data/downloads/zarr/<scene>.zarr
```

## Supported inputs

### Copernicus
- `SENTINEL-1`: `RAW`, `GRD`, `SLC`, `IW_SLC__1S`
- `SENTINEL-2`: `S2MSI1C`, `S2MSI2A`

### USGS
- `landsat_ot_c2_l1`: `L1TP`, `L1GT`, `L1GS`
- `landsat_ot_c2_l2`: `L2SP`, `L2SR`
- satellites: Landsat 8 and Landsat 9

## Resolution policy

The converter does not downsample to the coarsest grid.

### Sentinel-2
- target grid: `10 m`
- 20 m and 60 m bands are resampled to 10 m

### Landsat 8/9
- target grid: `30 m`
- thermal bands are aligned to the 30 m grid

### Sentinel-1
- target grid: source raster measurement grid
- no fake optical-style 10 m upsampling is introduced

## Canonical output intent

The service writes a Zarr store that is usable later for:

- spectral analytics
- temporal stacking
- cloud/water masking
- artifact indexing in the orchestrator

## Main endpoint

### `POST /convert`

Minimal payload:

```json
{
  "job_id": "job-1",
  "pipeline_id": "pipe-1",
  "trace_id": "trace-1",
  "provider": "usgs",
  "collection": "landsat_ot_c2_l2",
  "scene_id": "LC08_L2SP_...",
  "raw_uri": "/data/downloads/<job>/<scene>",
  "raw_format": "directory",
  "output_uri": "/data/downloads/zarr/<scene>.zarr"
}
```

## Service health

```bash
curl -s http://127.0.0.1:8010/health | python3 -m json.tool
```

## Artifact registration

After a successful conversion, the UI or orchestrator can register the Zarr store through:

```text
POST /v1/artifacts
```
