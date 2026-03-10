# TheConverter

Satellite data converter for Sentinel-2 and Landsat.

## Installation

```bash
pip install -e .
```

## CLI Commands

### Convert SAFE/Landsat to Zarr

```bash
# Auto-generates output: S2A_MSIL2A_20240115_T29RPQ.zarr
converter convert S2A_MSIL2A_20240115_T29RPQ.SAFE --config config.yaml

# Or specify output
converter convert /path/to/S2A_...SAFE --output cube.zarr --config config.yaml
```

Options:
- `--output, -o` - Output Zarr path (default: same name with .zarr)
- `--config, -c` - Config YAML file (required)
- `--chunks` - Chunk size as 'y,x' (default: 512,512)


### Upload Zarr to OCI

```bash
converter upload cube.zarr --bucket STAY --namespace lrdwfp6kyp5x --path cubes/cube.zarr
```

Options:
- `--bucket, -b` - OCI bucket name (required)
- `--namespace, -n` - OCI namespace (required)
- `--path, -p` - Remote path in bucket (required)
- `--profile` - OCI config profile (default: DEFAULT)

### Query Zarr with WKT and Time Range

```bash
# Query single zarr
converter query ./cube.zarr --wkt "POLYGON(...)" --output clipped.zarr

# Query multiple zarrs and stack on time
converter query cube1.zarr cube2.zarr cube3.zarr \
    --wkt "POLYGON(...)" \
    --output stacked.zarr

# Query with time range filter
converter query cubes/sentinel/*.zarr \
    --bucket STAY --namespace lrdwfp6kyp5x \
    --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
    --start 2025-12-01 --end 2026-02-01 \
    --output stacked.zarr --preview preview.png
```

Options:
- `--wkt` - WKT polygon string (required)
- `--output, -o` - Output zarr path (required)
- `--start` - Start date (YYYY-MM-DD)
- `--end` - End date (YYYY-MM-DD)  
- `--preview, -p` - Optional RGB preview image
- `--bands` - Bands to extract (default: all)
- `--rgb` - Bands for preview (default: B04,B03,B02)
- `--bucket, -b` - OCI bucket (for remote zarr)
- `--namespace, -n` - OCI namespace (for remote zarr)
- `--crs` - Dataset CRS (default: EPSG:32629)




## Python API

```python
from converter.readers import Sentinel2Reader, ZarrReader
from converter.writers import ZarrWriter
from converter.utilities import OCIStore

# Read Sentinel-2
reader = Sentinel2Reader("config.yaml")
ds = reader.read("/path/to/S2A_...SAFE")

# Write to local Zarr
ZarrWriter().write(ds, "cube.zarr")

# Upload to OCI
store = OCIStore(bucket="STAY", namespace="lrdwfp6kyp5x", profile="prof")
mapper = store.get_mapper("cubes/cube.zarr", create=True)
ZarrWriter().write(ds, mapper)

# Query from OCI
mapper = store.get_mapper("cubes/cube.zarr")
ds = ZarrReader().read(mapper)
```
