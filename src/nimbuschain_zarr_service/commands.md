## Zarr Converter CLI Commands

All commands are provided by the `converter` CLI (entrypoint defined in `src/converter/cli.py`).

---

### `converter convert`
Convert a single Sentinel-2 SAFE or Landsat (L1TP/L2SP) product to Zarr.

**Usage**

```bash
converter convert <input_path> \
  --config <config.yaml> \
  [--output/-o <out.zarr>] \
  [--chunks "Y,X"]
```

**Key points**
- Auto-detects product type (Sentinel-2 if name contains `MSIL`, Landsat if `L1TP`/`L2SP`).
- Default output: same folder/name with `.zarr` extension.
- `--chunks` sets chunking (default `512,512` for y,x; time is fixed to 1).

**Example**
```bash
# Sentinel-2 SAFE → Zarr (auto output name)
converter convert data/S2A_MSIL2A_20240115_T29RPQ.SAFE \
  --config config/config.yaml

# Landsat L2SP → Zarr with custom chunking and output name
converter convert data/LC08_L2SP_190030_20240115_20240123_02_T1 \
  --config config/config.yaml \
  --output cubes/LC08_190030.zarr \
  --chunks "1024,1024"
```

---

### `converter batch-convert`
Batch-convert all SAFE/Landsat products in a directory (local or OCI) to Zarr.

**Usage**

```bash
converter batch-convert \
  --input-dir <dir> \
  --config <config.yaml> \
  [--output-dir <dir>] \
  [--chunks "Y,X"] \
  [--bucket/-b <bucket> --namespace/-n <ns> --profile <profile>] \
  [--skip-existing]
```

**Key points**
- Scans `input-dir` for `MSIL*`, `L1TP*`, `L2SP*` folders.
- Works locally or directly against OCI when bucket+namespace provided (streaming, no local download).
- Default `output-dir` is `input-dir`; derives `<name>.zarr` per product.
- `--skip-existing` avoids rewriting if the target Zarr already exists (local or OCI).
- `--chunks` default `512,512` (y,x).

**Example**
```bash
# Local SAFEs → local Zarrs
converter batch-convert \
  --input-dir ./data/safes \
  --config config/config.yaml \
  --skip-existing

# OCI SAFEs → OCI Zarrs (streaming)
converter batch-convert \
  --input-dir safes/ \
  --output-dir zarrs/ \
  --config config/config.yaml \
  --bucket my-bucket --namespace my-ns \
  --profile DEFAULT \
  --skip-existing
```

---

### `converter upload`
Upload a local Zarr directory to OCI object storage.

**Usage**

```bash
converter upload <local_path> \
  --bucket/-b <bucket> \
  --namespace/-n <ns> \
  --path/-p <remote/path> \
  [--profile <profile>] \
  [--skip-existing]
```

**Key points**
- Preserves directory structure; streams files to OCI.
- `--skip-existing` returns early if the remote path already exists.

**Example**
```bash
converter upload ./cubes/S2A_MSIL2A_20240115_T29RPQ.zarr \
  --bucket my-bucket \
  --namespace my-ns \
  --path cubes/S2A_MSIL2A_20240115_T29RPQ.zarr \
  --profile DEFAULT \
  --skip-existing
```

---

### `converter query`
Clip and (optionally) time-filter one or more Zarrs by WKT polygon; stack on time and save as Zarr/Parquet/Tessera.

**Usage**

```bash
converter query <zarr_paths...> \
  --wkt "POLYGON(...)" \
  --output/-o <out> \
  [--start YYYY-MM-DD --end YYYY-MM-DD] \
  [--bands <BAND1,BAND2,...>] \
  [--rgb <B04,B03,B02>] \
  [--preview/-p <preview.png>] \
  [--bucket/-b <bucket> --namespace/-n <ns> --profile <profile>] \
  [--crs <CRS>] \
  [--format/-f zarr|parquet|tessera] \
  [--tile-size <n>]
```

**Key points**
- Supports local paths and OCI globs (e.g., `cubes/s2/*.zarr`).
- Default CRS `EPSG:32629`; preview RGB bands default `B04,B03,B02`.
- Time filtering via `--start/--end` (optional).
- Output formats: `zarr` (default), `parquet`, `tessera`; `--tile-size` applies to Tessera.
- `--preview` writes an RGB PNG from the first timestep if bands are available.

**Example**
```bash
# Clip three Zarrs locally, time filter, save stacked zarr + preview
converter query data/cubes/s2/*.zarr \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --start 2025-12-01 --end 2026-02-01 \
  --output data/cubes/stacked.zarr \
  --preview data/cubes/stacked_preview.png \
  --bands B04,B03,B02,B08 \
  --format zarr

# Clip remote Zarrs in OCI and write Parquet
converter query cubes/s2/*.zarr \
  --bucket my-bucket --namespace my-ns \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --output data/cubes/clipped.parquet \
  --format parquet
```

---

### `converter download-s1`
Download Sentinel-1 RTC (VV/VH) from Planetary Computer STAC for a polygon/date range and save as Zarr (optionally upload to OCI).

**Usage**

```bash
converter download-s1 \
  --wkt "POLYGON(...)" \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --output/-o <s1.zarr> \
  [--orbit ascending|descending] \
  [--crs <CRS>] \
  [--resolution <metres>] \
  [--max-items <n>] \
  [--bucket/-b <bucket> --namespace/-n <ns> --profile <profile>]
```

**Key points**
- Defaults: CRS `EPSG:4326`, resolution `10m`, max-items `200`.
- If bucket+namespace provided, writes directly to OCI via mapper; otherwise saves locally.

**Example**
```bash
# Local save
converter download-s1 \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --start 2025-03-01 --end 2025-07-31 \
  --output data/s1_tile.zarr \
  --orbit ascending \
  --max-items 100

# Upload to OCI after download
converter download-s1 \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --start 2025-03-01 --end 2025-07-31 \
  --output cubes/s1_tile.zarr \
  --bucket my-bucket --namespace my-ns \
  --profile DEFAULT
```

---

### `converter download`
Download Sentinel-2 (L2A) and/or Sentinel-1 RTC from STAC into tessera-ready tiles or Zarr.

**Usage**

```bash
converter download \
  --modality s1|s2|both \
  --wkt "POLYGON(...)" \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --output/-o <out_dir> \
  [--crs <CRS>] \
  [--resolution <metres>] \
  [--cloud-cover <percent>] \
  [--orbit ascending|descending] \
  [--max-items <n>] \
  [--format/-f zarr|tessera] \
  [--tile-size <n>]
```

**Key points**
- Defaults: modality `both`, CRS `EPSG:32629`, resolution `10m`, cloud-cover `30%`, max-items `200`, format `tessera`, tile-size `500`.
- Output layout: `<output>/s2/` (S2 L2A bands) and/or `<output>/s1/` (S1 RTC).
- Tessera format: tiles per modality; Zarr format: one Zarr per modality.
- Requires STAC deps: `planetary-computer` and `pystac-client`.

**Example**
```bash
# Download both modalities as tessera tiles (default format)
converter download \
  --modality both \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --start 2025-03-01 --end 2025-07-31 \
  --output /data/field_A \
  --cloud-cover 20 \
  --tile-size 500

# S2 only, Zarr format, stricter cloud cover
converter download \
  --modality s2 \
  --wkt "POLYGON((-8.0 34.0, -7.0 34.0, -7.0 33.0, -8.0 33.0, -8.0 34.0))" \
  --start 2025-03-01 --end 2025-07-31 \
  --output /data/field_A \
  --format zarr \
  --cloud-cover 10
```

---

### Notes & Dependencies
- OCI operations need `ocifs` and `oci` installed.
- `converter query` needs `rioxarray`.
- STAC downloads (`download` / `download-s1`) need `planetary-computer` and `pystac-client`.
