# sen2like Pipeline

End-to-end Landsat 8/9 → Sentinel-2 harmonisation that produces 10 m analysis-ready surface reflectance on the Sentinel-2 MGRS grid. The pipeline orchestrates geometric co-registration, atmospheric correction, spectral harmonisation, BRDF normalisation, data fusion, and packaging into SAFE-style Cloud-Optimised GeoTIFF outputs.

## Table of contents

- [Overview](#overview)
- [Key capabilities](#key-capabilities)
- [Pipeline workflow](#pipeline-workflow)
- [Prerequisites](#prerequisites)
- [Environment setup](#environment-setup)
- [Running the pipeline](#running-the-pipeline)
- [Command options](#command-options)
- [Cleanup policies](#cleanup-policies)
- [Inputs](#inputs)
- [Outputs](#outputs)
- [Adaptive routing](#adaptive-routing)
- [Monitoring and observability](#monitoring-and-observability)
- [Resuming runs](#resuming-runs)
- [Development notes](#development-notes)
- [Roadmap](#roadmap)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Overview

`sen2like` re-implements the ESA sen2like Level-2H processor with a Python-first, modular design optimised for production-scale Landsat harmonisation. Each processing stage is resumable, logged, and tracked through a manifest for reproducibility and auditing.

## Key capabilities

- Reprojects Landsat Level-1/2 inputs to Sentinel-2 GRI tiles.
- Runs Py6S-based atmospheric correction with cached lookup tables per sensor.
- Applies Spectral Band Adjustment Factors (SBAF) to align OLI with MSI responses.
- Computes BRDF-normalised (NBAR) reflectance using Roy et al. coefficients.
- Generates pixel-level masks (cloud, shadow, snow, water, clear) from QA bands.
- Upsamples spectra to 10 m; optional fusion with Sentinel-2 MSI inputs.
- Packages SAFE-like Level-2F products with COGs and complete provenance.
- Includes an adaptive router to skip unusable scenes and prioritise clear tiles.

## Pipeline workflow

| Stage | Description | Primary outputs |
|-------|-------------|-----------------|
| Geometric Processing | Coregisters Landsat imagery to Sentinel-2 GRI references. | `geo/` warped bands, alignment metrics |
| Atmospheric Correction | Converts DN → TOA → BOA reflectance via Py6S LUTs. | `atm_corr/` reflectance tiles |
| SBAF | Harmonises reflectance to MSI spectral response curves. | `sbaf/` adjusted bands |
| Valid Pixel Mask | Decodes QA bands into per-pixel quality masks. | `mask/valid_mask.tif` |
| BRDF Adjustment | Produces nadir BRDF-adjusted reflectance (NBAR). | `nbar/NBAR_<band>.tif` |
| Data Fusion | Upsamples harmonised bands to 10 m and merges optional Sentinel-2 data. | `fusion/` fused stacks |
| Packaging | Builds SAFE-style Level-2F hierarchy and metadata. | `<tile>_L2F/` bundle |
| Validation | Performs range, mask coverage, and geometry checks. | `manifest.json` validation entries |
| Cleanup *(optional)* | Removes intermediates according to retention policy. | Reduced working directory size |

## Prerequisites

- Python ≥ 3.10 (tested on CPython 3.10–3.12).
- Runtime packages listed in `requirements.txt` (install with pip).
- System dependencies:
  - GDAL ≥ 3.6 with Python bindings.
  - PROJ ≥ 9.
  - `libspatialindex` (for Rtree spatial indexing).
  - C/C++ build toolchain for platforms without binary wheels.
- Optional: Apache Spark 3.5+ and Java 11+ for distributed execution.
- Optional: Prometheus Pushgateway for metrics export.

> **Tip:** macOS users can `brew install gdal proj libspatialindex`. Ubuntu/Debian users can `apt install gdal-bin libgdal-dev proj-bin libspatialindex-dev` before installing Python wheels.

## Environment setup

```bash
git clone [https://github.com/<your-org>/sen2like_reimplementation.git](https://github.com/HamzaMPSY/Satellite-fetcher/tree/feature/sen2like_reimplementation)
cd sen2like_reimplementation
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Enable Spark extras when needed:

```bash
pip install pyspark==4.1.1 pyrasterframes==0.11.1
```

## Running the pipeline

Single-scene processing:

```bash
python Pipeline.py /data/LC08_L1TP_20240315_... \
  --working-dir ./output_pipeline \
  --workers 4 \
  --cleanup-mode medium
```

Processing multiple scenes with Spark:

```bash
python Pipeline.py \
  /data/LC08_L1TP_20240315_... \
  /data/LC09_L1TP_20240317_... \
  --working-dir s3://my-bucket/sen2like \
  --workers 16 \
  --cleanup-mode aggressive \
  --router-fallback-ok
```

Set `LANDSAT_UPSAMPLING_BASE` when running from a packaged environment to ensure stage modules are discoverable.

## Command options

`Pipeline.py` exposes the following arguments:

| Flag | Description |
|------|-------------|
| `products` | One or more Landsat scene directories (Level-1 or Level-2 SR). |
| `--working-dir` | Output root directory (default `./output_pipeline`). |
| `--steps` | Subset of stages to execute. Packaging always runs. |
| `--workers` | Thread pool size per stage (default 4). |
| `--s2-path` | Optional Sentinel-2 tile to support fusion. |
| `--no-resume` | Ignore existing manifests and recompute all stages. |
| `--no-routing` | Disable the adaptive router; run every step. |
| `--router-fallback-ok` | Continue with the full pipeline if routing fails. |
| `--exclude-water` | Mark water pixels as invalid in the output mask. |
| `--base-dir` | Override module discovery base (defaults to repo root). |
| `--cleanup-mode` | Retention policy (`none`, `light`, `medium`, `aggressive`, `strict`). |
| `--cleanup-dry-run` | Print cleanup actions without deleting files. |

## Cleanup policies

| Mode | Retained artifacts |
|------|--------------------|
| `none` | Retain all intermediates for debugging. |
| `light` | Drop TOA sidecars, keep geometric and atmospheric products. |
| `medium` | Preserve validation-ready outputs (mask, nbar, fusion). |
| `aggressive` | Keep SAFE bundle and manifest only; remove fusion intermediates. |
| `strict` | Keep SAFE bundle only (requires validation success). |

Use `--cleanup-dry-run` to preview deletions without modifying the filesystem.

## Inputs

- **Landsat scene directory** — must contain `*_MTL.json`, QA bands (`*_QA_PIXEL.TIF`, `*_QA_RADSAT.TIF`, `*_SR_QA_AEROSOL.TIF`), and reflectance bands (`*_B2..B7.TIF` or `*_SR_B2..SR_B7.TIF`).
- **Sentinel-2 GRI** — downloaded on demand using the scene tile ID; override via `--gri-path` for offline workflows.
- **Digital Elevation Model (optional)** — enables orthorectification and illumination correction.
- **6S lookup tables** — auto-generated on first run and cached under `lut/`.

## Outputs

```
output_pipeline/
└── <scene_id>/
    ├── manifest.json          # Step statuses, hashes, timings, provenance
    ├── checkpoint.json        # Resume cursor (legacy compatibility)
    ├── geo/                   # Co-registered rasters (intermediate)
    ├── atm_corr/              # BOA reflectance tiles
    ├── sbaf/                  # Spectrally adjusted bands
    ├── mask/                  # Valid pixel masks
    ├── nbar/                  # BRDF-normalised bands
    ├── fusion/                # 10 m upsampled stacks + validity mask
    └── <tile>_L2F/            # SAFE-like deliverable with metadata & COGs
```

`manifest.json` captures diagnostics for each stage, including configuration hashes, input paths, outputs, elapsed time, and any captured errors. Downstream systems can consume the manifest for auditing or incremental reruns.

## Adaptive routing

The router (`Routing/tile_router.py`) computes quick-look statistics (cloud fraction, NDVI, NDWI, brightness variance) to classify scenes:

- `SKIP` — skip scenes with ≥90 % cloud or invalid data.
- `MIXED` — retain but flag for cautious QA.
- `WATER`, `DENSE_VEGETATION`, `BARE_SOIL`, `URBAN` — provide context for QA dashboards.

Routing determines which stages to execute and can short-circuit work for poor-quality scenes. Override thresholds by importing the module and adjusting constants, or pass `--steps` to force specific stages.

## Monitoring and observability

- Metrics originate from `Monitoring/metrics.py` and push to a Prometheus Pushgateway.
- Enable via env vars (`METRICS_ENABLED=1` by default, configure gateway with `PROMETHEUS_PUSHGATEWAY`).
- Collects per-step duration, status, output counts, valid pixel fraction, and BRDF deltas.
- Logging is configured in `Pipeline.py`; control verbosity with `LOGLEVEL=DEBUG|INFO|...`.

## Resuming runs

The pipeline is resumable by default. On restart it reads `manifest.json` and skips completed stages unless:

- Configuration hashes differ from the previous run.
- Input paths changed or outputs are missing.
- A step was soft-skipped (e.g., router fallback) and now requires reprocessing.

Use `--no-resume` to force a full rerun from scratch.

## Development notes

- Entry point: `Pipeline.py`.
- Processing stages live under dedicated sub-packages (`Geometric_Processing/`, `SBAF/`, etc.).
- Tests are not yet published; add `pytest`-style suites under `tests/` as they become available.
- Prefer virtual environments; update dependencies in `requirements.txt` before committing.
- When using Spark, ensure worker environments set `IN_SPARK_EXECUTOR=1` to avoid nested Spark initialisation.


## Acknowledgments

- Inspired by the [ESA sen2like](https://github.com/senbox-org/sen2like) processor.
- BRDF correction uses coefficients from Roy et al. (2016).
- Atmospheric correction leverages the Py6S library.
- Landsat imagery courtesy of USGS/NASA; Sentinel-2 data courtesy of ESA.
