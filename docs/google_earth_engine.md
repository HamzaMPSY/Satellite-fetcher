# Google Earth Engine Provider

The Google Earth Engine (GEE) provider allows you to search and download satellite imagery directly from Google's multi-petabyte catalog.

## Prerequisites

1.  **Google Earth Engine Account**: You must be signed up for [Google Earth Engine](https://earthengine.google.com/).
2.  **Authentication**: You need to authenticate the Earth Engine Python API.
    -   Run `earthengine authenticate` in your terminal and follow the instructions.
    -   Alternatively, you can use a Service Account (see Configuration).

## Configuration

Add the following to your `config.yaml` file:

```yaml
providers:
  - google_earth_engine:
      name: Google Earth Engine
      description: Google Earth Engine combines a multi-petabyte catalog of satellite imagery and geospatial datasets with planetary-scale analysis capabilities.
      credentials:
        project_id: "your-project-id"  # Required: Your Google Cloud Project ID enabled for GEE
        service_account_json: "/path/to/service-account.json" # Optional: Path to service account key
```

## Usage

### CLI

You can search and download data using the command line interface.

**Example: Download Sentinel-2 Surface Reflectance**

```bash
python cli.py \
  --provider google_earth_engine \
  --collection COPERNICUS/S2_SR \
  --start-date 2023-06-01 \
  --end-date 2023-06-10 \
  --aoi_file example_aoi.wkt
```

**Arguments:**
-   `--provider`: Must be `google_earth_engine`.
-   `--collection`: The Earth Engine ImageCollection ID (e.g., `COPERNICUS/S2_SR`, `LANDSAT/LC08/C02/T1_L2`).
-   `--start-date`, `--end-date`: Date range for the search.
-   `--aoi_file`: Path to a file containing the Area of Interest (WKT or GeoJSON).

### Streamlit UI

1.  Start the application: `streamlit run satellite-fetcher.py`
2.  Select **GoogleEarthEngine** from the **Provider** dropdown.
3.  Choose a **Satellite/Collection** (e.g., `COPERNICUS/S2_SR`).
4.  Draw your Area of Interest on the map.
5.  Select dates and click **Download Products**.

## Supported Collections

You can use any `ImageCollection` ID available in the [Earth Engine Data Catalog](https://developers.google.com/earth-engine/datasets).

Common examples:
-   **Sentinel-2**: `COPERNICUS/S2_SR` (Surface Reflectance), `COPERNICUS/S2_HARMONIZED`
-   **Landsat 8**: `LANDSAT/LC08/C02/T1_L2` (Level 2, Collection 2)
-   **Landsat 9**: `LANDSAT/LC09/C02/T1_L2`
-   **MODIS**: `MODIS/006/MOD13Q1` (Vegetation Indices)
-   **SRTM**: `USGS/SRTMGL1_003` (Digital Elevation Model)

## Notes

-   **Download Limits**: The tool uses `getDownloadURL` which has size limitations (approx. 32MB per request). Large areas may fail or need to be split.
-   **Scale**: The current implementation attempts to download at a default scale (approx. 100m) to avoid hitting limits too quickly. For full resolution (e.g., 10m for Sentinel-2), modification to the code might be required to handle tiling or `Export` tasks.
