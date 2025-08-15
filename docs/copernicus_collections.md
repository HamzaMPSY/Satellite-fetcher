# Copernicus Data Collections & Product Types

This guide details **collection names**, **processing levels**, and **productType** strings for use in Copernicus Data Space OData queries.

---

## Sentinel Collections and Product Types

### **Sentinel-1 (SAR Imagery)**
- **Collection Name:** `SENTINEL-1`
- **Supported `productType`:**
  - `RAW` — Single-Look Complex (Level-1)
  - `GRD` — Ground Range Detected (Level-1, ortho-corrected amplitude)
  - `SLC` — GRD in Cloud-Optimized GeoTIFF format
  - `IW_SLC__1S` — Level-2 Ocean products (wind, wave, current data)

### **Sentinel-2 (Optical Multispectral)**
- **Collection Name:** `SENTINEL-2`
- **Supported `productType`:**
  - `S2MSI1C` — Level-1C (Top-Of-Atmosphere reflectance)
  - `S2MSI2A` — Level-2A (Bottom-Of-Atmosphere with classification/cloud mask)

### **Sentinel-3 (OLCI & SLSTR)**
- **Collection Name:** `SENTINEL-3`
- **Supported `productType`:**
  - `S3OL1EFR`, `S3OL1ERR` — OLCI Level-1 (full/reduced resolution TOA radiances)
  - `S3SL1RBT` — SLSTR Level-1 brightness/temp
  - `S3OL2WFR`, `S3OL2WRR` — OLCI Level-2 (ocean full/reduced)
  - `S3OL2LFR`, `S3OL2LRR` — OLCI Level-2 (land full/reduced)
  - `S3SL2LST` — Land surface temperature
  - `S3SL2FRP` — Fire radiative power
  - `S3SR2LAN` — Land surface height
  - `S3SY2SYN`, `S3SY2VGP`, `S3SY2VG1`, `S3SY2V10`, `S3SY2AOD` — Vegetation and aerosol products

### **Sentinel-5P (Atmospheric Monitoring)**
- **Collection Name:** `SENTINEL-5P`
- **Supported `productType`:**
  - `L2__NO2___` — Nitrogen Dioxide
  - `L2__CH4___` — Methane
  - `L2__CO____` — Carbon Monoxide
  - `L2__O3____` — Ozone
  - `L2__SO2___` — Sulfur Dioxide
  - `L2__HCHO__` — Formaldehyde

---

## Collections Table

| Collection Name | Satellite / Data Type | ProductType (processing level) |
|----------------|-----------------------|-------------------------------|
| SENTINEL-1     | SAR imagery           | `RAW`, `GRD`, `SLC`, `IW_SLC__1S` |
| SENTINEL-2     | Optical & infrared    | `S2MSI1C`, `S2MSI2A`               |
| SENTINEL-3     | OLCI/SLSTR land/ocean | `S3OL1EFR`, `S3OL1ERR`, `S3SL1RBT`, ... see above |
| SENTINEL-5P    | Atmospheric gases     | `L2__NO2___`, `L2__CH4___`, `L2__CO____`, ... |

For detailed usage, see [OData Query Examples](./odata_examples.md).
