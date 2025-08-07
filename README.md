# Satellite-fetcher
# Copernicus Data Collections & Product Types

## 📡 Overview

This guide provides **collection names**, **processing levels**, and **productType** values you can use in your Copernicus Data Space OData queries.

---

## Collections & `productType` Values

Use these strings in your filter queries, e.g.:

```text
Attributes/OData.CSC.StringAttribute/any(
  att:att/Name eq 'productType' and
  att/OData.CSC.StringAttribute/Value eq '<PRODUCTTYPE>'
)
```

---

### **Sentinel‑1 (SAR Imagery)**
**Collection Name:** `SENTINEL-1`  
**Supported `productType`:**
- `SLC` — Level‑1 Single-Look Complex (complex SAR data, includes phase)
- `GRD` — Level‑1 Ground Range Detected (ortho‑corrected amplitude)
- `GRDCOG` — Level‑1 GRD in Cloud-Optimized GeoTIFF format
- `OCN` — Level‑2 Ocean products (wind, wave, current data)

---

### **Sentinel‑2 (Optical Multispectral)**
**Collection Name:** `SENTINEL-2`  
**Supported `productType`:**
- `S2MSI1C` — Level‑1C Top‑Of‑Atmosphere reflectance (no atmospheric correction)
- `S2MSI2A` — Level‑2A Bottom‑Of‑Atmosphere reflectance (with scene classification and cloud mask)

---

### **Sentinel‑3 (OLCI & SLSTR Instruments)**
**Collection Name:** `SENTINEL-3`  
**Supported `productType`:**  
- `S3OL1EFR`, `S3OL1ERR` — Level‑1 OLCI (full/reduced resolution TOA radiances)  
- `S3SL1RBT` — Level‑1 SLSTR brightness temperature & radiance  
- `S3OL2WFR`, `S3OL2WRR` — Level‑2 OLCI ocean parameters (full/reduced resolution)  
- `S3OL2LFR`, `S3OL2LRR` — Level‑2 OLCI land parameters  
- `S3SL2LST` — Level‑2 land surface temperature  
- `S3SL2FRP` — Level‑2 fire radiative power  
- `S3SR2LAN` — Level‑2 land surface height  
- `S3SY2SYN`, `S3SY2VGP`, `S3SY2VG1`, `S3SY2V10`, `S3SY2AOD` — Various vegetation and aerosol syntheses & surface reflectance products

---

### **Sentinel‑5P (Atmospheric Composition)**
**Collection Name:** `SENTINEL-5P`  
**Supported `productType`:**
- `L2__NO2___` — Nitrogen Dioxide  
- `L2__CH4___` — Methane  
- `L2__CO____` — Carbon Monoxide  
- `L2__O3____` — Ozone  
- `L2__SO2___` — Sulfur Dioxide  
- `L2__HCHO__` — Formaldehyde  

---

## Collections Table

| **Collection Name** | **Satellite / Data Type** | **ProductType (processing level)** |
|---------------------|---------------------------|-------------------------------------|
| SENTINEL-1          | SAR imagery               | `RAW`, `GRD`, `SLC`, `IW_SLC__1S`.  |
| SENTINEL-2          | Optical & infrared        | `S2MSI1C`, `S2MSI2A`                 |
| SENTINEL-3          | OLCI / SLSTR land & ocean | `S3OL1EFR`, `S3OL1ERR`, `S3SL1RBT`, `S3OL2WFR`, `S3OL2WRR`, `S3OL2LFR`, `S3OL2LRR`, `S3SL2LST`, `S3SL2FRP`, `S3SR2LAN`, `S3SY2SYN`, `S3SY2VGP`, `S3SY2VG1`, `S3SY2V10`, `S3SY2AOD` |
| SENTINEL-5P         | Atmospheric gases         | `L2__NO2___`, `L2__CH4___`, `L2__CO____`, `L2__O3____`, `L2__SO2___`, `L2__HCHO__` |

---

## 📝 Example OData filter snippet

```text
$filter=
  Collection/Name eq 'SENTINEL-2'
  and Attributes/OData.CSC.StringAttribute/any(
    att:att/Name eq 'productType' and
    att/OData.CSC.StringAttribute/Value eq 'S2MSI1C'
  )
  and ContentDate/Start gt 2024-01-01T00:00:00.000Z
  and ContentDate/Start lt 2024-01-31T23:59:59.999Z
```

That filters for **Sentinel‑2 Level‑1C** raw scenes in January 2024, without atmospheric correction or cloud masks.

---

# AFKAR
* use multi-sessions to download + semaphore