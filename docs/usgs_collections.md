# USGS Landsat Collections & Product Types

This guide outlines **collection names**, **processing levels**, and **productType** usage for querying USGS Landsat-8/9 Collection 2 data.

---

## Landsat Collections Overview

### **Landsat 8 & 9 (Collection 2)**

- **Level-1 Collection Name:** `landsat_ot_c2_l1`
- **Level-2 Collection Name:** `landsat_ot_c2_l2`
- **Satellite Selection:**  
  Use the `Satellite` field:  
  - `08` = Landsat 8  
  - `09` = Landsat 9

#### **Product Type Codes**

- **Level-1 Product Types (`Data Type L1` field):**
  - `L1TP` — Terrain Precision Corrected
  - `L1GT` — Systematic Terrain Corrected
  - `L1GS` — Systematic Geometric Corrected

- **Level-2 Product Types (`Data Type L2` field):**
  - `L2SP` — Science Product (surface reflectance and surface temperature, with atmospheric correction)
  - `L2SR` — Surface Reflectance only

#### **Shortcut: productType Values**

To simplify, you can use these codes for the `productType` OData query:
- `XL1TP`, `XL1GT`, `XL1GS`, `XL2SP`, `XL2SR`  
  *(Replace `X` with `8` for Landsat 8, or `9` for Landsat 9; e.g., `8L1TP` for Landsat 8 Level-1 terrain precision)*

---

## Collections Table

| Collection Name       | Satellite  | Processing Level | ProductType (`productType` value)    |
|----------------------|------------|------------------|--------------------------------------|
| landsat_ot_c2_l1     | 8, 9       | Level-1          | `XL1TP`, `XL1GT`, `XL1GS`            |
| landsat_ot_c2_l2     | 8, 9       | Level-2          | `XL2SP`, `XL2SR`                     |

**Legend**:  
- Replace `X` in `productType` with the Landsat number: `8` (Landsat 8) or `9` (Landsat 9).

---

For more on OData queries and advanced usage, see [OData Query Examples](./odata_examples.md).
