# Product Band Reference

This document describes how NimbusChain preserves product content when converting raw satellite products into Zarr.

It is intentionally product-oriented:
- what the source product contains
- what the converter reads dynamically
- what goes into the `imagery` array
- what goes into ancillary arrays such as QA, masks, angles, or classification layers
- which target resolution policy is applied

## 1. General rule

NimbusChain no longer treats satellite products as a reduced RGB/NIR/SWIR subset.

The converter now follows these rules:

1. Read the real source product contents dynamically.
2. Preserve all native raster layers found in the product.
3. Keep physical imagery layers in the `imagery` array.
4. Store QA, masks, classification, cloud, snow, angle, aerosol, water vapour, or other ancillary layers in separate arrays when applicable.
5. Use a sensor-aware target grid:
   - Sentinel-2 -> `10 m`
   - Landsat 8/9 -> `10 m`
   - Sentinel-1 standard products -> native raster grid
   - Sentinel-1 RAW -> sample-space grid, not georeferenced

## 2. Zarr layout

The core layout is:

```text
imagery(time, band, y, x)
```

When ancillary layers are present, the store may also contain:

```text
ancillary(time, ancillary_layer, y, x)
```

Common coordinates:

```text
time
band
ancillary_layer
x
y
```

Meaning:
- `band`: physical imagery bands or radar polarizations
- `ancillary_layer`: QA, masks, classification, cloud probability, snow probability, angle rasters, or similar product-side layers

## 3. Sentinel-2

Supported products:
- `S2MSI1C`
- `S2MSI2A`

### 3.1 Sentinel-2 L1C (`S2MSI1C`)

Expected physical spectral bands:

- `B01`
- `B02`
- `B03`
- `B04`
- `B05`
- `B06`
- `B07`
- `B08`
- `B8A`
- `B09`
- `B10`
- `B11`
- `B12`

Expected imagery count:
- `13`

Resolution policy:
- target grid: `10 m`
- native `20 m` and `60 m` bands are reprojected to the `10 m` grid

Band meaning:

| Source band | Meaning | Native resolution |
|---|---|---|
| `B01` | Aerosol / coastal | `60 m` |
| `B02` | Blue | `10 m` |
| `B03` | Green | `10 m` |
| `B04` | Red | `10 m` |
| `B05` | Red edge 1 | `20 m` |
| `B06` | Red edge 2 | `20 m` |
| `B07` | Red edge 3 | `20 m` |
| `B08` | NIR | `10 m` |
| `B8A` | Narrow NIR | `20 m` |
| `B09` | Water vapour | `60 m` |
| `B10` | Cirrus | `60 m` |
| `B11` | SWIR 1 | `20 m` |
| `B12` | SWIR 2 | `20 m` |

Ancillary handling:
- if a given L1C package contains additional rasterized product-side layers, they should go into ancillary arrays
- the standard expected L1C product is mostly spectral

### 3.2 Sentinel-2 L2A (`S2MSI2A`)

Expected physical spectral bands:

- `B01`
- `B02`
- `B03`
- `B04`
- `B05`
- `B06`
- `B07`
- `B08`
- `B8A`
- `B09`
- `B11`
- `B12`

Expected imagery count:
- `12`

Why not 13:
- `B10` is not part of the standard atmospheric-corrected L2A imagery stack

Resolution policy:
- target grid: `10 m`
- native `20 m` and `60 m` bands are reprojected to the `10 m` grid

Typical ancillary layers if present:

- `SCL`
- `AOT`
- `WVP`
- `CLDPRB`
- `SNWPRB`
- `CLD`
- `SNW`
- `TCI`

Ancillary meaning:

| Layer | Meaning |
|---|---|
| `SCL` | Scene classification |
| `AOT` | Aerosol optical thickness |
| `WVP` | Water vapour |
| `CLDPRB` | Cloud probability |
| `SNWPRB` | Snow probability |
| `CLD` | Cloud mask / cloud layer when product provides it |
| `SNW` | Snow mask / snow layer when product provides it |
| `TCI` | True colour image |

Rule:
- spectral content stays in `imagery`
- QA / classification / cloud / snow / product-side support layers go into `ancillary`

## 4. Landsat 8 / Landsat 9

Supported collections:
- `landsat_ot_c2_l1`
- `landsat_ot_c2_l2`

Supported satellites:
- Landsat 8
- Landsat 9

### 4.1 Landsat Collection 2 Level 1

Typical imagery bands preserved:

- `B1`
- `B2`
- `B3`
- `B4`
- `B5`
- `B6`
- `B7`
- `B8`
- `B9`
- `B10`
- `B11`

Expected imagery count:
- `11`

Resolution policy:
- target grid: `30 m`
- `B8` (`15 m`, panchromatic) is preserved
- but the multispectral normalized grid remains `30 m`

Band meaning:

| Source band | Meaning | Native resolution |
|---|---|---|
| `B1` | Coastal / aerosol | `30 m` |
| `B2` | Blue | `30 m` |
| `B3` | Green | `30 m` |
| `B4` | Red | `30 m` |
| `B5` | NIR | `30 m` |
| `B6` | SWIR 1 | `30 m` |
| `B7` | SWIR 2 | `30 m` |
| `B8` | Panchromatic | `15 m` |
| `B9` | Cirrus | `30 m` |
| `B10` | Thermal infrared 1 | native thermal product resolution |
| `B11` | Thermal infrared 2 | native thermal product resolution |

Typical ancillary layers if present:

- `QA_PIXEL`
- `QA_RADSAT`
- `ANGLE` rasters
- `SAA`
- `SZA`
- `VAA`
- `VZA`
- support / metadata-derived rasters exposed as TIFF

Rule:
- physical imagery bands remain in `imagery`
- QA and angle layers go to `ancillary`

### 4.2 Landsat Collection 2 Level 2

Expected imagery bands:

- `SR_B1`
- `SR_B2`
- `SR_B3`
- `SR_B4`
- `SR_B5`
- `SR_B6`
- `SR_B7`
- `ST_B10`

Expected imagery count:
- `8`

This matches:
- 7 surface reflectance bands
- 1 surface temperature band (`ST_B10`)

Resolution policy:
- target grid: `30 m`

Typical ancillary layers if present:

- `QA_PIXEL`
- `QA_RADSAT`
- `SR_QA_AEROSOL`
- `ST_QA`
- `ST_CDIST`
- `ST_DRAD`
- `ST_TRAD`
- `ST_URAD`
- `ST_ATRAN`
- `ST_EMIS`
- `ST_EMSD`
- angle rasters if the product exposes them as TIFF

Rule:
- reflectance and temperature imagery remain in `imagery`
- QA, aerosol, temperature support layers, and angles go to `ancillary`

## 5. Sentinel-1

Supported products:
- `RAW`
- `GRD`
- `SLC`
- `IW_SLC__1S`

### 5.1 Sentinel-1 GRD / SLC / IW_SLC__1S

Expected imagery content:
- all available polarizations found in the source product

Typical imagery polarizations:
- `VV`
- `VH`
- `HH`
- `HV`

Rule:
- do not force a fixed 2-band or 4-band expectation
- preserve exactly the polarizations present in the source product

Typical ancillary layers if present as rasterized files:
- incidence angle rasters
- noise rasters
- support rasters generated by the product packaging
- other rasterized SAR-side layers that are not primary polarizations

Resolution policy:
- native raster grid of the product

### 5.2 Sentinel-1 RAW

Expected imagery content:
- all available decoded polarizations present in the RAW bundle

Typical imagery polarizations:
- `VV`
- `VH`
- `HH`
- `HV`

Important difference:
- RAW conversion is not map-georeferenced
- output is written in sample space
- `x` and `y` describe sample coordinates, not projected map coordinates

Resolution policy:
- native acquisition sample grid

Output notes:
- data family remains `sar`
- output is suitable for downstream algorithmic processing
- it is not directly a geocoded map product

## 6. Validation expectations by product type

The converter tests should validate exact imagery layer counts for the product families below.

### Sentinel-2 L1C
- imagery count: `13`
- expected imagery layers:
  - `B01`, `B02`, `B03`, `B04`, `B05`, `B06`, `B07`, `B08`, `B8A`, `B09`, `B10`, `B11`, `B12`

### Sentinel-2 L2A
- imagery count: `12`
- expected imagery layers:
  - `B01`, `B02`, `B03`, `B04`, `B05`, `B06`, `B07`, `B08`, `B8A`, `B09`, `B11`, `B12`
- ancillary count: dynamic, but should include QA/support layers when present

### Landsat 8/9 L1
- imagery count: `11`
- expected imagery layers:
  - `B1` through `B11`
- ancillary count: dynamic, QA and angle layers if present

### Landsat 8/9 L2
- imagery count: `8`
- expected imagery layers:
  - `SR_B1` through `SR_B7`
  - `ST_B10`
- ancillary count: dynamic, QA/aerosol/temperature support/angle layers if present

### Sentinel-1 GRD / SLC / IW_SLC__1S
- imagery count: dynamic
- expected imagery layers:
  - exactly the polarizations present in the source
- ancillary count: dynamic

### Sentinel-1 RAW
- imagery count: dynamic
- expected imagery layers:
  - exactly the decoded polarizations present in the source
- ancillary count: usually limited compared with standard georeferenced products

## 7. Practical interpretation

If a product contains a raster layer and it is:
- a physical imagery band
- a radar polarization
- a temperature raster

it should remain in `imagery`.

If a product contains a raster layer and it is:
- QA
- mask
- cloud
- snow
- aerosol
- water vapour support
- classification
- angle
- radiometric quality
- support or intermediate science layer

it should go into `ancillary`.

The converter should not silently drop those layers by default.

## 8. Current engineering intent

The engineering target for NimbusChain is:
- preserve native source fidelity
- normalize geometry, not semantics
- avoid throwing away scientific content
- keep Zarr output predictable enough for downstream analysis and masking pipelines

That is why the project now prefers:
- dynamic source discovery
- product-exact band naming
- sensor-aware target grids
- separate imagery vs ancillary storage
