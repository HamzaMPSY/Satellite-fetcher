from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SensorMaskSpec:
    sensor_key: str
    water_input_bands: tuple[str, ...]
    water_fallback_bands: tuple[str, ...]
    cloud_required_bands: tuple[str, ...]
    cloud_rgbnir_bands: tuple[str, ...]
    scale_hint: str
    cloud_threshold_default: float
    water_threshold_default: float


_SENTINEL2_SPEC = SensorMaskSpec(
    sensor_key="sentinel-2",
    water_input_bands=("B04", "B03", "B02", "B08"),
    water_fallback_bands=("B02", "B03", "B04", "B08", "B11", "B12"),
    cloud_required_bands=("B02", "B03", "B04", "B08", "B11", "B12"),
    cloud_rgbnir_bands=("B04", "B03", "B08"),
    scale_hint="reflectance_0_10000",
    cloud_threshold_default=0.32,
    water_threshold_default=0.18,
)

_LANDSAT_L1_SPEC = SensorMaskSpec(
    sensor_key="landsat-8-9-l1",
    water_input_bands=("B4", "B3", "B2", "B5"),
    water_fallback_bands=("B2", "B3", "B4", "B5", "B6", "B7"),
    cloud_required_bands=("B2", "B3", "B4", "B5", "B6", "B7"),
    cloud_rgbnir_bands=("B4", "B3", "B5"),
    scale_hint="landsat_l1_reflectance",
    cloud_threshold_default=0.28,
    water_threshold_default=0.10,
)

_LANDSAT_L2_SPEC = SensorMaskSpec(
    sensor_key="landsat-8-9-l2",
    water_input_bands=("SR_B4", "SR_B3", "SR_B2", "SR_B5"),
    water_fallback_bands=("SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"),
    cloud_required_bands=("SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"),
    cloud_rgbnir_bands=("SR_B4", "SR_B3", "SR_B5"),
    scale_hint="landsat_l2_reflectance",
    cloud_threshold_default=0.28,
    water_threshold_default=0.18,
)


def resolve_sensor_mask_spec(
    *,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
) -> SensorMaskSpec:
    provider_name = str(provider or "").strip().lower()
    collection_name = str(collection or "").strip().upper()
    ptype = str(product_type or "").strip().upper()

    if provider_name == "copernicus" and collection_name == "SENTINEL-2":
        if ptype and ptype not in {"S2MSI1C", "S2MSI2A"}:
            raise ValueError(
                "Masking currently supports Sentinel-2 product types S2MSI1C and S2MSI2A only."
            )
        return _SENTINEL2_SPEC

    if provider_name == "usgs" and collection_name in {"LANDSAT_OT_C2_L1", "LANDSAT_OT_C2_L2"}:
        if collection_name.endswith("_L1"):
            return _LANDSAT_L1_SPEC
        if ptype == "L2SR":
            return _LANDSAT_L2_SPEC
        return _LANDSAT_L2_SPEC

    raise ValueError(
        "Masking currently supports Sentinel-2 (L1C/L2A) and Landsat 8/9 collections only."
    )
