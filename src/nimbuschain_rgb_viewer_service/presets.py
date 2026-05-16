from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RgbPreset:
    name: str
    label: str
    provider: str
    collection_family: str
    bands: tuple[str, str, str]
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "provider": self.provider,
            "collection_family": self.collection_family,
            "bands": list(self.bands),
            "description": self.description,
        }


RGB_PRESETS: tuple[RgbPreset, ...] = (
    RgbPreset(
        name="sentinel2_true_color",
        label="Sentinel-2 true color",
        provider="copernicus",
        collection_family="SENTINEL-2",
        bands=("B04", "B03", "B02"),
        description="Natural color RGB for Sentinel-2 optical products.",
    ),
    RgbPreset(
        name="landsat_l1_true_color",
        label="Landsat Collection 2 L1 true color",
        provider="usgs",
        collection_family="landsat_ot_c2_l1",
        bands=("B4", "B3", "B2"),
        description="Natural color RGB for Landsat 8/9 Collection 2 Level-1 products.",
    ),
    RgbPreset(
        name="landsat_l2_true_color",
        label="Landsat Collection 2 L2 true color",
        provider="usgs",
        collection_family="landsat_ot_c2_l2",
        bands=("SR_B4", "SR_B3", "SR_B2"),
        description="Natural color RGB for Landsat Collection 2 surface reflectance products.",
    ),
    RgbPreset(
        name="sentinel1_vv_vh",
        label="Sentinel-1 VV/VH composite",
        provider="copernicus",
        collection_family="SENTINEL-1",
        bands=("VV", "VH", "VV"),
        description="Pseudo-RGB composite for Sentinel-1 SAR products.",
    ),
)


def preset_catalog() -> list[dict[str, Any]]:
    return [preset.to_dict() for preset in RGB_PRESETS]


def choose_rgb_bands(
    *,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
    band_names: list[str],
    requested_bands: list[str] | None = None,
) -> tuple[list[str], str]:
    available = {band.upper(): band for band in band_names}
    if requested_bands:
        missing = [band for band in requested_bands if band.upper() not in available]
        if missing:
            raise ValueError(f"Requested RGB bands are missing from the Zarr store: {', '.join(missing)}")
        return [available[band.upper()] for band in requested_bands], "custom"

    for candidate in _provider_candidates(
        provider=provider,
        collection=collection,
        product_type=product_type,
    ):
        resolved = _resolve_candidate(candidate.bands, available)
        if resolved:
            return resolved, candidate.name

    for fallback in (
        ("B04", "B03", "B02"),
        ("B4", "B3", "B2"),
        ("SR_B4", "SR_B3", "SR_B2"),
        ("VV", "VH", "VV"),
    ):
        resolved = _resolve_candidate(fallback, available)
        if resolved:
            return resolved, "auto_band_match"

    if len(band_names) >= 3:
        return list(band_names[:3]), "first_three_bands"
    if len(band_names) == 2:
        return [band_names[0], band_names[1], band_names[0]], "two_band_repeat"
    if len(band_names) == 1:
        return [band_names[0], band_names[0], band_names[0]], "single_band_grayscale"
    raise ValueError("The Zarr store has no band labels to render as RGB.")


def _provider_candidates(
    *,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
) -> list[RgbPreset]:
    provider_norm = str(provider or "").strip().lower()
    collection_norm = str(collection or "").strip().lower()
    product_norm = str(product_type or "").strip().upper()

    matches: list[RgbPreset] = []
    for preset in RGB_PRESETS:
        if provider_norm and preset.provider.lower() != provider_norm:
            continue
        family_norm = preset.collection_family.lower()
        if collection_norm and family_norm not in collection_norm and collection_norm not in family_norm:
            continue
        matches.append(preset)

    if provider_norm == "usgs" and "l2" in product_norm.lower():
        return [preset for preset in RGB_PRESETS if preset.name == "landsat_l2_true_color"] + matches
    return matches


def _resolve_candidate(
    candidate: tuple[str, str, str],
    available: dict[str, str],
) -> list[str] | None:
    if all(band.upper() in available for band in candidate):
        return [available[band.upper()] for band in candidate]
    return None
