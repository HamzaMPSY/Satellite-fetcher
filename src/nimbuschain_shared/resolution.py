from __future__ import annotations


_TARGET_PIXEL_SIZE_METERS: dict[tuple[str, str], float | None] = {
    ("copernicus", "SENTINEL-2"): 10.0,
    ("copernicus", "SENTINEL-1"): None,
    ("usgs", "landsat_ot_c2_l1"): 10.0,
    ("usgs", "landsat_ot_c2_l2"): 10.0,
}


def target_pixel_size_for(provider: str, collection: str) -> float | None:
    normalized_provider = str(provider or "").strip().lower()
    normalized_collection = (
        str(collection or "").strip().upper()
        if normalized_provider == "copernicus"
        else str(collection or "").strip().lower()
    )
    return _TARGET_PIXEL_SIZE_METERS.get((normalized_provider, normalized_collection))
