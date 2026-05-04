from __future__ import annotations

import logging
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import rasterio
from rasterio.crs import CRS
from rasterio.warp import transform

log = logging.getLogger("sen2like")

_WGS84 = CRS.from_epsg(4326)

# Planetary Computer STAC endpoint for sentinel-2-l2a.
_PC_STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
_PC_COLLECTION = "sentinel-2-l2a"

# Cloud-cover and recency bounds when searching for a GRI source scene.
_MAX_CLOUD_COVER_PCT = 10
_SEARCH_LOOKBACK_DAYS = 365 * 3  # search up to 3 years back if needed


# ---------------------------------------------------------------------------
# MGRS tile derivation from scene centre
# ---------------------------------------------------------------------------

def derive_mgrs_tile(scene_dir: str | Path) -> str | None:
    try:
        import mgrs as mgrs_lib
    except ImportError:
        log.warning(
            "[gri-fetch] `mgrs` library not installed — cannot derive MGRS tile. "
            "Install with: pip install mgrs"
        )
        return None

    scene_path = Path(str(scene_dir))
    if not scene_path.exists():
        log.warning("[gri-fetch] Scene path does not exist: %s", scene_path)
        return None

    # Pick any non-mask TIF from the scene for centre derivation.
    ref_tif = next(
        (t for t in sorted(scene_path.glob("*.TIF"))
         if "MASK" not in t.name.upper() and "QA_" not in t.name.upper()),
        None,
    )
    if ref_tif is None:
        log.warning("[gri-fetch] No reference TIF found in %s", scene_path)
        return None

    try:
        with rasterio.open(ref_tif) as src:
            if src.crs is None:
                log.warning("[gri-fetch] %s has no CRS", ref_tif.name)
                return None
            b = src.bounds
            cx = b.left + (b.right - b.left) / 2.0
            cy = b.bottom + (b.top - b.bottom) / 2.0
            xs, ys = transform(src.crs, _WGS84, [cx], [cy])
            lon, lat = float(xs[0]), float(ys[0])
    except Exception as exc:
        log.warning("[gri-fetch] Could not read centre from %s: %s", ref_tif.name, exc)
        return None

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        log.warning("[gri-fetch] Centre out of range (lat=%.4f lon=%.4f)", lat, lon)
        return None

    try:
        m = mgrs_lib.MGRS()
        # Precision 0 → 100 km grid square → 5-char code like '30STB'.
        code = m.toMGRS(lat, lon, MGRSPrecision=0)
        tile = f"T{code}"
        log.info(
            "[gri-fetch] MGRS tile derived from scene centre (lat=%.4f lon=%.4f): %s",
            lat, lon, tile,
        )
        return tile
    except Exception as exc:
        log.warning("[gri-fetch] MGRS conversion failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Planetary Computer search & download
# ---------------------------------------------------------------------------

def _fetch_s2_safe_for_tile(mgrs_tile: str, target_dir: Path) -> Path:
    try:
        from pystac_client import Client
        import planetary_computer
        import rasterio
        from rasterio.shutil import copy as rio_copy
    except ImportError as exc:
        raise RuntimeError(
            "[gri-fetch] Required packages not installed. Run: "
            "pip install pystac-client planetary-computer rasterio"
        ) from exc

    mgrs_code = mgrs_tile.lstrip("T")
    log.info("[gri-fetch] Searching Planetary Computer for tile %s…", mgrs_tile)

    # Pour cette scène Landsat de 2025-06-16, cherche une S2 de juin-août 2025
    from datetime import datetime as _dt
    landsat_date = _dt(2025, 6, 16)  # ou extrait du nom de la scène
    start = landsat_date - timedelta(days=30)
    end   = landsat_date + timedelta(days=60)

    catalog = Client.open(
        _PC_STAC_URL,
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=[_PC_COLLECTION],
        datetime=f"{start.isoformat()}Z/{end.isoformat()}Z",
        query={
            "s2:mgrs_tile":   {"eq": mgrs_code},
            "eo:cloud_cover": {"lt": _MAX_CLOUD_COVER_PCT},
        },
        sortby=[{"field": "eo:cloud_cover", "direction": "asc"}],
        limit=5,  # try a few in case the first has issues
    )

    items = list(search.items())
    if not items:
        raise RuntimeError(
            f"[gri-fetch] No cloud-free (<{_MAX_CLOUD_COVER_PCT}%) Sentinel-2 "
            f"scene found for tile {mgrs_tile} in the last "
            f"{_SEARCH_LOOKBACK_DAYS // 365} years."
        )

    last_err = None
    for item in items:
        try:
            log.info(
                "[gri-fetch] Selected scene %s (cloud=%.1f%%, date=%s)",
                item.id,
                item.properties.get("eo:cloud_cover", -1),
                item.properties.get("datetime", "?"),
            )

            band_asset = item.assets.get("B04")
            if band_asset is None:
                raise RuntimeError(f"Item {item.id} has no B04 asset")

            # Build a SAFE-like directory structure expected by build_gri.
            safe_dir = target_dir / f"{item.id}.SAFE"
            granule_dir = safe_dir / "GRANULE" / item.id / "IMG_DATA"
            granule_dir.mkdir(parents=True, exist_ok=True)
            band_path = granule_dir / f"{item.id}_B04_10m.jp2"

            # Download via rasterio → converts COG to a real local GeoTIFF
            # with a .jp2 extension (extension is cosmetic; build_gri reads
            # via rioxarray which uses GDAL's format sniffing).
            #
            # Critically: this materializes ALL the pixels locally rather
            # than relying on urllib.urlretrieve which downloads the raw
            # COG stream (which may contain external references that break
            # once the signed URL expires).
            log.info("[gri-fetch] Downloading B04 (full raster ~100 MB)…")
            with rasterio.open(band_asset.href) as src:
                # Sanity check: the asset should have data
                if src.count == 0 or src.width < 1000 or src.height < 1000:
                    raise RuntimeError(
                        f"Downloaded asset looks malformed: count={src.count}, "
                        f"shape={src.height}x{src.width}"
                    )

                profile = src.profile.copy()
                profile.update(
                    driver="GTiff",  # write as real GeoTIFF
                    compress="deflate",
                    tiled=True,
                    blockxsize=512,
                    blockysize=512,
                )
                data = src.read()  # force full read from the signed URL

                with rasterio.open(band_path, "w", **profile) as dst:
                    dst.write(data)

            # Verify the written file opens cleanly and has sane stats.
            with rasterio.open(band_path) as verify:
                stats = verify.statistics(1, approx=True)
                if stats.std < 10:
                    raise RuntimeError(
                        f"Downloaded band has suspiciously low variance "
                        f"(std={stats.std:.1f}) — likely corrupted download."
                    )
                log.info(
                    "[gri-fetch] Downloaded B04 OK: shape=%dx%d, mean=%.0f, std=%.0f",
                    verify.height, verify.width, stats.mean, stats.std,
                )

            return safe_dir

        except Exception as exc:
            log.warning(
                "[gri-fetch] Scene %s failed (%s) — trying next candidate.",
                item.id, exc,
            )
            last_err = exc
            # Clean up partial files before trying next item
            if 'safe_dir' in locals() and safe_dir.exists():
                shutil.rmtree(safe_dir, ignore_errors=True)
            continue

    raise RuntimeError(
        f"[gri-fetch] All {len(items)} candidate scenes failed. "
        f"Last error: {last_err}"
    )


# ---------------------------------------------------------------------------
# Cache-first GRI lookup
# ---------------------------------------------------------------------------

def get_or_fetch_gri(mgrs_tile: str, cache_dir: Path) -> Path:
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    gri_path = cache_dir / f"GRI_{mgrs_tile}.tif"
    if gri_path.exists():
        log.info("[gri-fetch] Cache hit: %s", gri_path.name)
        return gri_path

    log.info("[gri-fetch] Cache miss for %s — fetching from Planetary Computer…", mgrs_tile)

    # Download the S2 SAFE to a temp directory; delete it after build.
    with tempfile.TemporaryDirectory(prefix="gri_s2_") as tmpdir:
        tmp_path = Path(tmpdir)
        safe_dir = _fetch_s2_safe_for_tile(mgrs_tile, tmp_path)

        log.info("[gri-fetch] Building GRI from downloaded scene…")
        try:
            # Import inside function so pipelineGRI doesn't load at module import.
            from pipelineGRI import build_gri
            build_gri(sentinel2_safe=safe_dir, gri_out=gri_path, resolution=10.0)
        except Exception as exc:
            # Clean up partial output on failure.
            if gri_path.exists():
                try:
                    gri_path.unlink()
                except OSError:
                    pass
            raise RuntimeError(f"[gri-fetch] build_gri failed: {exc}") from exc

    if not gri_path.exists():
        raise RuntimeError(f"[gri-fetch] build_gri did not produce {gri_path}")

    log.info("[gri-fetch] GRI cached → %s", gri_path)
    return gri_path