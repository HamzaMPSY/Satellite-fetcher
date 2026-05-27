
from __future__ import annotations
import logging
import math
import re
import shutil
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.warp import transform
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
log = logging.getLogger("sen2like")

# ---------------------------------------------------------------------------
# Supported product levels
# ---------------------------------------------------------------------------
_VALID_PRODUCT_LEVELS: frozenset[str] = frozenset({"L2F", "L2H"})

# ---------------------------------------------------------------------------
# Band mapping  —  logical fusion key → Sentinel-2 band id
# ---------------------------------------------------------------------------
BAND_MAP: dict[str, dict[str, str]] = {
    "B2": {"s2_id": "B02", "description": "Blue (490 nm)",      "nbar_name": "NBAR_Blue"},
    "B3": {"s2_id": "B03", "description": "Green (560 nm)",     "nbar_name": "NBAR_Green"},
    "B4": {"s2_id": "B04", "description": "Red (665 nm)",       "nbar_name": "NBAR_Red"},
    "B5": {"s2_id": "B08", "description": "NIR (842 nm)",        "nbar_name": "NBAR_NIR"},
    "B6": {"s2_id": "B11", "description": "SWIR 1 (1610 nm)",   "nbar_name": "NBAR_SWIR1"},
    "B7": {"s2_id": "B12", "description": "SWIR 2 (2190 nm)",   "nbar_name": "NBAR_SWIR2"},
    "B8": {"s2_id": "B08", "description": "NIR (842 nm)",       "nbar_name": "NBAR_NIR"},
}

# Fusion output filename stem  →  BAND_MAP key
FUSION_FILENAME_TO_BAND: dict[str, str] = {
    "B2_10m": "B2", "B3_10m": "B3", "B4_10m": "B4",
    "B5_10m": "B5", "B6_10m": "B6", "B7_10m": "B7", "B8_10m": "B8",
}

PIPELINE_NODATA: float = -9999.0

# WGS-84 CRS used for coordinate conversion
_WGS84 = CRS.from_epsg(4326)



def _expected_required_band_keys(optional_bands: frozenset[str] | None = None) -> frozenset[str]:

    if optional_bands is None:
        optional_bands = frozenset({"B8"})
    return frozenset(BAND_MAP.keys()) - optional_bands


def _expected_required_s2_ids(optional_bands: frozenset[str] | None = None) -> frozenset[str]:
    return frozenset(
        BAND_MAP[k]["s2_id"] for k in _expected_required_band_keys(optional_bands)
    )


# ---------------------------------------------------------------------------
# Small helpers — public so they can be unit-tested independently
# ---------------------------------------------------------------------------

def _normalize_product_level(raw: str) -> str:
    normalized = str(raw).upper().strip()
    if normalized not in _VALID_PRODUCT_LEVELS:
        raise ValueError(
            f"[packaging] Invalid product_level '{raw}'. "
            f"Must be one of: {sorted(_VALID_PRODUCT_LEVELS)}."
        )
    return normalized


def _product_metadata_filename(product_level: str) -> str:

    return f"MTD_MSI{product_level}.xml"


def _is_safe_complete(safe_dir: Path, product_level: str) -> bool:
    if not safe_dir.is_dir():
        return False

    # 1 — level-dependent product metadata
    if not (safe_dir / _product_metadata_filename(product_level)).exists():
        return False

    # 2 — manifest
    if not (safe_dir / "manifest.safe").exists():
        return False

    # 3 — granule directory is present, non-empty, and has tile metadata
    granule_root = safe_dir / "GRANULE"
    if not granule_root.exists():
        return False
    granule_dirs = [d for d in granule_root.iterdir() if d.is_dir()]
    if not granule_dirs:
        return False
    granule_dir = granule_dirs[0]
    if not (granule_dir / "MTD_TL.xml").exists():
        return False

    # 4 — every required S2 band TIF is present
    img_dir = granule_dir / "IMG_DATA" / "RESOLUTION_10M"
    if not img_dir.exists():
        return False

    expected_s2_ids = _expected_required_s2_ids()
    found_ids: set[str] = set()
    for tif in img_dir.glob("*_10m.TIF"):

        parts = tif.stem.split("_")
        if len(parts) >= 2:
            candidate = parts[-2].upper()   # e.g. "B02"
            if candidate in expected_s2_ids:
                found_ids.add(candidate)

    if not expected_s2_ids.issubset(found_ids):
        log.debug(
            "[packaging] Incomplete SAFE %s — missing bands: %s",
            safe_dir.name, sorted(expected_s2_ids - found_ids),
        )
        return False

    return True


def _safe_matches_context(safe_dir: Path, product_level: str, acq_date: str) -> bool:
    expected_prefix = f"S2L_MSI{product_level}_{acq_date}T"
    return safe_dir.name.startswith(expected_prefix)


_MTL_KV_RE = re.compile(r'^\s*([A-Z0-9_]+)\s*=\s*"?([^"]*)"?\s*$')


# ===========================================================================
# PackagingStep
# ===========================================================================

class PackagingStep:

    name = "packaging"

    _DEFAULTS: dict[str, Any] = {
        "mgrs_tile":            None,     # auto-derived from scene coordinates
        "product_level":        "L2F",    # "L2F" or "L2H" (case-insensitive)
        "processing_baseline":  "05.00",
        "quantification_value": 10000,
        "output_dtype":         "uint16",
        "compress":             "deflate",
        "cog":                  True,
        "nodata_out":           0,
    }

    def __init__(self, config: dict):
        merged = {**self._DEFAULTS, **config}
        # Normalize and validate product_level eagerly so misconfiguration
        merged["product_level"] = _normalize_product_level(merged["product_level"])
        self.config = merged

    # ------------------------------------------------------------------
    # Resume support
    # ------------------------------------------------------------------

    def restore_context(self, ctx) -> None:
        product_level = self.config["product_level"]
        safe_root     = ctx.working_dir / "SAFE"
        if not safe_root.exists():
            return

        acq_date = _resolve_acquisition_metadata(ctx)["date"]

        matching = [
            s for s in safe_root.glob("*.SAFE")
            if _is_safe_complete(s, product_level)
            and _safe_matches_context(s, product_level, acq_date)
        ]
        if not matching:
            return

        # Prefer newest by mtime; name as deterministic tie-breaker.
        best = max(matching, key=lambda s: (s.stat().st_mtime, s.name))
        ctx.data["safe_dir"] = str(best)
        log.debug("[packaging] Restored safe_dir from %s", best.name)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self, ctx) -> dict:
        fused_dir = ctx.data.get("fused_dir")
        if not fused_dir or not Path(str(fused_dir)).exists():
            raise RuntimeError(
                "[packaging] fused_dir is not available. "
                "DataFusionStep must run before PackagingStep."
            )
        fused_path = Path(str(fused_dir))

        # ── Preflight ─────────────────────────────────────────────────
        band_files = _discover_fusion_bands(fused_path)
        _verify_required_bands(band_files, fused_path)
        log.info("[packaging] Preflight: %d band files verified", len(band_files))

        # ── Configuration ──────────────────────────────────────────────
        product_level = self.config["product_level"]   # already normalized
        baseline      = self.config["processing_baseline"]
        quant         = int(self.config["quantification_value"])
        output_dtype  = self.config["output_dtype"]
        compress      = self.config["compress"]
        make_cog      = bool(self.config["cog"])
        nodata_out    = int(self.config["nodata_out"])

        # ── Identifiers ────────────────────────────────────────────────
        mgrs_tile  = self._resolve_mgrs_tile(ctx, fused_path)
        acq_meta   = _resolve_acquisition_metadata(ctx)
        acq_date   = acq_meta["date"]
        acq_stamp  = acq_meta["timestamp"]
        ctx.data["source_product_id"] = acq_meta["product_id"]
        ctx.data["acquisition_datetime"] = acq_meta["iso"]
        safe_name  = _build_safe_name(mgrs_tile, product_level, baseline, acq_stamp)
        granule_id = _build_granule_id(product_level, mgrs_tile, acq_stamp, baseline)

        log.info("[packaging] Acquisition date : %s", acq_date)
        log.info("[packaging] Acquisition time : %s", acq_meta["iso"])
        log.info("[packaging] MGRS tile        : %s", mgrs_tile)
        log.info("[packaging] Product level    : %s", product_level)

        # ── Paths (stage first, rename on success) ─────────────────────
        safe_root   = ctx.working_dir / "SAFE"
        final_dir   = safe_root / safe_name
        staging_dir = safe_root / f"{safe_name}.tmp"

        # Remove any leftover staging dir from a previous failed attempt.
        if staging_dir.exists():
            log.info("[packaging] Removing leftover staging dir: %s", staging_dir.name)
            shutil.rmtree(staging_dir)

        granule_dir = staging_dir / "GRANULE" / granule_id
        img_10m_dir = granule_dir / "IMG_DATA" / "RESOLUTION_10M"
        qi_dir      = granule_dir / "QI_DATA"

        for d in (img_10m_dir, qi_dir):
            d.mkdir(parents=True, exist_ok=True)

        log.info("[packaging] Staging → %s", staging_dir)

        # ── Band images (parallelized: each band is an independent
        # read-transform-write pipeline, no shared state) ──────────────
        copied_bands: list[str] = []

        # Build the list of conversion jobs, skipping unknown band keys.
        conversion_jobs: list[tuple[str, str, Path, Path]] = []
        for band_key, src_path in band_files.items():
            info = BAND_MAP.get(band_key)
            if info is None:
                log.warning("[packaging] Unknown band key %s — skipping", band_key)
                continue
            s2_id    = info["s2_id"]
            dst_name = f"{mgrs_tile}_{acq_date}_{s2_id}_10m.TIF"
            dst_path = img_10m_dir / dst_name
            conversion_jobs.append((band_key, s2_id, src_path, dst_path))

        def _run_conversion(job: tuple[str, str, Path, Path]) -> tuple[str, str, str]:
            band_key, s2_id, src_path, dst_path = job
            _convert_band(
                src=src_path, dst=dst_path, quant=quant, dtype=output_dtype,
                compress=compress, nodata_in=PIPELINE_NODATA,
                nodata_out=nodata_out, cog=make_cog,
            )
            return s2_id, src_path.name, dst_path.name

        n_workers = min(len(conversion_jobs), 6)
        log.info(
            "[packaging] Converting %d bands with %d threads…",
            len(conversion_jobs), n_workers,
        )
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_run_conversion, job): job for job in conversion_jobs}
            for future in as_completed(futures):
                s2_id, src_name, dst_name = future.result()
                copied_bands.append(s2_id)
                log.info("[packaging]   %s → %s", src_name, dst_name)

        if not copied_bands:
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise RuntimeError(
                f"[packaging] No band images written to {img_10m_dir}. "
                "Check that DataFusionStep produced *_10m.TIF files."
            )

        # ── Validity mask ──────────────────────────────────────────────
        validity_src = fused_path / "FUSION_VALIDITY_MASK.TIF"
        if validity_src.exists():
            dst_name = f"{mgrs_tile}_{acq_date}_VALIDITY_MASK_10m.TIF"
            shutil.copy2(validity_src, img_10m_dir / dst_name)
            log.info("[packaging]   FUSION_VALIDITY_MASK.TIF → %s", dst_name)
        else:
            log.warning("[packaging] FUSION_VALIDITY_MASK.TIF not found — skipped")

        # ── Cloud mask ─────────────────────────────────────────────────
        mask_path = ctx.data.get("mask_path")
        if mask_path and Path(str(mask_path)).exists():
            qi_name = f"{mgrs_tile}_{acq_date}_CLOUD_MASK.TIF"
            shutil.copy2(str(mask_path), qi_dir / qi_name)
            log.info("[packaging]   cloud mask → QI_DATA/%s", qi_name)
        else:
            log.warning("[packaging] mask_path not available — QI_DATA cloud mask skipped")

        # ── XML metadata ───────────────────────────────────────────────
        _write_product_metadata(
            safe_dir=staging_dir, ctx=ctx, mgrs_tile=mgrs_tile,
            product_level=product_level, baseline=baseline,
            acq_date=acq_date, granule_id=granule_id,
            copied_bands=copied_bands, quant=quant,
        )
        _write_tile_metadata(
            granule_dir=granule_dir, ctx=ctx, mgrs_tile=mgrs_tile,
            acq_date=acq_date, copied_bands=copied_bands,
            img_10m_dir=img_10m_dir, product_level=product_level,
        )
        _write_inspire_metadata(
            safe_dir=staging_dir, ctx=ctx,
            mgrs_tile=mgrs_tile, acq_date=acq_date,
            product_level=product_level,
        )
        _write_manifest_safe(staging_dir, granule_id, img_10m_dir, qi_dir, product_level)

        # ── Atomic rename: staging → final ─────────────────────────────
        if final_dir.exists():
            shutil.rmtree(final_dir)
        staging_dir.rename(final_dir)
        log.info("[packaging] Done — %d bands → %s", len(copied_bands), final_dir)

        return {"safe_dir": str(final_dir), "packaged_bands": copied_bands}

    # ------------------------------------------------------------------
    # MGRS tile resolution
    # ------------------------------------------------------------------

    def _resolve_mgrs_tile(self, ctx, fused_path: Path) -> str:
        explicit = self.config.get("mgrs_tile")
        if explicit:
            log.info("[packaging] Using explicit MGRS tile from config: %s", explicit)
            return str(explicit).upper()

        tile = _mgrs_from_scene_centre(ctx, fused_path)
        if tile:
            return tile

        wrs_match = re.search(r"_(\d{3})(\d{3})_", ctx.product_id)
        if wrs_match:
            path_num = int(wrs_match.group(1))
            row_num  = int(wrs_match.group(2))
            fallback = f"T{path_num:03d}R{row_num:03d}"
            log.warning(
                "[packaging] mgrs library unavailable — using WRS-2 fallback %s. "
                "Install with: pip install mgrs",
                fallback,
            )
            return fallback

        log.warning("[packaging] Could not derive MGRS tile — using T00XXX")
        return "T00XXX"


# ===========================================================================
# MGRS derivation from scene centre coordinates
# ===========================================================================

def _mgrs_from_scene_centre(ctx, fused_path: Path) -> str | None:
    try:
        import mgrs as mgrs_lib
    except ImportError:
        log.warning(
            "[packaging] `mgrs` library not installed — cannot auto-derive MGRS tile. "
            "Run:  pip install mgrs"
        )
        return None

    candidate_dirs: list[Path] = [fused_path]
    landsat_path = ctx.config.get("landsat_path")
    if landsat_path:
        candidate_dirs.append(Path(str(landsat_path)))

    ref_tif: Path | None = None
    for d in candidate_dirs:
        candidates = sorted(
            t for t in d.glob("*.TIF")
            if "VALIDITY" not in t.name.upper() and "MASK" not in t.name.upper()
        )
        if candidates:
            ref_tif = candidates[0]
            break

    if ref_tif is None:
        log.warning("[packaging] No reference TIF found for MGRS derivation")
        return None

    try:
        with rasterio.open(ref_tif) as src:
            src_crs = src.crs
            if src_crs is None:
                log.warning(
                    "[packaging] Reference TIF %s has no CRS — cannot derive MGRS tile",
                    ref_tif.name,
                )
                return None
            bounds = src.bounds
            cx = bounds.left   + (bounds.right - bounds.left)  / 2.0
            cy = bounds.bottom + (bounds.top   - bounds.bottom) / 2.0
    except Exception as exc:
        log.warning("[packaging] Could not open reference TIF for MGRS derivation: %s", exc)
        return None

    try:
        xs, ys   = transform(src_crs, _WGS84, [cx], [cy])
        lon, lat = float(xs[0]), float(ys[0])
    except Exception as exc:
        log.warning("[packaging] Coordinate transform failed: %s", exc)
        return None

    # Sanity-check the projected coordinates before passing to mgrs.
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        log.warning(
            "[packaging] Derived coordinates out of range (lat=%.4f lon=%.4f) — "
            "skipping MGRS derivation", lat, lon,
        )
        return None

    try:
        m        = mgrs_lib.MGRS()
        # MGRSPrecision=0 → 100 km grid square → 5-char code e.g. '30STB'
        mgrs_str = m.toMGRS(lat, lon, MGRSPrecision=0)
        tile     = f"T{mgrs_str}"
        log.info(
            "[packaging] MGRS tile derived from scene centre (lat=%.4f lon=%.4f): %s",
            lat, lon, tile,
        )
        return tile
    except Exception as exc:
        log.warning(
            "[packaging] MGRS conversion failed (lat=%.4f lon=%.4f): %s", lat, lon, exc,
        )
        return None


# ===========================================================================
# Band discovery
# ===========================================================================

def _discover_fusion_bands(fused_path: Path) -> dict[str, Path]:
    """Return ``{band_key: Path}`` for all ``*_10m.TIF`` fusion outputs."""
    result: dict[str, Path] = {}
    for tif in sorted(fused_path.glob("*_10m.TIF")):
        if "VALIDITY" in tif.name.upper() or "MASK" in tif.name.upper():
            continue
        stem = tif.stem
        # Exact match first (e.g. stem "B4_10m").
        for pattern, band_key in FUSION_FILENAME_TO_BAND.items():
            if stem.upper() == pattern.upper():
                result[band_key] = tif
                break
        else:
            # Fallback: extract band token from stem (e.g. "NBAR_Red_B4_10m").
            m = re.search(r"(B\d{1,2})_10m", stem, re.IGNORECASE)
            if m:
                bk = m.group(1).upper()
                if bk in BAND_MAP:
                    result[bk] = tif
    return result


def _verify_required_bands(band_files: dict[str, Path], fused_path: Path) -> None:
    required = _expected_required_band_keys()
    missing  = required - band_files.keys()
    if missing:
        raise RuntimeError(
            f"[packaging] Required fusion bands missing in {fused_path}: "
            f"{sorted(missing)}. Re-run DataFusionStep with --no-resume."
        )
    for band_key in required:
        src = band_files[band_key]
        if not src.exists():
            raise RuntimeError(
                f"[packaging] Band file does not exist: {src}. "
                "Re-run DataFusionStep with --no-resume."
            )
        if src.stat().st_size < 1024:
            raise RuntimeError(
                f"[packaging] Band file is suspiciously small "
                f"({src.stat().st_size} B): {src}. "
                "Re-run DataFusionStep with --no-resume."
            )


# ===========================================================================
# Band conversion
# ===========================================================================

def _build_nodata_mask(data: np.ndarray, src_nodata: float | None) -> np.ndarray:

    finite_mask = np.isfinite(data)   # rules out NaN and ±Inf unconditionally

    if src_nodata is None:
        return finite_mask

    if math.isnan(src_nodata):
        # nodata encoded as NaN: invalid where data is NaN (already handled).
        return finite_mask

    # Finite sentinel — avoid exact float equality with isclose.
    not_sentinel = ~np.isclose(data, src_nodata, rtol=0.0, atol=1e-6, equal_nan=False)
    return finite_mask & not_sentinel


def _convert_band(
    src: Path,
    dst: Path,
    quant: int,
    dtype: str,
    compress: str,
    nodata_in: float,
    nodata_out: int,
    cog: bool,
) -> None:

    with rasterio.open(src) as src_ds:
        data    = src_ds.read(1).astype(np.float32)
        profile = src_ds.profile.copy()
        src_nd  = src_ds.nodata

    effective_nodata = float(src_nd) if src_nd is not None else nodata_in
    valid = _build_nodata_mask(data, effective_nodata)

    if dtype == "uint16":
        out        = np.full(data.shape, nodata_out, dtype=np.uint16)
        scaled     = np.clip(data[valid] * quant, 0, 65534).astype(np.uint16)
        out[valid] = scaled
        rio_dtype  = "uint16"
        nd_write   = nodata_out
    else:
        out          = np.full(data.shape, nodata_in, dtype=np.float32)
        out[valid]   = data[valid]
        rio_dtype    = "float32"
        nd_write     = nodata_in

    profile.update(dtype=rio_dtype, count=1, nodata=nd_write, compress=compress)
    if cog:
        profile.update(tiled=True, blockxsize=512, blockysize=512)

    # Write to a sibling .tmp file; rename atomically on success.
    tmp = dst.with_suffix(".tmp.TIF")
    with rasterio.open(tmp, "w", **profile) as dst_ds:
        dst_ds.write(out, 1)

    if cog:
        try:
            with rasterio.open(tmp, "r+") as ds:
                ds.build_overviews([2, 4, 8, 16], Resampling.average)
                ds.update_tags(ns="rio_overview", resampling="average")
        except Exception as exc:
            log.warning("[packaging] Could not add overviews to %s: %s", dst.name, exc)

    tmp.replace(dst)


# ===========================================================================
# XML metadata writers
# ===========================================================================

def _write_product_metadata(
    safe_dir: Path,
    ctx,
    mgrs_tile: str,
    product_level: str,
    baseline: str,
    acq_date: str,
    granule_id: str,
    copied_bands: list[str],
    quant: int,
) -> None:
    now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    source_product_id = str(ctx.data.get("source_product_id") or ctx.product_id)
    acquisition_time = str(ctx.data.get("acquisition_datetime") or "").strip()
    root = ET.Element(f"Level-{product_level}_Product_Metadata")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

    gi = ET.SubElement(root, "General_Info")
    ET.SubElement(gi, "Product_Info").text     = product_level
    ET.SubElement(gi, "Product_ID").text       = source_product_id
    ET.SubElement(gi, "MGRS_Tile").text        = mgrs_tile
    ET.SubElement(gi, "Processing_Level").text = product_level
    ET.SubElement(gi, "Baseline").text         = baseline
    ET.SubElement(gi, "Generation_Time").text  = now
    ET.SubElement(gi, "Acquisition_Date").text = acq_date
    if acquisition_time:
        ET.SubElement(gi, "Acquisition_Time").text = acquisition_time
    ET.SubElement(gi, "Input_Product").text    = str(ctx.config.get("landsat_path", ""))
    if ctx.config.get("s2_path"):
        ET.SubElement(gi, "S2_Reference").text = str(ctx.config["s2_path"])

    qi_node = ET.SubElement(root, "Quantification_Info")
    ET.SubElement(qi_node, "BOA_Quantification_Value", unit="none").text = str(quant)
    ET.SubElement(qi_node, "BOA_Add_Offset").text = "0"
    ET.SubElement(qi_node, "Nodata_Value").text   = "0"

    bl = ET.SubElement(root, "Band_List")
    for b in copied_bands:
        ET.SubElement(bl, "Band_Name").text = b

    ps = ET.SubElement(root, "Processing_Steps")
    for step in [
        "atmospheric_correction", "sbaf", "valid_pixel_mask",
        "brdf_adjustment", "data_fusion", "packaging",
    ]:
        ET.SubElement(ps, "Step").text = step

    mask_stats = ctx.data.get("mask_stats", {})
    if mask_stats:
        ms = ET.SubElement(root, "Mask_Info")
        vf = mask_stats.get("valid_fraction")
        if vf is not None:
            ET.SubElement(ms, "Valid_Pixel_Fraction").text = f"{vf:.6f}"

    out_path = safe_dir / _product_metadata_filename(product_level)
    _write_xml(root, out_path)
    log.info("[packaging]   %s written", out_path.name)


def _write_tile_metadata(
    granule_dir: Path,
    ctx,
    mgrs_tile: str,
    acq_date: str,
    copied_bands: list[str],
    img_10m_dir: Path,
    product_level: str,
) -> None:
    """Write tile-level metadata XML inside the GRANULE directory."""
    sensing_time = str(ctx.data.get("acquisition_datetime") or "").strip()
    if not sensing_time:
        sensing_time = f"{acq_date[:4]}-{acq_date[4:6]}-{acq_date[6:8]}T00:00:00Z"
    root = ET.Element(f"Level-{product_level}_Tile_Metadata")
    gi   = ET.SubElement(root, "General_Info")
    ET.SubElement(gi, "TILE_ID").text      = mgrs_tile
    ET.SubElement(gi, "DATASTRIP_ID").text = f"DS_{mgrs_tile}_{acq_date}"
    ET.SubElement(gi, "SENSING_TIME").text = sensing_time

    geo_info = ET.SubElement(root, "Geometric_Info")
    tci      = ET.SubElement(geo_info, "Tile_Coordinate_Info")
    first_tif = next(iter(sorted(img_10m_dir.glob("*.TIF"))), None)
    if first_tif:
        try:
            with rasterio.open(first_tif) as ds:
                t = ds.transform
                ET.SubElement(tci, "ULX").text   = str(int(t.c))
                ET.SubElement(tci, "ULY").text   = str(int(t.f))
                ET.SubElement(tci, "XDIM").text  = str(int(abs(t.a)))
                ET.SubElement(tci, "YDIM").text  = str(int(abs(t.e)))
                ET.SubElement(tci, "NROWS").text = str(ds.height)
                ET.SubElement(tci, "NCOLS").text = str(ds.width)
                if ds.crs:
                    ET.SubElement(tci, "CRS_EPSG").text = str(
                        ds.crs.to_epsg() or ds.crs.to_wkt()
                    )
        except Exception as exc:
            log.warning("[packaging] Could not read tile coordinate info: %s", exc)

    sun_zenith = ctx.data.get("sun_zenith")
    if sun_zenith is not None:
        angle_node = ET.SubElement(root, "Sun_Angles")
        ET.SubElement(angle_node, "Sun_Zenith_Angle", unit="deg").text = f"{sun_zenith:.4f}"

    ic = ET.SubElement(root, "Image_Content_QI")
    for b in copied_bands:
        ET.SubElement(ic, "Band").text = b

    _write_xml(root, granule_dir / "MTD_TL.xml")
    log.info("[packaging]   MTD_TL.xml written")


def _write_inspire_metadata(
    safe_dir: Path,
    ctx,
    mgrs_tile: str,
    acq_date: str,
    product_level: str,
) -> None:
    """Write a minimal INSPIRE discovery metadata file."""
    now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    source_product_id = str(ctx.data.get("source_product_id") or ctx.product_id)
    root = ET.Element("MD_Metadata")
    root.set("xmlns",     "http://www.isotc211.org/2005/gmd")
    root.set("xmlns:gco", "http://www.isotc211.org/2005/gco")

    fid = ET.SubElement(root, "fileIdentifier")
    ET.SubElement(fid, "{http://www.isotc211.org/2005/gco}CharacterString").text = (
        f"{source_product_id}_{mgrs_tile}"
    )
    lang = ET.SubElement(root, "language")
    ET.SubElement(lang, "{http://www.isotc211.org/2005/gco}CharacterString").text = "eng"

    contact = ET.SubElement(root, "contact")
    resp    = ET.SubElement(contact, "CI_ResponsibleParty")
    org     = ET.SubElement(resp, "organisationName")
    ET.SubElement(org, "{http://www.isotc211.org/2005/gco}CharacterString").text = (
        "Sen2Like Pipeline"
    )
    role = ET.SubElement(resp, "role")
    ET.SubElement(role, "CI_RoleCode", codeList="", codeListValue="originator")

    dt = ET.SubElement(root, "dateStamp")
    ET.SubElement(dt, "{http://www.isotc211.org/2005/gco}DateTime").text = now

    ident  = ET.SubElement(root, "identificationInfo")
    md_di  = ET.SubElement(ident, "MD_DataIdentification")
    cit    = ET.SubElement(md_di, "citation")
    ci_cit = ET.SubElement(cit, "CI_Citation")
    title  = ET.SubElement(ci_cit, "title")
    ET.SubElement(title, "{http://www.isotc211.org/2005/gco}CharacterString").text = (
        f"Sentinel-2 Like {source_product_id} {product_level} Tile {mgrs_tile} {acq_date}"
    )

    _write_xml(root, safe_dir / "INSPIRE.xml")
    log.info("[packaging]   INSPIRE.xml written")


def _write_manifest_safe(
    safe_dir: Path,
    granule_id: str,
    img_10m_dir: Path,
    qi_dir: Path,
    product_level: str,
) -> None:
    """Write a ``manifest.safe`` listing all product files."""
    now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    # Strip staging suffix (.tmp) from the display name if present.
    stem = safe_dir.name.replace(".tmp", "").replace(".SAFE", "")
    lines = [
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>',
        f'<xfdu:XFDU xmlns:xfdu="urn:ccsds:schema:xfdu:1" '
        f'version="esa/safe/sentinel/1.1/sentinel-2/msi/'
        f'archive_{product_level.lower()}_user_product">',
        f'  <!-- Generated by sen2like-pipeline at {now} -->',
        '  <informationPackageMap>',
        f'    <xfdu:contentUnit unitType="Sentinel-2 MSI {stem}" '
        f'textInfo="Sentinel-2 MSI {product_level} User Product">',
    ]
    for f in sorted(safe_dir.glob("*.xml")):
        lines.append(_manifest_entry(f, safe_dir))
    for f in sorted(img_10m_dir.glob("*.TIF")):
        lines.append(_manifest_entry(f, safe_dir))
    for f in sorted(qi_dir.glob("*.TIF")):
        lines.append(_manifest_entry(f, safe_dir))
    lines += ["    </xfdu:contentUnit>", "  </informationPackageMap>", "</xfdu:XFDU>"]
    (safe_dir / "manifest.safe").write_text("\n".join(lines), encoding="utf-8")
    log.info("[packaging]   manifest.safe written")


def _manifest_entry(f: Path, safe_dir: Path) -> str:
    """Return a single ``<dataObject>`` manifest line for file *f*."""
    return (
        f'      <dataObject><byteStream>'
        f'<fileLocation href="{f.relative_to(safe_dir)}"/>'
        f'</byteStream></dataObject>'
    )


# ===========================================================================
# XML utility
# ===========================================================================

def _write_xml(root: ET.Element, path: Path) -> None:
    """Pretty-print *root* to *path* as UTF-8 XML with standard indentation."""
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")   # requires Python 3.9+
    tree.write(str(path), encoding="utf-8", xml_declaration=True)


# ===========================================================================
# Date / naming helpers
# ===========================================================================

def _extract_acquisition_date(product_id: str) -> str:
    date_value = _try_extract_acquisition_date(product_id)
    if date_value:
        return date_value

    # Strategy 3 — last resort fallback
    today = time.strftime("%Y%m%d", time.gmtime())
    log.warning(
        "[packaging] Could not extract acquisition date from '%s' — using %s",
        product_id, today,
    )
    return today


def _try_extract_acquisition_date(product_id: str) -> str:
    # Strategy 1 — token split (reliable for standard naming)
    for token in product_id.split("_"):
        if len(token) == 8 and token.isdigit() and token[:2] in ("19", "20"):
            return token

    # Strategy 2 — substring scan (handles non-standard separators)
    matches = re.findall(r"(?<!\d)((?:19|20)\d{6})(?!\d)", product_id)
    if matches:
        return matches[0]

    return ""


def _resolve_acquisition_metadata(ctx) -> dict[str, str]:
    for mtl_path in _candidate_mtl_paths(ctx):
        values = _read_mtl_values(mtl_path)
        date_value = _normalize_mtl_date(values.get("DATE_ACQUIRED"))
        if not date_value:
            continue
        time_value = str(values.get("SCENE_CENTER_TIME") or "00:00:00Z")
        product_id = str(values.get("LANDSAT_PRODUCT_ID") or ctx.product_id).strip()
        return {
            "date": date_value,
            "timestamp": _build_acquisition_timestamp(date_value, time_value),
            "iso": _build_acquisition_iso(date_value, time_value),
            "product_id": product_id,
            "source": str(mtl_path),
        }

    date_value = _try_extract_acquisition_date(str(ctx.product_id))
    if not date_value:
        raise RuntimeError(
            "[packaging] Could not resolve acquisition metadata from Landsat MTL "
            f"or product id '{ctx.product_id}'. Refusing to write a SAFE product "
            "with a synthetic acquisition date."
        )
    return {
        "date": date_value,
        "timestamp": f"{date_value}T000000",
        "iso": f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}T00:00:00Z",
        "product_id": str(ctx.product_id),
        "source": "",
    }


def _candidate_mtl_paths(ctx) -> list[Path]:
    candidates: list[Path] = []
    for raw in (
        getattr(ctx, "product_id", ""),
        getattr(ctx, "config", {}).get("landsat_path", ""),
    ):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text)
        if path.is_file() and path.name.endswith("_MTL.txt"):
            candidates.append(path)
        elif path.is_dir():
            candidates.extend(sorted(path.glob("*_MTL.txt")))
            candidates.extend(sorted(path.glob("**/*_MTL.txt")))
    return list(dict.fromkeys(candidates))


def _read_mtl_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return values
    for line in lines:
        match = _MTL_KV_RE.match(line)
        if not match:
            continue
        key, value = match.groups()
        values[key] = value.strip().strip('"')
    return values


def _normalize_mtl_date(value: str | None) -> str:
    text = str(value or "").strip().strip('"')
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return "".join(match.groups())
    if re.fullmatch(r"\d{8}", text):
        return text
    return ""


def _build_acquisition_timestamp(acq_date: str, scene_center_time: str | None) -> str:
    match = re.search(r"(\d{2}):(\d{2}):(\d{2})", str(scene_center_time or ""))
    if not match:
        return f"{acq_date}T000000"
    return f"{acq_date}T{match.group(1)}{match.group(2)}{match.group(3)}"


def _build_acquisition_iso(acq_date: str, scene_center_time: str | None) -> str:
    text = str(scene_center_time or "00:00:00Z").strip().strip('"')
    match = re.match(r"(?P<hms>\d{2}:\d{2}:\d{2})(?:\.(?P<fraction>\d+))?Z?$", text)
    if not match:
        text = "00:00:00Z"
    else:
        hms = match.group("hms")
        fraction = str(match.group("fraction") or "")
        text = hms
        if fraction:
            text = f"{text}.{fraction[:6]}"
        text = f"{text}Z"
    return f"{acq_date[:4]}-{acq_date[4:6]}-{acq_date[6:8]}T{text}"


def _build_safe_name(
    mgrs_tile: str,
    product_level: str,
    baseline: str,
    acquisition_timestamp: str,
) -> str:
    """Build the .SAFE folder name following the official sen2like convention."""
    baseline_tag = baseline.replace(".", "")
    timestamp    = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return (
        f"S2L_MSI{product_level}_{acquisition_timestamp}_"
        f"N{baseline_tag}_R000_{mgrs_tile}_{timestamp}.SAFE"
    )


def _build_granule_id(
    product_level: str,
    mgrs_tile: str,
    acquisition_timestamp: str,
    baseline: str,
) -> str:
    """Build the GRANULE sub-folder ID consistent with *product_level*."""
    baseline_tag = baseline.replace(".", "")
    return f"{product_level}_{mgrs_tile}_{acquisition_timestamp}_N{baseline_tag}_R000"
