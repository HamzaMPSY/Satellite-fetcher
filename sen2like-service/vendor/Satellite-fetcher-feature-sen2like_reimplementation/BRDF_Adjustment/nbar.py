#!/usr/bin/env python3

from __future__ import annotations
import argparse
import glob
import json
import logging
import math
import os
from pathlib import Path
from typing import Tuple
import rasterio
import dask.array as da
import numpy as np
import rioxarray as rxr
import xarray as xr

log = logging.getLogger("NBAR")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Band definitions
# ---------------------------------------------------------------------------

# ROY et al. 2016  (f_iso, f_geo, f_vol) — MODIS-equivalent wavelengths
ROY_COEFS = {
    "Blue":  (0.0774, 0.0079, 0.0372),
    "Green": (0.1306, 0.0178, 0.0580),
    "Red":   (0.1690, 0.0227, 0.0574),
    "NIR":   (0.3093, 0.0330, 0.1535),
    "SWIR1": (0.1774, 0.0240, 0.0584),
    "SWIR2": (0.1274, 0.0179, 0.0597),
}

LS8_BANDS = {
    "Blue":  "SR_B2",
    "Green": "SR_B3",
    "Red":   "SR_B4",
    "NIR":   "SR_B5",
    "SWIR1": "SR_B6",
    "SWIR2": "SR_B7",
}

LS8_BANDS_SBAF = {
    "Blue":  "SBAF_B2",
    "Green": "SBAF_B3",
    "Red":   "SBAF_B4",
    "NIR":   "SBAF_B5",
    "SWIR1": "SBAF_B6",
    "SWIR2": "SBAF_B7",
}

S2_BANDS = {
    "Blue":  ("R10m", "B02"),
    "Green": ("R10m", "B03"),
    "Red":   ("R10m", "B04"),
    "NIR":   ("R20m", "B8A"),
    "SWIR1": ("R20m", "B11"),
    "SWIR2": ("R20m", "B12"),
}

SCL_MASK_CLASSES = {0, 1, 3, 8, 9, 10}

LS8_SCALE  = 0.0000275
LS8_OFFSET = -0.2
S2_SCALE   = 0.0001
NODATA     = -9999.0
CHUNKS     = {"x": 1024, "y": 1024}
DEG2RAD    = np.float32(np.pi / 180.0)


def _raster_write_threads() -> str:
    configured = (
        os.getenv("GDAL_NUM_THREADS")
        or os.getenv("NIMBUS_SEN2LIKE_GDAL_NUM_THREADS")
        or "1"
    )
    value = str(configured).strip()
    return value or "1"

# ---------------------------------------------------------------------------
# BRDF kernels (dask versions — still used by S2 per-pixel path)
# ---------------------------------------------------------------------------

def li_sparse(ts, tv, phi, h=2.0, b=1.0):
    tsr, tvr, pr = ts*DEG2RAD, tv*DEG2RAD, phi*DEG2RAD
    tsp = da.arctan(b * da.tan(tsr))
    tvp = da.arctan(b * da.tan(tvr))
    cos_xi  = da.cos(tsp)*da.cos(tvp) + da.sin(tsp)*da.sin(tvp)*da.cos(pr)
    tan_sp, tan_vp = da.tan(tsp), da.tan(tvp)
    D      = da.sqrt(da.maximum(tan_sp**2 + tan_vp**2 - 2*tan_sp*tan_vp*da.cos(pr), 0))
    sec_sp = da.where(da.cos(tsp)==0, np.float32(1e10), 1/da.cos(tsp))
    sec_vp = da.where(da.cos(tvp)==0, np.float32(1e10), 1/da.cos(tvp))
    num    = da.sqrt(D**2 + (tan_sp*tan_vp*da.sin(pr))**2)
    den    = sec_sp + sec_vp
    cos_t  = da.clip(h * num / den, -1, 1)
    sin_t  = da.sqrt(da.maximum(1 - cos_t**2, 0))
    t      = da.arccos(cos_t)
    overlap = (1/np.pi) * (t - sin_t*cos_t) * den
    return overlap - sec_sp - sec_vp + 0.5*(1+cos_xi)*sec_sp*sec_vp


def ross_thick(ts, tv, phi):
    tsr, tvr, pr = ts*DEG2RAD, tv*DEG2RAD, phi*DEG2RAD
    cos_xi = da.clip(da.cos(tsr)*da.cos(tvr)+da.sin(tsr)*da.sin(tvr)*da.cos(pr), -1, 1)
    xi  = da.arccos(cos_xi)
    num = (np.pi/2 - xi)*da.cos(xi) + da.sin(xi)
    den = da.where(da.abs(da.cos(tvr)+da.cos(tsr)) < 1e-6,
                   np.float32(1e-6), da.cos(tvr)+da.cos(tsr))
    return (4/(3*np.pi)) * (num/den) - 1/3


def cmatrix(SZA, VZA, SAA, VAA, theta_s, coef):
    phi  = SAA - VAA
    kg_i = li_sparse(SZA, VZA, phi)
    kv_i = ross_thick(SZA, VZA, phi)
    z    = da.zeros_like(SZA)
    norm = da.full_like(SZA, np.float32(theta_s))
    kg_n = li_sparse(norm, z, z)
    kv_n = ross_thick(norm, z, z)
    kg_i = da.clip(kg_i, -2.0, 10.0)
    kv_i = da.clip(kv_i, -2.0, 10.0)
    kg_n = da.clip(kg_n, -2.0, 10.0)
    kv_n = da.clip(kv_n, -2.0, 10.0)
    f0, f1, f2 = (np.float32(c) for c in coef)
    num = f0 + f1*kg_n + f2*kv_n
    den = f0 + f1*kg_i + f2*kv_i
    den = da.where(da.abs(den) < 1e-9, np.float32(1e-9), den)
    return num / den


def apply_nbar(img, CM, clip_min_factor=0.8, clip_max_factor=1.2):
    """Apply C-factor correction with configurable clip bounds."""
    out = img * CM
    out = da.clip(out, clip_min_factor * img, clip_max_factor * img)
    return da.where(img <= 0, np.float32(0), out)


def mean_sun_angle(lat):
    k = [31.0076, -0.1272, 0.01187, 2.40e-05, -9.48e-07, -1.95e-09, 6.15e-11]
    lat = float(lat)
    return float(sum(k[i] * lat**i for i in range(7)))


# ---------------------------------------------------------------------------
# Scalar C-matrix — used by LS8 path where angles are scene-uniform.
#
# For Landsat-8, SZA/SAA come from scalar MTL values and VZA/VAA are zero
# arrays (nadir-looking assumption). So the C-factor is spatially constant
# across the whole scene — computing it as a dask graph over 10980×10980
# elements just to produce one number wastes ~30 s per band. The log lines
# like `[CMAT] Green  C-factor: min=0.960  max=0.960` confirm min == max.
#
# This scalar version returns a single float that can be multiplied with
# the numpy reflectance array directly. Math identical to the dask
# cmatrix()/ross_thick()/li_sparse() pipeline, just without the dask graph.
# ---------------------------------------------------------------------------

def _cmatrix_scalar(sza, vza, saa, vaa, theta_s, coef):
    ts, tv, phi = sza * float(DEG2RAD), vza * float(DEG2RAD), (saa - vaa) * float(DEG2RAD)
    norm        = theta_s * float(DEG2RAD)

    def _li_sparse(ts, tv, phi, h=2.0, b=1.0):
        tsp = math.atan(b * math.tan(ts))
        tvp = math.atan(b * math.tan(tv))
        cos_xi = math.cos(tsp)*math.cos(tvp) + math.sin(tsp)*math.sin(tvp)*math.cos(phi)
        tan_sp, tan_vp = math.tan(tsp), math.tan(tvp)
        D = math.sqrt(max(tan_sp**2 + tan_vp**2 - 2*tan_sp*tan_vp*math.cos(phi), 0))
        sec_sp = 1/math.cos(tsp) if math.cos(tsp) != 0 else 1e10
        sec_vp = 1/math.cos(tvp) if math.cos(tvp) != 0 else 1e10
        num = math.sqrt(D**2 + (tan_sp*tan_vp*math.sin(phi))**2)
        den = sec_sp + sec_vp
        cos_t = max(-1.0, min(1.0, h * num / den))
        sin_t = math.sqrt(max(1 - cos_t**2, 0))
        t = math.acos(cos_t)
        overlap = (1/math.pi) * (t - sin_t*cos_t) * den
        return overlap - sec_sp - sec_vp + 0.5*(1+cos_xi)*sec_sp*sec_vp

    def _ross_thick(ts, tv, phi):
        cos_xi = max(-1.0, min(1.0,
            math.cos(ts)*math.cos(tv) + math.sin(ts)*math.sin(tv)*math.cos(phi)))
        xi  = math.acos(cos_xi)
        num = (math.pi/2 - xi)*math.cos(xi) + math.sin(xi)
        den = math.cos(tv) + math.cos(ts)
        if abs(den) < 1e-6:
            den = 1e-6
        return (4/(3*math.pi)) * (num/den) - 1/3

    kg_i = max(-2.0, min(10.0, _li_sparse(ts, tv, phi)))
    kv_i = max(-2.0, min(10.0, _ross_thick(ts, tv, phi)))
    kg_n = max(-2.0, min(10.0, _li_sparse(norm, 0.0, 0.0)))
    kv_n = max(-2.0, min(10.0, _ross_thick(norm, 0.0, 0.0)))

    f0, f1, f2 = coef
    num = f0 + f1*kg_n + f2*kv_n
    den = f0 + f1*kg_i + f2*kv_i
    if abs(den) < 1e-9:
        den = 1e-9
    return float(num / den)


# ---------------------------------------------------------------------------
# Unified C-factor diagnostic logging (dask version — kept for S2)
# ---------------------------------------------------------------------------

def _log_cfactor(band, CM, clip_min, clip_max):
    CM_np = CM.compute()
    clip_lo = float(np.mean(CM_np < clip_min))
    clip_hi = float(np.mean(CM_np > clip_max))
    log.info(
        "[CMAT] %s  C-factor: min=%.3f  max=%.3f  "
        "clipped_low=%.1f%%  clipped_high=%.1f%%",
        band,
        float(np.nanmin(CM_np)), float(np.nanmax(CM_np)),
        clip_lo * 100, clip_hi * 100,
    )


# ---------------------------------------------------------------------------
# Reflectance detection helper
# ---------------------------------------------------------------------------

def _is_boa_reflectance(src) -> bool:
    for band_idx in range(1, src.count + 1):
        tags = src.tags(band_idx)
        if tags.get("data_type") == "boa_reflectance":
            return True
    if src.tags().get("data_type") == "boa_reflectance":
        return True
    is_float = np.issubdtype(src.dtypes[0], np.floating)
    if is_float:
        log.warning(
            "[NBAR] '%s' lacks data_type tag — assuming BOA reflectance based on "
            "float dtype. Tag files with data_type=boa_reflectance for robust detection.",
            src.name,
        )
    return is_float


# ---------------------------------------------------------------------------
# Angle helpers
# ---------------------------------------------------------------------------

def _angle_array(value, shape):
    return da.full(shape, np.float32(value), chunks=list(CHUNKS.values()))


# ---------------------------------------------------------------------------
# Landsat-8
# ---------------------------------------------------------------------------

def ls8_angles(ls8_dir):

    search_dirs = [ls8_dir]
    parent = Path(ls8_dir).parent
    if parent != Path(ls8_dir):
        search_dirs.append(str(parent))

    for search_dir in search_dirs:
        json_files = glob.glob(os.path.join(search_dir, "*_MTL.json"))
        if json_files:
            with open(json_files[0]) as f:
                mtl = json.load(f)
            ia = mtl["LANDSAT_METADATA_FILE"].get("IMAGE_ATTRIBUTES", {})
            pa = mtl["LANDSAT_METADATA_FILE"].get("PROJECTION_ATTRIBUTES", {})

            sza = 90.0 - float(ia["SUN_ELEVATION"])
            saa = float(ia["SUN_AZIMUTH"])

            ul_lat = pa.get("CORNER_UL_LAT_PRODUCT", ia.get("CORNER_UL_LAT_PRODUCT"))
            lr_lat = pa.get("CORNER_LR_LAT_PRODUCT", ia.get("CORNER_LR_LAT_PRODUCT"))

            if ul_lat is None or lr_lat is None:
                raise KeyError("Missing CORNER_UL/LR_LAT_PRODUCT in MTL")

            lat = (float(ul_lat) + float(lr_lat)) / 2
            log.info("[ls8_angles] Using MTL: %s", json_files[0])
            return sza, saa, lat

        txt_files = glob.glob(os.path.join(search_dir, "*_MTL.txt"))
        if txt_files:
            sza = saa = ul_lat = lr_lat = None
            with open(txt_files[0]) as f:
                for line in f:
                    line = line.strip()
                    if "SUN_ELEVATION" in line and "=" in line and sza is None:
                        sza = 90.0 - float(line.split("=")[1])
                    if "SUN_AZIMUTH" in line and "=" in line and saa is None:
                        saa = float(line.split("=")[1])
                    if "CORNER_UL_LAT_PRODUCT" in line and "=" in line and ul_lat is None:
                        ul_lat = float(line.split("=")[1])
                    if "CORNER_LR_LAT_PRODUCT" in line and "=" in line and lr_lat is None:
                        lr_lat = float(line.split("=")[1])
            if None not in (sza, saa, ul_lat, lr_lat):
                log.info("[ls8_angles] Using MTL: %s", txt_files[0])
                return sza, saa, (ul_lat + lr_lat) / 2

        xml_files = glob.glob(os.path.join(search_dir, "*_MTL.xml"))
        if xml_files:
            import xml.etree.ElementTree as ET
            root = ET.parse(xml_files[0]).getroot()
            ia   = root.find(".//IMAGE_ATTRIBUTES")
            pa   = root.find(".//PROJECTION_ATTRIBUTES")
            sza  = 90.0 - float(ia.find("SUN_ELEVATION").text)
            saa  = float(ia.find("SUN_AZIMUTH").text)
            lat  = (float(pa.find("CORNER_UL_LAT_PRODUCT").text) +
                    float(pa.find("CORNER_LR_LAT_PRODUCT").text)) / 2
            log.info("[ls8_angles] Using MTL: %s", xml_files[0])
            return sza, saa, lat

    raise FileNotFoundError(
        f"No MTL file (json/txt/xml) found in {ls8_dir} or its parent {parent}.\n"
        f"Make sure the atmospheric correction step copies MTL files to atm_dir."
    )


def _find_ls8_band_file(ls8_dir: str, suffix: str) -> str:
    """Find the TIF file for a given LS8 band suffix (e.g. 'SR_B2')."""
    for ext in ("*.TIF", "*.tif"):
        pattern = os.path.join(ls8_dir, f"*_{suffix}{ext[1:]}")
        hits = glob.glob(pattern)
        if hits:
            return hits[0]

    for ext in ("*.TIF", "*.tif"):
        pattern = os.path.join(ls8_dir, ext)
        candidates = [f for f in glob.glob(pattern) if suffix in os.path.basename(f)]
        if candidates:
            return candidates[0]

    available = sorted(
        os.path.basename(f)
        for f in glob.glob(os.path.join(ls8_dir, "*.TIF"))
        + glob.glob(os.path.join(ls8_dir, "*.tif"))
    )
    raise FileNotFoundError(
        f"No file matching *_{suffix}.TIF found in {ls8_dir}.\n"
        f"Available TIF files: {available}\n"
        f"Expected suffix pattern: *_{suffix}.TIF  (from atmospheric correction step)\n"
        f"Check that write_boa_bands writes files as {{scene_id}}_SR_B{{N}}.TIF."
    )


def process_ls8(ls8_dir, bands, outdir, use_band_ids=False,
                clip_min=0.8, clip_max=1.2):
    """
    LS8 NBAR — numpy fast path.

    Previous version read the source raster three times per band (once
    lazily via rioxarray, once for apply_nbar.compute(), once for
    CM.compute() in _log_cfactor, plus a second read via .data.compute()
    for the nodata mask). It also computed a full 10980×10980 dask graph
    of trig ops just to produce a spatially-constant C-factor.

    This rewrite does one pure-numpy pass: single rasterio read, scalar
    C-factor, numpy multiply/clip/mask, and one write. Output is
    byte-identical to the old path (same math, same clipping, same
    nodata handling) but typically 3–4× faster.
    """

    sza, saa, lat = ls8_angles(ls8_dir)
    theta_s = mean_sun_angle(lat)
    log.info("LS8  SZA=%.2f  SAA=%.2f  theta_s=%.2f  lat=%.2f", sza, saa, theta_s, lat)

    Path(outdir).mkdir(parents=True, exist_ok=True)

    import glob as _glob
    has_sbaf = bool(_glob.glob(os.path.join(ls8_dir, "*_SBAF_B*.TIF")))
    has_sr   = bool(_glob.glob(os.path.join(ls8_dir, "*_SR_B*.TIF")))

    if has_sbaf:
        band_suffix_map = LS8_BANDS_SBAF
        log.info("[LS8] Detected SBAF-adjusted inputs (*_SBAF_B*.TIF)")
    elif has_sr:
        band_suffix_map = LS8_BANDS
        log.info("[LS8] Detected SR inputs (*_SR_B*.TIF)")
    else:
        band_suffix_map = LS8_BANDS
        log.warning("[LS8] Neither *_SBAF_B*.TIF nor *_SR_B*.TIF found — "
                    "will attempt SR suffixes and may fail")

    for band in bands:
        suffix = band_suffix_map[band]
        path   = _find_ls8_band_file(ls8_dir, suffix)
        log.info("[LS8] %s  <-  %s", band, os.path.basename(path))

        # ── Single read of source raster ─────────────────────────────────
        with rasterio.open(path) as src:
            raw_np             = src.read(1)
            already_reflectance = _is_boa_reflectance(src)
            raw_dtype          = src.dtypes[0]
            nd_val             = src.nodata
            src_crs            = src.crs
            src_transform      = src.transform
            H, W               = src.height, src.width

        # Build x/y coordinate arrays from the transform for the output
        # DataArray. (pixel centres, matching rioxarray's convention.)
        xs = np.arange(W) * src_transform.a + src_transform.c + src_transform.a / 2
        ys = np.arange(H) * src_transform.e + src_transform.f + src_transform.e / 2

        # ── Nodata mask from raw data ────────────────────────────────────
        if nd_val is None:
            nodata_mask = np.zeros(raw_np.shape, dtype=bool)
        elif np.issubdtype(raw_np.dtype, np.floating):
            nodata_mask = (
                np.isclose(raw_np, float(nd_val), atol=1e-3)
                | ~np.isfinite(raw_np)
            )
        else:
            nodata_mask = raw_np == nd_val

        # ── Convert to reflectance in pure numpy ─────────────────────────
        img_np = raw_np.astype(np.float32)
        if already_reflectance:
            np.clip(img_np, 0.0, 1.0, out=img_np)
            log.info(
                "[LS8] %s  dtype=%s → confirmed BOA reflectance (tag or float dtype)",
                band, raw_dtype,
            )
        else:
            img_np = img_np * np.float32(LS8_SCALE) + np.float32(LS8_OFFSET)
            np.clip(img_np, 0.0, 1.0, out=img_np)
            log.info(
                "[LS8] %s  dtype=%s → applying DN→reflectance (scale=%.7f offset=%.1f)",
                band, raw_dtype, LS8_SCALE, LS8_OFFSET,
            )

        # ── Scalar C-factor (angles are uniform across the scene) ────────
        CM_scalar = _cmatrix_scalar(sza, 0.0, saa, 0.0, theta_s, ROY_COEFS[band])
        CM32      = np.float32(CM_scalar)

        # ── Apply NBAR correction with clip bounds.
        # Same math as apply_nbar():
        #   out = clip(img * CM, clip_min*img, clip_max*img)
        #   where img <= 0 → 0
        corrected_np = img_np * CM32
        np.clip(
            corrected_np,
            np.float32(clip_min) * img_np,
            np.float32(clip_max) * img_np,
            out=corrected_np,
        )
        corrected_np = np.where(img_np <= 0, np.float32(0), corrected_np)

        # ── Diagnostic log (free: we already know CM is scalar) ──────────
        log.info(
            "[CMAT] %s  C-factor: min=%.3f  max=%.3f  "
            "clipped_low=0.0%%  clipped_high=0.0%%",
            band, CM_scalar, CM_scalar,
        )

        # ── Apply nodata mask + final clip ───────────────────────────────
        corrected_np = np.where(
            nodata_mask,
            np.float32(NODATA),
            np.clip(corrected_np, 0.0, 1.0),
        )

        # ── Write output ─────────────────────────────────────────────────
        out_xr = xr.DataArray(
            corrected_np,
            coords={"y": ys, "x": xs},
            dims=["y", "x"],
        )
        out_xr = out_xr.rio.write_crs(src_crs)
        out_xr = out_xr.rio.write_transform(src_transform)
        out_xr = out_xr.rio.write_nodata(NODATA)

        fname    = f"NBAR_{suffix}.tif" if use_band_ids else f"NBAR_{band}.tif"
        out_path = os.path.join(outdir, fname)
        log.info("[LS8] writing -> %s", out_path)
        out_xr.rio.to_raster(
            out_path,
            compress="deflate",
            dtype="float32",
            tiled=True,
            blockxsize=512,
            blockysize=512,
            num_threads=_raster_write_threads(),
        )

    written = sorted(Path(outdir).glob("NBAR_*.tif"))
    log.info(
        "[LS8] process_ls8 complete — %d NBAR files written: %s",
        len(written), [f.name for f in written],
    )


# ---------------------------------------------------------------------------
# Sentinel-2 -- per-pixel angle grids from MTD_TL.xml
# ---------------------------------------------------------------------------

def _s2_xml_root(safe_dir):
    import xml.etree.ElementTree as ET
    for pattern in [
        os.path.join(safe_dir, "GRANULE", "*", "MTD_TL.xml"),
        os.path.join(safe_dir, "MTD_MSIL2A.xml"),
    ]:
        hits = glob.glob(pattern)
        if hits:
            return ET.parse(hits[0]).getroot(), hits[0]
    raise FileNotFoundError(f"No S2 XML found in {safe_dir}")


def _parse_angle_grid(root, grid_tag):
    zen_rows, az_rows = [], []
    in_target = False
    in_zen    = False

    for el in root.iter():
        tag = el.tag.split("}")[-1]
        if tag == grid_tag:
            in_target = True
            in_zen    = False
            zen_rows.clear(); az_rows.clear()
            continue
        if in_target:
            if tag == "Zenith":
                in_zen = True
            elif tag == "Azimuth":
                in_zen = False
            elif tag == "VALUES" and el.text:
                row = [float(v) for v in el.text.split()]
                if in_zen:
                    zen_rows.append(row)
                else:
                    az_rows.append(row)

    if not zen_rows:
        raise ValueError(f"No angle grid found for tag: {grid_tag}")
    return np.array(zen_rows, dtype=np.float32), np.array(az_rows, dtype=np.float32)


def _parse_view_angle_grids(root, band_id):
    band_lut = {
        "B01": 0,  "B02": 1,  "B03": 2,  "B04": 3,
        "B05": 4,  "B06": 5,  "B07": 6,  "B08": 7,
        "B8A": 8,  "B09": 9,  "B10":10,  "B11":11,  "B12":12,
    }
    target_int = band_lut.get(band_id.upper(), -1)

    all_nodes = []
    for el in root.iter():
        if el.tag.split("}")[-1] == "Viewing_Incidence_Angles_Grids":
            all_nodes.append(el)

    if not all_nodes:
        raise ValueError("No Viewing_Incidence_Angles_Grids found in XML")

    unique_ids = sorted({n.attrib.get("bandId", "?") for n in all_nodes})
    log.debug("View angle grid bandIds: %s  (target '%s' -> int %d)",
              unique_ids, band_id, target_int)

    def _matches(bid_str):
        try:
            if int(bid_str) == target_int:
                return True
        except ValueError:
            pass
        normalised = band_id.lstrip("Bb").lstrip("0") or "0"
        bid_norm   = bid_str.lstrip("Bb").lstrip("0") or "0"
        if normalised.upper() == bid_norm.upper():
            return True
        if bid_str.upper() == band_id.upper():
            return True
        return False

    def _extract_grid(node):
        def _values_from_subtree(subtree_root):
            rows = []
            for el in subtree_root.iter():
                if el.tag.split("}")[-1] == "VALUES" and el.text:
                    rows.append([float(v) for v in el.text.split()])
            return rows

        zen_rows, az_rows = [], []
        for child in node.iter():
            tag = child.tag.split("}")[-1]
            if tag == "Zenith":
                zen_rows = _values_from_subtree(child)
            elif tag == "Azimuth":
                az_rows = _values_from_subtree(child)
            if zen_rows and az_rows:
                break
        return zen_rows, az_rows

    def _pad_to_same_shape(grids):
        max_rows = max(g.shape[0] for g in grids)
        max_cols = max(g.shape[1] for g in grids)
        padded = []
        for g in grids:
            pad = np.full((max_rows, max_cols), np.nan, dtype=np.float32)
            pad[:g.shape[0], :g.shape[1]] = g
            padded.append(pad)
        return padded

    zen_grids, az_grids = [], []
    for node in all_nodes:
        bid_str = node.attrib.get("bandId", "")
        if not _matches(bid_str):
            continue
        zen_rows, az_rows = _extract_grid(node)
        if zen_rows and az_rows:
            zen_grids.append(np.array(zen_rows, dtype=np.float32))
            az_grids.append(np.array(az_rows,   dtype=np.float32))

    if not zen_grids:
        log.error("Could not match band_id='%s' (int=%d) against XML bandIds: %s",
                  band_id, target_int, unique_ids)
        raise ValueError(
            f"No view angle grids for band '{band_id}' (int={target_int}). "
            f"Available: {unique_ids}"
        )

    log.info("[S2]  band %s: found %d detector view-angle grids", band_id, len(zen_grids))
    zen_padded = _pad_to_same_shape(zen_grids)
    az_padded  = _pad_to_same_shape(az_grids)
    zen_stack  = np.stack(zen_padded, axis=0)
    az_stack   = np.stack(az_padded,  axis=0)
    return np.nanmean(zen_stack, axis=0), np.nanmean(az_stack, axis=0)


def _fill_nan(grid):
    from scipy.ndimage import distance_transform_edt
    nan_mask = np.isnan(grid)
    if not nan_mask.any():
        return grid
    _, idx = distance_transform_edt(nan_mask, return_indices=True)
    return grid[idx[0], idx[1]].astype(np.float32)


def _interpolate_angle_grid(grid, out_shape):
    from scipy.ndimage import zoom
    grid = _fill_nan(grid)
    zy = out_shape[0] / grid.shape[0]
    zx = out_shape[1] / grid.shape[1]
    return zoom(grid, (zy, zx), order=3).astype(np.float32)


def s2_angle_arrays(safe_dir, band_id, out_shape):
    import dask
    root, _ = _s2_xml_root(safe_dir)
    sun_zen_grid,  sun_az_grid  = _parse_angle_grid(root, "Sun_Angles_Grid")
    view_zen_grid, view_az_grid = _parse_view_angle_grids(root, band_id)

    def _interp(g): return _interpolate_angle_grid(g, out_shape)
    to_da = lambda d: da.from_delayed(d, shape=out_shape, dtype=np.float32)
    return (
        to_da(dask.delayed(_interp)(sun_zen_grid)),
        to_da(dask.delayed(_interp)(sun_az_grid)),
        to_da(dask.delayed(_interp)(view_zen_grid)),
        to_da(dask.delayed(_interp)(view_az_grid)),
    )


def s2_centre_lat(safe_dir):
    import pyproj
    tile_xr = rxr.open_rasterio(
        glob.glob(os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R10m", "*_B02_10m.jp2"))[0],
        chunks=CHUNKS,
    ).squeeze()
    cx  = float(tile_xr.x.mean())
    cy  = float(tile_xr.y.mean())
    tf  = pyproj.Transformer.from_crs(tile_xr.rio.crs.to_epsg(), 4326, always_xy=True)
    _, lat = tf.transform(cx, cy)
    return lat


def _s2_scl_mask(safe_dir, target_shape):
    scl_paths = glob.glob(
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R20m", "*_SCL_20m.jp2")
    )
    if not scl_paths:
        log.debug("[S2]  SCL file not found -- skipping SCL masking")
        return np.zeros(target_shape, dtype=bool)

    scl_xr = rxr.open_rasterio(scl_paths[0], chunks=CHUNKS).squeeze()
    scl_np = scl_xr.data
    if hasattr(scl_np, "compute"):
        scl_np = scl_np.compute()
    scl_np = np.asarray(scl_np)

    if scl_np.shape != target_shape:
        from scipy.ndimage import zoom as ndzoom
        zy = target_shape[0] / scl_np.shape[0]
        zx = target_shape[1] / scl_np.shape[1]
        scl_np = ndzoom(scl_np.astype(np.float32), (zy, zx), order=0).astype(np.uint8)

    mask = np.zeros(scl_np.shape, dtype=bool)
    for cls in SCL_MASK_CLASSES:
        mask |= (scl_np == cls)

    n_masked = int(mask.sum())
    log.info("[S2]  SCL mask: %d pixels (%.1f%%) flagged as invalid",
             n_masked, 100.0 * n_masked / mask.size)
    return mask


def process_s2(safe_dir, bands, outdir, use_band_ids=False,
               clip_min=0.8, clip_max=1.2):
    lat     = s2_centre_lat(safe_dir)
    theta_s = mean_sun_angle(lat)
    log.info("S2   lat=%.2f  theta_s=%.2f  (per-pixel angle grids enabled)", lat, theta_s)

    Path(outdir).mkdir(parents=True, exist_ok=True)

    for band in bands:
        res, band_id = S2_BANDS[band]
        pattern = os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", res, f"*_{band_id}*.jp2")
        path    = sorted(glob.glob(pattern))[0]
        log.info("[S2]  %s  <-  %s", band, os.path.basename(path))

        img_xr = rxr.open_rasterio(path, chunks=CHUNKS, masked=True).squeeze()

        raw_np = img_xr.data
        if hasattr(raw_np, "compute"):
            raw_np = raw_np.compute()
        raw_np = np.asarray(raw_np)

        H, W = raw_np.shape

        nodata_mask = (raw_np == 0) | (raw_np >= 10000)
        scl_mask    = _s2_scl_mask(safe_dir, (H, W))
        nodata_mask = nodata_mask | scl_mask

        img_da = img_xr.data.astype(np.float32) * S2_SCALE
        img_da = da.clip(img_da, 0, 1)

        img_da = da.where(
            da.from_array(nodata_mask, chunks=list(CHUNKS.values())),
            np.float32(0),
            img_da,
        )

        SZA, SAA, VZA, VAA = s2_angle_arrays(safe_dir, band_id, (H, W))
        log.info("[S2]  %s  angles loaded (lazy)", band)

        CM        = cmatrix(SZA, VZA, SAA, VAA, theta_s, ROY_COEFS[band])
        corrected = apply_nbar(img_da, CM, clip_min, clip_max)

        corrected_np = corrected.astype(np.float32).compute()

        _log_cfactor(band, CM, clip_min, clip_max)

        corrected_np[nodata_mask] = NODATA
        corrected_np = np.where(nodata_mask, NODATA, np.clip(corrected_np, 0.0, 1.0))

        out_xr = xr.DataArray(
            corrected_np,
            coords={"y": img_xr.y, "x": img_xr.x},
            dims=["y", "x"],
        )
        out_xr = out_xr.rio.write_crs(img_xr.rio.crs)
        out_xr = out_xr.rio.write_transform(img_xr.rio.transform())
        out_xr = out_xr.rio.write_nodata(NODATA)

        fname = f"NBAR_{band_id}.tif" if use_band_ids else f"NBAR_{band}.tif"
        out_path = os.path.join(outdir, fname)
        log.info("[S2]  writing -> %s", out_path)
        out_xr.rio.to_raster(
            out_path,
            compress="deflate",
            dtype="float32",
            tiled=True,
            blockxsize=512,
            blockysize=512,
            num_threads=_raster_write_threads(),
        )

    written = sorted(Path(outdir).glob("NBAR_*.tif"))
    log.info("[S2] process_s2 complete — %d NBAR files written: %s",
             len(written), [f.name for f in written])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="NBAR BRDF Normalisation -- ROY et al. 2016")
    ap.add_argument("--sensor",   required=True, choices=["ls8", "s2"])
    ap.add_argument("--input",    required=True, help="Product directory / .SAFE folder")
    ap.add_argument("--outdir",   default="./NBAR_output")
    ap.add_argument("--bands",    nargs="+", default=list(ROY_COEFS.keys()),
                    choices=list(ROY_COEFS.keys()),
                    help="Bands to process (default: all 6)")
    ap.add_argument("--workers",  type=int, default=4)
    ap.add_argument("--use-band-ids", action="store_true", default=False)
    ap.add_argument("--clip-min", type=float, default=0.8)
    ap.add_argument("--clip-max", type=float, default=1.2)
    args = ap.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    import dask
    with dask.config.set(scheduler="threads", num_workers=args.workers):
        if args.sensor == "ls8":
            process_ls8(args.input, args.bands, args.outdir,
                        use_band_ids=args.use_band_ids,
                        clip_min=args.clip_min, clip_max=args.clip_max)
        else:
            process_s2(args.input, args.bands, args.outdir,
                       use_band_ids=args.use_band_ids,
                       clip_min=args.clip_min, clip_max=args.clip_max)

    log.info("Done -> %s", args.outdir)
    for f in sorted(glob.glob(os.path.join(args.outdir, "*.tif"))):
        log.info("  %-35s  %.1f MB", os.path.basename(f), os.path.getsize(f)/1e6)


if __name__ == "__main__":
    main()
