#!/usr/bin/env python3

import os
import json
import glob
import shutil
import warnings
import numpy as np
import rasterio
import dask.array as da
from dask.diagnostics import ProgressBar
from itertools import product
from pathlib import Path
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────
# BAND CONFIG
# ─────────────────────────────────────────────────────────────────────
LANDSAT8_BANDS = {
    "B1": {"wl": 0.443, "thermal": False, "correct": True},
    "B2": {"wl": 0.482, "thermal": False, "correct": True},
    "B3": {"wl": 0.561, "thermal": False, "correct": True},
    "B4": {"wl": 0.655, "thermal": False, "correct": True},
    "B5": {"wl": 0.865, "thermal": False, "correct": True},
    "B6": {"wl": 1.609, "thermal": False, "correct": True},
    "B7": {"wl": 2.201, "thermal": False, "correct": True},
}

# Maps band id (B1..B7) → SR suffix used by BRDF step
# BRDF expects files named *_SR_B2.TIF, *_SR_B3.TIF, etc.
BAND_TO_SR_SUFFIX = {
    "B1": "SR_B1", "B2": "SR_B2", "B3": "SR_B3",
    "B4": "SR_B4", "B5": "SR_B5", "B6": "SR_B6", "B7": "SR_B7",
}

LUT_GRID = {
    "aod550":       [0.05, 0.10, 0.15, 0.20, 0.30, 0.50],
    "wv":           [1.0, 2.0, 3.0, 4.0],
    "o3":           [0.25, 0.35, 0.45],
    "solar_zenith": [10, 20, 30, 40, 50, 60],
}

AOD550     = 0.15
WV         = 2.5
O3         = 0.35
CHUNK_ROWS = 512

# ─────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────
def mkdir(d):
    os.makedirs(d, exist_ok=True)
    return d


# ─────────────────────────────────────────────────────────────────────
# 1. LOAD BANDS
# ─────────────────────────────────────────────────────────────────────
def load_l8_l1(input_dir, scene_id):
    input_dir = str(input_dir)
    bands, profile = {}, None

    for fname in sorted(os.listdir(input_dir)):
        if not fname.upper().endswith((".TIF", ".TIFF")):
            continue
        # Skip panchromatic (B8) and thermal (B10, B11)
        if "_B8" in fname or "_B10" in fname or "_B11" in fname:
            continue
        # Match *_B1.TIF ... *_B7.TIF  (but not *_SR_B*.TIF)
        if "_SR_" in fname:
            continue
        if "_B" not in fname:
            continue

        # Extract band id: last _BX or _BXX before .TIF
        stem = fname.upper().replace(".TIFF", "").replace(".TIF", "")
        parts = stem.rsplit("_B", 1)
        if len(parts) != 2:
            continue
        bid = "B" + parts[1]
        if bid not in LANDSAT8_BANDS:
            continue

        fpath = os.path.join(input_dir, fname)
        with rasterio.open(fpath) as src:
            arr = src.read(1).astype(np.float32)
            if profile is None:
                profile = src.profile.copy()
        bands[bid] = arr
        print(f"  ✓ L8 band {fname}  mean={arr.mean():.1f}")

    if not bands:
        raise FileNotFoundError(
            f"No Landsat band TIFs found in {input_dir}\n"
            f"Expected files like *_B2.TIF ... *_B7.TIF"
        )

    names  = [b for b in LANDSAT8_BANDS if b in bands]
    cube   = np.stack([bands[b] for b in names], axis=0)
    profile.update(count=len(names), dtype="float32", compress="lzw")
    return cube, names, profile


# ─────────────────────────────────────────────────────────────────────
# 2. READ MTL
# ─────────────────────────────────────────────────────────────────────
def read_l8_mtl(input_dir, scene_id):
    input_dir = str(input_dir)

    # Try _MTL.txt first (has reflectance coefficients)
    mtl_files = glob.glob(os.path.join(input_dir, "*_MTL.txt"))
    if not mtl_files:
        mtl_files = glob.glob(os.path.join(input_dir, "*MTL*.txt"))

    if not mtl_files:
        raise FileNotFoundError(
            f"No MTL.txt found in {input_dir}\n"
            f"Make sure pipelineGRI.py copied the MTL files to the geo output directory."
        )


    coeffs = {}
    seen_mult = set()
    seen_add  = set()
    with open(mtl_files[0]) as f:
        for line in f:
            line = line.strip()
            # Detect processing level — first occurrence wins
            if "PROCESSING_LEVEL" in line and "=" in line and "processing_level" not in coeffs:
                val = line.split("=")[1].strip().strip('"')
                coeffs["processing_level"] = val
            for bid in ["1", "2", "3", "4", "5", "6", "7"]:
                bkey = f"B{bid}"
                if bkey not in coeffs:
                    coeffs[bkey] = {}
                mult_key = f"REFLECTANCE_MULT_BAND_{bid}"
                add_key  = f"REFLECTANCE_ADD_BAND_{bid}"
                if mult_key in line and "=" in line and mult_key not in seen_mult:
                    coeffs[bkey]["refl_mult"] = float(line.split("=")[1])
                    seen_mult.add(mult_key)
                if add_key in line and "=" in line and add_key not in seen_add:
                    coeffs[bkey]["refl_add"] = float(line.split("=")[1])
                    seen_add.add(add_key)
            if "SUN_ELEVATION" in line and "=" in line and "sun_elevation" not in coeffs:
                try:
                    coeffs["sun_elevation"] = float(line.split("=")[1])
                except Exception:
                    pass
    return coeffs


# ─────────────────────────────────────────────────────────────────────
# 3. DN → TOA
# ─────────────────────────────────────────────────────────────────────
def l8_dn_to_toa(dn_cube, names, meta):
    toa  = np.zeros_like(dn_cube)
    elev = meta.get("sun_elevation", 60.0)
    sz   = 90.0 - elev
    level = meta.get("processing_level", "L2SP")

    is_l1 = "L1" in str(level).upper()
    print(f"  L8 processing_level={level}  sun_elevation={elev:.2f}°  sun_zenith={sz:.2f}°")
    if is_l1:
        print(f"  L1TP mode: applying TOA formula with sin(sun_elevation) division")
    else:
        print(f"  L2SP mode: applying surface reflectance formula (no sun-angle division)")

    for i, b in enumerate(names):
        c  = meta.get(b, {})
        mr = c.get("refl_mult", 2e-5  if is_l1 else 2.75e-5)
        ar = c.get("refl_add",  -0.1  if is_l1 else -0.2)
        dn = dn_cube[i]
        if is_l1:
            r = (mr * dn + ar) / np.sin(np.radians(elev))
        else:
            r = mr * dn + ar
        r[dn == 0] = 0
        toa[i] = r
    return toa, sz


# ─────────────────────────────────────────────────────────────────────
# 4. LUT
# ─────────────────────────────────────────────────────────────────────
def build_or_load_lut(lut_path, band_config, sensor_label):
    if os.path.exists(lut_path):
        print(f"  ✓ LUT existante : {lut_path}")
        with open(lut_path) as f:
            return json.load(f)

    try:
        from Py6S import SixS, Geometry, AtmosProfile, AeroProfile, Wavelength, Altitudes
    except ImportError:
        raise ImportError(
            "Py6S not installed. Activate the sixs environment:\n"
            "  conda activate sixs_env"
        )

    optical = [(b, v["wl"]) for b, v in band_config.items() if v.get("correct")]
    combos  = list(product(*LUT_GRID.values()))
    keys    = list(LUT_GRID.keys())
    total   = len(combos) * len(optical)
    print(f"  Building LUT {sensor_label}: {len(combos)}×{len(optical)}={total} 6S calls")

    lut, done = {}, 0
    for combo in combos:
        params = dict(zip(keys, combo))
        ckey   = json.dumps(params)
        lut[ckey] = {}
        s = SixS()
        s.geometry = Geometry.User()
        s.geometry.solar_z = params["solar_zenith"]
        s.geometry.solar_a = 150.0
        s.geometry.view_z  = 0.0
        s.geometry.view_a  = 0.0
        s.atmos_profile = AtmosProfile.UserWaterAndOzone(params["wv"], params["o3"])
        s.aero_profile  = AeroProfile.Continental
        s.aot550        = params["aod550"]
        s.altitudes = Altitudes()
        s.altitudes.set_target_custom_altitude(0)
        s.altitudes.set_sensor_satellite_level()
        for bname, wl in optical:
            from Py6S import Wavelength as WL
            s.wavelength = WL(wl)
            s.run()
            rp  = s.outputs.atmospheric_intrinsic_reflectance or 0.0
            td  = s.outputs.transmittance_global_gas.downward  or 1.0
            tu  = s.outputs.transmittance_global_gas.upward    or 1.0
            ts  = s.outputs.transmittance_total_scattering.total or 1.0
            den = td * tu * ts if td * tu * ts > 1e-6 else 1.0
            lut[ckey][bname] = {"rho_path": rp, "denom": den}
            done += 1
            if done % 100 == 0:
                print(f"    {done}/{total}...")

    mkdir(os.path.dirname(lut_path))
    with open(lut_path, "w") as f:
        json.dump(lut, f)
    print(f"  ✓ LUT saved: {lut_path}")
    return lut


def interpolate_lut(lut, aod550, wv, o3, solar_zenith, band_name):
    def bounds(vals, t):
        vals = sorted(vals)
        for i in range(len(vals) - 1):
            if vals[i] <= t <= vals[i + 1]:
                return vals[i], vals[i + 1]
        return (vals[0], vals[0]) if t < vals[0] else (vals[-1], vals[-1])

    def w(lo, hi, v):
        return 0.0 if hi == lo else (v - lo) / (hi - lo)

    alo, ahi = bounds(LUT_GRID["aod550"],       aod550)
    wlo, whi = bounds(LUT_GRID["wv"],           wv)
    olo, ohi = bounds(LUT_GRID["o3"],           o3)
    slo, shi = bounds(LUT_GRID["solar_zenith"], solar_zenith)
    wa, ww   = w(alo, ahi, aod550), w(wlo, whi, wv)
    wo, ws   = w(olo, ohi, o3),     w(slo, shi, solar_zenith)

    rho = den = 0.0
    for (a, wa_), (wv_, ww_), (o, wo_), (s, ws_) in product(
        [(alo, 1 - wa), (ahi, wa)], [(wlo, 1 - ww), (whi, ww)],
        [(olo, 1 - wo), (ohi, wo)], [(slo, 1 - ws), (shi, ws)]
    ):
        key  = json.dumps({"aod550": a, "wv": wv_, "o3": o, "solar_zenith": s})
        coef = lut.get(key, {}).get(band_name, {"rho_path": 0.0, "denom": 1.0})
        wi   = wa_ * ww_ * wo_ * ws_
        rho += wi * coef["rho_path"]
        den += wi * coef["denom"]
    return {"rho_path": rho, "denom": den}


# ─────────────────────────────────────────────────────────────────────
# 5. DASK ATMOSPHERIC CORRECTION
# ─────────────────────────────────────────────────────────────────────
def correct_block(block, rho_path, denom):
    r = (block - rho_path) / denom
    r[block == 0] = 0
    return r.astype(np.float32)


def apply_6s_correction(toa_cube, names, band_config, lut, solar_zenith,
                         toa_path=None, output_dir=None, scene_id=None, spark=None):

    # ── Pre-compute per-band coefficients on the driver ──────────────────
    band_jobs = []
    for i, b in enumerate(names):
        if not band_config[b].get("correct", False):
            band_jobs.append((i, b, None, None))
            continue
        c  = interpolate_lut(lut, AOD550, WV, O3, solar_zenith, b)
        rp = float(c["rho_path"])
        dn = float(c["denom"])
        print(f"    {b}: rho_path={rp:.5f}  denom={dn:.5f}")
        band_jobs.append((i, b, rp, dn))

    # ── Sequential fallback ───────────────────────────────────────────────
    if spark is None or toa_path is None or output_dir is None:
        boa = np.empty_like(toa_cube)
        for i, b, rp, dn in band_jobs:
            band = toa_cube[i].copy()
            if rp is None:
                boa[i] = band
            else:
                r = (band - np.float32(rp)) / np.float32(dn)
                r[band == 0] = 0
                boa[i] = r.astype(np.float32)
        return boa

    # ── Spark path: write-to-disk pattern ────────────────────────────────
    sc = spark.sparkContext

    # Broadcast only small scalars/strings — never arrays
    bc_toa_path  = sc.broadcast(str(toa_path))
    bc_output_dir = sc.broadcast(str(output_dir))

    def _correct_and_write(job):
        idx, bname, rho_path, denom = job
        import numpy as _np
        import rasterio as _rio
        from pathlib import Path as _P

        toa_p  = bc_toa_path.value
        out_d  = _P(bc_output_dir.value)

        with _rio.open(toa_p) as src:
            band    = src.read(idx + 1).astype(_np.float32)   # 1-indexed
            profile = src.profile.copy()

        if rho_path is not None:
            r = (band - _np.float32(rho_path)) / _np.float32(denom)
            r[band == 0] = 0
            band = r.astype(_np.float32)

        # Write corrected band directly to disk — nothing large returned
        out_path = out_d / f"_boa_band_{idx}_{bname}.tif"
        band_profile = profile.copy()
        band_profile.update(count=1, dtype="float32")
        with _rio.open(out_path, "w", **band_profile) as dst:
            dst.write(band, 1)

        return (idx, bname, str(out_path))   # tiny strings only

    print("  Computing BOA (Spark, write-to-disk)...")
    job_results = (
        sc.parallelize(band_jobs, numSlices=len(band_jobs))
          .map(_correct_and_write)
          .collect()                          # collects only strings
    )

    # ── Driver: reassemble cube from the per-band files ──────────────────
    boa = np.empty_like(toa_cube)
    for idx, bname, out_path_str in job_results:
        with rasterio.open(out_path_str) as src:
            boa[idx] = src.read(1)
        os.remove(out_path_str)              # clean up temp file

    for bc in [bc_toa_path, bc_output_dir]:
        bc.unpersist()

    return boa


# ─────────────────────────────────────────────────────────────────────
# 6. WRITE BOA PER-BAND TIFs
# ─────────────────────────────────────────────────────────────────────
def write_boa_bands(boa_cube, names, profile, output_dir, scene_id, input_dir,
                    nodata: float = -9999.0):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    band_profile = profile.copy()
    band_profile.update(
        count=1,
        dtype="float32",
        compress="lzw",
        nodata=nodata,
    )

    for i, band_id in enumerate(names):
        band = boa_cube[i].copy()

        src_nodata = profile.get("nodata")
        if src_nodata is not None:
            fill_mask = band == float(src_nodata)
        else:
            fill_mask = ~np.isfinite(band)

        band[fill_mask] = np.float32(nodata)

        sr_suffix = BAND_TO_SR_SUFFIX.get(band_id, f"SR_{band_id}")
        out_path  = output_dir / f"{scene_id}_{sr_suffix}.TIF"

        with rasterio.open(out_path, "w", **band_profile) as dst:
            dst.write(band, 1)
            dst.update_tags(band_id=band_id)
            # Tag the output so downstream steps (nbar.py) can confirm
            dst.update_tags(data_type="boa_reflectance")

        valid = band[band != np.float32(nodata)]
        mean_str = f"{valid.mean():.4f}" if valid.size else "N/A"
        print(f"  ✓ BOA {out_path.name}  mean={mean_str}")

    for mtl_file in Path(input_dir).glob("*_MTL*"):
        dest = output_dir / mtl_file.name
        shutil.copy2(mtl_file, dest)
        print(f"  ✓ MTL copied: {mtl_file.name}")

    return output_dir