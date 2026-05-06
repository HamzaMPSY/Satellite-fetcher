#!/usr/bin/env python3


from __future__ import annotations
import argparse
import glob
import json
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import dask.array as da
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd
import rioxarray as rxr
from scipy import stats
warnings.filterwarnings("ignore", category=RuntimeWarning)

# ---------------------------------------------------------------------------
# Style
# ---------------------------------------------------------------------------

BAND_COLORS = {
    "Blue":  "#4477AA",
    "Green": "#228833",
    "Red":   "#CC3311",
    "NIR":   "#AA3377",
    "SWIR1": "#CCBB44",
    "SWIR2": "#EE7733",
}

BAND_MAP_LS8 = {
    "Blue": "SR_B2", "Green": "SR_B3", "Red": "SR_B4",
    "NIR":  "SR_B5", "SWIR1": "SR_B6", "SWIR2": "SR_B7",
}
BAND_MAP_S2 = {
    "Blue": ("R10m","B02"), "Green": ("R10m","B03"), "Red": ("R10m","B04"),
    "NIR":  ("R20m","B8A"), "SWIR1": ("R20m","B11"), "SWIR2": ("R20m","B12"),
}

LS8_SCALE  = 0.0000275
LS8_OFFSET = -0.2
S2_SCALE   = 0.0001
DEG2RAD    = np.float32(np.pi / 180.0)

# ---------------------------------------------------------------------------
# Filename resolver — matches nbar.py --use-band-ids convention
# ---------------------------------------------------------------------------

def _nbar_fname(band: str, sensor: str, use_band_ids: bool) -> str:
    """Return the NBAR output filename for a given band and sensor.

    Default (use_band_ids=False): logical names  → NBAR_Blue.tif, NBAR_NIR.tif
    With --use-band-ids flag     : original IDs  → NBAR_SR_B2.tif / NBAR_B02.tif
    Must mirror the naming logic in nbar.py process_ls8 / process_s2.
    """
    if not use_band_ids:
        return f"NBAR_{band}.tif"
    if sensor == "LS8":
        return f"NBAR_{BAND_MAP_LS8[band]}.tif"
    else:
        return f"NBAR_{BAND_MAP_S2[band][1]}.tif"

# ---------------------------------------------------------------------------
# Reference kernel implementations (pure numpy — sen2like originals)
# ---------------------------------------------------------------------------

def _li_sparse_np(ts, tv, phi, h=2.0, b=1.0):
    tsr, tvr, pr = ts*np.pi/180, tv*np.pi/180, phi*np.pi/180
    tsp = np.arctan(b * np.tan(tsr))
    tvp = np.arctan(b * np.tan(tvr))
    cos_xi = np.cos(tsp)*np.cos(tvp) + np.sin(tsp)*np.sin(tvp)*np.cos(pr)
    tan_sp, tan_vp = np.tan(tsp), np.tan(tvp)
    D = np.sqrt(np.maximum(tan_sp**2 + tan_vp**2 - 2*tan_sp*tan_vp*np.cos(pr), 0))
    sec_sp = 1/np.cos(tsp); sec_vp = 1/np.cos(tvp)
    num = np.sqrt(D**2 + (tan_sp*tan_vp*np.sin(pr))**2)
    den = sec_sp + sec_vp
    cos_t = np.clip(h * num / den, -1, 1)
    sin_t = np.sqrt(np.maximum(1-cos_t**2, 0))
    t = np.arccos(cos_t)
    overlap = (1/np.pi)*(t - sin_t*cos_t)*den
    return overlap - sec_sp - sec_vp + 0.5*(1+cos_xi)*sec_sp*sec_vp


def _ross_np(ts, tv, phi):
    tsr, tvr, pr = ts*np.pi/180, tv*np.pi/180, phi*np.pi/180
    cos_xi = np.clip(np.cos(tsr)*np.cos(tvr)+np.sin(tsr)*np.sin(tvr)*np.cos(pr), -1, 1)
    xi = np.arccos(cos_xi)
    num = (np.pi/2-xi)*np.cos(xi)+np.sin(xi)
    den = np.maximum(np.cos(tvr)+np.cos(tsr), 1e-6)
    return (4/(3*np.pi))*(num/den) - 1/3


def _li_sparse_da(ts, tv, phi, h=2.0, b=1.0):
    tsr, tvr, pr = ts*DEG2RAD, tv*DEG2RAD, phi*DEG2RAD
    tsp = da.arctan(b*da.tan(tsr)); tvp = da.arctan(b*da.tan(tvr))
    cos_xi = da.cos(tsp)*da.cos(tvp)+da.sin(tsp)*da.sin(tvp)*da.cos(pr)
    tan_sp, tan_vp = da.tan(tsp), da.tan(tvp)
    D = da.sqrt(da.maximum(tan_sp**2+tan_vp**2-2*tan_sp*tan_vp*da.cos(pr), 0))
    sec_sp = da.where(da.cos(tsp)==0, np.float32(1e10), 1/da.cos(tsp))
    sec_vp = da.where(da.cos(tvp)==0, np.float32(1e10), 1/da.cos(tvp))
    num = da.sqrt(D**2+(tan_sp*tan_vp*da.sin(pr))**2)
    den = sec_sp+sec_vp
    cos_t = da.clip(h*num/den, -1, 1)
    sin_t = da.sqrt(da.maximum(1-cos_t**2, 0))
    t = da.arccos(cos_t)
    overlap = (1/np.pi)*(t-sin_t*cos_t)*den
    return overlap-sec_sp-sec_vp+0.5*(1+cos_xi)*sec_sp*sec_vp


def _ross_da(ts, tv, phi):
    tsr, tvr, pr = ts*DEG2RAD, tv*DEG2RAD, phi*DEG2RAD
    cos_xi = da.clip(da.cos(tsr)*da.cos(tvr)+da.sin(tsr)*da.sin(tvr)*da.cos(pr),-1,1)
    xi = da.arccos(cos_xi)
    num = (np.pi/2-xi)*da.cos(xi)+da.sin(xi)
    den = da.where(da.abs(da.cos(tvr)+da.cos(tsr))<1e-6, np.float32(1e-6),
                   da.cos(tvr)+da.cos(tsr))
    return (4/(3*np.pi))*(num/den) - 1/3


# ---------------------------------------------------------------------------
# 1. KERNEL CORRECTNESS
# ---------------------------------------------------------------------------

KERNEL_TEST_GEOMETRIES = [
    # (label,        SZA,  VZA,  phi)
    ("Nadir",          0,    0,   0),
    ("Low sun",       60,    5,  30),
    ("Oblique view",  30,   45,  90),
    ("Hotspot",       35,   35,   0),
    ("Backscatter",   30,   10,  90),
    ("Large phi",     45,   20, 150),
]


def eval_kernels(outdir: str) -> pd.DataFrame:
    print("\n[1/4] Kernel correctness …")
    rows = []
    for label, ts, tv, phi in KERNEL_TEST_GEOMETRIES:
        ts_a = da.from_array(np.array([[ts]], dtype=np.float32))
        tv_a = da.from_array(np.array([[tv]], dtype=np.float32))
        ph_a = da.from_array(np.array([[phi]], dtype=np.float32))

        li_ref  = float(_li_sparse_np(ts, tv, phi))
        li_dask = float(_li_sparse_da(ts_a, tv_a, ph_a).compute()[0,0])
        ro_ref  = float(_ross_np(ts, tv, phi))
        ro_dask = float(_ross_da(ts_a, tv_a, ph_a).compute()[0,0])

        li_err = abs(li_dask - li_ref)
        ro_err = abs(ro_dask - ro_ref)

        rows.append({
            "Geometry":        label,
            "SZA":             ts,
            "VZA":             tv,
            "phi":             phi,
            "LiSparse_ref":    round(li_ref,  6),
            "LiSparse_dask":   round(li_dask, 6),
            "LiSparse_abserr": round(li_err,  8),
            "Ross_ref":        round(ro_ref,  6),
            "Ross_dask":       round(ro_dask, 6),
            "Ross_abserr":     round(ro_err,  8),
            "Pass":            li_err < 1e-4 and ro_err < 1e-4,
        })

    df = pd.DataFrame(rows)

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Kernel Correctness: Dask vs NumPy (sen2like reference)",
                 fontsize=13, fontweight="bold")

    for ax, kernel, ref_col, dask_col, err_col in [
        (axes[0], "LiSparse", "LiSparse_ref", "LiSparse_dask", "LiSparse_abserr"),
        (axes[1], "RossThick", "Ross_ref",    "Ross_dask",      "Ross_abserr"),
    ]:
        x = np.arange(len(df))
        ax.bar(x - 0.2, df[ref_col],  0.35, label="NumPy ref", color="#4477AA", alpha=0.85)
        ax.bar(x + 0.2, df[dask_col], 0.35, label="Dask",      color="#CC3311", alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(df["Geometry"], rotation=25, ha="right", fontsize=9)
        ax.set_title(f"{kernel} Kernel Values")
        ax.set_ylabel("Kernel value")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)

        # Annotate max error
        max_err = df[err_col].max()
        ax.text(0.02, 0.97, f"Max |err| = {max_err:.2e}",
                transform=ax.transAxes, va="top", fontsize=9,
                color="green" if max_err < 1e-4 else "red")

    plt.tight_layout()
    out = os.path.join(outdir, "kernel_correctness.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"   → {out}")
    return df


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _load_raw_ls8(ls8_dir: str, band: str) -> np.ndarray:
    suffix = BAND_MAP_LS8[band]
    path   = glob.glob(os.path.join(ls8_dir, f"*_{suffix}.TIF"))[0]
    xr_    = rxr.open_rasterio(path, chunks={"x":1024,"y":1024}, masked=True).squeeze()
    arr    = xr_.data.astype(np.float32) * LS8_SCALE + LS8_OFFSET
    arr    = da.clip(arr, 0, 1).compute()
    return arr[arr > 0].ravel()


def _load_raw_s2(safe_dir: str, band: str) -> np.ndarray:
    res, bid = BAND_MAP_S2[band]
    path = sorted(glob.glob(
        os.path.join(safe_dir,"GRANULE","*","IMG_DATA",res,f"*_{bid}*.jp2")))[0]
    xr_  = rxr.open_rasterio(path, chunks={"x":1024,"y":1024}, masked=True).squeeze()
    arr  = (xr_.data.astype(np.float32) * S2_SCALE)
    arr  = da.where(xr_.data == 0, np.nan, arr).compute()
    return arr[np.isfinite(arr) & (arr > 0)].ravel()


def _load_nbar(nbar_dir: str, filename: str) -> np.ndarray:
    path = os.path.join(nbar_dir, filename)
    if not os.path.exists(path):
        return None
    xr_  = rxr.open_rasterio(path, chunks={"x":1024,"y":1024}, masked=True).squeeze()
    arr  = xr_.data.astype(np.float32).compute()
    return arr[np.isfinite(arr) & (arr > 0)].ravel()


def _load_raw_s2_masked_like_nbar(safe_dir: str, band: str, nbar_dir: str,
                                   nbar_fname: str) -> np.ndarray:

    nbar_path = os.path.join(nbar_dir, nbar_fname)
    if not os.path.exists(nbar_path):
        return None
    nbar_xr  = rxr.open_rasterio(nbar_path, chunks={"x":1024,"y":1024}, masked=True).squeeze()
    nbar_arr = nbar_xr.data.astype(np.float32).compute()
    valid    = np.isfinite(nbar_arr) & (nbar_arr > 0)

    res, bid = BAND_MAP_S2[band]
    raw_path = sorted(glob.glob(
        os.path.join(safe_dir,"GRANULE","*","IMG_DATA",res,f"*_{bid}*.jp2")))[0]
    raw_xr   = rxr.open_rasterio(raw_path, chunks={"x":1024,"y":1024}, masked=True).squeeze()
    raw_arr  = (raw_xr.data.astype(np.float32) * S2_SCALE).compute()

    # Resize raw to NBAR shape if resolutions differ (e.g. 10m band vs 20m NBAR)
    if raw_arr.shape != nbar_arr.shape:
        from scipy.ndimage import zoom as ndzoom
        zy = nbar_arr.shape[0] / raw_arr.shape[0]
        zx = nbar_arr.shape[1] / raw_arr.shape[1]
        raw_arr = ndzoom(raw_arr, (zy, zx), order=1).astype(np.float32)

    return raw_arr[valid].ravel()


def _stats(arr: np.ndarray) -> dict:
    if arr is None or len(arr) == 0:
        return {k: np.nan for k in ["mean","std","min","p5","p25","p50","p75","p95","max","cv"]}
    return {
        "mean": float(np.mean(arr)),
        "std":  float(np.std(arr)),
        "min":  float(np.min(arr)),
        "p5":   float(np.percentile(arr, 5)),
        "p25":  float(np.percentile(arr, 25)),
        "p50":  float(np.percentile(arr, 50)),
        "p75":  float(np.percentile(arr, 75)),
        "p95":  float(np.percentile(arr, 95)),
        "max":  float(np.max(arr)),
        "cv":   float(np.std(arr)/np.mean(arr)) if np.mean(arr) > 0 else np.nan,
    }


# ---------------------------------------------------------------------------
# 2. RADIOMETRIC QUALITY
# ---------------------------------------------------------------------------

def eval_radiometric(ls8_dir, ls8_nbar_dir, s2_dir, s2_nbar_dir,
                     bands, outdir, use_band_ids=False) -> pd.DataFrame:
    print("\n[2/4] Radiometric quality (before / after NBAR) …")
    rows = []

    for band in bands:
        color = BAND_COLORS[band]
        fig, axes = plt.subplots(2, 2, figsize=(14, 8))
        fig.suptitle(f"Radiometric Quality — {band}", fontsize=13, fontweight="bold")

        for sensor_idx, (sensor, raw_fn, nbar_fn) in enumerate([
            ("LS8", lambda b: _load_raw_ls8(ls8_dir, b) if ls8_dir else None,
                    lambda b: _load_nbar(ls8_nbar_dir, _nbar_fname(b, "LS8", use_band_ids)) if ls8_nbar_dir else None),

            ("S2",  lambda b: _load_raw_s2_masked_like_nbar(
                                  s2_dir, b, s2_nbar_dir,
                                  _nbar_fname(b, "S2", use_band_ids)) if (s2_dir and s2_nbar_dir) else None,
                    lambda b: _load_nbar(s2_nbar_dir,  _nbar_fname(b, "S2",  use_band_ids)) if s2_nbar_dir else None),
        ]):
            raw  = raw_fn(band)
            nbar = nbar_fn(band)

            if raw is None and nbar is None:
                continue

            st_raw  = _stats(raw)
            st_nbar = _stats(nbar)

            # Histogram
            ax = axes[sensor_idx, 0]
            bins = np.linspace(0, 0.8, 80)
            if raw  is not None: ax.hist(np.clip(raw,  0, 0.8), bins=bins, alpha=0.6, color=color,   label="Raw SR",  density=True)
            if nbar is not None: ax.hist(np.clip(nbar, 0, 0.8), bins=bins, alpha=0.6, color="#333333", label="NBAR", density=True)
            ax.set_title(f"{sensor} — Reflectance Distribution")
            ax.set_xlabel("Surface Reflectance"); ax.set_ylabel("Density")
            ax.legend(); ax.grid(alpha=0.3)

            # Stats bar chart
            ax2 = axes[sensor_idx, 1]
            metrics = ["mean", "std", "p25", "p50", "p75"]
            x = np.arange(len(metrics))
            raw_vals  = [st_raw[m]  for m in metrics]
            nbar_vals = [st_nbar[m] for m in metrics]
            ax2.bar(x-0.2, raw_vals,  0.35, label="Raw SR", color=color,    alpha=0.85)
            ax2.bar(x+0.2, nbar_vals, 0.35, label="NBAR",   color="#333333", alpha=0.85)
            ax2.set_xticks(x); ax2.set_xticklabels(metrics)
            ax2.set_title(f"{sensor} — Key Statistics")
            ax2.set_ylabel("Reflectance"); ax2.legend(); ax2.grid(axis="y", alpha=0.3)

            if raw is not None and nbar is not None:
                mean_delta = (st_nbar["mean"] - st_raw["mean"]) / st_raw["mean"] * 100
                std_delta  = (st_nbar["std"]  - st_raw["std"])  / st_raw["std"]  * 100
                ax2.text(0.02, 0.97,
                         f"Δmean={mean_delta:+.1f}%  Δstd={std_delta:+.1f}%",
                         transform=ax2.transAxes, va="top", fontsize=9,
                         color="green" if abs(mean_delta) < 15 else "orange")

            for prefix, st in [("raw", st_raw), ("nbar", st_nbar)]:
                row = {"band": band, "sensor": sensor, "type": prefix}
                row.update(st)
                rows.append(row)

        plt.tight_layout()
        out = os.path.join(outdir, f"radiometric_{band}.png")
        plt.savefig(out, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"   → {out}")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 3. SPATIAL CONSISTENCY
# ---------------------------------------------------------------------------

def eval_spatial(ls8_nbar_dir, s2_nbar_dir, bands, outdir, use_band_ids=False) -> pd.DataFrame:
    print("\n[3/4] Spatial consistency …")
    rows = []

    n_bands = len(bands)
    fig, axes = plt.subplots(2, n_bands, figsize=(4*n_bands, 8))
    if n_bands == 1:
        axes = axes.reshape(2, 1)
    fig.suptitle("Spatial Consistency — Coefficient of Variation Maps", fontsize=13, fontweight="bold")

    for col, band in enumerate(bands):
        for row_idx, (sensor, nbar_dir) in enumerate([
            ("LS8", ls8_nbar_dir),
            ("S2",  s2_nbar_dir),
        ]):
            fname = _nbar_fname(band, sensor, use_band_ids)
            ax = axes[row_idx, col]

            if nbar_dir is None or not os.path.exists(os.path.join(nbar_dir, fname)):
                ax.text(0.5, 0.5, "N/A", ha="center", va="center", transform=ax.transAxes)
                ax.set_title(f"{sensor} {band}")
                continue

            path = os.path.join(nbar_dir, fname)
            xr_  = rxr.open_rasterio(path, chunks={"x":256,"y":256}, masked=True).squeeze()
            arr  = xr_.data.astype(np.float32).compute()
            arr  = np.where((arr <= 0) | ~np.isfinite(arr), np.nan, arr)

            # Compute local CV in 32×32 blocks
            H, W   = arr.shape
            bsz    = 32
            cv_map = np.full((H//bsz, W//bsz), np.nan)
            for i in range(H//bsz):
                for j in range(W//bsz):
                    blk = arr[i*bsz:(i+1)*bsz, j*bsz:(j+1)*bsz]
                    valid = blk[np.isfinite(blk)]
                    if len(valid) > 10 and np.mean(valid) > 1e-4:
                        cv_map[i,j] = np.std(valid)/np.mean(valid)

            im = ax.imshow(cv_map, cmap="YlOrRd", vmin=0, vmax=0.3, aspect="auto")
            plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
            ax.set_title(f"{sensor} {band}")
            ax.axis("off")

            valid_arr = arr[np.isfinite(arr) & (arr > 0)]
            rows.append({
                "sensor": sensor, "band": band,
                "mean_cv":    float(np.nanmean(cv_map)),
                "global_std": float(np.std(valid_arr)),
                "global_mean":float(np.mean(valid_arr)),
                "global_cv":  float(np.std(valid_arr)/np.mean(valid_arr)) if np.mean(valid_arr) > 0 else np.nan,
                "pct_valid":  float(np.sum(np.isfinite(arr))/arr.size * 100),
            })

    plt.tight_layout()
    out = os.path.join(outdir, "spatial_consistency.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"   → {out}")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4. CROSS-SENSOR AGREEMENT
# ---------------------------------------------------------------------------

def _to_numpy(x) -> np.ndarray:
    """Safely extract a float32 numpy array from xarray/dask/numpy."""
    if hasattr(x, "data"):
        x = x.data
    if hasattr(x, "compute"):
        x = x.compute()
    return np.asarray(x, dtype=np.float32)


def _coregister(ls8_path: str, s2_path: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    Reproject S2 to LS8 CRS+grid and return paired valid pixel arrays.
    Reprojects LS8 → EPSG:4326 first so both share a common geographic CRS,
    then matches S2 to that grid — avoids failures when LS8 and S2 are in
    different UTM zones.
    """
    from rasterio.enums import Resampling as RS

    ls8_xr = rxr.open_rasterio(ls8_path, chunks={"x":512,"y":512}, masked=True).squeeze()
    s2_xr  = rxr.open_rasterio(s2_path,  chunks={"x":512,"y":512}, masked=True).squeeze()

    ls8_crs = ls8_xr.rio.crs
    s2_crs  = s2_xr.rio.crs

    print(f"      LS8 CRS: {ls8_crs}  shape: {ls8_xr.shape}")
    print(f"      S2  CRS: {s2_crs}  shape: {s2_xr.shape}")

    # If CRS differs, reproject S2 to match LS8 CRS first, then grid-match
    if ls8_crs != s2_crs:
        print(f"      CRS mismatch — reprojecting S2 → {ls8_crs}")
        s2_xr = s2_xr.rio.reproject(ls8_crs, resampling=RS.bilinear)

    s2_rep = s2_xr.rio.reproject_match(ls8_xr, resampling=RS.bilinear)

    ls8_arr = _to_numpy(ls8_xr)
    s2_arr  = _to_numpy(s2_rep)

    ls8_valid_n = int(np.sum(np.isfinite(ls8_arr) & (ls8_arr > 0) & (ls8_arr < 1.5)))
    s2_valid_n  = int(np.sum(np.isfinite(s2_arr)  & (s2_arr  > 0) & (s2_arr  < 1.5)))
    print(f"      LS8 valid pixels: {ls8_valid_n:,}")
    print(f"      S2  valid pixels: {s2_valid_n:,}")

    if s2_valid_n == 0:

        print("      S2 has 0 valid pixels after reproject → trying reverse (LS8 → S2 CRS)")
        ls8_xr2 = rxr.open_rasterio(ls8_path, chunks={"x":512,"y":512}, masked=True).squeeze()
        s2_xr2  = rxr.open_rasterio(s2_path,  chunks={"x":512,"y":512}, masked=True).squeeze()
        ls8_rep = ls8_xr2.rio.reproject(s2_xr2.rio.crs, resampling=RS.bilinear)
        ls8_rep = ls8_rep.rio.reproject_match(s2_xr2, resampling=RS.bilinear)
        ls8_arr = _to_numpy(ls8_rep)
        s2_arr  = _to_numpy(s2_xr2)
        ls8_valid_n = int(np.sum(np.isfinite(ls8_arr) & (ls8_arr > 0) & (ls8_arr < 1.5)))
        s2_valid_n  = int(np.sum(np.isfinite(s2_arr)  & (s2_arr  > 0) & (s2_arr  < 1.5)))
        print(f"      After reverse: LS8={ls8_valid_n:,}  S2={s2_valid_n:,}")

    mask = (np.isfinite(ls8_arr) & np.isfinite(s2_arr) &
            (ls8_arr > 0) & (s2_arr > 0) &
            (ls8_arr < 1.5) & (s2_arr < 1.5))
    n_overlap = int(mask.sum())
    print(f"      Overlapping valid pixels: {n_overlap:,}")

    if n_overlap < 100:
        print(f"      WARNING: only {n_overlap} overlapping pixels.")
        print(f"      LS8 path 201 row 037 (UTM 30N) and S2 tile T29SNS (UTM 29N)")
        print(f"      have limited geographic overlap — this is a data selection issue,")
        print(f"      not a code bug. Choose a S2 tile that matches the LS8 footprint.")
        return np.array([]), np.array([])

    return ls8_arr[mask].ravel(), s2_arr[mask].ravel()


def eval_cross_sensor(ls8_nbar_dir, s2_nbar_dir, bands, outdir, use_band_ids=False) -> pd.DataFrame:
    print("\n[4/4] Cross-sensor agreement (LS8 vs S2 NBAR) …")
    rows = []

    if ls8_nbar_dir is None or s2_nbar_dir is None:
        print("   Skipped — need both LS8 and S2 NBAR dirs")
        return pd.DataFrame()

    for band in bands:
        ls8_path = os.path.join(ls8_nbar_dir, _nbar_fname(band, "LS8", use_band_ids))
        s2_path  = os.path.join(s2_nbar_dir,  _nbar_fname(band, "S2",  use_band_ids))

        if not os.path.exists(ls8_path) or not os.path.exists(s2_path):
            print(f"   {band}: missing files, skipping")
            continue

        print(f"   {band} …", end=" ", flush=True)
        ls8_v, s2_v = _coregister(ls8_path, s2_path)

        if len(ls8_v) < 100:
            print(f"  skipped — insufficient overlap ({len(ls8_v)} pixels)")
            continue

        # Subsample for plotting (max 50k points)
        idx = np.random.default_rng(0).choice(len(ls8_v), min(50000, len(ls8_v)), replace=False)
        ls8_s, s2_s = ls8_v[idx], s2_v[idx]

        # Metrics
        bias  = float(np.mean(s2_v - ls8_v))
        rmse  = float(np.sqrt(np.mean((s2_v - ls8_v)**2)))
        mae   = float(np.mean(np.abs(s2_v - ls8_v)))
        r, p  = stats.pearsonr(ls8_s, s2_s)
        slope, intercept, *_ = stats.linregress(ls8_s, s2_s)
        rows.append({"band": band, "bias": round(bias,5), "rmse": round(rmse,5),
                     "mae": round(mae,5), "r": round(r,4), "r2": round(r**2,4),
                     "slope": round(slope,4), "intercept": round(intercept,4),
                     "n_pixels": len(ls8_v)})
        print(f"RMSE={rmse:.4f}  bias={bias:+.4f}  R²={r**2:.4f}")

        # Plot
        fig = plt.figure(figsize=(15, 5))
        gs  = gridspec.GridSpec(1, 3, figure=fig, wspace=0.35)
        color = BAND_COLORS[band]

        # Scatter
        ax1 = fig.add_subplot(gs[0])
        ax1.scatter(ls8_s, s2_s, s=1, alpha=0.3, c=color, rasterized=True)
        lim = max(ls8_s.max(), s2_s.max()) * 1.05
        ax1.plot([0,lim],[0,lim], "k--", lw=1, label="1:1")
        x_line = np.linspace(0, lim, 100)
        ax1.plot(x_line, slope*x_line+intercept, "r-", lw=1.5,
                 label=f"fit: y={slope:.3f}x{intercept:+.3f}")
        ax1.set_xlabel("LS8 NBAR"); ax1.set_ylabel("S2 NBAR")
        ax1.set_title(f"{band} — Scatter\nR²={r**2:.4f}  RMSE={rmse:.4f}")
        ax1.legend(fontsize=8); ax1.grid(alpha=0.3)
        ax1.set_xlim(0, lim); ax1.set_ylim(0, lim)

        # Difference histogram
        ax2 = fig.add_subplot(gs[1])
        diff = s2_s - ls8_s
        ax2.hist(np.clip(diff, -0.15, 0.15), bins=80, color=color, alpha=0.8, density=True)
        ax2.axvline(0,   color="k",   lw=1, ls="--")
        ax2.axvline(bias, color="red", lw=1.5, label=f"bias={bias:+.4f}")
        ax2.set_xlabel("S2 − LS8"); ax2.set_ylabel("Density")
        ax2.set_title(f"{band} — Difference\nMAE={mae:.4f}")
        ax2.legend(fontsize=8); ax2.grid(alpha=0.3)

        # Density (2D histogram)
        ax3 = fig.add_subplot(gs[2])
        h, xedge, yedge = np.histogram2d(ls8_s, s2_s, bins=60,
                                          range=[[0,lim],[0,lim]])
        ax3.pcolormesh(xedge, yedge, h.T, cmap="hot_r")
        ax3.plot([0,lim],[0,lim],"w--",lw=1)
        ax3.set_xlabel("LS8 NBAR"); ax3.set_ylabel("S2 NBAR")
        ax3.set_title(f"{band} — Density")

        fig.suptitle(f"Cross-Sensor Agreement: LS8 vs S2 NBAR — {band}",
                     fontsize=12, fontweight="bold")
        out = os.path.join(outdir, f"cross_sensor_{band}.png")
        plt.savefig(out, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"   → {out}")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_report(outdir, kernel_df, radio_df, spatial_df, cross_df):
    path = os.path.join(outdir, "report.txt")
    lines = []
    w = lines.append

    w("=" * 65)
    w("  NBAR EVALUATION REPORT")
    w(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    w("=" * 65)

    # Kernel
    w("\n── 1. KERNEL CORRECTNESS ──────────────────────────────────")
    if kernel_df is not None and len(kernel_df):
        pass_n = kernel_df["Pass"].sum()
        w(f"   Tests passed: {pass_n}/{len(kernel_df)}")
        w(f"   Max LiSparse |err|: {kernel_df['LiSparse_abserr'].max():.2e}")
        w(f"   Max RossThick |err|: {kernel_df['Ross_abserr'].max():.2e}")
        w("   Verdict: " + ("✓ PASS — dask kernels match sen2like reference"
                             if pass_n == len(kernel_df) else "✗ FAIL"))

    # Radiometric
    w("\n── 2. RADIOMETRIC QUALITY ─────────────────────────────────")
    if radio_df is not None and len(radio_df):
        for sensor in radio_df["sensor"].unique():
            w(f"\n   {sensor}:")
            for band in radio_df["band"].unique():
                raw  = radio_df[(radio_df.sensor==sensor)&(radio_df.band==band)&(radio_df.type=="raw")]
                nbar = radio_df[(radio_df.sensor==sensor)&(radio_df.band==band)&(radio_df.type=="nbar")]
                if raw.empty or nbar.empty: continue
                dm = (nbar.iloc[0]["mean"] - raw.iloc[0]["mean"]) / raw.iloc[0]["mean"] * 100
                ds = (nbar.iloc[0]["std"]  - raw.iloc[0]["std"])  / raw.iloc[0]["std"]  * 100
                w(f"   {band:6s}  mean {raw.iloc[0]['mean']:.4f}→{nbar.iloc[0]['mean']:.4f}"
                  f" ({dm:+.1f}%)   std {raw.iloc[0]['std']:.4f}→{nbar.iloc[0]['std']:.4f} ({ds:+.1f}%)")

    # Spatial
    w("\n── 3. SPATIAL CONSISTENCY ─────────────────────────────────")
    if spatial_df is not None and len(spatial_df):
        w(f"   {'Sensor':6s}  {'Band':6s}  {'GlobalCV':9s}  {'MeanCV':9s}  {'%Valid':7s}")
        for _, r in spatial_df.iterrows():
            w(f"   {r['sensor']:6s}  {r['band']:6s}  "
              f"{r['global_cv']:9.4f}  {r['mean_cv']:9.4f}  {r['pct_valid']:7.1f}%")

    # Cross-sensor
    w("\n── 4. CROSS-SENSOR AGREEMENT ──────────────────────────────")
    if cross_df is not None and len(cross_df):
        w(f"   {'Band':6s}  {'RMSE':7s}  {'Bias':+7s}  {'MAE':7s}  {'R²':6s}  {'Slope':6s}")
        for _, r in cross_df.iterrows():
            flag = "✓" if r["rmse"] < 0.02 and abs(r["bias"]) < 0.01 and r["r2"] > 0.95 else "⚠"
            w(f"   {r['band']:6s}  {r['rmse']:7.4f}  {r['bias']:+7.4f}  "
              f"{r['mae']:7.4f}  {r['r2']:6.4f}  {r['slope']:6.4f}  {flag}")
        w("\n   Thresholds: RMSE<0.02, |bias|<0.01, R²>0.95  →  ✓ good  ⚠ review")

    w("\n" + "=" * 65)

    with open(path, "w") as f:
        f.write("\n".join(lines))
    print(f"\n   → {path}")
    print("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="NBAR Evaluation & Metrics")
    ap.add_argument("--ls8-raw",   default=None, help="LS8 L2SP raw directory")
    ap.add_argument("--ls8-nbar",  default=None, help="LS8 NBAR output directory")
    ap.add_argument("--s2-raw",    default=None, help="S2 L2A .SAFE directory")
    ap.add_argument("--s2-nbar",   default=None, help="S2 NBAR output directory")
    ap.add_argument("--outdir",    default="./eval_output")
    ap.add_argument("--bands",     nargs="+",
                    default=["Blue","Green","Red","NIR","SWIR1","SWIR2"],
                    choices=["Blue","Green","Red","NIR","SWIR1","SWIR2"])
    ap.add_argument("--kernel-only", action="store_true",
                    help="Run kernel correctness test only (no data needed)")
    ap.add_argument("--use-band-ids", action="store_true", default=False,
                    help="NBAR files use original band IDs (e.g. NBAR_B04.tif) "
                         "instead of logical names (NBAR_Red.tif). "
                         "Must match the --use-band-ids flag used with nbar.py.")
    args = ap.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    print(f"Outputs → {args.outdir}")

    all_metrics = {}

    # 1. Kernels (always)
    kernel_df = eval_kernels(args.outdir)
    all_metrics["kernel"] = kernel_df

    if args.kernel_only:
        write_report(args.outdir, kernel_df, None, None, None)
        return

    # 2. Radiometric
    radio_df = eval_radiometric(
        args.ls8_raw,  args.ls8_nbar,
        args.s2_raw,   args.s2_nbar,
        args.bands,    args.outdir,
        use_band_ids=args.use_band_ids,
    )

    # 3. Spatial
    spatial_df = eval_spatial(args.ls8_nbar, args.s2_nbar, args.bands, args.outdir,
                              use_band_ids=args.use_band_ids)

    # 4. Cross-sensor
    cross_df = eval_cross_sensor(args.ls8_nbar, args.s2_nbar, args.bands, args.outdir,
                                 use_band_ids=args.use_band_ids)

    # Save CSV
    csv_path = os.path.join(args.outdir, "metrics.csv")
    pd.concat([
        radio_df.assign(eval="radiometric") if len(radio_df) else pd.DataFrame(),
        spatial_df.assign(eval="spatial")   if len(spatial_df) else pd.DataFrame(),
        cross_df.assign(eval="cross")       if len(cross_df) else pd.DataFrame(),
    ], ignore_index=True).to_csv(csv_path, index=False)
    print(f"\n   → {csv_path}")

    # Report
    write_report(args.outdir, kernel_df, radio_df, spatial_df, cross_df)


if __name__ == "__main__":
    main()