
import os
import re
import gc
import glob
import shutil
import tarfile
import tempfile
import time
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import cv2
import rasterio
from rasterio.transform import Affine


# ─────────────────────────────────────────────────────────────────────────────
# Sensor detection
# ─────────────────────────────────────────────────────────────────────────────

LANDSAT_RE  = re.compile(r"L[COTEM]\d{2}_L\w+_\d{6}_\d{8}_\d{8}_\d{2}_\w+")
SENTINEL_RE = re.compile(r"S2[AB]_MSI\w+\.SAFE")

def detect_sensor(path: str) -> str:
    name = Path(path).name
    if LANDSAT_RE.match(name) or name.endswith((".tar", ".tar.gz")):
        return "landsat"
    if SENTINEL_RE.match(name):
        return "sentinel2"
    if glob.glob(os.path.join(path, "**", "*.jp2"), recursive=True):
        return "sentinel2"
    if (glob.glob(os.path.join(path, "**", "*.TIF"), recursive=True) or
            glob.glob(os.path.join(path, "**", "*.tif"), recursive=True)):
        return "landsat"
    raise ValueError(f"Cannot determine sensor from: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Landsat helpers
# ─────────────────────────────────────────────────────────────────────────────

LANDSAT_BANDS = {
    "B1":  "_B1.TIF",  "B2":  "_B2.TIF",  "B3":  "_B3.TIF",
    "B4":  "_B4.TIF",  "B5":  "_B5.TIF",  "B6":  "_B6.TIF",
    "B7":  "_B7.TIF",  "B8":  "_B8.TIF",  "B9":  "_B9.TIF",
    "B10": "_B10.TIF", "B11": "_B11.TIF",
}

def _maybe_extract(scene_path: str) -> str:
    """Extract .tar / .tar.gz archive if needed, return usable directory."""
    if not (scene_path.endswith(".tar") or scene_path.endswith(".tar.gz")):
        return scene_path
    tmp = tempfile.mkdtemp(prefix="landsat_extract_")
    print(f"  Extracting archive → {tmp}")
    with tarfile.open(scene_path) as tf:
        tf.extractall(tmp)
    return tmp

def find_landsat_file(scene_dir: str, band: str) -> str | None:
    suffix = LANDSAT_BANDS.get(band)
    if not suffix:
        return None
    hits = (glob.glob(os.path.join(scene_dir, f"*{suffix}")) +
            glob.glob(os.path.join(scene_dir, f"*{suffix.lower()}")))
    return hits[0] if hits else None


# ─────────────────────────────────────────────────────────────────────────────
# Sentinel-2 helpers
# ─────────────────────────────────────────────────────────────────────────────

SENTINEL_BANDS = {
    "B01": 60, "B02": 10, "B03": 10, "B04": 10,
    "B05": 20, "B06": 20, "B07": 20, "B08": 10,
    "B8A": 20, "B09": 60, "B10": 60,
    "B11": 20, "B12": 20,
}

def find_sentinel_jp2(safe_dir: str, band_id: str) -> str | None:
    hits = glob.glob(os.path.join(safe_dir, "**", f"*_{band_id}.jp2"), recursive=True)
    if not hits:
        return None
    def _priority(p):
        if "R10m" in p: return 0
        if "R20m" in p: return 1
        return 2
    return sorted(hits, key=_priority)[0]


# ─────────────────────────────────────────────────────────────────────────────
# Core: read → upsample → write
# ─────────────────────────────────────────────────────────────────────────────

def process_band(
    file_path:  str,
    band_name:  str,
    output_dir: str,
    scale:      int,
    sensor:     str,   # "landsat" | "sentinel2"
) -> str:
    t0 = time.perf_counter()

    # ── 1. Read source ───────────────────────────────────────────────────────
    with rasterio.open(file_path) as src:
        data      = src.read(1).astype(np.float32)
        nodata    = src.nodata      # declared nodata value (may be None)
        transform = src.transform
        crs       = src.crs

    H, W = data.shape
    new_H, new_W = H * scale, W * scale

    if nodata is not None:
        nodata_mask = data == float(nodata)
        fill_value  = float(nodata)
    elif sensor == "sentinel2":
        # Sentinel-2 L1C: 0 is the fill value, never a valid DN
        nodata_mask = data == 0.0
        fill_value  = 0.0
    else:
        # No declared nodata and not Sentinel — treat as fully valid
        nodata_mask = np.zeros(data.shape, dtype=bool)
        fill_value  = -9999.0

    # Zero-fill nodata before interpolation to avoid value bleed at borders
    data[nodata_mask] = 0.0

    # ── 3. Upsample with cv2 Lanczos-4 ──────────────────────────────────────
    upsampled = cv2.resize(
        data,
        (new_W, new_H),          # cv2 uses (width, height) order
        interpolation=cv2.INTER_LANCZOS4,
    )


    valid_min = float(data[~nodata_mask].min()) if nodata_mask.any() else float(data.min())
    valid_max = float(data[~nodata_mask].max()) if nodata_mask.any() else float(data.max())
    np.clip(upsampled, valid_min, valid_max, out=upsampled)

    # ── 4. Upsample nodata mask and restore fill value ───────────────────────
    if nodata_mask.any():
        mask_up = cv2.resize(
            nodata_mask.astype(np.uint8),
            (new_W, new_H),
            interpolation=cv2.INTER_NEAREST,
        )
        # Dilate by Lanczos kernel radius (4 source px → 4*scale output px)
        kernel_radius = 4 * scale
        kernel = cv2.getStructuringElement(
            cv2.MORPH_RECT,
            (2 * kernel_radius + 1, 2 * kernel_radius + 1),
        )
        mask_up = cv2.dilate(mask_up, kernel).astype(bool)
        upsampled[mask_up] = fill_value
    else:
        mask_up = None

    # ── 5. Write GeoTIFF ─────────────────────────────────────────────────────
    new_transform = Affine(
        transform.a / scale, transform.b, transform.c,
        transform.d, transform.e / scale, transform.f,
    )

    out_profile = {
        "driver":     "GTiff",
        "dtype":      "float32",
        "width":      new_W,
        "height":     new_H,
        "count":      1,
        "crs":        crs,
        "transform":  new_transform,
        "nodata":     fill_value,
        "compress":   "lzw",
        "tiled":      True,
        "blockxsize": 512,
        "blockysize": 512,
        "bigtiff":    "IF_SAFER",
        "num_threads": "ALL_CPUS",
    }

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{band_name}_10m.TIF")

    with rasterio.open(out_path, "w", **out_profile) as dst:
        dst.write(upsampled, 1)

    elapsed = time.perf_counter() - t0
    size_mb  = upsampled.nbytes / 1e6

    del data, upsampled, nodata_mask, mask_up
    gc.collect()

    print(f"  ✓ {band_name:<5}  {W}×{H} → {new_W}×{new_H}  "
          f"({elapsed:.1f}s,  {size_mb:.0f} MB)  → {Path(out_path).name}")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("scene_path", help="Landsat directory or Sentinel-2 .SAFE")
    parser.add_argument("output_dir", nargs="?", default=None,
                        help="Output directory (default: <scene>_10m_geotiffs/)")
    parser.add_argument("--workers", type=int, default=os.cpu_count(),
                        help="Parallel worker processes (default: all CPUs)")
    args = parser.parse_args()

    scene_path = args.scene_path.rstrip("/\\")
    output_dir = args.output_dir or os.path.join(
        os.path.dirname(scene_path) or ".",
        Path(scene_path).stem + "_10m_geotiffs",
    )
    n_workers = max(1, args.workers)
    sensor    = detect_sensor(scene_path)

    print("\n" + "=" * 60)
    print(f"  Sensor   : {sensor.upper()}")
    print(f"  Scene    : {Path(scene_path).name}")
    print(f"  Output   : {output_dir}")
    print(f"  Workers  : {n_workers}")
    print(f"  Method   : cv2.INTER_LANCZOS4  (8-tap, C++ multi-threaded)")
    print("=" * 60)

    # ── Build job list ───────────────────────────────────────────────────────
    jobs = []  # (file_path, band_name, output_dir, scale, sensor)

    if sensor == "landsat":
        scene_path = _maybe_extract(scene_path)
        for band in LANDSAT_BANDS:
            fpath = find_landsat_file(scene_path, band)
            if fpath is None:
                print(f"  [skip] {band} — file not found")
                continue
            if band == "B8":
                # Panchromatic 15 m → 10 m is non-integer scale; copy as-is
                out = os.path.join(output_dir, f"{band}_10m.TIF")
                os.makedirs(output_dir, exist_ok=True)
                shutil.copy2(fpath, out)
                print(f"  [copy] {band} (15 m pan, non-integer scale) → {out}")
                continue
            jobs.append((fpath, band, output_dir, 3, sensor))

    else:  # sentinel2
        for band, native_res in SENTINEL_BANDS.items():
            fpath = find_sentinel_jp2(scene_path, band)
            if fpath is None:
                print(f"  [skip] {band} — JP2 not found")
                continue
            scale = native_res // 10
            if scale == 1:
                out = os.path.join(output_dir, f"{band}_10m.TIF")
                os.makedirs(output_dir, exist_ok=True)
                shutil.copy2(fpath, out)
                print(f"  [copy] {band} already 10 m → {out}")
                continue
            jobs.append((fpath, band, output_dir, scale, sensor))

    print(f"\n  {len(jobs)} bands to upsample\n")

    # ── Run ──────────────────────────────────────────────────────────────────
    t_start = time.perf_counter()

    if n_workers == 1 or len(jobs) == 1:
        for fpath, band, outdir, scale, snsr in jobs:
            try:
                process_band(fpath, band, outdir, scale, snsr)
            except Exception as exc:
                import traceback
                print(f"  [error] {band}: {exc}")
                traceback.print_exc()
    else:
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(process_band, fpath, band, outdir, scale, snsr): band
                for fpath, band, outdir, scale, snsr in jobs
            }
            for fut in as_completed(futures):
                band = futures[fut]
                try:
                    fut.result()
                except Exception as exc:
                    import traceback
                    print(f"  [error] {band}: {exc}")
                    traceback.print_exc()

    total = time.perf_counter() - t_start
    print("\n" + "=" * 60)
    print(f"  Done — {len(jobs)} bands in {total:.1f}s "
          f"({total/max(len(jobs),1):.1f}s avg per band)")
    print(f"  GeoTIFFs in: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()