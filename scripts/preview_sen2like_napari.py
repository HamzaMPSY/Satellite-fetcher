#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


DEFAULT_JOB_DIR = Path(
    "data/downloads/sen2like/1b7f26e3124044468409f7f0352c053d"
)
SPECTRAL_BANDS = ("B02", "B03", "B04", "B08", "B11", "B12")
MASK_BANDS = ("VALIDITY_MASK",)


def _read_raster(path: Path) -> np.ndarray:
    try:
        import rasterio
    except ModuleNotFoundError:
        rasterio = None

    if rasterio is not None:
        with rasterio.open(path) as dataset:
            return dataset.read(1)

    try:
        import tifffile
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing raster reader. Install rasterio or tifffile in the same env as napari."
        ) from exc
    return tifffile.imread(path)


def _robust_rgb(red: np.ndarray, green: np.ndarray, blue: np.ndarray) -> np.ndarray:
    stack = np.stack([red, green, blue], axis=-1).astype(np.float32, copy=False)
    valid = np.isfinite(stack) & (stack > 0)
    if not np.any(valid):
        return np.zeros_like(stack, dtype=np.float32)
    low, high = np.percentile(stack[valid], [2, 98])
    if high <= low:
        high = float(stack[valid].max() or 1.0)
        low = float(stack[valid].min())
    return np.clip((stack - low) / max(high - low, 1e-6), 0.0, 1.0)


def _scene_label(safe_dir: Path) -> str:
    parent = safe_dir.parents[1].name if len(safe_dir.parents) > 1 else safe_dir.name
    stem = safe_dir.name.replace(".SAFE", "")
    return f"{parent} | {stem}"


def _find_layer(img_dir: Path, token: str) -> Path | None:
    matches = sorted(img_dir.glob(f"*_{token}_10m.TIF"))
    return matches[0] if matches else None


def _find_safe_dirs(job_dir: Path) -> list[Path]:
    return sorted(path for path in job_dir.glob("*/SAFE/*.SAFE") if path.is_dir())


def _add_scene(viewer, safe_dir: Path) -> None:
    img_dirs = sorted(safe_dir.glob("GRANULE/*/IMG_DATA/RESOLUTION_10M"))
    if not img_dirs:
        print(f"[skip] no RESOLUTION_10M dir: {safe_dir}")
        return
    img_dir = img_dirs[0]
    label = _scene_label(safe_dir)

    arrays: dict[str, np.ndarray] = {}
    for band in (*SPECTRAL_BANDS, *MASK_BANDS):
        path = _find_layer(img_dir, band)
        if path is None:
            print(f"[warn] {label}: missing {band}")
            continue
        arrays[band] = _read_raster(path)

    if {"B04", "B03", "B02"} <= arrays.keys():
        viewer.add_image(
            _robust_rgb(arrays["B04"], arrays["B03"], arrays["B02"]),
            name=f"{label} RGB B04/B03/B02",
            rgb=True,
            blending="translucent",
        )

    for band in SPECTRAL_BANDS:
        if band in arrays:
            viewer.add_image(
                arrays[band],
                name=f"{label} {band}",
                visible=False,
                contrast_limits=(0, float(np.nanpercentile(arrays[band], 99.5) or 1.0)),
            )

    if "VALIDITY_MASK" in arrays:
        viewer.add_labels(
            arrays["VALIDITY_MASK"].astype(np.uint8, copy=False),
            name=f"{label} VALIDITY_MASK",
            visible=False,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview Sen2Like SAFE outputs in napari.")
    parser.add_argument("--job-dir", type=Path, default=DEFAULT_JOB_DIR)
    args, _unknown = parser.parse_known_args()

    try:
        import napari
    except ModuleNotFoundError as exc:
        raise SystemExit("Missing napari. Run this from a Python env with napari installed.") from exc

    safe_dirs = _find_safe_dirs(args.job_dir)
    if not safe_dirs:
        raise SystemExit(f"No Sen2Like SAFE outputs found under {args.job_dir}")

    viewer = napari.Viewer(title="Nimbus Sen2Like preview")
    for safe_dir in safe_dirs:
        _add_scene(viewer, safe_dir)
    napari.run()


if __name__ == "__main__":
    main()
