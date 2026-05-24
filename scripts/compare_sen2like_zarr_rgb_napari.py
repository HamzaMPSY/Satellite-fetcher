#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.abc
import os
import re
import sys
from pathlib import Path

import numpy as np


DEFAULT_JOB_ID = "9bbe22161c2b403c9619bb568c823442"
RGB_BANDS = ("B04", "B03", "B02")


class _BlockNumbaImport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):
        if fullname == "numba" or fullname.startswith("numba."):
            raise ImportError("numba disabled for this napari preview")
        return None


def _disable_numba_for_napari() -> None:
    """Napari works without numba; this avoids broken llvmlite imports on macOS."""
    if any(isinstance(finder, _BlockNumbaImport) for finder in sys.meta_path):
        return
    for module_name in list(sys.modules):
        if module_name == "numba" or module_name.startswith("numba."):
            sys.modules.pop(module_name, None)
    sys.meta_path.insert(0, _BlockNumbaImport())


def _configure_napari_runtime_cache() -> None:
    cache_root = Path("/private/tmp/nimbus-napari")
    cache_root.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("NUMBA_CACHE_DIR", str(cache_root / "numba"))
    os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root / "xdg-cache"))
    os.environ.setdefault("XDG_CONFIG_HOME", str(cache_root / "xdg-config"))


def _scene_date(text: str) -> str:
    match = re.search(r"_(20\d{6})T", text)
    if match:
        return match.group(1)
    match = re.search(r"_(20\d{6})_", text)
    return match.group(1) if match else text


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


def _read_tif(path: Path) -> np.ndarray:
    try:
        import rasterio
    except ModuleNotFoundError as exc:
        raise SystemExit("Missing rasterio. Install it in the same env as napari.") from exc

    with rasterio.open(path) as dataset:
        return dataset.read(1)


def _find_safe_dirs(job_dir: Path) -> list[Path]:
    safe_dirs = sorted(path for path in job_dir.glob("*/SAFE/*.SAFE") if path.is_dir())
    if not safe_dirs:
        raise SystemExit(f"No Sen2Like SAFE outputs found under {job_dir}")
    return safe_dirs


def _find_safe_band(safe_dir: Path, band: str) -> Path:
    matches = sorted(safe_dir.glob(f"GRANULE/*/IMG_DATA/RESOLUTION_10M/*_{band}_10m.TIF"))
    if not matches:
        raise SystemExit(f"Missing Sen2Like band {band}: {safe_dir}")
    return matches[0]


def _load_sen2like_rgb(safe_dir: Path) -> tuple[str, np.ndarray]:
    layers = {band: _read_tif(_find_safe_band(safe_dir, band)) for band in RGB_BANDS}
    label = _scene_date(safe_dir.name)
    return label, _robust_rgb(layers["B04"], layers["B03"], layers["B02"])


def _read_zarr_band_names(root) -> list[str]:
    names: list[str] = []
    for value in root["band"][:]:
        if isinstance(value, bytes):
            names.append(value.decode("utf-8"))
        else:
            names.append(str(value))
    return names


def _load_zarr_rgb(zarr_dir: Path) -> tuple[str, np.ndarray]:
    try:
        import zarr
    except ModuleNotFoundError as exc:
        raise SystemExit("Missing zarr. Install it in the same env as napari.") from exc

    root = zarr.open_group(str(zarr_dir), mode="r", use_consolidated=False)
    band_names = _read_zarr_band_names(root)
    band_index = {name: index for index, name in enumerate(band_names)}
    missing = [band for band in RGB_BANDS if band not in band_index]
    if missing:
        raise SystemExit(f"Missing Zarr bands {missing}: {zarr_dir}")

    imagery = root["imagery"]
    red = imagery[0, band_index["B04"], :, :]
    green = imagery[0, band_index["B03"], :, :]
    blue = imagery[0, band_index["B02"], :, :]
    label = _scene_date(zarr_dir.name)
    return label, _robust_rgb(red, green, blue)


def _find_optional_tif(safe_dir: Path, pattern: str) -> Path | None:
    matches = sorted(safe_dir.glob(pattern))
    return matches[0] if matches else None


def _load_sen2like_masks(safe_dir: Path) -> dict[str, np.ndarray]:
    layers: dict[str, np.ndarray] = {}
    cloud_path = _find_optional_tif(safe_dir, "GRANULE/*/QI_DATA/*_CLOUD_MASK.TIF")
    if cloud_path is not None:
        layers["Sen2Like CLOUD_MASK"] = _read_tif(cloud_path).astype(np.uint8, copy=False)

    validity_path = _find_optional_tif(
        safe_dir,
        "GRANULE/*/IMG_DATA/RESOLUTION_10M/*_VALIDITY_MASK_10m.TIF",
    )
    if validity_path is not None:
        layers["Sen2Like VALIDITY_MASK"] = _read_tif(validity_path).astype(
            np.uint8,
            copy=False,
        )
    return layers


def _load_zarr_masks(zarr_dir: Path) -> dict[str, np.ndarray]:
    try:
        import zarr
    except ModuleNotFoundError as exc:
        raise SystemExit("Missing zarr. Install it in the same env as napari.") from exc

    root = zarr.open_group(str(zarr_dir), mode="r", use_consolidated=False)
    if "masks" not in root:
        print(f"[warn] no masks group in {zarr_dir}")
        return {}

    masks = root["masks"]
    layers: dict[str, np.ndarray] = {}
    for mask_name in ("cloud", "cloud_probability", "water", "water_probability"):
        if mask_name not in masks:
            print(f"[warn] missing masks/{mask_name} in {zarr_dir}")
            continue
        layers[f"Mask output {mask_name}"] = np.asarray(masks[mask_name][0, :, :])
    return layers


def _add_mask_layer(viewer, data: np.ndarray, name: str) -> None:
    if name.endswith("_probability"):
        viewer.add_image(
            data.astype(np.float32, copy=False),
            name=name,
            colormap="magma",
            contrast_limits=(0.0, 1.0),
        )
        return

    viewer.add_labels(data.astype(np.uint8, copy=False), name=name)


def _matching_zarr_for_date(zarr_dirs: list[Path], date_label: str) -> Path | None:
    matches = [path for path in zarr_dirs if _scene_date(path.name) == date_label]
    return matches[0] if matches else None


def _mask_image(data: np.ndarray) -> np.ndarray:
    return (np.asarray(data) > 0).astype(np.float32, copy=False)


def _add_overlay_mask_layer(
    viewer,
    data: np.ndarray,
    *,
    name: str,
    colormap: str,
    opacity: float,
    visible: bool = True,
) -> None:
    viewer.add_image(
        data.astype(np.float32, copy=False),
        name=name,
        colormap=colormap,
        contrast_limits=(0.0, 1.0),
        opacity=opacity,
        blending="additive",
        visible=visible,
    )


def _open_overlay_viewer(
    *,
    napari,
    safe_dirs: list[Path],
    zarr_dirs: list[Path],
    rgb_source: str,
    include_probability: bool,
) -> None:
    dates: list[str] = []
    rgb_layers: list[np.ndarray] = []
    cloud_layers: list[np.ndarray] = []
    water_layers: list[np.ndarray] = []
    cloud_probability_layers: list[np.ndarray] = []
    water_probability_layers: list[np.ndarray] = []

    for safe_dir in sorted(safe_dirs, key=lambda path: _scene_date(path.name)):
        date_label = _scene_date(safe_dir.name)
        zarr_dir = _matching_zarr_for_date(zarr_dirs, date_label)
        if zarr_dir is None:
            print(f"[warn] no matching Zarr for Sen2Like scene {date_label}")
            continue

        if rgb_source == "sen2like":
            _, rgb = _load_sen2like_rgb(safe_dir)
        else:
            _, rgb = _load_zarr_rgb(zarr_dir)
        masks = _load_zarr_masks(zarr_dir)

        missing = [
            layer
            for layer in ("Mask output cloud", "Mask output water")
            if layer not in masks
        ]
        if missing:
            print(f"[warn] missing overlay masks {missing} for {zarr_dir}")
            continue

        dates.append(date_label)
        rgb_layers.append(rgb)
        cloud_layers.append(_mask_image(masks["Mask output cloud"]))
        water_layers.append(_mask_image(masks["Mask output water"]))
        if include_probability:
            cloud_probability_layers.append(
                masks.get("Mask output cloud_probability", np.zeros_like(cloud_layers[-1]))
            )
            water_probability_layers.append(
                masks.get("Mask output water_probability", np.zeros_like(water_layers[-1]))
            )

    if not rgb_layers:
        raise SystemExit("No scene could be loaded with matching RGB + cloud/water masks.")

    viewer = napari.Viewer(title="Nimbus RGB + Cloud/Water masks")
    date_hint = ", ".join(f"{index}={date}" for index, date in enumerate(dates))
    viewer.add_image(
        np.stack(rgb_layers, axis=0),
        name=f"RGB {rgb_source} ({date_hint})",
        rgb=True,
    )
    _add_overlay_mask_layer(
        viewer,
        np.stack(cloud_layers, axis=0),
        name="mask cloud",
        colormap="red",
        opacity=0.35,
    )
    _add_overlay_mask_layer(
        viewer,
        np.stack(water_layers, axis=0),
        name="mask water",
        colormap="blue",
        opacity=0.45,
    )

    if include_probability:
        _add_overlay_mask_layer(
            viewer,
            np.stack(cloud_probability_layers, axis=0),
            name="cloud probability",
            colormap="magma",
            opacity=0.35,
            visible=False,
        )
        _add_overlay_mask_layer(
            viewer,
            np.stack(water_probability_layers, axis=0),
            name="water probability",
            colormap="cyan",
            opacity=0.35,
            visible=False,
        )

    viewer.dims.axis_labels = ("scene", "y", "x")
    viewer.text_overlay.visible = True
    viewer.text_overlay.text = f"Scene slider: {date_hint}"
    napari.run()


def _open_compare_grid_viewer(*, napari, safe_dirs: list[Path], zarr_dirs: list[Path]) -> None:
    viewer = napari.Viewer(title="Sen2Like vs Zarr RGB + masks")
    for safe_dir in sorted(safe_dirs, key=lambda path: _scene_date(path.name)):
        date_label, rgb = _load_sen2like_rgb(safe_dir)
        viewer.add_image(rgb, name=f"{date_label} Sen2Like RGB B04/B03/B02", rgb=True)
        for layer_name, data in _load_sen2like_masks(safe_dir).items():
            _add_mask_layer(viewer, data, f"{date_label} {layer_name}")

        matching_zarr = _matching_zarr_for_date(zarr_dirs, date_label)
        if matching_zarr is None:
            print(f"[warn] no matching Zarr for Sen2Like scene {date_label}")
            continue
        _, zarr_rgb = _load_zarr_rgb(matching_zarr)
        viewer.add_image(zarr_rgb, name=f"{date_label} Zarr RGB B04/B03/B02", rgb=True)
        for layer_name, data in _load_zarr_masks(matching_zarr).items():
            _add_mask_layer(viewer, data, f"{date_label} {layer_name}")

    viewer.grid.enabled = True
    viewer.grid.stride = 1
    napari.run()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare Sen2Like SAFE RGB, Zarr RGB, and mask-service outputs in napari."
        )
    )
    parser.add_argument(
        "--sen2like-job-dir",
        type=Path,
        default=Path(f"data/downloads/sen2like/{DEFAULT_JOB_ID}"),
        help="Sen2Like job directory containing */SAFE/*.SAFE outputs.",
    )
    parser.add_argument(
        "--zarr-dir",
        type=Path,
        action="append",
        default=None,
        help="One Zarr directory to open. Repeat for multiple scenes.",
    )
    parser.add_argument(
        "--zarr-glob",
        default="data/downloads/zarr/*20260520T143344.zarr",
        help="Glob used when --zarr-dir is not provided.",
    )
    parser.add_argument(
        "--allow-numba",
        action="store_true",
        help="Let napari import numba. Default blocks numba to avoid llvmlite hangs.",
    )
    parser.add_argument(
        "--rgb-source",
        choices=("zarr", "sen2like"),
        default="zarr",
        help="Base RGB layer to show under the cloud/water overlays.",
    )
    parser.add_argument(
        "--include-probability",
        action="store_true",
        help="Also load cloud_probability and water_probability layers, hidden by default.",
    )
    parser.add_argument(
        "--compare-grid",
        action="store_true",
        help="Use the old comparison grid instead of one overlaid RGB+mask view.",
    )
    args = parser.parse_args()

    safe_dirs = _find_safe_dirs(args.sen2like_job_dir)
    zarr_dirs = sorted(args.zarr_dir or [Path(path) for path in Path().glob(args.zarr_glob)])
    if not zarr_dirs:
        raise SystemExit(f"No Zarr outputs matched {args.zarr_glob}")

    _configure_napari_runtime_cache()
    if not args.allow_numba:
        _disable_numba_for_napari()
    import napari

    if args.compare_grid:
        _open_compare_grid_viewer(napari=napari, safe_dirs=safe_dirs, zarr_dirs=zarr_dirs)
    else:
        _open_overlay_viewer(
            napari=napari,
            safe_dirs=safe_dirs,
            zarr_dirs=zarr_dirs,
            rgb_source=args.rgb_source,
            include_probability=args.include_probability,
        )


if __name__ == "__main__":
    main()
