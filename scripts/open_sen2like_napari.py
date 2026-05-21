#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling


BANDS = ("B02", "B03", "B04", "B08", "B11", "B12")
RGB_BANDS = ("B04", "B03", "B02")
FALSE_COLOR_BANDS = ("B08", "B04", "B03")


def _configure_napari_runtime_cache() -> None:
    cache_root = Path("/private/tmp/nimbus-napari")
    cache_root.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("NUMBA_CACHE_DIR", str(cache_root / "numba"))
    os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root / "xdg-cache"))
    os.environ.setdefault("XDG_CONFIG_HOME", str(cache_root / "xdg-config"))

    try:
        import appdirs
    except Exception:
        return

    def _wrap_appdirs(original, kind: str):
        def _wrapped(appname=None, appauthor=None, version=None, *args, **kwargs):
            if appname == "napari":
                target = cache_root / kind
                if version:
                    target = target / str(version)
                target.mkdir(parents=True, exist_ok=True)
                return str(target)
            return original(appname, appauthor, version, *args, **kwargs)

        return _wrapped

    appdirs.user_cache_dir = _wrap_appdirs(appdirs.user_cache_dir, "cache")
    appdirs.user_config_dir = _wrap_appdirs(appdirs.user_config_dir, "config")
    appdirs.user_data_dir = _wrap_appdirs(appdirs.user_data_dir, "data")
    appdirs.user_state_dir = _wrap_appdirs(appdirs.user_state_dir, "state")
    appdirs.user_log_dir = _wrap_appdirs(appdirs.user_log_dir, "log")


def _find_scene_dirs(job_dir: Path) -> list[Path]:
    scenes = sorted(job_dir.glob("*/SAFE/*.SAFE"))
    if not scenes:
        raise SystemExit(f"No Sen2Like SAFE outputs found under {job_dir}")
    return scenes


def _find_band(scene_dir: Path, band: str) -> Path:
    matches = sorted(scene_dir.glob(f"GRANULE/*/IMG_DATA/RESOLUTION_10M/*_{band}_10m.TIF"))
    if not matches:
        raise SystemExit(f"Band {band} not found in {scene_dir}")
    return matches[0]


def _read_band(path: Path, *, downsample: int) -> np.ndarray:
    with rasterio.open(path) as src:
        height = max(1, src.height // downsample)
        width = max(1, src.width // downsample)
        return src.read(
            1,
            out_shape=(height, width),
            resampling=Resampling.average,
        )


def _robust_limits(array: np.ndarray) -> tuple[float, float]:
    valid = array[np.isfinite(array) & (array > 0)]
    if valid.size == 0:
        return (0.0, 1.0)
    low, high = np.percentile(valid, [2, 98])
    return (float(low), float(max(high, low + 1)))


def _rgb_stack(layers: dict[str, np.ndarray], band_order: tuple[str, str, str]) -> np.ndarray:
    channels = []
    for band in band_order:
        arr = layers[band].astype(np.float32)
        low, high = _robust_limits(arr)
        arr = np.clip((arr - low) / (high - low), 0.0, 1.0)
        channels.append(arr)
    return np.dstack(channels)


def main() -> None:
    parser = argparse.ArgumentParser(description="Open Sen2Like output bands in Napari.")
    parser.add_argument(
        "--job-dir",
        default="data/downloads/sen2like/21998ac90c7c4e37bdc39714657244cb",
        help="Sen2Like job directory containing product/SAFE/*.SAFE outputs.",
    )
    parser.add_argument(
        "--scene-index",
        type=int,
        default=0,
        help="Zero-based SAFE scene index to open.",
    )
    parser.add_argument(
        "--all-scenes",
        action="store_true",
        help="Load every SAFE scene found under the job directory.",
    )
    parser.add_argument(
        "--downsample",
        type=int,
        default=4,
        help="Read every output at reduced resolution. Use 1 for full resolution.",
    )
    args = parser.parse_args()

    downsample = max(1, int(args.downsample))
    scenes = _find_scene_dirs(Path(args.job_dir))
    if args.all_scenes:
        selected_scenes = scenes
    else:
        if args.scene_index < 0 or args.scene_index >= len(scenes):
            raise SystemExit(f"--scene-index must be between 0 and {len(scenes) - 1}")
        selected_scenes = [scenes[args.scene_index]]

    _configure_napari_runtime_cache()
    import napari

    viewer = napari.Viewer(title=f"Sen2Like bands - {len(selected_scenes)} scene(s)")
    for scene_index, scene_dir in enumerate(selected_scenes):
        layers = {band: _read_band(_find_band(scene_dir, band), downsample=downsample) for band in BANDS}
        scene_name = scene_dir.parent.parent.name
        viewer.add_image(
            _rgb_stack(layers, RGB_BANDS),
            name=f"{scene_name} true color B04/B03/B02",
            rgb=True,
            visible=scene_index == 0,
        )
        viewer.add_image(
            _rgb_stack(layers, FALSE_COLOR_BANDS),
            name=f"{scene_name} false color B08/B04/B03",
            rgb=True,
            visible=False,
        )
        for band, array in layers.items():
            viewer.add_image(
                array,
                name=f"{scene_name} {band}",
                contrast_limits=_robust_limits(array),
                visible=False,
            )

    print("Opened scenes:")
    for scene_dir in selected_scenes:
        print(f"- {scene_dir}")
    print(f"Downsample: {downsample}x")
    print("Bands:", ", ".join(BANDS))
    napari.run()


if __name__ == "__main__":
    main()
