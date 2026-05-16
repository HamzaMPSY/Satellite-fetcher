#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import numpy as np
import zarr

from nimbuschain_rgb_viewer_service.presets import choose_rgb_bands


os.environ.setdefault("NUMBA_DISABLE_JIT", "1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Open Nimbus Zarr outputs in Napari.")
    parser.add_argument("--job-id", help="Fetch zarr_outputs/cube_outputs from the local API.")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="Nimbus API base URL.")
    parser.add_argument("--zarr", action="append", default=[], help="Scene Zarr path to open.")
    parser.add_argument("--cube", action="append", default=[], help="Cube Zarr path to open.")
    parser.add_argument("--max-size", type=int, default=1400, help="Max preview width/height per layer.")
    parser.add_argument("--no-masks", action="store_true", help="Do not add masks/* label layers.")
    args = parser.parse_args()

    zarr_paths = [Path(item) for item in args.zarr]
    cube_paths = [Path(item) for item in args.cube]
    if args.job_id:
        job = _fetch_job(args.api_url, args.job_id)
        zarr_paths.extend(_map_data_path(item) for item in job.get("zarr_outputs") or [])
        cube_paths.extend(_map_data_path(item) for item in job.get("cube_outputs") or [])

    zarr_paths = _unique_existing(zarr_paths)
    cube_paths = _unique_existing(cube_paths)
    if not zarr_paths and not cube_paths:
        raise SystemExit("No existing Zarr outputs were found to open.")

    import napari

    viewer = napari.Viewer(title=f"Nimbus job {args.job_id or 'Zarr outputs'}")
    for path in zarr_paths:
        _add_scene(viewer, path, max_size=args.max_size, include_masks=not args.no_masks)
    for path in cube_paths:
        _add_cube(viewer, path, max_size=args.max_size)

    if viewer.layers:
        viewer.layers.selection.active = viewer.layers[0]
    print(
        f"Opened {len(viewer.layers)} Napari layers from {len(zarr_paths)} scenes and {len(cube_paths)} cubes.",
        flush=True,
    )
    from napari._qt import qt_event_loop

    qt_event_loop._ipython_has_eventloop = lambda: False
    napari.run(force=True, max_loop_level=10)


def _fetch_job(api_url: str, job_id: str) -> dict[str, Any]:
    url = f"{api_url.rstrip('/')}/v1/jobs/{job_id}"
    with urlopen(url, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _map_data_path(path: str) -> Path:
    value = Path(str(path))
    if value.is_absolute() and value.parts[:2] == ("/", "data"):
        return Path.cwd() / "data" / Path(*value.parts[2:])
    return value


def _unique_existing(paths: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    existing: list[Path] = []
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        existing.append(resolved)
    return existing


def _add_scene(viewer: Any, path: Path, *, max_size: int, include_masks: bool) -> None:
    group = zarr.open_group(str(path), mode="r", zarr_format=2)
    rgb, rgb_bands, stride = _render_rgb(group, max_size=max_size, time_index=0)
    scene_id = _scene_id(group, path)
    viewer.add_image(
        rgb,
        name=f"{scene_id} RGB ({', '.join(rgb_bands)})",
        rgb=True,
    )

    if include_masks and "masks" in group:
        masks_group = group["masks"]
        for mask_name in ("cloud", "water", "cloud_probability", "water_probability"):
            if mask_name not in masks_group:
                continue
            mask = np.asarray(masks_group[mask_name][0, ::stride, ::stride])
            if "probability" in mask_name:
                viewer.add_image(
                    mask.astype(np.float32, copy=False),
                    name=f"{scene_id} {mask_name}",
                    colormap="magma" if "cloud" in mask_name else "viridis",
                    opacity=0.35,
                    visible=False,
                )
            else:
                viewer.add_labels(
                    mask.astype(np.uint8, copy=False),
                    name=f"{scene_id} {mask_name}",
                    opacity=0.35,
                    visible=False,
                )


def _add_cube(viewer: Any, path: Path, *, max_size: int) -> None:
    group = zarr.open_group(str(path), mode="r", zarr_format=2)
    imagery = group["imagery"]
    time_count = int(imagery.shape[0])
    rgb_frames = []
    rgb_bands: list[str] = []
    for time_index in range(time_count):
        rgb, rgb_bands, _stride = _render_rgb(group, max_size=max_size, time_index=time_index)
        rgb_frames.append(rgb)
    stack = np.stack(rgb_frames, axis=0)
    viewer.add_image(
        stack,
        name=f"{path.stem} RGB cube ({', '.join(rgb_bands)})",
        rgb=True,
    )


def _render_rgb(group: Any, *, max_size: int, time_index: int) -> tuple[np.ndarray, list[str], int]:
    imagery = group["imagery"]
    band_names = _read_band_names(group)
    rgb_bands, _preset_name = choose_rgb_bands(
        provider=_clean_attr(group.attrs.get("provider")),
        collection=_clean_attr(group.attrs.get("collection")),
        product_type=_clean_attr(group.attrs.get("product_type")),
        band_names=band_names,
    )
    band_lookup = {name.upper(): index for index, name in enumerate(band_names)}
    indexes = [band_lookup[name.upper()] for name in rgb_bands]
    height = int(imagery.shape[2])
    width = int(imagery.shape[3])
    stride = max(1, int(math.ceil(max(height, width) / max(128, int(max_size)))))
    channels = [
        _stretch_channel(np.asarray(imagery[time_index, index, ::stride, ::stride], dtype=np.float32))
        for index in indexes
    ]
    return np.stack(channels, axis=-1), rgb_bands, stride


def _stretch_channel(channel: np.ndarray) -> np.ndarray:
    finite = channel[np.isfinite(channel)]
    positive = finite[finite > 0]
    sample = positive if positive.size >= max(16, finite.size // 20) else finite
    if sample.size == 0:
        return np.zeros(channel.shape, dtype=np.uint8)
    low, high = np.percentile(sample, [2, 98])
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        low = float(np.nanmin(sample))
        high = float(np.nanmax(sample))
    if high <= low:
        high = low + 1.0
    scaled = np.clip((channel - low) / (high - low), 0.0, 1.0)
    scaled[~np.isfinite(scaled)] = 0.0
    return (scaled * 255.0).astype(np.uint8)


def _read_band_names(group: Any) -> list[str]:
    if "band" in group:
        return [str(item) for item in np.asarray(group["band"][:]).tolist()]
    return [str(item) for item in list(group.attrs.get("band_names") or [])]


def _scene_id(group: Any, path: Path) -> str:
    return _clean_attr(group.attrs.get("scene_id")) or path.stem


def _clean_attr(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


if __name__ == "__main__":
    main()
