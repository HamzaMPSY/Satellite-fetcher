from __future__ import annotations

import argparse
import math
from pathlib import Path

import napari
import numpy as np
import zarr


ROOT = Path(__file__).resolve().parents[1]
MAX_SAFE_VIEW_SIZE = 12000

DEFAULT_SCENES = {
    "20260422": ROOT / "data/downloads/zarr/S2L_MSIL2F_20260422T075243_N0500_R000_T37REP_20260526T173021.zarr",
    "20260430": ROOT / "data/downloads/zarr/S2L_MSIL2F_20260430T075226_N0500_R000_T37REP_20260526T171058.zarr",
    "water_cloud": ROOT
    / "data/downloads/zarr/S2L_MSIL2F_20260516T080050_N0500_R000_T36MVD_20260526T220503.zarr",
}


def _band_names(group: zarr.Group) -> list[str]:
    names: list[str] = []
    for value in group["band"][:]:
        names.append(value.decode("utf-8") if isinstance(value, bytes) else str(value))
    return names


def _sample_stride(length: int, max_side: int = 1024) -> int:
    if length <= 0:
        return 1
    return max(1, int(math.ceil(length / max_side)))


def _resolve_step(shape: tuple[int, int], requested_step: int | None) -> int:
    if requested_step and requested_step > 0:
        return requested_step
    longest = max(int(shape[0] or 0), int(shape[1] or 0))
    if longest <= MAX_SAFE_VIEW_SIZE:
        return 1
    return max(1, int(math.ceil(longest / MAX_SAFE_VIEW_SIZE)))


def _window_from_crop(
    *,
    height: int,
    width: int,
    crop: list[int] | None,
    step: int,
) -> tuple[slice, slice, str]:
    if not crop:
        return slice(None, None, step), slice(None, None, step), "full scene"
    row_start, row_stop, col_start, col_stop = [int(value) for value in crop]
    row_start = max(0, min(height, row_start))
    row_stop = max(row_start + 1, min(height, row_stop))
    col_start = max(0, min(width, col_start))
    col_stop = max(col_start + 1, min(width, col_stop))
    label = f"crop y={row_start}:{row_stop}, x={col_start}:{col_stop}"
    return slice(row_start, row_stop, step), slice(col_start, col_stop, step), label


def _auto_crop(
    group: zarr.Group,
    *,
    crop_size: int,
    sample_step: int = 64,
) -> list[int]:
    water = np.asarray(group["masks/water"][0, ::sample_step, ::sample_step], dtype=np.float32)
    cloud = np.asarray(group["masks/cloud"][0, ::sample_step, ::sample_step], dtype=np.float32)
    height = int(group["imagery"].shape[2])
    width = int(group["imagery"].shape[3])
    coarse_window = max(1, min(water.shape[0], water.shape[1], int(math.ceil(crop_size / sample_step))))
    stride = max(1, coarse_window // 4)

    best_score = -1.0
    best_row = 0
    best_col = 0
    for row in range(0, max(1, water.shape[0] - coarse_window + 1), stride):
        for col in range(0, max(1, water.shape[1] - coarse_window + 1), stride):
            water_view = water[row : row + coarse_window, col : col + coarse_window]
            cloud_view = cloud[row : row + coarse_window, col : col + coarse_window]
            water_fraction = float(water_view.mean()) if water_view.size else 0.0
            cloud_fraction = float(cloud_view.mean()) if cloud_view.size else 0.0
            score = min(water_fraction, 1.0 - water_fraction) * 1.4
            score += min(cloud_fraction, 1.0 - cloud_fraction)
            score += 0.4 if water_fraction > 0.05 and cloud_fraction > 0.05 else 0.0
            if score > best_score:
                best_score = score
                best_row = row
                best_col = col

    row_start = min(max(0, best_row * sample_step), max(0, height - crop_size))
    col_start = min(max(0, best_col * sample_step), max(0, width - crop_size))
    return [
        int(row_start),
        int(min(height, row_start + crop_size)),
        int(col_start),
        int(min(width, col_start + crop_size)),
    ]


def _stretch_channel(array: np.ndarray) -> np.ndarray:
    data = np.asarray(array, dtype=np.float32)
    y_step = _sample_stride(int(data.shape[-2] or 0))
    x_step = _sample_stride(int(data.shape[-1] or 0))
    sampled = data[::y_step, ::x_step]
    finite = sampled[np.isfinite(sampled) & (sampled > 0)]
    if finite.size == 0:
        finite = sampled[np.isfinite(sampled)]
    if finite.size == 0:
        return np.zeros_like(data, dtype=np.float32)
    low, high = np.percentile(finite, (2.0, 98.0))
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        low = float(finite.min())
        high = float(finite.max())
    if high <= low:
        return np.zeros_like(data, dtype=np.float32)
    return np.clip((data - float(low)) / float(high - low), 0.0, 1.0).astype(np.float32, copy=False)


def _rgb_layer(group: zarr.Group, indexes: list[int], *, y_slice: slice, x_slice: slice) -> np.ndarray:
    imagery = group["imagery"]
    channels: list[np.ndarray] = []
    for index in indexes:
        band = np.asarray(imagery[0, index, y_slice, x_slice], dtype=np.float32)
        channels.append(_stretch_channel(band))
    return np.stack(channels, axis=-1)


def _mask_layer(group: zarr.Group, name: str, *, y_slice: slice, x_slice: slice) -> np.ndarray:
    return np.asarray(group[f"masks/{name}"][0, y_slice, x_slice], dtype=np.float32)


def _load_scene(
    viewer: napari.Viewer,
    path: Path,
    *,
    requested_step: int | None,
    crop: list[int] | None,
    auto_crop: bool,
    crop_size: int,
) -> None:
    group = zarr.open_group(path, mode="r")
    bands = _band_names(group)
    band_index = {band: index for index, band in enumerate(bands)}
    rgb_indexes = [band_index[band] for band in ("B04", "B03", "B02")]
    height = int(group["imagery"].shape[2])
    width = int(group["imagery"].shape[3])
    if auto_crop and crop is None:
        crop = _auto_crop(group, crop_size=max(512, int(crop_size)))
    step = _resolve_step((height, width), requested_step)
    y_slice, x_slice, view_label = _window_from_crop(
        height=height,
        width=width,
        crop=crop,
        step=step,
    )

    rgb = _rgb_layer(group, rgb_indexes, y_slice=y_slice, x_slice=x_slice)
    cloud = _mask_layer(group, "cloud", y_slice=y_slice, x_slice=x_slice)
    water = _mask_layer(group, "water", y_slice=y_slice, x_slice=x_slice)
    print(
        f"Loaded {view_label}; step={step}; "
        f"rgb_shape={rgb.shape}; water_fraction={float(water.mean()):.4f}; "
        f"cloud_fraction={float(cloud.mean()):.4f}",
        flush=True,
    )

    scene_name = path.name.removesuffix(".zarr")
    viewer.add_image(
        rgb,
        name=f"{scene_name} RGB B04/B03/B02 stretched",
        rgb=True,
        contrast_limits=(0.0, 1.0),
    )
    viewer.add_image(
        cloud,
        name="cloud mask",
        colormap="yellow",
        contrast_limits=(0, 1),
        opacity=0.42,
        blending="translucent",
    )
    viewer.add_image(
        water,
        name="water mask",
        colormap="cyan",
        contrast_limits=(0, 1),
        opacity=0.50,
        blending="translucent",
    )
    viewer.text_overlay.visible = True
    viewer.text_overlay.text = f"{view_label}; display step={step}x; source masks are {height}x{width}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Open one NimbusChain Landsat/Sen2Like scene in Napari.")
    parser.add_argument(
        "--scene",
        choices=sorted(DEFAULT_SCENES),
        default="water_cloud",
        help="Known E2E scene to open.",
    )
    parser.add_argument("--zarr", default=None, help="Override with an explicit Zarr path.")
    parser.add_argument(
        "--overview-step",
        type=int,
        default=0,
        help="Read every Nth pixel. 0 = auto, which keeps 10980x10980 scenes full-res.",
    )
    parser.add_argument(
        "--crop",
        nargs=4,
        type=int,
        metavar=("Y0", "Y1", "X0", "X1"),
        default=None,
        help="Open a full-resolution crop window.",
    )
    parser.add_argument(
        "--auto-crop",
        action="store_true",
        help="Pick a full-resolution crop containing water/cloud variation.",
    )
    parser.add_argument(
        "--crop-size",
        type=int,
        default=4096,
        help="Crop side length used with --auto-crop.",
    )
    args = parser.parse_args()

    path = Path(args.zarr).expanduser().resolve() if args.zarr else DEFAULT_SCENES[args.scene]
    if not path.exists():
        raise FileNotFoundError(f"Missing Zarr scene: {path}")

    viewer = napari.Viewer(title=f"NimbusChain scene: {path.name}")
    _load_scene(
        viewer,
        path,
        requested_step=int(args.overview_step) if int(args.overview_step) > 0 else None,
        crop=args.crop,
        auto_crop=bool(args.auto_crop),
        crop_size=int(args.crop_size),
    )
    viewer.dims.ndisplay = 2
    napari.run()


if __name__ == "__main__":
    main()
