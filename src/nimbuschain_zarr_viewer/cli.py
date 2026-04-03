from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dask.array as da
import napari
import zarr


MAX_SAFE_VIEW_SIZE = 12000
PRESET_CHOICES = ("rgb", "rgb-masks", "all-bands", "masks-only")


@dataclass(frozen=True)
class StoreSummary:
    path: Path
    band_names: list[str]
    ancillary_names: list[str]
    mask_layers: list[str]
    imagery_shape: tuple[int, ...]


@dataclass(frozen=True)
class ViewerOptions:
    show_bands: bool
    show_rgb: bool
    show_ancillary: bool
    show_masks: bool


def _normalize_name(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode(errors="replace")
    return str(value)


def _read_label_array(group: zarr.Group, key: str) -> list[str]:
    if key not in group:
        return []
    values = group[key][:]
    try:
        items = values.tolist()
    except Exception:
        items = list(values)
    return [_normalize_name(item) for item in items]


def summarize_store(path: str | Path) -> StoreSummary:
    store_path = Path(path).expanduser().resolve()
    group = zarr.open_group(store_path, mode="r", use_consolidated=False)
    band_names = _read_label_array(group, "band")
    ancillary_names = _read_label_array(group, "ancillary_layer")
    mask_layers: list[str] = []
    if "masks" in group:
        mask_group = group["masks"]
        for label in ("cloud", "cloud_probability", "water", "water_probability"):
            if label in mask_group:
                mask_layers.append(label)
    imagery_shape = tuple(group["imagery"].shape) if "imagery" in group else ()
    return StoreSummary(
        path=store_path,
        band_names=band_names,
        ancillary_names=ancillary_names,
        mask_layers=mask_layers,
        imagery_shape=imagery_shape,
    )


def _default_rgb_bands(band_names: list[str]) -> tuple[str, str, str] | None:
    candidates = [
        ("B04", "B03", "B02"),
        ("B4", "B3", "B2"),
        ("SR_B4", "SR_B3", "SR_B2"),
    ]
    available = set(band_names)
    for triplet in candidates:
        if all(label in available for label in triplet):
            return triplet
    return None


def _resolve_step(shape: tuple[int, ...], requested_step: int | None) -> int:
    if requested_step and requested_step > 0:
        return requested_step
    if len(shape) < 2:
        return 1
    height = int(shape[-2] or 0)
    width = int(shape[-1] or 0)
    longest = max(height, width)
    if longest <= MAX_SAFE_VIEW_SIZE:
        return 1
    return max(1, int(math.ceil(longest / MAX_SAFE_VIEW_SIZE)))


def preset_viewer_options(preset: str) -> ViewerOptions:
    normalized = str(preset or "").strip().lower()
    if normalized == "rgb":
        return ViewerOptions(
            show_bands=False,
            show_rgb=True,
            show_ancillary=False,
            show_masks=False,
        )
    if normalized == "rgb-masks":
        return ViewerOptions(
            show_bands=False,
            show_rgb=True,
            show_ancillary=False,
            show_masks=True,
        )
    if normalized == "masks-only":
        return ViewerOptions(
            show_bands=False,
            show_rgb=False,
            show_ancillary=False,
            show_masks=True,
        )
    return ViewerOptions(
        show_bands=True,
        show_rgb=True,
        show_ancillary=True,
        show_masks=True,
    )


def default_zarr_root() -> Path:
    env_root = str(os.environ.get("NIMBUS_ZARR_ROOT") or "").strip()
    if env_root:
        return Path(env_root).expanduser()
    return Path(__file__).resolve().parents[2] / "data" / "downloads" / "zarr"


def discover_local_zarr_stores(root: str | Path | None = None) -> list[Path]:
    search_root = Path(root or default_zarr_root()).expanduser().resolve()
    if not search_root.exists():
        return []
    stores = [path for path in search_root.glob("*.zarr") if path.is_dir()]
    stores.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return stores


def build_launch_command(
    store_path: str | Path,
    *,
    preset: str = "all-bands",
    step: int | None = None,
    grid: bool = False,
    python_executable: str | None = None,
) -> list[str]:
    repo_root = Path(__file__).resolve().parents[2]
    executable = python_executable or sys.executable
    command = [
        executable,
        str(repo_root / "scripts" / "open_zarr_napari.py"),
        str(Path(store_path).expanduser().resolve()),
        "--preset",
        str(preset or "all-bands"),
    ]
    if step and step > 0:
        command.extend(["--step", str(int(step))])
    if grid:
        command.append("--grid")
    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nimbuschain-zarr-viewer",
        description="Open a NimbusChain Zarr store in Napari.",
    )
    parser.add_argument("path", help="Path to the Zarr store.")
    parser.add_argument(
        "--preset",
        choices=list(PRESET_CHOICES),
        default="all-bands",
        help="Viewer preset: rgb, rgb-masks, all-bands, or masks-only.",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=0,
        help="Downsample step applied to y/x for safer previews. Default: auto.",
    )
    parser.add_argument(
        "--no-rgb",
        action="store_true",
        help="Do not add an RGB composite layer.",
    )
    parser.add_argument(
        "--no-ancillary",
        action="store_true",
        help="Do not add ancillary layers.",
    )
    parser.add_argument(
        "--no-masks",
        action="store_true",
        help="Do not add masks and probability layers.",
    )
    parser.add_argument(
        "--grid",
        action="store_true",
        help="Enable Napari grid view at startup.",
    )
    return parser


def open_store_in_napari(
    path: str | Path,
    *,
    preset: str = "all-bands",
    step: int | None = None,
    show_bands: bool | None = None,
    show_rgb: bool | None = None,
    show_ancillary: bool | None = None,
    show_masks: bool | None = None,
    grid: bool = False,
) -> StoreSummary:
    summary = summarize_store(path)
    group = zarr.open_group(summary.path, mode="r", use_consolidated=False)
    imagery = da.from_zarr(group["imagery"])[0]
    preview_step = _resolve_step(tuple(imagery.shape), step)
    y_slice = slice(None, None, preview_step)
    x_slice = slice(None, None, preview_step)
    preset_options = preset_viewer_options(preset)
    bands_enabled = preset_options.show_bands if show_bands is None else bool(show_bands)
    rgb_enabled = preset_options.show_rgb if show_rgb is None else bool(show_rgb)
    ancillary_enabled = preset_options.show_ancillary if show_ancillary is None else bool(show_ancillary)
    masks_enabled = preset_options.show_masks if show_masks is None else bool(show_masks)

    viewer = napari.Viewer(title=f"NimbusChain Zarr Viewer — {summary.path.name}")
    if bands_enabled:
        viewer.add_image(
            imagery[:, y_slice, x_slice],
            channel_axis=0,
            name=summary.band_names or None,
        )

    if rgb_enabled:
        rgb_triplet = _default_rgb_bands(summary.band_names)
        if rgb_triplet is not None:
            band_index = {label: idx for idx, label in enumerate(summary.band_names)}
            rgb = da.stack(
                [
                    imagery[band_index[rgb_triplet[0]], y_slice, x_slice],
                    imagery[band_index[rgb_triplet[1]], y_slice, x_slice],
                    imagery[band_index[rgb_triplet[2]], y_slice, x_slice],
                ],
                axis=-1,
            )
            viewer.add_image(rgb, rgb=True, name="RGB")

    if ancillary_enabled and "ancillary" in group and summary.ancillary_names:
        ancillary = da.from_zarr(group["ancillary"])[0]
        viewer.add_image(
            ancillary[:, y_slice, x_slice],
            channel_axis=0,
            name=summary.ancillary_names,
        )

    if masks_enabled and "masks" in group:
        mask_group = group["masks"]
        if "cloud" in mask_group:
            viewer.add_labels(
                da.from_zarr(mask_group["cloud"])[0, y_slice, x_slice].astype("uint8"),
                name="cloud_mask",
            )
        if "water" in mask_group:
            viewer.add_labels(
                da.from_zarr(mask_group["water"])[0, y_slice, x_slice].astype("uint8"),
                name="water_mask",
            )
        if "cloud_probability" in mask_group:
            viewer.add_image(
                da.from_zarr(mask_group["cloud_probability"])[0, y_slice, x_slice],
                name="cloud_probability",
                opacity=0.35,
                colormap="magma",
                visible=False,
            )
        if "water_probability" in mask_group:
            viewer.add_image(
                da.from_zarr(mask_group["water_probability"])[0, y_slice, x_slice],
                name="water_probability",
                opacity=0.35,
                colormap="viridis",
                visible=False,
            )

    viewer.grid.enabled = bool(grid)

    print(f"Opened: {summary.path}")
    print(f"Bands: {', '.join(summary.band_names) if summary.band_names else '-'}")
    print(
        "Ancillary: "
        + (", ".join(summary.ancillary_names) if summary.ancillary_names else "-")
    )
    print(f"Masks: {', '.join(summary.mask_layers) if summary.mask_layers else '-'}")
    print(f"Preset: {preset}")
    print(f"Preview step: {preview_step}x")

    napari.run()
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    open_store_in_napari(
        args.path,
        preset=args.preset,
        step=args.step or None,
        show_rgb=False if args.no_rgb else None,
        show_ancillary=False if args.no_ancillary else None,
        show_masks=False if args.no_masks else None,
        grid=args.grid,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
