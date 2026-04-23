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
import numpy as np
import zarr


MAX_SAFE_VIEW_SIZE = 12000
PRESET_CHOICES = ("rgb", "rgb-masks", "all-bands", "masks-only")


@dataclass(frozen=True)
class StoreSummary:
    path: Path
    provider: str | None
    collection: str | None
    product_type: str | None
    band_names: list[str]
    ancillary_names: list[str]
    mask_layers: list[str]
    mask_state_label: str
    imagery_shape: tuple[int, ...]


@dataclass(frozen=True)
class ViewerOptions:
    show_bands: bool
    show_rgb: bool
    show_ancillary: bool
    show_masks: bool


@dataclass(frozen=True)
class SensorDisplayConfig:
    sensor_key: str
    scale_hint: str | None
    rgb_bands: tuple[str, str, str] | None
    rgb_gamma: float
    rgb_percentiles: tuple[float, float]


def mask_state_label(mask_layers: list[str]) -> str:
    available = {str(item).strip().lower() for item in mask_layers if str(item).strip()}
    has_water = "water" in available
    has_cloud = "cloud" in available
    has_water_probability = "water_probability" in available
    has_cloud_probability = "cloud_probability" in available
    if has_water and has_cloud:
        return "Water + cloud"
    if has_water:
        return "Water only"
    if has_cloud:
        return "Cloud only"
    if has_water_probability or has_cloud_probability:
        return "Probability layers only"
    if available:
        return "Partial / irregular"
    return "Zarr only"


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
        provider=str(group.attrs.get("provider") or "").strip() or None,
        collection=str(group.attrs.get("collection") or "").strip() or None,
        product_type=str(group.attrs.get("product_type") or "").strip() or None,
        band_names=band_names,
        ancillary_names=ancillary_names,
        mask_layers=mask_layers,
        mask_state_label=mask_state_label(mask_layers),
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


def resolve_sensor_display_config(
    *,
    provider: str | None,
    collection: str | None,
    product_type: str | None,
    band_names: list[str],
) -> SensorDisplayConfig:
    provider_name = str(provider or "").strip().lower()
    collection_name = str(collection or "").strip().upper()
    product_name = str(product_type or "").strip().upper()
    available = set(band_names)

    if provider_name == "copernicus" and collection_name == "SENTINEL-2":
        rgb_bands = ("B04", "B03", "B02") if {"B04", "B03", "B02"} <= available else _default_rgb_bands(band_names)
        return SensorDisplayConfig(
            sensor_key="sentinel-2",
            scale_hint="reflectance_0_10000",
            rgb_bands=rgb_bands,
            rgb_gamma=0.95,
            rgb_percentiles=(2.0, 98.5),
        )

    if provider_name == "usgs" and collection_name == "LANDSAT_OT_C2_L1":
        rgb_bands = ("B4", "B3", "B2") if {"B4", "B3", "B2"} <= available else _default_rgb_bands(band_names)
        return SensorDisplayConfig(
            sensor_key="landsat-8-9-l1",
            scale_hint="landsat_l1_reflectance",
            rgb_bands=rgb_bands,
            rgb_gamma=0.95,
            rgb_percentiles=(2.0, 98.5),
        )

    if provider_name == "usgs" and collection_name == "LANDSAT_OT_C2_L2":
        rgb_bands = ("SR_B4", "SR_B3", "SR_B2") if {"SR_B4", "SR_B3", "SR_B2"} <= available else _default_rgb_bands(band_names)
        return SensorDisplayConfig(
            sensor_key="landsat-8-9-l2",
            scale_hint="landsat_l2_reflectance",
            rgb_bands=rgb_bands,
            rgb_gamma=0.95,
            rgb_percentiles=(2.0, 98.5),
        )

    rgb_bands = _default_rgb_bands(band_names)
    fallback_scale = None
    if product_name in {"S2MSI1C", "S2MSI2A"} or {"B04", "B03", "B02"} <= available:
        fallback_scale = "reflectance_0_10000"
    elif {"B4", "B3", "B2"} <= available:
        fallback_scale = "landsat_l1_reflectance"
    elif {"SR_B4", "SR_B3", "SR_B2"} <= available:
        fallback_scale = "landsat_l2_reflectance"
    return SensorDisplayConfig(
        sensor_key="generic",
        scale_hint=fallback_scale,
        rgb_bands=rgb_bands,
        rgb_gamma=1.0,
        rgb_percentiles=(2.0, 98.0),
    )


def _coerce_numeric_metadata(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
    if not np.isfinite(numeric):
        return None
    integer = int(numeric)
    if abs(numeric - integer) < 1e-9:
        return integer
    return numeric


def _resolve_landsat_scaling(
    *,
    scale_hint: str,
    band_name: str,
    root_attrs: dict[str, Any],
) -> dict[str, float | bool]:
    radiometry = dict(root_attrs.get("radiometric_metadata") or {})
    band_scaling = dict((radiometry.get("bands") or {}).get(str(band_name or ""), {}) or {})
    if band_scaling:
        return {
            "mult": float(band_scaling.get("mult") or 0.0),
            "add": float(band_scaling.get("add") or 0.0),
            "apply_sun_elevation": bool(band_scaling.get("apply_sun_elevation")),
            "sun_elevation": float(radiometry.get("sun_elevation") or 0.0),
        }
    if scale_hint == "landsat_l1_reflectance":
        return {
            "mult": 2.0e-5,
            "add": -0.1,
            "apply_sun_elevation": True,
            "sun_elevation": float(root_attrs.get("sun_elevation") or 45.0),
        }
    return {
        "mult": 2.75e-5,
        "add": -0.2,
        "apply_sun_elevation": False,
        "sun_elevation": 0.0,
    }


def _valid_pixels_for_display(
    array: da.Array,
    *,
    band_name: str,
    root_attrs: dict[str, Any],
) -> da.Array:
    band_metadata = dict((root_attrs.get("band_metadata") or {}).get(str(band_name), {}) or {})
    valid = da.isfinite(array)
    for key in ("target_nodata", "source_nodata"):
        nodata = _coerce_numeric_metadata(band_metadata.get(key))
        if nodata is None:
            continue
        valid = da.logical_and(valid, array != float(nodata))
    return valid


def _normalize_channel_for_display(
    array: da.Array,
    *,
    scale_hint: str | None,
    band_name: str,
    root_attrs: dict[str, Any],
) -> da.Array:
    normalized = array.astype(np.float32)
    if scale_hint == "reflectance_0_10000":
        sampled = np.asarray(
            normalized[
                ::_sample_stride(int(normalized.shape[-2] or 0), max_side=512),
                ::_sample_stride(int(normalized.shape[-1] or 0), max_side=512),
            ].compute(),
            dtype=np.float32,
        )
        finite = sampled[np.isfinite(sampled)]
        if finite.size and float(np.nanmax(finite)) > 1.5:
            normalized = normalized / 10000.0
        normalized = da.clip(normalized, 0.0, 1.0)
    elif scale_hint in {"landsat_l1_reflectance", "landsat_l2_reflectance"}:
        scaling = _resolve_landsat_scaling(
            scale_hint=scale_hint,
            band_name=band_name,
            root_attrs=root_attrs,
        )
        normalized = normalized * float(scaling["mult"]) + float(scaling["add"])
        if bool(scaling.get("apply_sun_elevation")):
            sun_elevation = float(scaling.get("sun_elevation") or 0.0)
            if 0.0 < sun_elevation < 90.0:
                normalized = normalized / max(math.sin(math.radians(sun_elevation)), 1e-3)
        normalized = da.clip(normalized, 0.0, 1.0)
    return normalized


def _sample_stride(length: int, max_side: int = 1024) -> int:
    if length <= 0:
        return 1
    return max(1, int(math.ceil(length / max_side)))


def _sample_percentile_limits(
    array: da.Array,
    *,
    valid_mask: da.Array | None = None,
    low_percentile: float = 2.0,
    high_percentile: float = 98.0,
) -> tuple[float, float]:
    height = int(array.shape[-2] or 0)
    width = int(array.shape[-1] or 0)
    y_step = _sample_stride(height)
    x_step = _sample_stride(width)
    sampled = np.asarray(array[::y_step, ::x_step].compute(), dtype=np.float32)
    if valid_mask is not None:
        sampled_valid = np.asarray(valid_mask[::y_step, ::x_step].compute(), dtype=bool)
        sampled = sampled[np.logical_and(np.isfinite(sampled), sampled_valid)]
    else:
        sampled = sampled[np.isfinite(sampled)]
    if sampled.size == 0:
        return (0.0, 1.0)
    low = float(np.percentile(sampled, low_percentile))
    high = float(np.percentile(sampled, high_percentile))
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        high = low + 1.0
    return (low, high)


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
        help="Do not add in-place mask and probability layers from masks/...",
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
    root_attrs = dict(group.attrs)
    display_config = resolve_sensor_display_config(
        provider=summary.provider,
        collection=summary.collection,
        product_type=summary.product_type,
        band_names=summary.band_names,
    )
    imagery = da.from_zarr(group["imagery"])[0]
    preview_step = _resolve_step(tuple(imagery.shape), step)
    y_slice = slice(None, None, preview_step)
    x_slice = slice(None, None, preview_step)
    preset_options = preset_viewer_options(preset)
    bands_enabled = preset_options.show_bands if show_bands is None else bool(show_bands)
    rgb_enabled = preset_options.show_rgb if show_rgb is None else bool(show_rgb)
    ancillary_enabled = preset_options.show_ancillary if show_ancillary is None else bool(show_ancillary)
    masks_enabled = preset_options.show_masks if show_masks is None else bool(show_masks)

    viewer = napari.Viewer(title=f"Nimbus Pipeline Zarr Viewer — {summary.path.name}")
    if bands_enabled:
        viewer.add_image(
            imagery[:, y_slice, x_slice],
            channel_axis=0,
            name=summary.band_names or None,
        )

    if rgb_enabled:
        rgb_triplet = display_config.rgb_bands
        if rgb_triplet is not None:
            band_index = {label: idx for idx, label in enumerate(summary.band_names)}
            rgb_channels: list[da.Array] = []
            rgb_masks: list[da.Array] = []
            for band_name in rgb_triplet:
                band_array = imagery[band_index[band_name], y_slice, x_slice]
                rgb_masks.append(
                    _valid_pixels_for_display(
                        band_array,
                        band_name=band_name,
                        root_attrs=root_attrs,
                    )
                )
                rgb_channels.append(
                    _normalize_channel_for_display(
                        band_array,
                        scale_hint=display_config.scale_hint,
                        band_name=band_name,
                        root_attrs=root_attrs,
                    )
                )
            rgb_valid = da.stack(rgb_masks, axis=0).any(axis=0) if rgb_masks else None
            rgb = da.stack(
                [
                    da.where(rgb_valid, rgb_channels[0], 0.0) if rgb_valid is not None else rgb_channels[0],
                    da.where(rgb_valid, rgb_channels[1], 0.0) if rgb_valid is not None else rgb_channels[1],
                    da.where(rgb_valid, rgb_channels[2], 0.0) if rgb_valid is not None else rgb_channels[2],
                ],
                axis=-1,
            )
            low_limits: list[float] = []
            high_limits: list[float] = []
            for channel_array, valid_mask in zip(rgb_channels, rgb_masks):
                low, high = _sample_percentile_limits(
                    channel_array,
                    valid_mask=valid_mask,
                    low_percentile=display_config.rgb_percentiles[0],
                    high_percentile=display_config.rgb_percentiles[1],
                )
                low_limits.append(low)
                high_limits.append(high)
            viewer.add_image(
                rgb,
                rgb=True,
                name="RGB",
                contrast_limits=(min(low_limits), max(high_limits)),
                gamma=display_config.rgb_gamma,
            )

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
            viewer.add_image(
                da.from_zarr(mask_group["cloud"])[0, y_slice, x_slice].astype(np.float32),
                name="cloud_mask",
                contrast_limits=(0.0, 1.0),
                opacity=0.42,
                colormap="yellow",
                blending="translucent",
            )
        if "water" in mask_group:
            viewer.add_image(
                da.from_zarr(mask_group["water"])[0, y_slice, x_slice].astype(np.float32),
                name="water_mask",
                contrast_limits=(0.0, 1.0),
                opacity=0.42,
                colormap="cyan",
                blending="translucent",
            )
        if "cloud_probability" in mask_group:
            viewer.add_image(
                da.from_zarr(mask_group["cloud_probability"])[0, y_slice, x_slice],
                name="cloud_probability",
                contrast_limits=(0.0, 1.0),
                opacity=0.35,
                colormap="magma",
                visible=False,
            )
        if "water_probability" in mask_group:
            viewer.add_image(
                da.from_zarr(mask_group["water_probability"])[0, y_slice, x_slice],
                name="water_probability",
                contrast_limits=(0.0, 1.0),
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
    print(f"Mask state: {summary.mask_state_label}")
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
