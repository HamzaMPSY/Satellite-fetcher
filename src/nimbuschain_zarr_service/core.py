from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import unquote, urlparse
import os
import shutil
import tarfile
import zipfile

import numpy as np

from nimbuschain_zarr_service.oci_storage import (
    OCIStorageError,
    OCIStore,
    is_oci_uri,
)
from nimbuschain_zarr_service.schema import ChunkShape, ZARR_FORMAT_VERSION
from nimbuschain_zarr_service.utils.tile_math import TileMath


class ConversionError(ValueError):
    """Raised when the raw product cannot be converted."""


class ConversionDependencyError(RuntimeError):
    """Raised when a required runtime dependency is missing."""


class CleanupBundle:
    """Composable cleanup handler for staged and extracted temporary sources."""

    def __init__(self, *entries: TemporaryDirectory[str]) -> None:
        self._entries: list[TemporaryDirectory[str]] = list(entries)

    def add(self, entry: TemporaryDirectory[str]) -> None:
        self._entries.append(entry)

    def cleanup(self) -> None:
        while self._entries:
            entry = self._entries.pop()
            try:
                entry.cleanup()
            except Exception:
                continue


@dataclass(frozen=True)
class PreparedSource:
    root: Path
    source_kind: str
    raw_path: Path
    cleanup: CleanupBundle | TemporaryDirectory[str] | None = None


@dataclass(frozen=True)
class TargetGrid:
    height: int
    width: int
    crs: str | None
    transform: list[float] | tuple[float, ...]
    pixel_size: list[float] | tuple[float, ...] | None = None
    reference_band: str | None = None


def resolve_local_path(raw_uri: str) -> Path:
    if raw_uri.startswith("file://"):
        parsed = urlparse(raw_uri)
        candidate = Path(unquote(parsed.path)).expanduser().resolve()
        if candidate.exists():
            return candidate
        mapped = _fallback_mounted_data_path(candidate)
        if mapped is not None:
            return mapped
        return candidate
    parsed = urlparse(raw_uri)
    if parsed.scheme:
        raise ConversionError(
            "Only local file paths are supported by the Zarr converter in v1."
        )
    candidate = Path(raw_uri).expanduser().resolve()
    if candidate.exists():
        return candidate
    mapped = _fallback_mounted_data_path(candidate)
    if mapped is not None:
        return mapped
    return candidate


def prepare_source(raw_uri: str, *, label: str) -> PreparedSource:
    cleanup_bundle: CleanupBundle | None = None
    if _is_remote_uri(raw_uri):
        raw_path, cleanup_bundle = _stage_remote_source(raw_uri, label=label)
    else:
        raw_path = resolve_local_path(raw_uri)
    if not raw_path.exists():
        raise ConversionError(f"{label} source not found: {raw_path}")

    if raw_path.is_dir():
        return PreparedSource(
            root=raw_path,
            source_kind="directory",
            raw_path=raw_path,
            cleanup=cleanup_bundle,
        )

    if raw_path.is_file() and raw_path.suffix.lower() == ".nc":
        if cleanup_bundle is not None:
            return PreparedSource(
                root=raw_path.parent,
                source_kind="netcdf",
                raw_path=raw_path,
                cleanup=cleanup_bundle,
            )
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        copied = Path(tmp_dir.name) / raw_path.name
        shutil.copy2(raw_path, copied)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="netcdf",
            raw_path=raw_path,
            cleanup=CleanupBundle(tmp_dir),
        )

    if zipfile.is_zipfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with zipfile.ZipFile(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        cleanup = cleanup_bundle or CleanupBundle()
        cleanup.add(tmp_dir)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="zip",
            raw_path=raw_path,
            cleanup=cleanup,
        )

    if tarfile.is_tarfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with tarfile.open(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        cleanup = cleanup_bundle or CleanupBundle()
        cleanup.add(tmp_dir)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="tar",
            raw_path=raw_path,
            cleanup=cleanup,
        )

    raise ConversionError(
        f"Unsupported {label} source. Expected a directory, zip, or tar archive."
    )


def load_aligned_raster_stack(
    band_paths: dict[str, Path],
    *,
    ordered_bands: list[str],
    reference_band: str | None = None,
    categorical_bands: set[str] | None = None,
    target_pixel_size: float | None = None,
    target_grid: TargetGrid | None = None,
) -> dict[str, Any]:
    try:
        import rasterio
        from rasterio.errors import RasterioIOError
        from rasterio.enums import Resampling
        from rasterio.transform import array_bounds, from_origin
        from rasterio.warp import reproject
    except ImportError as exc:
        raise ConversionDependencyError(
            "Raster support is not available in the zarr-service runtime "
            f"({exc}). Ensure rasterio and its system libraries are installed."
        ) from exc

    categorical_bands = categorical_bands or set()

    if not ordered_bands:
        raise ConversionError("No bands were selected for raster conversion.")

    ref_name = reference_band if reference_band in band_paths else ordered_bands[0]
    if ref_name not in band_paths and target_grid is None:
        raise ConversionError(f"Reference band '{ref_name}' is not available.")

    if target_grid is not None:
        ref_height = int(target_grid.height)
        ref_width = int(target_grid.width)
        ref_crs = target_grid.crs
        ref_transform = rasterio.Affine(*list(target_grid.transform)[:6])
        ref_pixel_size = (
            [float(v) for v in target_grid.pixel_size[:2]]
            if target_grid.pixel_size is not None
            else [float(ref_transform.a), float(abs(ref_transform.e))]
        )
        native_ref_pixel_size = ref_pixel_size
    else:
        try:
            ref_ctx = rasterio.open(band_paths[ref_name])
        except RasterioIOError as exc:
            raise ConversionError(
                f"Reference raster for band '{ref_name}' is not readable by rasterio: {band_paths[ref_name]}"
            ) from exc

        with ref_ctx as ref_src:
            ref_height = ref_src.height
            ref_width = ref_src.width
            ref_crs = ref_src.crs.to_string() if ref_src.crs else None
            ref_transform = ref_src.transform
            native_ref_pixel_size = [float(ref_transform.a), float(abs(ref_transform.e))]

            if target_pixel_size is not None:
                left, bottom, right, top = array_bounds(ref_height, ref_width, ref_transform)
                resolution = float(target_pixel_size)
                ref_width = max(1, int(np.ceil((right - left) / resolution)))
                ref_height = max(1, int(np.ceil((top - bottom) / resolution)))
                ref_transform = from_origin(left, top, resolution, resolution)
                ref_pixel_size = [resolution, resolution]
            else:
                ref_pixel_size = native_ref_pixel_size

    arrays: list[np.ndarray] = []
    available_bands: list[str] = []
    band_metadata: dict[str, dict[str, Any]] = {}
    for band_name in ordered_bands:
        band_path = band_paths.get(band_name)
        if band_path is None:
            continue

        try:
            src_ctx = rasterio.open(band_path)
        except RasterioIOError as exc:
            raise ConversionError(
                f"Raster band '{band_name}' is not readable by rasterio: {band_path}"
            ) from exc

        with src_ctx as src:
            src_crs = src.crs.to_string() if src.crs else None
            src_transform = src.transform
            src_pixel_size = [float(src_transform.a), float(abs(src_transform.e))]
            resampled = not (
                src.height == ref_height
                and src.width == ref_width
                and src_crs == ref_crs
                and tuple(src.transform) == tuple(ref_transform)
            )
            for expanded_name, source_band_index in _expand_raster_layer_names(band_name, src.count):
                source_nodata = _band_nodata_value(src, source_band_index)
                target_nodata = _target_nodata_value(src, source_band_index)
                if not resampled:
                    data = src.read(source_band_index)
                else:
                    data = np.full(
                        (ref_height, ref_width),
                        target_nodata,
                        dtype=np.dtype(src.dtypes[source_band_index - 1]),
                    )
                    resampling = (
                        Resampling.nearest if band_name in categorical_bands else Resampling.bilinear
                    )
                    reproject(
                        source=rasterio.band(src, source_band_index),
                        destination=data,
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=ref_transform,
                        dst_crs=ref_crs,
                        src_nodata=source_nodata,
                        dst_nodata=target_nodata,
                        init_dest_nodata=True,
                        resampling=resampling,
                    )

                arrays.append(data)
                available_bands.append(expanded_name)
                band_metadata[expanded_name] = {
                    "path": str(band_path),
                    "source_layer": band_name,
                    "source_band_index": int(source_band_index),
                    "source_raster_band_count": int(src.count),
                    "dtype": str(src.dtypes[source_band_index - 1]),
                    "source_height": int(src.height),
                    "source_width": int(src.width),
                    "source_crs": src_crs,
                    "source_transform": list(src_transform)[:6],
                    "source_pixel_size": src_pixel_size,
                    "reference_native_pixel_size": native_ref_pixel_size,
                    "reference_pixel_size": ref_pixel_size,
                    "target_pixel_size_requested": float(target_pixel_size) if target_pixel_size is not None else None,
                    "resampled_to_reference": bool(resampled),
                    "categorical": bool(band_name in categorical_bands),
                    "source_nodata": _serialize_metadata_scalar(source_nodata),
                    "target_nodata": _serialize_metadata_scalar(target_nodata),
                }

    if not arrays:
        raise ConversionError("No valid raster bands were loaded.")

    stacked = np.stack(arrays, axis=0)
    return {
        "arrays": stacked,
        "band_names": available_bands,
        "height": ref_height,
        "width": ref_width,
        "dtype": str(stacked.dtype),
        "crs": ref_crs,
        "transform": list(ref_transform)[:6],
        "pixel_size": ref_pixel_size,
        "reference_band": ref_name,
        "band_metadata": band_metadata,
    }


def inspect_aligned_raster_stack(
    band_paths: dict[str, Path],
    *,
    ordered_bands: list[str],
    reference_band: str | None = None,
    categorical_bands: set[str] | None = None,
    target_pixel_size: float | None = None,
    target_grid: TargetGrid | None = None,
) -> dict[str, Any]:
    try:
        import rasterio
        from rasterio.transform import array_bounds, from_origin
    except ImportError as exc:
        raise ConversionDependencyError(
            "Raster support is not available in the zarr-service runtime "
            f"({exc}). Ensure rasterio and its system libraries are installed."
        ) from exc

    categorical_bands = categorical_bands or set()

    if not ordered_bands:
        raise ConversionError("No bands were selected for raster conversion.")

    ref_name = reference_band if reference_band in band_paths else ordered_bands[0]
    if ref_name not in band_paths and target_grid is None:
        raise ConversionError(f"Reference band '{ref_name}' is not available.")

    if target_grid is not None:
        ref_height = int(target_grid.height)
        ref_width = int(target_grid.width)
        ref_crs = target_grid.crs
        ref_transform = rasterio.Affine(*list(target_grid.transform)[:6])
        ref_pixel_size = (
            [float(v) for v in target_grid.pixel_size[:2]]
            if target_grid.pixel_size is not None
            else [float(ref_transform.a), float(abs(ref_transform.e))]
        )
        native_ref_pixel_size = ref_pixel_size
    else:
        with rasterio.open(band_paths[ref_name]) as ref_src:
            ref_height = ref_src.height
            ref_width = ref_src.width
            ref_crs = ref_src.crs.to_string() if ref_src.crs else None
            ref_transform = ref_src.transform
            native_ref_pixel_size = [float(ref_transform.a), float(abs(ref_transform.e))]

            if target_pixel_size is not None:
                left, bottom, right, top = array_bounds(ref_height, ref_width, ref_transform)
                resolution = float(target_pixel_size)
                ref_width = max(1, int(np.ceil((right - left) / resolution)))
                ref_height = max(1, int(np.ceil((top - bottom) / resolution)))
                ref_transform = from_origin(left, top, resolution, resolution)
                ref_pixel_size = [resolution, resolution]
            else:
                ref_pixel_size = native_ref_pixel_size

    band_metadata: dict[str, dict[str, Any]] = {}
    available_bands: list[str] = []
    dtype_candidates: list[np.dtype[Any]] = []
    for band_name in ordered_bands:
        band_path = band_paths.get(band_name)
        if band_path is None:
            continue
        with rasterio.open(band_path) as src:
            src_crs = src.crs.to_string() if src.crs else None
            src_transform = src.transform
            src_pixel_size = [float(src_transform.a), float(abs(src_transform.e))]
            resampled = not (
                src.height == ref_height
                and src.width == ref_width
                and src_crs == ref_crs
                and tuple(src.transform) == tuple(ref_transform)
            )
            for expanded_name, source_band_index in _expand_raster_layer_names(band_name, src.count):
                source_nodata = _band_nodata_value(src, source_band_index)
                target_nodata = _target_nodata_value(src, source_band_index)
                band_metadata[expanded_name] = {
                    "path": str(band_path),
                    "source_layer": band_name,
                    "source_band_index": int(source_band_index),
                    "source_raster_band_count": int(src.count),
                    "dtype": str(src.dtypes[source_band_index - 1]),
                    "source_height": int(src.height),
                    "source_width": int(src.width),
                    "source_crs": src_crs,
                    "source_transform": list(src_transform)[:6],
                    "source_pixel_size": src_pixel_size,
                    "reference_native_pixel_size": native_ref_pixel_size,
                    "reference_pixel_size": ref_pixel_size,
                    "target_pixel_size_requested": float(target_pixel_size) if target_pixel_size is not None else None,
                    "resampled_to_reference": bool(resampled),
                    "categorical": bool(band_name in categorical_bands),
                    "source_nodata": _serialize_metadata_scalar(source_nodata),
                    "target_nodata": _serialize_metadata_scalar(target_nodata),
                }
                dtype_candidates.append(np.dtype(src.dtypes[source_band_index - 1]))
                available_bands.append(expanded_name)

    if not available_bands:
        raise ConversionError("No valid raster bands were discovered for streaming conversion.")

    common_dtype = np.result_type(*dtype_candidates)
    return {
        "band_names": available_bands,
        "height": ref_height,
        "width": ref_width,
        "dtype": str(common_dtype),
        "crs": ref_crs,
        "transform": list(ref_transform)[:6],
        "pixel_size": ref_pixel_size,
        "reference_band": ref_name,
        "band_metadata": band_metadata,
    }


def build_standard_dataset(
    *,
    arrays: "np.ndarray",
    band_names: list[str],
    metadata: dict[str, Any],
    acquisition_datetime: str | None,
) -> "xr.Dataset":
    import numpy as np
    import xarray as xr

    if arrays.ndim != 3:
        raise ConversionError(
            f"Expected a 3D band stack shaped (band, y, x), got ndim={arrays.ndim}."
        )

    band_count, height, width = arrays.shape
    if band_count != len(band_names):
        raise ConversionError(
            f"Band count mismatch: arrays={band_count}, band_names={len(band_names)}."
        )

    timestamp = _coerce_timestamp(acquisition_datetime)
    coords: dict[str, Any] = {
        "time": [np.datetime64(timestamp.replace(tzinfo=None))],
        "band": band_names,
    }

    x_coords, y_coords = _derive_spatial_coords(
        metadata.get("transform"),
        width=width,
        height=height,
    )
    if x_coords is not None and y_coords is not None:
        coords["x"] = x_coords
        coords["y"] = y_coords

    data = arrays[np.newaxis, ...]
    data_array = xr.DataArray(
        data,
        dims=("time", "band", "y", "x"),
        coords=coords,
        name="imagery",
    )

    dataset = xr.Dataset({"imagery": data_array})
    dataset.attrs.update(metadata)
    dataset.attrs["band_names"] = list(band_names)
    dataset.attrs["zarr_format_version"] = ZARR_FORMAT_VERSION
    return dataset


def attach_layer_array(
    dataset: "xr.Dataset",
    *,
    arrays: "np.ndarray",
    layer_names: list[str],
    acquisition_datetime: str | None,
    variable_name: str,
    coord_name: str,
) -> "xr.Dataset":
    import numpy as np
    import xarray as xr

    if arrays.ndim != 3:
        raise ConversionError(
            f"Expected a 3D layer stack shaped (layer, y, x), got ndim={arrays.ndim}."
        )

    layer_count, height, width = arrays.shape
    if layer_count != len(layer_names):
        raise ConversionError(
            f"Layer count mismatch for {variable_name}: arrays={layer_count}, names={len(layer_names)}."
        )

    imagery = dataset["imagery"]
    if int(imagery.sizes["y"]) != height or int(imagery.sizes["x"]) != width:
        raise ConversionError(
            f"Spatial shape mismatch when attaching {variable_name}: expected "
            f"({int(imagery.sizes['y'])}, {int(imagery.sizes['x'])}), got ({height}, {width})."
        )

    timestamp = _coerce_timestamp(acquisition_datetime)
    coords: dict[str, Any] = {
        "time": [np.datetime64(timestamp.replace(tzinfo=None))],
        coord_name: layer_names,
    }
    if "x" in dataset.coords:
        coords["x"] = dataset.coords["x"].values
    if "y" in dataset.coords:
        coords["y"] = dataset.coords["y"].values

    dataset[variable_name] = xr.DataArray(
        arrays[np.newaxis, ...],
        dims=("time", coord_name, "y", "x"),
        coords=coords,
        name=variable_name,
    )
    return dataset


def write_dataset_to_zarr(dataset: xr.Dataset, output_uri: str) -> str:
    output_store, public_uri = _prepare_output_store(output_uri)

    imagery = dataset["imagery"]
    chunk_spec = ChunkShape()
    chunks = (
        min(chunk_spec.time, int(imagery.sizes["time"])),
        min(chunk_spec.band, int(imagery.sizes["band"])),
        min(chunk_spec.y, int(imagery.sizes["y"])),
        min(chunk_spec.x, int(imagery.sizes["x"])),
    )
    encoding = {"imagery": {"chunks": chunks}}
    dataset.to_zarr(
        output_store,
        mode="w",
        consolidated=True,
        encoding=encoding,
        zarr_format=2,
    )
    try:
        import zarr

        root = zarr.open_group(output_store, mode="a", zarr_format=2)
        quadkey_attrs = _build_quadkey_metadata(
            crs=dataset.attrs.get("crs"),
            transform=dataset.attrs.get("transform"),
            width=int(imagery.sizes["x"]),
            height=int(imagery.sizes["y"]),
            time_values=[str(item) for item in imagery.coords["time"].values.tolist()],
            pixel_size=dataset.attrs.get("reference_pixel_size"),
        )
        if quadkey_attrs:
            root.attrs.update(quadkey_attrs)
            zarr.consolidate_metadata(output_store)
    except Exception:
        # Quadkeys are optional indexing metadata.
        pass
    return public_uri


def stream_raster_stack_to_zarr(
    *,
    band_paths: dict[str, Path],
    ordered_bands: list[str],
    output_uri: str,
    metadata: dict[str, Any],
    acquisition_datetime: str | None,
    reference_band: str | None = None,
    categorical_bands: set[str] | None = None,
    target_pixel_size: float | None = None,
    target_grid: TargetGrid | None = None,
    output_mode: str = "w",
    array_name: str = "imagery",
    coord_name: str = "band",
) -> tuple[str, dict[str, Any]]:
    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.transform import Affine
        from rasterio.vrt import WarpedVRT
        from rasterio.windows import Window
        import zarr
        from numcodecs import Blosc
    except ImportError as exc:
        raise ConversionDependencyError(
            "Streaming raster-to-zarr conversion dependencies are unavailable "
            f"({exc}). Ensure rasterio, zarr, and numcodecs are installed."
        ) from exc

    stack = inspect_aligned_raster_stack(
        band_paths,
        ordered_bands=ordered_bands,
        reference_band=reference_band,
        categorical_bands=categorical_bands,
        target_pixel_size=target_pixel_size,
        target_grid=target_grid,
    )
    band_names = list(stack["band_names"])
    height = int(stack["height"])
    width = int(stack["width"])
    transform = stack["transform"]
    affine_transform = Affine(*transform)
    crs = stack["crs"]
    band_metadata = dict(stack["band_metadata"])
    chunk_spec = ChunkShape()
    chunks = (
        min(chunk_spec.time, 1),
        min(chunk_spec.band, len(band_names)),
        min(chunk_spec.y, height),
        min(chunk_spec.x, width),
    )
    if output_mode == "w":
        output_store, public_uri = _prepare_output_store(output_uri)
        root = zarr.open_group(output_store, mode="w", zarr_format=2)
    else:
        output_store = _open_existing_output_store(output_uri)
        public_uri = output_uri if is_oci_uri(output_uri) else str(resolve_output_path(output_uri))
        root = zarr.open_group(output_store, mode="a", zarr_format=2)

    group_attrs = dict(metadata)
    if coord_name == "band":
        group_attrs["band_names"] = band_names
    else:
        group_attrs[f"{coord_name}_names"] = band_names
    group_attrs["zarr_format_version"] = ZARR_FORMAT_VERSION
    root.attrs.update(group_attrs)

    compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)
    imagery = root.create_array(
        array_name,
        shape=(1, len(band_names), height, width),
        chunks=chunks,
        dtype=np.dtype(stack["dtype"]),
        compressor=compressor,
    )
    root.create_array(
        coord_name,
        data=np.asarray(band_names, dtype=f"<U{max(len(v) for v in band_names)}"),
        overwrite=True,
    )
    timestamp = _coerce_timestamp(acquisition_datetime)
    if "time" not in root:
        root.create_array("time", data=np.asarray([timestamp.isoformat()], dtype="<U32"))
    x_coords, y_coords = _derive_spatial_coords(transform, width=width, height=height)
    if x_coords is not None and y_coords is not None:
        if "x" not in root:
            root.create_array("x", data=x_coords, chunks=(min(chunk_spec.x, width),))
        if "y" not in root:
            root.create_array("y", data=y_coords, chunks=(min(chunk_spec.y, height),))

    quadkey_attrs: dict[str, Any] | None = None

    if array_name == "imagery":
        root.attrs.update(
            {
                "dimensions": ["time", coord_name, "y", "x"],
                "shape": [1, len(band_names), height, width],
                "dtype": str(np.dtype(stack["dtype"])),
                "crs": crs,
                "transform": transform,
                "reference_band": stack["reference_band"],
                "reference_pixel_size": stack["pixel_size"],
                "acquisition_datetime": timestamp.isoformat(),
                "band_metadata": band_metadata,
            }
        )
        if output_mode == "w":
            quadkey_attrs = _build_quadkey_metadata(
                crs=crs,
                transform=transform,
                width=width,
                height=height,
                time_values=[timestamp.isoformat()],
                pixel_size=stack["pixel_size"],
            )
            if quadkey_attrs:
                root.attrs.update(quadkey_attrs)
    elif array_name == "ancillary":
        root.attrs.update(
            {
                "ancillary_layer_names": band_names,
                "ancillary_dimensions": ["time", coord_name, "y", "x"],
                "ancillary_shape": [1, len(band_names), height, width],
                "ancillary_metadata": band_metadata,
            }
        )

    for band_index, band_name in enumerate(band_names):
        band_info = band_metadata[band_name]
        band_path = Path(str(band_info["path"]))
        source_band_index = int(band_info.get("source_band_index") or 1)
        with rasterio.open(band_path) as src:
            read_handle: Any = src
            vrt: Any = None
            if band_info["resampled_to_reference"]:
                resampling = (
                    Resampling.nearest if band_info.get("categorical") else Resampling.bilinear
                )
                vrt = WarpedVRT(
                    src,
                    crs=crs,
                    transform=affine_transform,
                    width=width,
                    height=height,
                    resampling=resampling,
                )
                read_handle = vrt
            try:
                for y0 in range(0, height, chunks[2]):
                    block_h = min(chunks[2], height - y0)
                    for x0 in range(0, width, chunks[3]):
                        block_w = min(chunks[3], width - x0)
                        window = Window(x0, y0, block_w, block_h)
                        block = read_handle.read(source_band_index, window=window)
                        imagery[0, band_index, y0 : y0 + block_h, x0 : x0 + block_w] = block
            finally:
                if vrt is not None:
                    vrt.close()

    zarr.consolidate_metadata(output_store)
    dataset_summary = {
        "data_family": str(metadata.get("data_family", "unknown")),
        "zarr_uri": public_uri,
        "dimensions": ["time", coord_name, "y", "x"],
        "shape": [1, len(band_names), height, width],
        "band_names": band_names,
        f"{coord_name}_names": band_names,
        "time_values": [timestamp.isoformat()],
        "dtype": str(np.dtype(stack["dtype"])),
        "crs": crs,
        "transform": transform,
        "pixel_size": stack["pixel_size"],
        "band_metadata": band_metadata,
    }
    if quadkey_attrs:
        dataset_summary.update(_quadkey_summary_from_attrs(quadkey_attrs))
    return public_uri, dataset_summary


def stream_raster_product_to_zarr(
    *,
    imagery_band_paths: dict[str, Path],
    imagery_layer_names: list[str],
    output_uri: str,
    metadata: dict[str, Any],
    acquisition_datetime: str | None,
    reference_band: str | None = None,
    categorical_imagery_layers: set[str] | None = None,
    target_pixel_size: float | None = None,
    ancillary_band_paths: dict[str, Path] | None = None,
    ancillary_layer_names: list[str] | None = None,
    ancillary_categorical_layers: set[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    written_uri, imagery_summary = stream_raster_stack_to_zarr(
        band_paths=imagery_band_paths,
        ordered_bands=imagery_layer_names,
        output_uri=output_uri,
        metadata=metadata,
        acquisition_datetime=acquisition_datetime,
        reference_band=reference_band,
        categorical_bands=categorical_imagery_layers,
        target_pixel_size=target_pixel_size,
        output_mode="w",
        array_name="imagery",
        coord_name="band",
    )
    target_grid = TargetGrid(
        height=int(imagery_summary["shape"][2]),
        width=int(imagery_summary["shape"][3]),
        crs=imagery_summary.get("crs"),
        transform=list(imagery_summary.get("transform") or []),
        pixel_size=list(imagery_summary.get("pixel_size") or []),
        reference_band=reference_band,
    )

    product_summary = dict(imagery_summary)
    product_summary["ancillary_layer_names"] = []
    product_summary["ancillary_dimensions"] = ["time", "ancillary_layer", "y", "x"]
    product_summary["ancillary_shape"] = [1, 0, int(imagery_summary["shape"][2]), int(imagery_summary["shape"][3])]
    product_summary["ancillary_metadata"] = {}

    if ancillary_band_paths and ancillary_layer_names:
        _, ancillary_summary = stream_raster_stack_to_zarr(
            band_paths=ancillary_band_paths,
            ordered_bands=ancillary_layer_names,
            output_uri=output_uri,
            metadata=metadata,
            acquisition_datetime=acquisition_datetime,
            reference_band=ancillary_layer_names[0],
            categorical_bands=ancillary_categorical_layers,
            target_pixel_size=target_pixel_size,
            target_grid=target_grid,
            output_mode="a",
            array_name="ancillary",
            coord_name="ancillary_layer",
        )
        product_summary["ancillary_layer_names"] = list(ancillary_summary["band_names"])
        product_summary["ancillary_dimensions"] = list(ancillary_summary["dimensions"])
        product_summary["ancillary_shape"] = list(ancillary_summary["shape"])
        product_summary["ancillary_metadata"] = dict(ancillary_summary.get("band_metadata") or {})

    return written_uri, product_summary


def resolve_output_path(output_uri: str) -> Path:
    if output_uri.startswith("file://"):
        parsed = urlparse(output_uri)
        candidate = Path(unquote(parsed.path)).expanduser().resolve()
        mapped = _fallback_mounted_data_output(candidate)
        return mapped if mapped is not None else candidate
    parsed = urlparse(output_uri)
    if parsed.scheme:
        raise ConversionError(
            "Only local file paths can be resolved with resolve_output_path()."
        )
    candidate = Path(output_uri).expanduser().resolve()
    mapped = _fallback_mounted_data_output(candidate)
    return mapped if mapped is not None else candidate


def summarize_dataset(dataset: "xr.Dataset", *, data_family: str, zarr_uri: str) -> dict[str, Any]:
    imagery = dataset["imagery"]
    summary = {
        "data_family": data_family,
        "zarr_uri": zarr_uri,
        "dimensions": list(imagery.dims),
        "shape": [int(size) for size in imagery.shape],
        "band_names": [str(item) for item in imagery.coords["band"].values.tolist()],
        "time_values": [str(item) for item in imagery.coords["time"].values.tolist()],
        "crs": dataset.attrs.get("crs"),
        "transform": dataset.attrs.get("transform"),
        "pixel_size": dataset.attrs.get("reference_pixel_size"),
    }
    if "ancillary" in dataset:
        ancillary = dataset["ancillary"]
        summary["ancillary_dimensions"] = list(ancillary.dims)
        summary["ancillary_shape"] = [int(size) for size in ancillary.shape]
        summary["ancillary_layer_names"] = [
            str(item) for item in ancillary.coords["ancillary_layer"].values.tolist()
        ]
    quadkey_attrs = _build_quadkey_metadata(
        crs=dataset.attrs.get("crs"),
        transform=dataset.attrs.get("transform"),
        width=int(imagery.sizes["x"]),
        height=int(imagery.sizes["y"]),
        time_values=[str(item) for item in imagery.coords["time"].values.tolist()],
        pixel_size=dataset.attrs.get("reference_pixel_size"),
    )
    if quadkey_attrs:
        summary.update(_quadkey_summary_from_attrs(quadkey_attrs))
    return summary


def _quadkey_index_zoom() -> int:
    raw = str(os.getenv("NIMBUS_QUADKEY_INDEX_ZOOM", "12")).strip()
    try:
        value = int(raw)
    except ValueError:
        value = 12
    return max(1, min(value, 23))


def _estimate_native_zoom(pixel_size: Any) -> int | None:
    if not isinstance(pixel_size, (list, tuple)) or len(pixel_size) < 2:
        return None
    try:
        x_size = abs(float(pixel_size[0]))
        y_size = abs(float(pixel_size[1]))
    except (TypeError, ValueError):
        return None
    if x_size <= 0.0 or y_size <= 0.0:
        return None
    meters_per_pixel = (x_size + y_size) / 2.0
    if meters_per_pixel <= 0.0:
        return None
    zoom = int(round(float(np.log2(156543.03392804097 / meters_per_pixel))))
    return max(1, min(zoom, 23))


def _build_quadkey_metadata(
    *,
    crs: Any,
    transform: Any,
    width: int,
    height: int,
    time_values: list[str],
    pixel_size: Any,
) -> dict[str, Any] | None:
    index_zoom = _quadkey_index_zoom()
    native_zoom = _estimate_native_zoom(pixel_size)
    index_quadkeys = _quadkeys_for_grid(
        crs=crs,
        transform=transform,
        width=width,
        height=height,
        zoom=index_zoom,
    )
    if not index_quadkeys:
        return None
    native_quadkeys = (
        _quadkeys_for_grid(
            crs=crs,
            transform=transform,
            width=width,
            height=height,
            zoom=native_zoom,
        )
        if native_zoom is not None and native_zoom != index_zoom
        else list(index_quadkeys)
    )
    by_time: dict[str, dict[str, Any]] = {}
    for item in time_values:
        stamp = str(item)
        by_time[stamp] = {
            "index_zoom": index_zoom,
            "native_zoom": native_zoom,
            "quadkeys_index": list(index_quadkeys),
            "quadkeys_native": list(native_quadkeys),
        }
    return {
        "quadkey_schema_version": 1,
        "quadkey_coverage_mode": "bbox_intersection",
        "quadkey_crs": "EPSG:4326",
        "quadkey_zoom_index": index_zoom,
        "quadkey_zoom_native": native_zoom,
        "quadkeys_index": list(index_quadkeys),
        "quadkeys_native": list(native_quadkeys),
        "quadkey_primary_index": index_quadkeys[0] if index_quadkeys else None,
        "quadkey_primary_native": native_quadkeys[0] if native_quadkeys else None,
        "quadkeys_by_time": by_time,
        "quadkeys_generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _quadkey_summary_from_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    return {
        "quadkey_schema_version": attrs.get("quadkey_schema_version"),
        "quadkey_coverage_mode": attrs.get("quadkey_coverage_mode"),
        "quadkey_zoom_index": attrs.get("quadkey_zoom_index"),
        "quadkey_zoom_native": attrs.get("quadkey_zoom_native"),
        "quadkeys_index": list(attrs.get("quadkeys_index") or []),
        "quadkeys_native": list(attrs.get("quadkeys_native") or []),
        "quadkey_primary_index": attrs.get("quadkey_primary_index"),
        "quadkey_primary_native": attrs.get("quadkey_primary_native"),
    }


def _quadkeys_for_grid(
    *,
    crs: Any,
    transform: Any,
    width: int,
    height: int,
    zoom: int | None,
) -> list[str]:
    if zoom is None:
        return []
    if not crs:
        return []
    if not isinstance(transform, (list, tuple)) or len(transform) < 6:
        return []
    if width <= 0 or height <= 0:
        return []
    try:
        import rasterio
        from rasterio.transform import Affine, array_bounds
        from rasterio.warp import transform_bounds

        affine = Affine(*[float(v) for v in transform[:6]])
        left, bottom, right, top = array_bounds(height, width, affine)
        min_lon, min_lat, max_lon, max_lat = transform_bounds(
            src_crs=crs,
            dst_crs="EPSG:4326",
            left=left,
            bottom=bottom,
            right=right,
            top=top,
            densify_pts=21,
        )
    except Exception:
        return []

    if not np.isfinite([min_lon, min_lat, max_lon, max_lat]).all():
        return []

    if min_lon > max_lon:
        first = _bbox_to_quadkeys(min_lon, min_lat, 180.0, max_lat, zoom)
        second = _bbox_to_quadkeys(-180.0, min_lat, max_lon, max_lat, zoom)
        return sorted(set(first + second))

    return _bbox_to_quadkeys(min_lon, min_lat, max_lon, max_lat, zoom)


def _bbox_to_quadkeys(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    zoom: int,
) -> list[str]:
    min_lat = TileMath.clip(min_lat, TileMath.MIN_LAT, TileMath.MAX_LAT)
    max_lat = TileMath.clip(max_lat, TileMath.MIN_LAT, TileMath.MAX_LAT)
    min_lon = TileMath.clip(min_lon, TileMath.MIN_LON, TileMath.MAX_LON)
    max_lon = TileMath.clip(max_lon, TileMath.MIN_LON, TileMath.MAX_LON)

    if max_lat < min_lat:
        min_lat, max_lat = max_lat, min_lat
    if max_lon < min_lon:
        min_lon, max_lon = max_lon, min_lon

    px_min, py_min = TileMath.lat_lon_to_pixel_xy(max_lat, min_lon, zoom)
    px_max, py_max = TileMath.lat_lon_to_pixel_xy(min_lat, max_lon, zoom)
    tx_min, ty_min = TileMath.pixel_xy_to_tile_xy(px_min, py_min)
    tx_max, ty_max = TileMath.pixel_xy_to_tile_xy(px_max, py_max)

    if tx_max < tx_min:
        tx_min, tx_max = tx_max, tx_min
    if ty_max < ty_min:
        ty_min, ty_max = ty_max, ty_min

    quadkeys: list[str] = []
    for tile_y in range(ty_min, ty_max + 1):
        for tile_x in range(tx_min, tx_max + 1):
            quadkeys.append(TileMath.tile_xy_to_quadkey(tile_x, tile_y, zoom))
    return sorted(set(quadkeys))


def _derive_spatial_coords(
    transform_values: Any,
    *,
    width: int,
    height: int,
) -> tuple["np.ndarray | None", "np.ndarray | None"]:
    import numpy as np

    if not isinstance(transform_values, (list, tuple)) or len(transform_values) < 6:
        return None, None
    a, b, c, d, e, f = [float(v) for v in transform_values[:6]]
    if b != 0.0 or d != 0.0:
        return None, None
    x = c + a * (np.arange(width, dtype=np.float64) + 0.5)
    y = f + e * (np.arange(height, dtype=np.float64) + 0.5)
    return x, y


def _coerce_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _band_nodata_value(src: Any, source_band_index: int) -> float | int | None:
    nodata_values = getattr(src, "nodatavals", None)
    value = None
    if isinstance(nodata_values, (list, tuple)) and len(nodata_values) >= source_band_index:
        value = nodata_values[source_band_index - 1]
    if value is None:
        value = getattr(src, "nodata", None)
    return _serialize_metadata_scalar(value)


def _target_nodata_value(src: Any, source_band_index: int) -> float | int:
    source_nodata = _band_nodata_value(src, source_band_index)
    if source_nodata is not None:
        return source_nodata
    dtype = np.dtype(src.dtypes[source_band_index - 1])
    if np.issubdtype(dtype, np.floating):
        return np.nan
    return dtype.type(0).item()


def _serialize_metadata_scalar(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if not np.isfinite(value):
            return None
        return float(value)
    if isinstance(value, int):
        return int(value)
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


def _fallback_mounted_data_path(candidate: Path) -> Path | None:
    # When running in containers, host absolute paths are unavailable.
    # We map any path containing /data/downloads/... to the mounted /data/downloads.
    parts = list(candidate.parts)
    for idx in range(len(parts) - 1):
        if parts[idx] == "data" and parts[idx + 1] == "downloads":
            suffix = parts[idx + 2 :]
            mapped = Path("/data/downloads").joinpath(*suffix)
            if mapped.exists():
                return mapped
            return None
    return None


def _fallback_mounted_data_output(candidate: Path) -> Path | None:
    parts = list(candidate.parts)
    for idx in range(len(parts)):
        if parts[idx] == "data":
            suffix = parts[idx + 1 :]
            if not suffix:
                return Path("/data")
            return Path("/data").joinpath(*suffix)
    return None


def _is_remote_uri(uri: str) -> bool:
    parsed = urlparse(str(uri or "").strip())
    return bool(parsed.scheme and parsed.scheme.lower() not in {"", "file"})


def _stage_remote_source(raw_uri: str, *, label: str) -> tuple[Path, CleanupBundle]:
    if is_oci_uri(raw_uri):
        return _stage_oci_source(raw_uri, label=label)
    raise ConversionError(f"Unsupported remote source URI: {raw_uri}")


def _stage_oci_source(raw_uri: str, *, label: str) -> tuple[Path, CleanupBundle]:
    try:
        store, parsed = OCIStore.from_uri(raw_uri)
    except OCIStorageError as exc:
        raise ConversionDependencyError(str(exc)) from exc

    staging_dir = TemporaryDirectory(prefix=f"nimbus_{label}_oci_")
    cleanup = CleanupBundle(staging_dir)
    staging_root = Path(staging_dir.name)

    if store.is_file(parsed.path):
        local_path = staging_root / Path(parsed.path).name
        store.download_file(parsed.path, local_path)
        return local_path, cleanup

    if store.is_dir(parsed.path):
        dest_name = Path(parsed.path.rstrip("/")).name or "source"
        local_root = staging_root / dest_name
        store.download_tree(parsed.path, local_root)
        return local_root, cleanup

    raise ConversionError(f"OCI source not found: {raw_uri}")


def _prepare_output_store(output_uri: str) -> tuple[Any, str]:
    if is_oci_uri(output_uri):
        try:
            store, parsed = OCIStore.from_uri(output_uri)
        except OCIStorageError as exc:
            raise ConversionDependencyError(str(exc)) from exc
        store.delete(parsed.path, recursive=True)
        return store.get_mapper(parsed.path, create=True), output_uri

    output_path = resolve_output_path(output_uri)
    if output_path.exists() and output_path.is_file():
        raise ConversionError(f"Output path is a file, expected a directory: {output_path}")
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path, str(output_path)


def _open_existing_output_store(output_uri: str) -> Any:
    if is_oci_uri(output_uri):
        try:
            store, parsed = OCIStore.from_uri(output_uri)
        except OCIStorageError as exc:
            raise ConversionDependencyError(str(exc)) from exc
        if not store.exists(parsed.path):
            raise ConversionError(f"Output store does not exist yet: {output_uri}")
        return store.get_mapper(parsed.path, create=False)

    output_path = resolve_output_path(output_uri)
    if not output_path.exists():
        raise ConversionError(f"Output store does not exist yet: {output_path}")
    return output_path


def _expand_raster_layer_names(layer_name: str, source_band_count: int) -> list[tuple[str, int]]:
    if source_band_count <= 1:
        return [(layer_name, 1)]
    if layer_name.upper() == "TCI" and source_band_count == 3:
        return [("TCI_R", 1), ("TCI_G", 2), ("TCI_B", 3)]
    return [
        (f"{layer_name}_{source_band_index}", source_band_index)
        for source_band_index in range(1, source_band_count + 1)
    ]
