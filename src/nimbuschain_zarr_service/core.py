from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import unquote, urlparse
import shutil
import tarfile
import zipfile

import numpy as np

from nimbuschain_zarr_service.schema import ChunkShape, ZARR_FORMAT_VERSION


class ConversionError(ValueError):
    """Raised when the raw product cannot be converted."""


class ConversionDependencyError(RuntimeError):
    """Raised when a required runtime dependency is missing."""


@dataclass(frozen=True)
class PreparedSource:
    root: Path
    source_kind: str
    raw_path: Path
    cleanup: TemporaryDirectory[str] | None = None


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
    raw_path = resolve_local_path(raw_uri)
    if not raw_path.exists():
        raise ConversionError(f"{label} source not found: {raw_path}")

    if raw_path.is_dir():
        return PreparedSource(root=raw_path, source_kind="directory", raw_path=raw_path)

    if raw_path.is_file() and raw_path.suffix.lower() == ".nc":
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        copied = Path(tmp_dir.name) / raw_path.name
        shutil.copy2(raw_path, copied)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="netcdf",
            raw_path=raw_path,
            cleanup=tmp_dir,
        )

    if zipfile.is_zipfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with zipfile.ZipFile(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="zip",
            raw_path=raw_path,
            cleanup=tmp_dir,
        )

    if tarfile.is_tarfile(raw_path):
        tmp_dir = TemporaryDirectory(prefix=f"nimbus_{label}_")
        with tarfile.open(raw_path) as archive:
            archive.extractall(tmp_dir.name)
        return PreparedSource(
            root=Path(tmp_dir.name),
            source_kind="tar",
            raw_path=raw_path,
            cleanup=tmp_dir,
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
) -> dict[str, Any]:
    try:
        import rasterio
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
    if ref_name not in band_paths:
        raise ConversionError(f"Reference band '{ref_name}' is not available.")

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

        arrays: list[np.ndarray] = []
        available_bands: list[str] = []
        band_metadata: dict[str, dict[str, Any]] = {}
        for band_name in ordered_bands:
            band_path = band_paths.get(band_name)
            if band_path is None:
                continue

            with rasterio.open(band_path) as src:
                if src.count != 1:
                    raise ConversionError(
                        f"Band {band_name} is expected to be single-band, got {src.count}."
                    )

                src_crs = src.crs.to_string() if src.crs else None
                src_transform = src.transform
                src_pixel_size = [float(src_transform.a), float(abs(src_transform.e))]
                resampled = not (
                    src.height == ref_height
                    and src.width == ref_width
                    and src_crs == ref_crs
                    and tuple(src.transform) == tuple(ref_transform)
                )
                if not resampled:
                    data = src.read(1)
                else:
                    data = np.empty((ref_height, ref_width), dtype=src.dtypes[0])
                    resampling = (
                        Resampling.nearest if band_name in categorical_bands else Resampling.bilinear
                    )
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=data,
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=ref_transform,
                        dst_crs=ref_src.crs,
                        resampling=resampling,
                    )

                arrays.append(data)
                available_bands.append(band_name)
                band_metadata[band_name] = {
                    "path": str(band_path),
                    "dtype": str(src.dtypes[0]),
                    "source_height": int(src.height),
                    "source_width": int(src.width),
                    "source_crs": src_crs,
                    "source_transform": list(src_transform)[:6],
                    "source_pixel_size": src_pixel_size,
                    "reference_native_pixel_size": native_ref_pixel_size,
                    "reference_pixel_size": ref_pixel_size,
                    "target_pixel_size_requested": float(target_pixel_size) if target_pixel_size is not None else None,
                    "resampled_to_reference": bool(resampled),
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
    if ref_name not in band_paths:
        raise ConversionError(f"Reference band '{ref_name}' is not available.")

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
            if src.count != 1:
                raise ConversionError(
                    f"Band {band_name} is expected to be single-band, got {src.count}."
                )
            src_crs = src.crs.to_string() if src.crs else None
            src_transform = src.transform
            src_pixel_size = [float(src_transform.a), float(abs(src_transform.e))]
            resampled = not (
                src.height == ref_height
                and src.width == ref_width
                and src_crs == ref_crs
                and tuple(src.transform) == tuple(ref_transform)
            )
            band_metadata[band_name] = {
                "path": str(band_path),
                "dtype": str(src.dtypes[0]),
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
            }
            dtype_candidates.append(np.dtype(src.dtypes[0]))
            available_bands.append(band_name)

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


def write_dataset_to_zarr(dataset: xr.Dataset, output_uri: str) -> str:
    output_path = resolve_output_path(output_uri)
    if output_path.exists() and output_path.is_file():
        raise ConversionError(f"Output path is a file, expected a directory: {output_path}")
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

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
        str(output_path),
        mode="w",
        consolidated=True,
        encoding=encoding,
        zarr_format=2,
    )
    return str(output_path)


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
    output_path = resolve_output_path(output_uri)
    if output_path.exists() and output_path.is_file():
        raise ConversionError(f"Output path is a file, expected a directory: {output_path}")
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    group_attrs = dict(metadata)
    group_attrs["band_names"] = band_names
    group_attrs["zarr_format_version"] = ZARR_FORMAT_VERSION
    root = zarr.open_group(output_path, mode="w", zarr_format=2)
    root.attrs.update(group_attrs)

    compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)
    imagery = root.create_array(
        "imagery",
        shape=(1, len(band_names), height, width),
        chunks=chunks,
        dtype=np.dtype(stack["dtype"]),
        compressor=compressor,
    )
    root.create_array("band", data=np.asarray(band_names, dtype=f"<U{max(len(v) for v in band_names)}"))
    timestamp = _coerce_timestamp(acquisition_datetime)
    root.create_array("time", data=np.asarray([timestamp.isoformat()], dtype="<U32"))
    x_coords, y_coords = _derive_spatial_coords(transform, width=width, height=height)
    if x_coords is not None and y_coords is not None:
        root.create_array("x", data=x_coords, chunks=(min(chunk_spec.x, width),))
        root.create_array("y", data=y_coords, chunks=(min(chunk_spec.y, height),))

    for band_index, band_name in enumerate(band_names):
        band_path = Path(band_paths[band_name])
        band_info = band_metadata[band_name]
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
                        block = read_handle.read(1, window=window)
                        imagery[0, band_index, y0 : y0 + block_h, x0 : x0 + block_w] = block
            finally:
                if vrt is not None:
                    vrt.close()

    zarr.consolidate_metadata(output_path)
    dataset_summary = {
        "data_family": str(metadata.get("data_family", "unknown")),
        "zarr_uri": str(output_path),
        "dimensions": ["time", "band", "y", "x"],
        "shape": [1, len(band_names), height, width],
        "band_names": band_names,
        "time_values": [timestamp.isoformat()],
        "dtype": str(np.dtype(stack["dtype"])),
        "crs": crs,
        "transform": transform,
        "pixel_size": stack["pixel_size"],
        "band_metadata": band_metadata,
    }
    return str(output_path), dataset_summary


def resolve_output_path(output_uri: str) -> Path:
    if output_uri.startswith("file://"):
        parsed = urlparse(output_uri)
        candidate = Path(unquote(parsed.path)).expanduser().resolve()
        mapped = _fallback_mounted_data_output(candidate)
        return mapped if mapped is not None else candidate
    parsed = urlparse(output_uri)
    if parsed.scheme:
        raise ConversionError(
            "Only local output paths are supported by the Zarr converter in v1."
        )
    candidate = Path(output_uri).expanduser().resolve()
    mapped = _fallback_mounted_data_output(candidate)
    return mapped if mapped is not None else candidate


def summarize_dataset(dataset: "xr.Dataset", *, data_family: str, zarr_uri: str) -> dict[str, Any]:
    imagery = dataset["imagery"]
    return {
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
