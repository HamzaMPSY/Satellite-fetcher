from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

import numpy as np

from nimbuschain_zarr_service.core import (
    ConversionDependencyError,
    ConversionError,
    _coerce_timestamp,
    _prepare_output_store,
    build_standard_dataset,
)
from nimbuschain_zarr_service.schema import ChunkShape, ZARR_FORMAT_VERSION


_POLARIZATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "vv": re.compile(r"(?:^|[_-])vv(?:[_\.-]|$)", re.IGNORECASE),
    "vh": re.compile(r"(?:^|[_-])vh(?:[_\.-]|$)", re.IGNORECASE),
    "hh": re.compile(r"(?:^|[_-])hh(?:[_\.-]|$)", re.IGNORECASE),
    "hv": re.compile(r"(?:^|[_-])hv(?:[_\.-]|$)", re.IGNORECASE),
}


def raw_support_status() -> dict[str, Any]:
    try:
        import sentinel1decoder  # type: ignore
    except ImportError as exc:
        return {
            "available": False,
            "decoder": "sentinel1decoder",
            "error": str(exc),
        }
    return {
        "available": True,
        "decoder": "sentinel1decoder",
        "version": getattr(sentinel1decoder, "__version__", None),
    }


def build_sentinel1_raw_dataset(
    *,
    root: Path,
    provider: str,
    collection: str,
    scene_id: str,
) -> tuple[Any, dict[str, Any]]:
    try:
        from sentinel1decoder import Level0File  # type: ignore
    except ImportError as exc:
        raise ConversionDependencyError(
            "Sentinel-1 RAW conversion requires the optional 'sentinel1decoder' dependency."
        ) from exc

    raw_files = _discover_raw_measurement_files(root)
    if not raw_files:
        raise ConversionError(
            "No Sentinel-1 RAW measurement .dat files were found in the SAFE bundle."
        )

    per_band_arrays: dict[str, np.ndarray] = {}
    band_metadata: dict[str, dict[str, Any]] = {}
    decode_failures: list[str] = []

    for band_name, data_path in raw_files.items():
        try:
            level0 = Level0File(str(data_path))
            echo_chunks: list[np.ndarray] = []
            echo_chunk_ids: list[int] = []
            for chunk_id in list(getattr(level0, "acquisition_chunks", []) or []):
                constants = {}
                try:
                    constants = level0.get_acquisition_chunk_constants(chunk_id)
                except Exception:
                    pass
                if not _is_echo_signal(constants.get("signal_type")):
                    continue
                chunk = level0.get_acquisition_chunk_data(chunk_id, try_load_from_file=False)
                if chunk is None:
                    continue
                chunk_array = np.abs(np.asarray(chunk)).astype(np.float32, copy=False)
                if chunk_array.ndim != 2 or chunk_array.size == 0:
                    continue
                echo_chunks.append(chunk_array)
                echo_chunk_ids.append(int(chunk_id))

            if not echo_chunks:
                decode_failures.append(f"{band_name}: no echo acquisition chunks were decoded")
                continue

            band_array = _stack_chunks_vertically(echo_chunks)
            per_band_arrays[band_name] = band_array
            band_metadata[band_name] = {
                "path": str(data_path.relative_to(root)),
                "decoder": "sentinel1decoder",
                "representation": "amplitude_from_complex_iq",
                "acquisition_chunk_ids": echo_chunk_ids,
                "acquisition_chunk_count": len(echo_chunk_ids),
                "source_shape": [int(v) for v in band_array.shape],
                "source_pixel_size": None,
                "source_crs": None,
                "source_transform": None,
                "reference_native_pixel_size": None,
                "reference_pixel_size": None,
                "target_pixel_size_requested": None,
                "resampled_to_reference": False,
                "georeferenced": False,
            }
        except Exception as exc:
            decode_failures.append(f"{band_name}: {exc}")

    if not per_band_arrays:
        suffix = f" Failures: {decode_failures}" if decode_failures else ""
        raise ConversionError(
            "Sentinel-1 RAW decoding did not produce any usable acquisition chunks." + suffix
        )

    ordered_bands = [band for band in ("vv", "vh", "hh", "hv") if band in per_band_arrays]
    stacked = _align_band_grids(per_band_arrays, ordered_bands)
    acquisition = _extract_timestamp_from_scene_id(scene_id)
    dataset = build_standard_dataset(
        arrays=stacked,
        band_names=ordered_bands,
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "data_family": "sar",
            "crs": None,
            "transform": None,
            "reference_band": ordered_bands[0],
            "reference_pixel_size": None,
            "band_metadata": band_metadata,
            "georeferenced": False,
            "raw_decoder": "sentinel1decoder",
            "raw_representation": "amplitude_from_complex_iq",
        },
    )
    summary = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "data_family": "sar",
        "product_type": "RAW",
        "product_mode": _scene_mode(scene_id),
        "product_token": "RAW",
        "product_level": "sar_raw",
        "product_id": scene_id,
        "normalized_band_order": ordered_bands,
        "resolution_policy_meters": None,
        "band_sources": {band: str(raw_files[band].relative_to(root)) for band in ordered_bands},
        "band_resampling": {band: False for band in ordered_bands},
        "band_native_pixel_size": {band: None for band in ordered_bands},
        "acquisition_datetime": acquisition,
        "grid": {
            "height": int(stacked.shape[1]),
            "width": int(stacked.shape[2]),
            "dtype": str(stacked.dtype),
            "crs": None,
            "transform": None,
            "pixel_size": None,
            "reference_band": ordered_bands[0],
            "georeferenced": False,
        },
        "raw_decoder": "sentinel1decoder",
        "raw_decode_failures": decode_failures,
        "raw_acquisition_chunks": {
            band: band_metadata[band]["acquisition_chunk_count"]
            for band in ordered_bands
        },
    }
    return dataset, summary


def convert_sentinel1_raw_to_zarr(
    *,
    root: Path,
    provider: str,
    collection: str,
    scene_id: str,
    output_uri: str,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    try:
        from numcodecs import Blosc
        import zarr
        from sentinel1decoder import Level0File  # type: ignore
    except ImportError as exc:
        raise ConversionDependencyError(
            "Sentinel-1 RAW conversion requires sentinel1decoder, zarr, and numcodecs."
        ) from exc

    raw_files = _discover_raw_measurement_files(root)
    if not raw_files:
        raise ConversionError(
            "No Sentinel-1 RAW measurement .dat files were found in the SAFE bundle."
        )

    band_plans: dict[str, dict[str, Any]] = {}
    decode_failures: list[str] = []
    for band_name, data_path in raw_files.items():
        level0 = Level0File(str(data_path))
        chunks: list[dict[str, int]] = []
        total_height = 0
        max_width = 0
        echo_chunk_ids: list[int] = []
        for chunk_id in list(getattr(level0, "acquisition_chunks", []) or []):
            constants = {}
            try:
                constants = level0.get_acquisition_chunk_constants(chunk_id)
            except Exception:
                continue
            if not _is_echo_signal(constants.get("signal_type")):
                continue
            metadata = level0.get_acquisition_chunk_metadata(chunk_id)
            height = int(getattr(metadata, "shape", [0, 0])[0] or 0)
            width = int(constants.get("num_quads") or 0) * 2
            if height <= 0 or width <= 0:
                continue
            chunks.append({"id": int(chunk_id), "height": height, "width": width})
            total_height += height
            max_width = max(max_width, width)
            echo_chunk_ids.append(int(chunk_id))

        if not chunks:
            decode_failures.append(f"{band_name}: no echo acquisition chunks were decoded")
            continue

        band_plans[band_name] = {
            "path": data_path,
            "chunks": chunks,
            "total_height": total_height,
            "max_width": max_width,
            "echo_chunk_ids": echo_chunk_ids,
        }

    if not band_plans:
        suffix = f" Failures: {decode_failures}" if decode_failures else ""
        raise ConversionError(
            "Sentinel-1 RAW decoding did not produce any usable acquisition chunks." + suffix
        )

    ordered_bands = [band for band in ("vv", "vh", "hh", "hv") if band in band_plans]
    global_height = max(int(band_plans[band]["total_height"]) for band in ordered_bands)
    global_width = max(int(band_plans[band]["max_width"]) for band in ordered_bands)
    if global_height <= 0 or global_width <= 0:
        raise ConversionError("Sentinel-1 RAW chunk planning produced an empty output grid.")

    chunk_spec = ChunkShape()
    output_store, public_uri = _prepare_output_store(output_uri)
    root_group = zarr.open_group(output_store, mode="w", zarr_format=2)
    acquisition = _extract_timestamp_from_scene_id(scene_id)

    band_metadata: dict[str, dict[str, Any]] = {}
    root_group.attrs.update(
        {
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "data_family": "sar",
            "crs": None,
            "transform": None,
            "reference_band": ordered_bands[0],
            "reference_pixel_size": None,
            "band_names": ordered_bands,
            "zarr_format_version": ZARR_FORMAT_VERSION,
            "georeferenced": False,
            "raw_decoder": "sentinel1decoder",
            "raw_representation": "amplitude_from_complex_iq",
            "sample_axis_units": "samples",
        }
    )

    imagery = root_group.create_array(
        "imagery",
        shape=(1, len(ordered_bands), global_height, global_width),
        chunks=(
            1,
            1,
            min(chunk_spec.y, global_height),
            min(chunk_spec.x, global_width),
        ),
        dtype=np.dtype("float32"),
        compressor=Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE),
        fill_value=np.nan,
    )
    root_group.create_array(
        "band",
        data=np.asarray(ordered_bands, dtype=f"<U{max(len(v) for v in ordered_bands)}"),
    )
    timestamp = _coerce_timestamp(acquisition)
    root_group.create_array("time", data=np.asarray([timestamp.isoformat()], dtype="<U32"))
    root_group.create_array(
        "x",
        data=np.arange(global_width, dtype=np.int32),
        chunks=(min(chunk_spec.x, global_width),),
    )
    root_group.create_array(
        "y",
        data=np.arange(global_height, dtype=np.int32),
        chunks=(min(chunk_spec.y, global_height),),
    )

    for band_index, band_name in enumerate(ordered_bands):
        plan = band_plans[band_name]
        level0 = Level0File(str(plan["path"]))
        y0 = 0
        for chunk in plan["chunks"]:
            data = level0.get_acquisition_chunk_data(chunk["id"], try_load_from_file=False)
            if data is None:
                raise ConversionError(
                    f"Sentinel-1 RAW chunk {chunk['id']} for band {band_name} returned no data."
                )
            band_block = np.abs(np.asarray(data)).astype(np.float32, copy=False)
            if band_block.ndim != 2 or band_block.size == 0:
                raise ConversionError(
                    f"Sentinel-1 RAW chunk {chunk['id']} for band {band_name} is not a 2D array."
                )
            height = int(band_block.shape[0])
            width = int(band_block.shape[1])
            imagery[0, band_index, y0 : y0 + height, :width] = band_block
            y0 += height

        band_metadata[band_name] = {
            "path": str(Path(plan["path"]).relative_to(root)),
            "decoder": "sentinel1decoder",
            "representation": "amplitude_from_complex_iq",
            "acquisition_chunk_ids": list(plan["echo_chunk_ids"]),
            "acquisition_chunk_count": len(plan["echo_chunk_ids"]),
            "source_shape": [int(plan["total_height"]), int(plan["max_width"])],
            "source_pixel_size": None,
            "source_crs": None,
            "source_transform": None,
            "reference_native_pixel_size": None,
            "reference_pixel_size": None,
            "target_pixel_size_requested": None,
            "resampled_to_reference": False,
            "georeferenced": False,
        }

    zarr.consolidate_metadata(output_store)
    normalization_summary = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "data_family": "sar",
        "product_type": "RAW",
        "product_mode": _scene_mode(scene_id),
        "product_token": "RAW",
        "product_level": "sar_raw",
        "product_id": scene_id,
        "normalized_band_order": ordered_bands,
        "resolution_policy_meters": None,
        "band_sources": {band: str(Path(band_plans[band]["path"]).relative_to(root)) for band in ordered_bands},
        "band_resampling": {band: False for band in ordered_bands},
        "band_native_pixel_size": {band: None for band in ordered_bands},
        "acquisition_datetime": acquisition,
        "grid": {
            "height": global_height,
            "width": global_width,
            "dtype": "float32",
            "crs": None,
            "transform": None,
            "pixel_size": None,
            "reference_band": ordered_bands[0],
            "georeferenced": False,
        },
        "raw_decoder": "sentinel1decoder",
        "raw_decode_failures": decode_failures,
        "raw_acquisition_chunks": {
            band: len(band_plans[band]["echo_chunk_ids"])
            for band in ordered_bands
        },
        "sample_axis_units": "samples",
        "source_kind": "zip" if root.name.endswith(".zip") else "directory",
        "raw_path": str(root),
    }
    dataset_summary = {
        "data_family": "sar",
        "zarr_uri": public_uri,
        "dimensions": ["time", "band", "y", "x"],
        "shape": [1, len(ordered_bands), global_height, global_width],
        "band_names": ordered_bands,
        "time_values": [timestamp.isoformat()],
        "crs": None,
        "transform": None,
        "pixel_size": None,
        "band_metadata": band_metadata,
        "sample_axis_units": "samples",
    }
    return public_uri, normalization_summary, dataset_summary


def _discover_raw_measurement_files(root: Path) -> dict[str, Path]:
    band_paths: dict[str, Path] = {}
    for path in sorted(root.rglob("*.dat")):
        lower = path.name.lower()
        if lower.endswith("-annot.dat") or lower.endswith("-index.dat"):
            continue
        for band_name, pattern in _POLARIZATION_PATTERNS.items():
            if pattern.search(path.name):
                band_paths[band_name] = path
                break
    return band_paths


def _is_echo_signal(signal: Any) -> bool:
    value = getattr(signal, "value", signal)
    if value == 0:
        return True
    signal_name = getattr(signal, "name", None)
    if isinstance(signal_name, str) and signal_name.upper() == "ECHO":
        return True
    signal_text = str(signal).upper()
    return signal_text == "ECHO" or signal_text.endswith(".ECHO")


def _stack_chunks_vertically(chunks: list[np.ndarray]) -> np.ndarray:
    max_width = max(chunk.shape[1] for chunk in chunks)
    normalized: list[np.ndarray] = []
    for chunk in chunks:
        if chunk.shape[1] == max_width:
            normalized.append(chunk)
            continue
        padded = np.full((chunk.shape[0], max_width), np.nan, dtype=np.float32)
        padded[:, : chunk.shape[1]] = chunk
        normalized.append(padded)
    return np.concatenate(normalized, axis=0)


def _align_band_grids(per_band_arrays: dict[str, np.ndarray], ordered_bands: list[str]) -> np.ndarray:
    max_height = max(per_band_arrays[band].shape[0] for band in ordered_bands)
    max_width = max(per_band_arrays[band].shape[1] for band in ordered_bands)
    band_stack: list[np.ndarray] = []
    for band in ordered_bands:
        band_array = per_band_arrays[band]
        canvas = np.full((max_height, max_width), np.nan, dtype=np.float32)
        canvas[: band_array.shape[0], : band_array.shape[1]] = band_array
        band_stack.append(canvas)
    return np.stack(band_stack, axis=0)


def _extract_timestamp_from_scene_id(scene_id: str) -> str | None:
    match = re.search(r"_(\d{8}T\d{6})_", scene_id)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc).isoformat()


def _scene_mode(scene_id: str) -> str:
    parts = str(scene_id).split("_")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return "UNKNOWN"
