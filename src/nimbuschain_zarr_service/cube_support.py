from __future__ import annotations

import math
from datetime import date
from typing import Any

import numpy as np

from nimbuschain_shared.zarr import ConversionError, _coerce_timestamp
from nimbuschain_zarr_service.oci_storage import is_oci_uri
from nimbuschain_zarr_service.storage_support import resolve_output_path


def copy_time_slice_in_chunks(
    source_array: Any,
    target_array: Any,
    *,
    source_time_index: int,
    target_time_index: int,
    layer_name: str,
    label_names: list[str] | None = None,
    blocks_written: int = 0,
    total_blocks: int | None = None,
    progress_callback=None,
) -> int:
    shape = tuple(int(value) for value in target_array.shape)
    chunk_shape = normalized_chunk_shape(target_array)

    if len(shape) == 4:
        band_count = int(shape[1])
        height = int(shape[2])
        width = int(shape[3])
        band_chunk = max(1, int(chunk_shape[1]))
        y_chunk = max(1, int(chunk_shape[2]))
        x_chunk = max(1, int(chunk_shape[3]))
        for band_start in range(0, band_count, band_chunk):
            band_stop = min(band_count, band_start + band_chunk)
            band_name = None
            if label_names and band_stop - band_start == 1 and band_start < len(label_names):
                band_name = str(label_names[band_start])
            for y0 in range(0, height, y_chunk):
                y1 = min(height, y0 + y_chunk)
                for x0 in range(0, width, x_chunk):
                    x1 = min(width, x0 + x_chunk)
                    target_array[target_time_index, band_start:band_stop, y0:y1, x0:x1] = source_array[
                        source_time_index,
                        band_start:band_stop,
                        y0:y1,
                        x0:x1,
                    ]
                    blocks_written += 1
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "layer_name": layer_name,
                                "band_name": band_name,
                                "blocks_written": blocks_written,
                                "total_blocks": int(total_blocks or 0),
                                "fraction": (
                                    min(1.0, max(0.0, blocks_written / total_blocks))
                                    if total_blocks
                                    else 1.0
                                ),
                            }
                        )
        return blocks_written

    if len(shape) == 3:
        height = int(shape[1])
        width = int(shape[2])
        y_chunk = max(1, int(chunk_shape[1]))
        x_chunk = max(1, int(chunk_shape[2]))
        for y0 in range(0, height, y_chunk):
            y1 = min(height, y0 + y_chunk)
            for x0 in range(0, width, x_chunk):
                x1 = min(width, x0 + x_chunk)
                target_array[target_time_index, y0:y1, x0:x1] = source_array[source_time_index, y0:y1, x0:x1]
                blocks_written += 1
                if progress_callback is not None:
                    progress_callback(
                        {
                            "layer_name": layer_name,
                            "blocks_written": blocks_written,
                            "total_blocks": int(total_blocks or 0),
                            "fraction": (
                                min(1.0, max(0.0, blocks_written / total_blocks))
                                if total_blocks
                                else 1.0
                            ),
                        }
                    )
        return blocks_written

    raise ConversionError("Cube arrays must use either the (time, band, y, x) or (time, y, x) layout.")


def time_slice_block_count(array: Any) -> int:
    shape = tuple(int(value) for value in array.shape)
    chunk_shape = normalized_chunk_shape(array)

    if len(shape) == 4:
        return (
            math.ceil(shape[1] / max(1, int(chunk_shape[1])))
            * math.ceil(shape[2] / max(1, int(chunk_shape[2])))
            * math.ceil(shape[3] / max(1, int(chunk_shape[3])))
        )
    if len(shape) == 3:
        return (
            math.ceil(shape[1] / max(1, int(chunk_shape[1])))
            * math.ceil(shape[2] / max(1, int(chunk_shape[2])))
        )
    raise ConversionError("Cube arrays must use either the (time, band, y, x) or (time, y, x) layout.")


def normalized_chunk_shape(array: Any) -> tuple[int, ...]:
    shape = tuple(int(value) for value in array.shape)
    raw_chunks = getattr(array, "chunks", None)
    if not isinstance(raw_chunks, (list, tuple)) or len(raw_chunks) != len(shape):
        return shape

    normalized: list[int] = []
    for size, chunk in zip(shape, raw_chunks):
        chunk_size = int(chunk or size)
        normalized.append(min(int(size), max(1, chunk_size)))
    return tuple(normalized)


def read_label_array(group: Any, key: str) -> list[str]:
    if key not in group:
        return []
    values = group[key][:]
    try:
        items = values.tolist()
    except Exception:
        items = list(values)
    return [normalize_label(item) for item in items]


def normalize_label(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode(errors="replace")
    return str(value)


def resolve_acquisition_time(attrs: dict[str, Any], time_values: list[str], source_zarr_uri: str) -> str:
    candidates = [
        str(attrs.get("acquisition_datetime") or "").strip(),
        time_values[0] if time_values else "",
    ]
    for candidate in candidates:
        if candidate:
            return _coerce_timestamp(candidate).isoformat()
    raise ConversionError(f"Source Zarr is missing acquisition time metadata: {source_zarr_uri}")


def default_scene_id(source_zarr_uri: str) -> str:
    if is_oci_uri(source_zarr_uri):
        return str(source_zarr_uri).rstrip("/").split("/")[-1] or "scene"
    return resolve_output_path(source_zarr_uri).stem or "scene"


def normalize_public_uri(source_zarr_uri: str) -> str:
    if is_oci_uri(source_zarr_uri):
        return source_zarr_uri
    return str(resolve_output_path(source_zarr_uri))


def string_array(values: list[str]) -> np.ndarray:
    encoded = [str(value).encode("utf-8") for value in values]
    width = max(1, max((len(value) for value in encoded), default=1))
    return np.asarray(encoded, dtype=f"S{width}")


def sanitize_layer_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for layer_name, raw_payload in dict(metadata or {}).items():
        payload = dict(raw_payload or {})
        payload.pop("path", None)
        sanitized[str(layer_name)] = payload
    return sanitized


def sanitize_mask_array_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(attrs or {})
    sanitized["dimensions"] = ["time", "y", "x"]
    sanitized.pop("created_at", None)
    sanitized.pop("written_at", None)
    sanitized.pop("summary", None)

    metadata = dict(sanitized.get("metadata") or {})
    for key in [
        "scene_id",
        "input_zarr_uri",
        "output_zarr_uri",
        "artifact_uri",
        "status_path",
        "work_dir",
    ]:
        metadata.pop(key, None)
    if metadata:
        sanitized["metadata"] = metadata
    else:
        sanitized.pop("metadata", None)
    return sanitized


def clean_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def coerce_date_only(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return _coerce_timestamp(text).date()
