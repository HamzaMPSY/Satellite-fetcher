from __future__ import annotations

from typing import Any

import numpy as np


def derive_spatial_coords(
    transform_values: Any,
    *,
    width: int,
    height: int,
) -> tuple["np.ndarray | None", "np.ndarray | None"]:
    if not isinstance(transform_values, (list, tuple)) or len(transform_values) < 6:
        return None, None
    a, b, c, d, e, f = [float(v) for v in transform_values[:6]]
    if b != 0.0 or d != 0.0:
        return None, None
    x = c + a * (np.arange(width, dtype=np.float64) + 0.5)
    y = f + e * (np.arange(height, dtype=np.float64) + 0.5)
    return x, y


def band_nodata_value(src: Any, source_band_index: int) -> float | int | None:
    nodata_values = getattr(src, "nodatavals", None)
    value = None
    if isinstance(nodata_values, (list, tuple)) and len(nodata_values) >= source_band_index:
        value = nodata_values[source_band_index - 1]
    if value is None:
        value = getattr(src, "nodata", None)
    return serialize_metadata_scalar(value)


def target_nodata_value(src: Any, source_band_index: int) -> float | int:
    source_nodata = band_nodata_value(src, source_band_index)
    if source_nodata is not None:
        return source_nodata
    dtype = np.dtype(src.dtypes[source_band_index - 1])
    if np.issubdtype(dtype, np.floating):
        return np.nan
    return dtype.type(0).item()


def serialize_metadata_scalar(value: Any) -> float | int | None:
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
