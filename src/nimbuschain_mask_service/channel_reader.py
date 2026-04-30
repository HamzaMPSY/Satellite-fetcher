from __future__ import annotations

import math
from typing import Any

import numpy as np

from nimbuschain_shared.zarr import ConversionError


def read_required_channels(
    root: Any,
    *,
    band_names: tuple[str, ...],
    required_bands: tuple[str, ...],
    scale_hint: str,
    normalize: bool = True,
    time_index: int = 0,
    include_validity: bool = False,
) -> tuple[dict[str, np.ndarray], list[str]] | tuple[dict[str, np.ndarray], list[str], np.ndarray]:
    missing = [name for name in required_bands if name not in band_names]
    if missing:
        raise ConversionError(
            "Cannot compute mask because required imagery bands are missing: " + ", ".join(missing)
        )

    imagery = root["imagery"]
    root_attrs = dict(root.attrs)
    channels: dict[str, np.ndarray] = {}
    validity_masks: list[np.ndarray] = []
    nonzero_masks: list[np.ndarray] = []
    has_explicit_nodata = False
    for band_name in required_bands:
        band_index = band_names.index(band_name)
        raw = np.asarray(imagery[time_index, band_index, :, :])
        valid_band, nonzero_band, explicit_nodata = _valid_pixels_from_raw(
            raw,
            band_name=band_name,
            scale_hint=scale_hint,
            root_attrs=root_attrs,
        )
        validity_masks.append(valid_band)
        nonzero_masks.append(nonzero_band)
        has_explicit_nodata = has_explicit_nodata or explicit_nodata
        array = np.asarray(raw, dtype=np.float32)
        channels[band_name] = (
            _normalize_channel(
                array,
                scale_hint=scale_hint,
                band_name=band_name,
                root_attrs=root_attrs,
            )
            if normalize
            else array
        )
    if include_validity:
        return channels, missing, _combine_validity_masks(
            validity_masks=validity_masks,
            nonzero_masks=nonzero_masks,
            scale_hint=scale_hint,
            has_explicit_nodata=has_explicit_nodata,
        )
    return channels, missing


def read_required_channels_window(
    root: Any,
    *,
    band_names: tuple[str, ...],
    required_bands: tuple[str, ...],
    scale_hint: str,
    row_start: int,
    row_stop: int,
    col_start: int,
    col_stop: int,
    normalize: bool = True,
    time_index: int = 0,
    include_validity: bool = False,
) -> tuple[dict[str, np.ndarray], list[str]] | tuple[dict[str, np.ndarray], list[str], np.ndarray]:
    missing = [name for name in required_bands if name not in band_names]
    if missing:
        raise ConversionError(
            "Cannot compute mask because required imagery bands are missing: " + ", ".join(missing)
        )

    imagery = root["imagery"]
    root_attrs = dict(root.attrs)
    channels: dict[str, np.ndarray] = {}
    validity_masks: list[np.ndarray] = []
    nonzero_masks: list[np.ndarray] = []
    has_explicit_nodata = False
    for band_name in required_bands:
        band_index = band_names.index(band_name)
        raw = np.asarray(
            imagery[time_index, band_index, row_start:row_stop, col_start:col_stop],
        )
        valid_band, nonzero_band, explicit_nodata = _valid_pixels_from_raw(
            raw,
            band_name=band_name,
            scale_hint=scale_hint,
            root_attrs=root_attrs,
        )
        validity_masks.append(valid_band)
        nonzero_masks.append(nonzero_band)
        has_explicit_nodata = has_explicit_nodata or explicit_nodata
        array = np.asarray(raw, dtype=np.float32)
        channels[band_name] = (
            _normalize_channel(
                array,
                scale_hint=scale_hint,
                band_name=band_name,
                root_attrs=root_attrs,
            )
            if normalize
            else array
        )
    if include_validity:
        return channels, missing, _combine_validity_masks(
            validity_masks=validity_masks,
            nonzero_masks=nonzero_masks,
            scale_hint=scale_hint,
            has_explicit_nodata=has_explicit_nodata,
        )
    return channels, missing


def _normalize_channel(
    array: np.ndarray,
    *,
    scale_hint: str,
    band_name: str | None = None,
    root_attrs: dict[str, Any] | None = None,
) -> np.ndarray:
    normalized = np.asarray(array, dtype=np.float32)
    if scale_hint == "reflectance_0_10000":
        finite_max = float(np.nanmax(normalized)) if normalized.size else 0.0
        if finite_max > 1.5:
            normalized = normalized / 10000.0
        normalized = np.clip(normalized, 0.0, 1.0)
    elif scale_hint in {"landsat_l1_reflectance", "landsat_l2_reflectance"}:
        scaling = _resolve_landsat_scaling(
            scale_hint=scale_hint,
            band_name=band_name,
            root_attrs=root_attrs or {},
        )
        normalized = normalized * float(scaling["mult"]) + float(scaling["add"])
        if bool(scaling.get("apply_sun_elevation")):
            sun_elevation = float(scaling.get("sun_elevation") or 0.0)
            if 0.0 < sun_elevation < 90.0:
                normalized = normalized / max(math.sin(math.radians(sun_elevation)), 1e-3)
        normalized = np.clip(normalized, 0.0, 1.0)
    return normalized


def _resolve_landsat_scaling(
    *,
    scale_hint: str,
    band_name: str | None,
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


def _valid_pixels_from_raw(
    raw: np.ndarray,
    *,
    band_name: str,
    scale_hint: str,
    root_attrs: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray, bool]:
    del scale_hint
    raw_native = np.asarray(raw)
    raw_float = np.asarray(raw_native, dtype=np.float32)
    valid = np.isfinite(raw_float)
    nonzero = np.abs(raw_float) > 1e-6
    band_metadata = dict((root_attrs.get("band_metadata") or {}).get(str(band_name), {}) or {})
    explicit_nodata = False
    for key in ("target_nodata", "source_nodata"):
        nodata = _coerce_numeric_metadata(band_metadata.get(key))
        if nodata is None:
            continue
        explicit_nodata = True
        if np.issubdtype(raw_native.dtype, np.floating):
            valid = np.logical_and(valid, ~np.isclose(raw_float, nodata, atol=1e-6, rtol=0.0))
        else:
            valid = np.logical_and(valid, raw_native != raw_native.dtype.type(nodata))
    return valid, nonzero, explicit_nodata


def _combine_validity_masks(
    *,
    validity_masks: list[np.ndarray],
    nonzero_masks: list[np.ndarray],
    scale_hint: str,
    has_explicit_nodata: bool,
) -> np.ndarray:
    if not validity_masks:
        raise ConversionError("Cannot derive a validity mask because no bands were read.")
    valid = np.logical_or.reduce(validity_masks).astype(bool, copy=False)
    if not has_explicit_nodata and scale_hint in {
        "reflectance_0_10000",
        "landsat_l1_reflectance",
        "landsat_l2_reflectance",
    }:
        any_nonzero = np.logical_or.reduce(nonzero_masks).astype(bool, copy=False)
        valid = np.logical_and(valid, any_nonzero)
    return valid.astype(bool, copy=False)


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


__all__ = ["read_required_channels", "read_required_channels_window"]
