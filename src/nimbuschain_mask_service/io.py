from __future__ import annotations

from dataclasses import dataclass
import math
import os
from pathlib import Path
import uuid
import shutil
import subprocess
from typing import Any
from urllib.parse import unquote, urlparse

import numpy as np

from nimbuschain_zarr_service.core import ConversionError


@dataclass(frozen=True)
class ZarrMaskContext:
    zarr_uri: str
    provider: str | None
    collection: str | None
    product_type: str | None
    scene_id: str | None
    band_names: tuple[str, ...]
    imagery_shape: tuple[int, ...]


_CONTAINER_DATA_PREFIXES = (
    "/data/downloads",
    "/app/data/downloads",
    "/download",
    "/downloads",
    "/app/download",
    "/app/downloads",
)


def _project_data_root() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "downloads"


def _resolve_candidate(path: Path) -> Path:
    try:
        return path.expanduser().resolve(strict=False)
    except TypeError:  # pragma: no cover - compatibility guard
        return path.expanduser().resolve()


def _host_data_root_candidates() -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for raw in (
        os.getenv("NIMBUS_HOST_DATA_DIR"),
        os.getenv("NIMBUS_DATA_DIR"),
        str(_project_data_root()),
    ):
        value = str(raw or "").strip()
        if not value:
            continue
        candidate = Path(value).expanduser()
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)
    return candidates


def _map_container_data_uri(raw_value: str, *, allow_missing: bool) -> Path | None:
    for prefix in _CONTAINER_DATA_PREFIXES:
        if raw_value == prefix or raw_value.startswith(prefix + "/"):
            suffix = raw_value[len(prefix) :].lstrip("/")
            fallback: Path | None = None
            for root in _host_data_root_candidates():
                mapped = root / suffix if suffix else root
                if mapped.exists():
                    return _resolve_candidate(mapped)
                if allow_missing and fallback is None:
                    fallback = _resolve_candidate(mapped)
            return fallback
    return None


def _map_data_downloads_suffix(candidate: Path, *, allow_missing: bool) -> Path | None:
    parts = list(candidate.parts)
    for idx in range(len(parts) - 1):
        if parts[idx] == "data" and parts[idx + 1] == "downloads":
            suffix = parts[idx + 2 :]
            fallback: Path | None = None
            for root in _host_data_root_candidates():
                mapped = root.joinpath(*suffix)
                if mapped.exists():
                    return _resolve_candidate(mapped)
                if allow_missing and fallback is None:
                    fallback = _resolve_candidate(mapped)
            return fallback
    return None


def local_path_for_uri(uri: str, *, allow_missing: bool = True) -> Path:
    raw_value = str(uri or "").strip()
    if not raw_value:
        return _resolve_candidate(Path("."))

    parsed = urlparse(raw_value)
    if parsed.scheme and parsed.scheme.lower() not in {"", "file"}:
        raise ConversionError(f"Unsupported local path URI for masking: {uri}")

    if parsed.scheme.lower() == "file":
        raw_value = unquote(parsed.path)

    mapped = _map_container_data_uri(raw_value, allow_missing=allow_missing)
    if mapped is not None:
        return mapped

    candidate = Path(raw_value).expanduser()
    if candidate.exists():
        return candidate.resolve()

    mapped = _map_data_downloads_suffix(candidate, allow_missing=allow_missing)
    if mapped is not None:
        return mapped

    return _resolve_candidate(candidate)


def open_zarr_group(zarr_uri: str, *, mode: str = "r") -> Any:
    try:
        import zarr
    except ImportError as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError(f"Masking requires zarr runtime support ({exc}).") from exc

    store_path = local_path_for_uri(zarr_uri)
    if not store_path.exists():
        raise ConversionError(f"Output store does not exist yet: {store_path}")
    return zarr.open_group(str(store_path), mode=mode, zarr_format=2)


def delete_mask_layers(zarr_uri: str, *, layer_names: tuple[str, ...]) -> None:
    if not layer_names:
        return
    try:
        import zarr
    except ImportError:
        return

    try:
        root = open_zarr_group(zarr_uri, mode="a")
    except Exception:
        return

    masks_group = root.get("masks")
    if masks_group is None:
        return

    touched = False
    for layer_name in layer_names:
        if layer_name in masks_group:
            del masks_group[layer_name]
            touched = True

    if not touched:
        return

    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass


def read_context(root: Any, *, zarr_uri: str) -> ZarrMaskContext:
    if "imagery" not in root:
        raise ConversionError("Zarr store does not contain an 'imagery' array.")
    imagery = root["imagery"]
    if len(imagery.shape) != 4:
        raise ConversionError(
            f"Expected imagery to be 4D (time, band, y, x), got shape={tuple(imagery.shape)}"
        )

    band_names: list[str] = []
    if "band" in root:
        raw_values = np.asarray(root["band"][:]).tolist()
        for value in raw_values:
            if isinstance(value, bytes):
                band_names.append(value.decode("utf-8", errors="replace"))
            else:
                band_names.append(str(value))
    else:
        band_names = [str(value) for value in (root.attrs.get("band_names") or [])]

    if not band_names:
        raise ConversionError("Zarr store does not expose band names (band coord/attrs missing).")

    return ZarrMaskContext(
        zarr_uri=zarr_uri,
        provider=_attr_as_text(root.attrs.get("provider")),
        collection=_attr_as_text(root.attrs.get("collection")),
        product_type=_attr_as_text(root.attrs.get("product_type")),
        scene_id=_attr_as_text(root.attrs.get("scene_id")),
        band_names=tuple(band_names),
        imagery_shape=tuple(int(v) for v in imagery.shape),
    )


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


def copy_source_zarr(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    source_path = local_path_for_uri(source_zarr_uri)
    output_path = local_path_for_uri(output_zarr_uri)
    if not source_path.exists():
        raise ConversionError(f"Source Zarr store not found: {source_zarr_uri}")
    if not source_path.is_dir():
        raise ConversionError(f"Source Zarr store must be a directory: {source_zarr_uri}")
    if source_path.resolve() == output_path.resolve():
        raise ConversionError("Masked Zarr output must differ from the source Zarr store.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        shutil.rmtree(output_path)
    copy_errors: list[str] = []
    for copier in (_copytree_via_cp, _copytree_via_rsync, _copytree_via_python):
        try:
            copier(source_path=source_path, output_path=output_path)
            return str(output_path)
        except Exception as exc:
            copy_errors.append(f"{copier.__name__}: {exc}")
            try:
                if output_path.exists():
                    shutil.rmtree(output_path)
            except OSError:
                pass
    raise ConversionError(
        "Failed to create derived masked Zarr copy. "
        + " | ".join(copy_errors)
    )


def cleanup_derived_zarr(output_zarr_uri: str) -> None:
    target = local_path_for_uri(output_zarr_uri)
    try:
        if target.exists():
            shutil.rmtree(target)
    except OSError:
        return


def temporary_derived_zarr_uri(output_zarr_uri: str) -> str:
    target = local_path_for_uri(output_zarr_uri)
    suffix = target.suffix
    tmp_name = f".{target.stem}.tmp-{uuid.uuid4().hex}{suffix}"
    return str(target.with_name(tmp_name))


def promote_derived_zarr(*, temp_zarr_uri: str, final_zarr_uri: str, overwrite: bool = True) -> str:
    temp_path = local_path_for_uri(temp_zarr_uri)
    final_path = local_path_for_uri(final_zarr_uri)
    if not temp_path.exists():
        raise ConversionError(f"Temporary masked Zarr store not found: {temp_zarr_uri}")
    if temp_path.resolve() == final_path.resolve():
        raise ConversionError("Temporary and final masked Zarr paths must differ.")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path: Path | None = None
    if final_path.exists():
        if not overwrite:
            raise ConversionError(f"Masked Zarr store already exists: {final_zarr_uri}")
        backup_path = final_path.with_name(
            f".{final_path.stem}.backup-{uuid.uuid4().hex}{final_path.suffix}"
        )
        if backup_path.exists():
            shutil.rmtree(backup_path)
        final_path.rename(backup_path)
    try:
        temp_path.rename(final_path)
    except Exception:
        if backup_path is not None and backup_path.exists() and not final_path.exists():
            backup_path.rename(final_path)
        raise
    if backup_path is not None and backup_path.exists():
        shutil.rmtree(backup_path)
    return str(final_path)


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


def _attr_as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _copytree_via_cp(*, source_path: Path, output_path: Path) -> None:
    cp_binary = shutil.which("cp")
    if not cp_binary:
        raise RuntimeError("cp is not available")
    output_path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [cp_binary, "-a", "--reflink=auto", f"{source_path}/.", str(output_path)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _copytree_via_rsync(*, source_path: Path, output_path: Path) -> None:
    rsync_binary = shutil.which("rsync")
    if not rsync_binary:
        raise RuntimeError("rsync is not available")
    output_path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [rsync_binary, "-a", f"{source_path}/", f"{output_path}/"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _copytree_via_python(*, source_path: Path, output_path: Path) -> None:
    shutil.copytree(source_path, output_path)
