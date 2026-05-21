from __future__ import annotations

import math
import json
from pathlib import Path
import struct
from typing import Any
from urllib.parse import unquote
import zlib

import numpy as np

from nimbuschain_rgb_viewer_service.presets import choose_rgb_bands


def list_zarr_scenes(zarr_root: Path, *, job_id: str | None = None) -> list[dict[str, Any]]:
    root = zarr_root.expanduser().resolve()
    if not root.exists():
        return []
    scene_paths = _matching_scene_paths(root, job_id=job_id)
    return [
        inspect_zarr_scene(path)
        for path in scene_paths
        if path.is_dir() and (path / ".zgroup").exists()
    ]


def inspect_zarr_scene(path: str | Path) -> dict[str, Any]:
    import zarr

    zarr_path = _normalize_local_uri(path)
    group = zarr.open_group(str(zarr_path), mode="r", zarr_format=2)
    band_names = _read_band_names(group)
    provider = _clean_attr(group.attrs.get("provider"))
    collection = _clean_attr(group.attrs.get("collection"))
    product_type = _clean_attr(group.attrs.get("product_type"))
    scene_id = _clean_attr(group.attrs.get("scene_id")) or zarr_path.stem
    acquisition_datetime = _clean_attr(group.attrs.get("acquisition_datetime"))
    recommended_bands, preset_name = choose_rgb_bands(
        provider=provider,
        collection=collection,
        product_type=product_type,
        band_names=band_names,
    )
    shape = list(getattr(group.get("imagery"), "shape", []) or [])
    return {
        "scene_id": scene_id,
        "path": str(zarr_path),
        "kind": _scene_kind(zarr_path),
        "provider": provider,
        "collection": collection,
        "product_type": product_type,
        "acquisition_datetime": acquisition_datetime,
        "band_names": band_names,
        "shape": shape,
        "recommended_rgb": {
            "preset": preset_name,
            "bands": recommended_bands,
        },
    }


def _matching_scene_paths(root: Path, *, job_id: str | None) -> list[Path]:
    if not job_id:
        return sorted(
            path
            for path in root.rglob("*.zarr")
            if path.is_dir() and (path / ".zgroup").exists()
        )

    expected_scene_stems = _job_scene_stems(root, job_id)
    matches: list[Path] = []
    for path in root.rglob("*.zarr"):
        if not path.is_dir() or not (path / ".zgroup").exists():
            continue
        if path.stem in expected_scene_stems or job_id in path.parts:
            matches.append(path)
    return sorted(matches)


def _job_scene_stems(root: Path, job_id: str) -> set[str]:
    manifest_path = root.parent / job_id / "manifest.json"
    if not manifest_path.exists():
        return set()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    stems: set[str] = set()
    for item in list(manifest.get("paths") or []):
        stem = Path(str(item)).stem
        if stem:
            stems.add(stem)
    return stems


def _scene_kind(path: Path) -> str:
    return "cube" if "cubes" in path.parts else "scene"


def render_rgb_preview_png(
    path: str | Path,
    *,
    time_index: int = 0,
    max_size: int = 1024,
    bands: list[str] | None = None,
) -> tuple[bytes, dict[str, Any]]:
    import zarr

    zarr_path = _normalize_local_uri(path)
    group = zarr.open_group(str(zarr_path), mode="r", zarr_format=2)
    if "imagery" not in group:
        raise ValueError(f"Zarr store has no imagery array: {zarr_path}")

    imagery = group["imagery"]
    if len(imagery.shape) != 4:
        raise ValueError("Only imagery arrays with shape (time, band, y, x) are supported.")

    time_count, _, height, width = [int(value) for value in imagery.shape]
    if time_index < 0 or time_index >= time_count:
        raise ValueError(f"time_index must be between 0 and {time_count - 1}.")

    band_names = _read_band_names(group)
    rgb_bands, preset_name = choose_rgb_bands(
        provider=_clean_attr(group.attrs.get("provider")),
        collection=_clean_attr(group.attrs.get("collection")),
        product_type=_clean_attr(group.attrs.get("product_type")),
        band_names=band_names,
        requested_bands=bands,
    )
    band_lookup = {name.upper(): index for index, name in enumerate(band_names)}
    band_indexes = [band_lookup[name.upper()] for name in rgb_bands]

    safe_max_size = max(64, min(int(max_size), 2048))
    stride = max(1, int(math.ceil(max(height, width) / safe_max_size)))
    channels = [
        np.asarray(
            imagery[time_index, band_index, slice(None, None, stride), slice(None, None, stride)],
            dtype=np.float32,
        )
        for band_index in band_indexes
    ]
    rgb = np.stack([_stretch_channel(channel) for channel in channels], axis=-1)
    png = _encode_rgb_png(rgb)
    return png, {
        "scene_id": _clean_attr(group.attrs.get("scene_id")) or zarr_path.stem,
        "path": str(zarr_path),
        "time_index": int(time_index),
        "source_shape": [time_count, int(imagery.shape[1]), height, width],
        "preview_shape": [int(rgb.shape[0]), int(rgb.shape[1]), 3],
        "stride": int(stride),
        "rgb_bands": rgb_bands,
        "preset": preset_name,
    }


def resolve_scene_uri(uri: str, *, zarr_root: Path) -> Path:
    raw = unquote(str(uri or "").strip())
    if not raw:
        raise ValueError("Missing Zarr URI.")
    normalized = _normalize_local_uri(raw)
    if not normalized.is_absolute():
        cwd_relative = normalized.expanduser().resolve()
        normalized = cwd_relative if cwd_relative.exists() else zarr_root / normalized
    resolved = normalized.expanduser().resolve()
    if not resolved.exists():
        raise ValueError(f"Zarr store does not exist: {resolved}")
    if not resolved.is_dir() or resolved.suffix != ".zarr":
        raise ValueError(f"Expected a local .zarr directory: {resolved}")
    return resolved


def _normalize_local_uri(path: str | Path) -> Path:
    value = str(path).strip()
    if value.startswith("file://"):
        value = value.removeprefix("file://")
    return Path(unquote(value))


def _read_band_names(group: Any) -> list[str]:
    if "band" not in group:
        return [_decode_label(item) for item in list(group.attrs.get("band_names") or [])]
    return [_decode_label(item) for item in np.asarray(group["band"][:]).tolist()]


def _decode_label(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _clean_attr(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _stretch_channel(channel: np.ndarray) -> np.ndarray:
    values = np.asarray(channel, dtype=np.float32)
    finite = values[np.isfinite(values)]
    positive = finite[finite > 0]
    sample = positive if positive.size >= max(16, finite.size // 20) else finite
    if sample.size == 0:
        return np.zeros(values.shape, dtype=np.uint8)

    low, high = np.percentile(sample, [2, 98])
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        low = float(np.nanmin(sample))
        high = float(np.nanmax(sample))
    if high <= low:
        high = low + 1.0

    scaled = (values - low) / (high - low)
    scaled = np.clip(scaled, 0.0, 1.0)
    scaled[~np.isfinite(scaled)] = 0.0
    return (scaled * 255.0).astype(np.uint8)


def _encode_rgb_png(rgb: np.ndarray) -> bytes:
    if rgb.ndim != 3 or rgb.shape[2] != 3 or rgb.dtype != np.uint8:
        raise ValueError("PNG encoder expects an uint8 RGB array.")

    height, width, _ = rgb.shape
    raw_rows = b"".join(b"\x00" + rgb[row].tobytes() for row in range(height))
    payload = b"\x89PNG\r\n\x1a\n"
    payload += _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    payload += _png_chunk(b"IDAT", zlib.compress(raw_rows, level=6))
    payload += _png_chunk(b"IEND", b"")
    return payload


def _png_chunk(kind: bytes, data: bytes) -> bytes:
    checksum = zlib.crc32(kind)
    checksum = zlib.crc32(data, checksum) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", checksum)
