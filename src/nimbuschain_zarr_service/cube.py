from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

from nimbuschain_zarr_service.core import (
    ConversionDependencyError,
    ConversionError,
    _build_quadkey_metadata,
    _coerce_timestamp,
    _derive_spatial_coords,
    _open_existing_output_store,
    _prepare_output_store,
    is_oci_uri,
    resolve_output_path,
)
from nimbuschain_zarr_service.schema import ChunkShape, ZARR_FORMAT_VERSION


@dataclass(frozen=True)
class SourceScene:
    zarr_uri: str
    scene_id: str
    acquisition_time: str
    sort_key: tuple[str, str, str]
    provider: str | None
    collection: str | None
    product_type: str | None
    data_family: str | None
    crs: Any
    transform: Any
    reference_pixel_size: Any
    reference_band: str | None
    imagery_dtype: str
    imagery_shape: tuple[int, int, int, int]
    band_names: list[str]
    imagery_attrs: dict[str, Any]
    x_coords: np.ndarray | None
    y_coords: np.ndarray | None
    ancillary_dtype: str | None
    ancillary_shape: tuple[int, int, int, int] | None
    ancillary_layer_names: list[str]
    ancillary_attrs: dict[str, Any]


def build_time_cube(
    source_zarr_uris: list[str],
    output_uri: str,
    *,
    include_ancillary: bool = True,
) -> dict[str, Any]:
    try:
        import zarr
        from numcodecs import Blosc
    except ImportError as exc:
        raise ConversionDependencyError(
            "Cube-building dependencies are unavailable "
            f"({exc}). Ensure zarr and numcodecs are installed."
        ) from exc

    if not source_zarr_uris:
        raise ConversionError("At least one source Zarr store is required to build a cube.")

    scenes = [_load_source_scene(uri, include_ancillary=include_ancillary) for uri in source_zarr_uris]
    baseline = scenes[0]
    ancillary_mode = _validate_scene_compatibility(scenes, include_ancillary=include_ancillary)
    ordered_scenes = sorted(scenes, key=lambda item: item.sort_key)

    source_count = len(ordered_scenes)
    band_count = len(baseline.band_names)
    height = int(baseline.imagery_shape[2])
    width = int(baseline.imagery_shape[3])
    imagery_dtype = np.dtype(baseline.imagery_dtype)
    chunk_spec = ChunkShape()
    imagery_chunks = (
        min(chunk_spec.time, source_count),
        min(chunk_spec.band, band_count),
        min(chunk_spec.y, height),
        min(chunk_spec.x, width),
    )

    output_store, public_uri = _prepare_output_store(output_uri)
    root = zarr.open_group(output_store, mode="w", zarr_format=2)
    compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)

    imagery = root.create_array(
        "imagery",
        shape=(source_count, band_count, height, width),
        chunks=imagery_chunks,
        dtype=imagery_dtype,
        compressor=compressor,
    )

    time_values = [scene.acquisition_time for scene in ordered_scenes]
    scene_ids = [scene.scene_id for scene in ordered_scenes]
    source_uris = [scene.zarr_uri for scene in ordered_scenes]

    root.create_array("time", data=_string_array(time_values), overwrite=True)
    root.create_array("band", data=_string_array(baseline.band_names), overwrite=True)
    root.create_array("scene_id", data=_string_array(scene_ids), overwrite=True)
    root.create_array("source_zarr_uri", data=_string_array(source_uris), overwrite=True)

    x_coords = baseline.x_coords
    y_coords = baseline.y_coords
    if x_coords is None or y_coords is None:
        x_coords, y_coords = _derive_spatial_coords(
            baseline.transform,
            width=width,
            height=height,
        )
    if x_coords is not None:
        root.create_array("x", data=x_coords, chunks=(min(chunk_spec.x, width),), overwrite=True)
    if y_coords is not None:
        root.create_array("y", data=y_coords, chunks=(min(chunk_spec.y, height),), overwrite=True)

    ancillary = None
    ancillary_layer_names: list[str] = []
    ancillary_dtype: np.dtype[Any] | None = None
    ancillary_chunks: tuple[int, int, int, int] | None = None
    if ancillary_mode == "stack":
        ancillary_layer_names = list(baseline.ancillary_layer_names)
        ancillary_dtype = np.dtype(str(baseline.ancillary_dtype))
        ancillary_chunks = (
            min(chunk_spec.time, source_count),
            min(chunk_spec.band, len(ancillary_layer_names)),
            min(chunk_spec.y, height),
            min(chunk_spec.x, width),
        )
        ancillary = root.create_array(
            "ancillary",
            shape=(source_count, len(ancillary_layer_names), height, width),
            chunks=ancillary_chunks,
            dtype=ancillary_dtype,
            compressor=compressor,
        )
        root.create_array(
            "ancillary_layer",
            data=_string_array(ancillary_layer_names),
            overwrite=True,
        )

    for time_index, scene in enumerate(ordered_scenes):
        source_store = _open_existing_output_store(scene.zarr_uri)
        source_root = zarr.open_group(source_store, mode="r", zarr_format=2)
        imagery[time_index, :, :, :] = source_root["imagery"][0, :, :, :]
        if ancillary is not None:
            ancillary[time_index, :, :, :] = source_root["ancillary"][0, :, :, :]

    imagery_metadata = _sanitize_layer_metadata(baseline.imagery_attrs)
    ancillary_metadata = _sanitize_layer_metadata(baseline.ancillary_attrs)

    root.attrs.update(
        {
            "zarr_format_version": ZARR_FORMAT_VERSION,
            "cube_kind": "time_series",
            "source_scene_count": source_count,
            "provider": baseline.provider,
            "collection": baseline.collection,
            "product_type": baseline.product_type,
            "data_family": baseline.data_family,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [source_count, band_count, height, width],
            "dtype": str(imagery_dtype),
            "band_names": list(baseline.band_names),
            "band_metadata": imagery_metadata,
            "crs": baseline.crs,
            "transform": baseline.transform,
            "reference_band": baseline.reference_band,
            "reference_pixel_size": baseline.reference_pixel_size,
            "time_start": time_values[0],
            "time_end": time_values[-1],
            "source_scene_ids": list(scene_ids),
        }
    )

    if ancillary is not None and ancillary_dtype is not None:
        root.attrs.update(
            {
                "ancillary_layer_names": list(ancillary_layer_names),
                "ancillary_dimensions": ["time", "ancillary_layer", "y", "x"],
                "ancillary_shape": [source_count, len(ancillary_layer_names), height, width],
                "ancillary_dtype": str(ancillary_dtype),
                "ancillary_metadata": ancillary_metadata,
            }
        )
    elif include_ancillary:
        root.attrs["ancillary_omitted_reason"] = (
            "Ancillary layers were not written because the source stores do not all share "
            "the same ancillary schema."
        )

    quadkey_attrs = _build_quadkey_metadata(
        crs=baseline.crs,
        transform=baseline.transform,
        width=width,
        height=height,
        time_values=time_values,
        pixel_size=baseline.reference_pixel_size,
    )
    if quadkey_attrs:
        # A time cube uses one shared spatial footprint, so keep only the
        # root-level quadkey coverage instead of repeating identical entries
        # for each timestamp.
        quadkey_attrs.pop("quadkeys_by_time", None)
        root.attrs.update(quadkey_attrs)

    zarr.consolidate_metadata(output_store)

    cube_summary: dict[str, Any] = {
        "zarr_uri": public_uri,
        "cube_kind": "time_series",
        "source_scene_count": source_count,
        "source_zarr_uris": list(source_uris),
        "band_names": list(baseline.band_names),
        "shape": [source_count, band_count, height, width],
        "time_values": list(time_values),
        "scene_ids": list(scene_ids),
        "provider": baseline.provider,
        "collection": baseline.collection,
        "product_type": baseline.product_type,
        "data_family": baseline.data_family,
        "crs": baseline.crs,
        "transform": baseline.transform,
        "pixel_size": baseline.reference_pixel_size,
        "dimensions": ["time", "band", "y", "x"],
        "ancillary_written": ancillary is not None,
        "ancillary_layer_names": list(ancillary_layer_names),
    }
    if quadkey_attrs:
        cube_summary.update(
            {
                "quadkey_schema_version": quadkey_attrs.get("quadkey_schema_version"),
                "quadkey_coverage_mode": quadkey_attrs.get("quadkey_coverage_mode"),
                "quadkey_zoom_index": quadkey_attrs.get("quadkey_zoom_index"),
                "quadkeys_index": list(quadkey_attrs.get("quadkeys_index") or []),
            }
        )
    return cube_summary


def build_grouped_time_cubes(
    source_zarr_uris: list[str],
    output_dir: str,
    *,
    include_ancillary: bool = True,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    stage_label: str | None = None,
) -> dict[str, Any]:
    if not source_zarr_uris:
        return {
            "status": "skipped",
            "reason": "no_source_zarrs",
            "cube_outputs": [],
            "items": [],
            "tiles_built": [],
            "tiles_skipped": [],
        }

    scenes = [_load_source_scene(uri, include_ancillary=include_ancillary) for uri in source_zarr_uris]
    start_bound = _coerce_date_only(start_date)
    end_bound = _coerce_date_only(end_date)
    if start_bound and end_bound and end_bound < start_bound:
        raise ConversionError("Cube end date must be greater or equal to cube start date.")

    filtered_scenes = [
        scene
        for scene in scenes
        if _scene_within_date_range(scene, start_date=start_bound, end_date=end_bound)
    ]
    if not filtered_scenes:
        return {
            "status": "skipped",
            "reason": "no_scenes_in_date_range",
            "cube_outputs": [],
            "items": [],
            "tiles_built": [],
            "tiles_skipped": [],
        }

    grouped: dict[str, list[SourceScene]] = {}
    for scene in filtered_scenes:
        grouped.setdefault(_scene_group_key(scene.scene_id), []).append(scene)

    output_root = resolve_output_path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    cube_outputs: list[str] = []
    items: list[dict[str, Any]] = []
    tiles_built: list[str] = []
    tiles_skipped: list[dict[str, Any]] = []

    for group_key in sorted(grouped):
        ordered = sorted(grouped[group_key], key=lambda item: item.sort_key)
        unique_scenes, skipped_duplicates = _deduplicate_scenes_by_time(ordered)
        if len(unique_scenes) < 2:
            tiles_skipped.append(
                {
                    "group_key": group_key,
                    "reason": "fewer_than_two_unique_times",
                    "candidate_scene_ids": [scene.scene_id for scene in ordered],
                }
            )
            continue

        output_uri = str(
            output_root / _cube_output_name(
                group_key=group_key,
                scenes=unique_scenes,
                stage_label=stage_label,
            )
        )
        summary = build_time_cube(
            [scene.zarr_uri for scene in unique_scenes],
            output_uri,
            include_ancillary=include_ancillary,
        )
        cube_outputs.append(str(summary["zarr_uri"]))
        items.append(
            {
                **summary,
                "group_key": group_key,
                "skipped_duplicate_scene_ids": skipped_duplicates,
            }
        )
        tiles_built.append(group_key)

    status = "written" if cube_outputs else "skipped"
    reason = "" if cube_outputs else "no_groups_with_multiple_times"
    return {
        "status": status,
        "reason": reason,
        "cube_outputs": cube_outputs,
        "items": items,
        "tiles_built": tiles_built,
        "tiles_skipped": tiles_skipped,
        "stage_label": str(stage_label or "").strip() or None,
        "date_range": {
            "start_date": start_bound.isoformat() if start_bound else None,
            "end_date": end_bound.isoformat() if end_bound else None,
        },
    }


def _load_source_scene(source_zarr_uri: str, *, include_ancillary: bool) -> SourceScene:
    try:
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "Cube-building dependencies are unavailable "
            f"({exc}). Ensure zarr is installed."
        ) from exc

    source_store = _open_existing_output_store(source_zarr_uri)
    group = zarr.open_group(source_store, mode="r", zarr_format=2)

    if "imagery" not in group:
        raise ConversionError(f"Source Zarr has no 'imagery' array: {source_zarr_uri}")

    imagery = group["imagery"]
    if len(imagery.shape) != 4:
        raise ConversionError(
            "Source imagery must use the (time, band, y, x) layout: "
            f"{source_zarr_uri}"
        )
    if int(imagery.shape[0]) != 1:
        raise ConversionError(
            "Cube v1 only supports source scene Zarrs with time=1: "
            f"{source_zarr_uri}"
        )

    band_names = _read_label_array(group, "band")
    if len(band_names) != int(imagery.shape[1]):
        raise ConversionError(
            "Source band labels do not match imagery shape: "
            f"{source_zarr_uri}"
        )

    time_values = _read_label_array(group, "time")
    acquisition_time = _resolve_acquisition_time(group.attrs, time_values, source_zarr_uri)
    scene_id = str(group.attrs.get("scene_id") or "").strip()
    if not scene_id:
        scene_id = _default_scene_id(source_zarr_uri)

    ancillary_dtype: str | None = None
    ancillary_shape: tuple[int, int, int, int] | None = None
    ancillary_layer_names: list[str] = []
    ancillary_attrs: dict[str, Any] = {}
    if include_ancillary and "ancillary" in group:
        ancillary = group["ancillary"]
        if len(ancillary.shape) != 4:
            raise ConversionError(
                "Source ancillary must use the (time, ancillary_layer, y, x) layout: "
                f"{source_zarr_uri}"
            )
        if int(ancillary.shape[0]) != 1:
            raise ConversionError(
                "Cube v1 only supports source ancillary arrays with time=1: "
                f"{source_zarr_uri}"
            )
        if tuple(ancillary.shape[2:]) != tuple(imagery.shape[2:]):
            raise ConversionError(
                "Source ancillary grid does not match source imagery grid: "
                f"{source_zarr_uri}"
            )
        ancillary_layer_names = _read_label_array(group, "ancillary_layer")
        if len(ancillary_layer_names) != int(ancillary.shape[1]):
            raise ConversionError(
                "Source ancillary labels do not match ancillary shape: "
                f"{source_zarr_uri}"
            )
        ancillary_dtype = str(ancillary.dtype)
        ancillary_shape = tuple(int(value) for value in ancillary.shape)
        ancillary_attrs = dict(group.attrs.get("ancillary_metadata") or {})

    x_coords = np.asarray(group["x"][:]) if "x" in group else None
    y_coords = np.asarray(group["y"][:]) if "y" in group else None
    timestamp = _coerce_timestamp(acquisition_time).isoformat()

    return SourceScene(
        zarr_uri=_normalize_public_uri(source_zarr_uri),
        scene_id=scene_id,
        acquisition_time=timestamp,
        sort_key=(timestamp, scene_id, str(source_zarr_uri)),
        provider=_clean_optional_text(group.attrs.get("provider")),
        collection=_clean_optional_text(group.attrs.get("collection")),
        product_type=_clean_optional_text(group.attrs.get("product_type")),
        data_family=_clean_optional_text(group.attrs.get("data_family")),
        crs=group.attrs.get("crs"),
        transform=group.attrs.get("transform"),
        reference_pixel_size=group.attrs.get("reference_pixel_size"),
        reference_band=_clean_optional_text(group.attrs.get("reference_band")),
        imagery_dtype=str(imagery.dtype),
        imagery_shape=tuple(int(value) for value in imagery.shape),
        band_names=band_names,
        imagery_attrs=dict(group.attrs.get("band_metadata") or {}),
        x_coords=x_coords,
        y_coords=y_coords,
        ancillary_dtype=ancillary_dtype,
        ancillary_shape=ancillary_shape,
        ancillary_layer_names=ancillary_layer_names,
        ancillary_attrs=ancillary_attrs,
    )


def _validate_scene_compatibility(
    scenes: list[SourceScene],
    *,
    include_ancillary: bool,
) -> str:
    baseline = scenes[0]
    ancillary_mode = "stack" if include_ancillary and baseline.ancillary_layer_names else "skip"

    for scene in scenes[1:]:
        _ensure_same(scene.provider, baseline.provider, field_name="provider", scene=scene)
        _ensure_same(scene.collection, baseline.collection, field_name="collection", scene=scene)
        _ensure_same(scene.product_type, baseline.product_type, field_name="product_type", scene=scene)
        _ensure_same(scene.data_family, baseline.data_family, field_name="data_family", scene=scene)
        _ensure_same(scene.crs, baseline.crs, field_name="crs", scene=scene)
        _ensure_same(
            list(scene.transform) if isinstance(scene.transform, (list, tuple)) else scene.transform,
            list(baseline.transform) if isinstance(baseline.transform, (list, tuple)) else baseline.transform,
            field_name="transform",
            scene=scene,
        )
        _ensure_same(
            list(scene.reference_pixel_size)
            if isinstance(scene.reference_pixel_size, (list, tuple))
            else scene.reference_pixel_size,
            list(baseline.reference_pixel_size)
            if isinstance(baseline.reference_pixel_size, (list, tuple))
            else baseline.reference_pixel_size,
            field_name="reference_pixel_size",
            scene=scene,
        )
        _ensure_same(scene.imagery_shape[1:], baseline.imagery_shape[1:], field_name="imagery_shape", scene=scene)
        _ensure_same(scene.imagery_dtype, baseline.imagery_dtype, field_name="imagery_dtype", scene=scene)
        _ensure_same(scene.band_names, baseline.band_names, field_name="band_names", scene=scene)

        if not _coords_equal(scene.x_coords, baseline.x_coords):
            raise ConversionError(
                f"Source Zarr x coordinates do not match the cube grid: {scene.zarr_uri}"
            )
        if not _coords_equal(scene.y_coords, baseline.y_coords):
            raise ConversionError(
                f"Source Zarr y coordinates do not match the cube grid: {scene.zarr_uri}"
            )

        if ancillary_mode == "stack":
            if not scene.ancillary_layer_names:
                ancillary_mode = "skip"
                continue
            if scene.ancillary_layer_names != baseline.ancillary_layer_names:
                ancillary_mode = "skip"
                continue
            if (
                (scene.ancillary_shape[1:] if scene.ancillary_shape else None)
                != (baseline.ancillary_shape[1:] if baseline.ancillary_shape else None)
            ):
                ancillary_mode = "skip"
                continue
            if scene.ancillary_dtype != baseline.ancillary_dtype:
                ancillary_mode = "skip"
                continue
        elif include_ancillary and scene.ancillary_layer_names:
            ancillary_mode = "skip"

    return ancillary_mode


def _ensure_same(expected: Any, actual: Any, *, field_name: str, scene: SourceScene) -> None:
    if expected != actual:
        raise ConversionError(
            f"Source Zarr {field_name} does not match the cube baseline: {scene.zarr_uri}"
        )


def _coords_equal(left: np.ndarray | None, right: np.ndarray | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return bool(np.array_equal(left, right))


def _read_label_array(group: Any, key: str) -> list[str]:
    if key not in group:
        return []
    values = group[key][:]
    try:
        items = values.tolist()
    except Exception:
        items = list(values)
    return [_normalize_label(item) for item in items]


def _normalize_label(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode(errors="replace")
    return str(value)


def _resolve_acquisition_time(
    attrs: dict[str, Any],
    time_values: list[str],
    source_zarr_uri: str,
) -> str:
    candidates = [
        str(attrs.get("acquisition_datetime") or "").strip(),
        time_values[0] if time_values else "",
    ]
    for candidate in candidates:
        if candidate:
            return _coerce_timestamp(candidate).isoformat()
    raise ConversionError(f"Source Zarr is missing acquisition time metadata: {source_zarr_uri}")


def _default_scene_id(source_zarr_uri: str) -> str:
    if is_oci_uri(source_zarr_uri):
        return str(source_zarr_uri).rstrip("/").split("/")[-1] or "scene"
    return resolve_output_path(source_zarr_uri).stem or "scene"


def _normalize_public_uri(source_zarr_uri: str) -> str:
    if is_oci_uri(source_zarr_uri):
        return source_zarr_uri
    return str(resolve_output_path(source_zarr_uri))


def _string_array(values: list[str]) -> np.ndarray:
    width = max(1, max(len(str(value)) for value in values))
    return np.asarray([str(value) for value in values], dtype=f"<U{width}")


def _sanitize_layer_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for layer_name, raw_payload in dict(metadata or {}).items():
        payload = dict(raw_payload or {})
        payload.pop("path", None)
        sanitized[str(layer_name)] = payload
    return sanitized


def _clean_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _coerce_date_only(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return _coerce_timestamp(text).date()


def _scene_within_date_range(
    scene: SourceScene,
    *,
    start_date: date | None,
    end_date: date | None,
) -> bool:
    scene_date = _coerce_timestamp(scene.acquisition_time).date()
    if start_date is not None and scene_date < start_date:
        return False
    if end_date is not None and scene_date > end_date:
        return False
    return True


def _scene_group_key(scene_id: str) -> str:
    sentinel_match = re.search(r"_T(\d{2}[A-Z]{3})_", str(scene_id or ""))
    if sentinel_match:
        return sentinel_match.group(1)
    landsat_parts = str(scene_id or "").split("_")
    if len(landsat_parts) >= 3 and landsat_parts[2].isdigit():
        return landsat_parts[2]
    return str(scene_id or "scene")


def _deduplicate_scenes_by_time(scenes: list[SourceScene]) -> tuple[list[SourceScene], list[str]]:
    unique: list[SourceScene] = []
    skipped: list[str] = []
    seen_times: set[str] = set()
    for scene in scenes:
        if scene.acquisition_time in seen_times:
            skipped.append(scene.scene_id)
            continue
        seen_times.add(scene.acquisition_time)
        unique.append(scene)
    return unique, skipped


def _cube_output_name(
    *,
    group_key: str,
    scenes: list[SourceScene],
    stage_label: str | None,
) -> str:
    start_stamp = _coerce_timestamp(scenes[0].acquisition_time).strftime("%Y%m%d")
    end_stamp = _coerce_timestamp(scenes[-1].acquisition_time).strftime("%Y%m%d")
    safe_group = re.sub(r"[^A-Za-z0-9._-]+", "_", str(group_key or "scene")).strip("._-") or "scene"
    suffix = ""
    if stage_label:
        safe_stage = re.sub(r"[^A-Za-z0-9._-]+", "_", str(stage_label)).strip("._-")
        if safe_stage:
            suffix = f"_{safe_stage}"
    return f"cube_{safe_group}_{start_stamp}_{end_stamp}{suffix}.zarr"
