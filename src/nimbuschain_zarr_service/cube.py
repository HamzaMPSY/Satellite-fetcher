from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from affine import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.warp import reproject, transform_bounds

import numpy as np

from nimbuschain_zarr_service.core import (
    ConversionDependencyError,
    ConversionError,
    _build_quadkey_metadata,
    _coerce_timestamp,
    _open_existing_output_store,
    _prepare_output_store,
    is_oci_uri,
)
from nimbuschain_zarr_service.cube_support import (
    clean_optional_text as _clean_optional_text,
    coerce_date_only as _coerce_date_only,
    copy_time_slice_in_chunks as _copy_time_slice_in_chunks,
    default_scene_id as _default_scene_id,
    normalize_label as _normalize_label,
    normalize_public_uri as _normalize_public_uri,
    normalized_chunk_shape as _normalized_chunk_shape,
    read_label_array as _read_label_array,
    resolve_acquisition_time as _resolve_acquisition_time,
    sanitize_layer_metadata as _sanitize_layer_metadata,
    sanitize_mask_array_attrs as _sanitize_mask_array_attrs,
    string_array as _string_array,
    time_slice_block_count as _time_slice_block_count,
)
from nimbuschain_zarr_service.models import (
    CubeBuildSummaryRecord,
    GroupedCubeItemRecord,
    GroupedCubeSkippedRecord,
    GroupedCubeSummaryRecord,
)
from nimbuschain_zarr_service.spatial_support import derive_spatial_coords as _derive_spatial_coords
from nimbuschain_zarr_service.storage_support import resolve_output_path
from nimbuschain_zarr_service.schema import ChunkShape, ZARR_FORMAT_VERSION


@dataclass(frozen=True)
class MaskLayer:
    name: str
    dtype: str
    shape: tuple[int, int, int]
    attrs: dict[str, Any]


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
    mask_layers: dict[str, MaskLayer]


CubeProgressCallback = Callable[[dict[str, Any]], None]

@dataclass(frozen=True)
class MosaicGrid:
    crs: str
    transform: list[float]
    width: int
    height: int
    x_coords: np.ndarray
    y_coords: np.ndarray


def build_time_cube(
    source_zarr_uris: list[str],
    output_uri: str,
    *,
    include_ancillary: bool = True,
    include_masks: bool = False,
    progress_callback: CubeProgressCallback | None = None,
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
    ancillary_mode, mask_layer_names = _validate_scene_compatibility(
        scenes,
        include_ancillary=include_ancillary,
        include_masks=include_masks,
    )
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

    masks_group = None
    mask_arrays: dict[str, Any] = {}
    if mask_layer_names:
        masks_group = root.require_group("masks")
        masks_group.attrs.update(
            {
                "dimensions": ["time", "y", "x"],
                "mask_layer_names": list(mask_layer_names),
            }
        )
        mask_chunks = (
            min(chunk_spec.time, source_count),
            min(chunk_spec.y, height),
            min(chunk_spec.x, width),
        )
        for mask_name in mask_layer_names:
            mask_spec = baseline.mask_layers[mask_name]
            target = masks_group.create_array(
                mask_name,
                shape=(source_count, height, width),
                chunks=mask_chunks,
                dtype=np.dtype(mask_spec.dtype),
                compressor=compressor,
            )
            target.attrs.update(_sanitize_mask_array_attrs(mask_spec.attrs))
            mask_arrays[mask_name] = target

    total_blocks = source_count * _time_slice_block_count(imagery)
    if ancillary is not None:
        total_blocks += source_count * _time_slice_block_count(ancillary)
    if mask_arrays:
        total_blocks += source_count * sum(
            _time_slice_block_count(target) for target in mask_arrays.values()
        )
    blocks_written = 0

    for time_index, scene in enumerate(ordered_scenes):
        source_store = _open_existing_output_store(scene.zarr_uri)
        source_root = zarr.open_group(source_store, mode="r", zarr_format=2)

        def _scene_progress(payload: dict[str, Any]) -> None:
            if progress_callback is None:
                return
            progress_callback(
                {
                    **payload,
                    "scene_id": scene.scene_id,
                    "scene_index": time_index + 1,
                    "scene_total": source_count,
                    "cube_output_uri": public_uri,
                }
            )

        blocks_written = _copy_time_slice_in_chunks(
            source_root["imagery"],
            imagery,
            source_time_index=0,
            target_time_index=time_index,
            layer_name="imagery",
            label_names=baseline.band_names,
            blocks_written=blocks_written,
            total_blocks=total_blocks,
            progress_callback=_scene_progress,
        )
        if ancillary is not None:
            blocks_written = _copy_time_slice_in_chunks(
                source_root["ancillary"],
                ancillary,
                source_time_index=0,
                target_time_index=time_index,
                layer_name="ancillary",
                label_names=ancillary_layer_names,
                blocks_written=blocks_written,
                total_blocks=total_blocks,
                progress_callback=_scene_progress,
            )
        if mask_arrays:
            source_masks = source_root["masks"]
            for mask_name, target in mask_arrays.items():
                blocks_written = _copy_time_slice_in_chunks(
                    source_masks[mask_name],
                    target,
                    source_time_index=0,
                    target_time_index=time_index,
                    layer_name=f"masks/{mask_name}",
                    blocks_written=blocks_written,
                    total_blocks=total_blocks,
                    progress_callback=_scene_progress,
                )

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

    if mask_arrays:
        root.attrs.update(
            {
                "mask_dimensions": ["time", "y", "x"],
                "mask_layer_names": list(mask_layer_names),
            }
        )
    elif include_masks:
        root.attrs["masks_omitted_reason"] = (
            "Mask layers were not written because the source stores do not all share "
            "the same masks schema."
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

    return CubeBuildSummaryRecord(
        zarr_uri=public_uri,
        cube_kind="time_series",
        source_scene_count=source_count,
        source_zarr_uris=list(source_uris),
        band_names=list(baseline.band_names),
        shape=[source_count, band_count, height, width],
        time_values=list(time_values),
        scene_ids=list(scene_ids),
        provider=baseline.provider,
        collection=baseline.collection,
        product_type=baseline.product_type,
        data_family=baseline.data_family,
        crs=baseline.crs,
        transform=baseline.transform,
        pixel_size=baseline.reference_pixel_size,
        ancillary_written=ancillary is not None,
        ancillary_layer_names=list(ancillary_layer_names),
        masks_written=bool(mask_arrays),
        mask_layer_names=list(mask_layer_names),
        quadkey_schema_version=quadkey_attrs.get("quadkey_schema_version") if quadkey_attrs else None,
        quadkey_coverage_mode=quadkey_attrs.get("quadkey_coverage_mode") if quadkey_attrs else None,
        quadkey_zoom_index=quadkey_attrs.get("quadkey_zoom_index") if quadkey_attrs else None,
        quadkeys_index=list(quadkey_attrs.get("quadkeys_index") or []) if quadkey_attrs else [],
    ).to_dict()


def build_grouped_time_cubes(
    source_zarr_uris: list[str],
    output_dir: str,
    *,
    include_ancillary: bool = True,
    include_masks: bool | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    stage_label: str | None = None,
    progress_callback: CubeProgressCallback | None = None,
) -> dict[str, Any]:
    if not source_zarr_uris:
        return GroupedCubeSummaryRecord(
            status="skipped",
            reason="no_source_zarrs",
        ).to_dict()

    resolved_include_masks = _include_masks_for_stage(stage_label) if include_masks is None else bool(include_masks)

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
        return GroupedCubeSummaryRecord(
            status="skipped",
            reason="no_scenes_in_date_range",
        ).to_dict()

    grouped: dict[str, list[SourceScene]] = {}
    for scene in filtered_scenes:
        grouped.setdefault(_scene_group_key(scene.scene_id), []).append(scene)

    output_root = resolve_output_path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    cube_outputs: list[str] = []
    items: list[dict[str, Any]] = []
    tiles_built: list[str] = []
    tiles_skipped: list[dict[str, Any]] = []
    buildable_groups: list[tuple[str, list[SourceScene], list[str]]] = []

    for group_key in sorted(grouped):
        ordered = sorted(grouped[group_key], key=lambda item: item.sort_key)
        unique_scenes, skipped_duplicates = _deduplicate_scenes_by_time(ordered)
        if len(unique_scenes) < 2:
            tiles_skipped.append(
                GroupedCubeSkippedRecord(
                    group_key=group_key,
                    reason="fewer_than_two_unique_times",
                    candidate_scene_ids=[scene.scene_id for scene in ordered],
                ).to_dict()
            )
            continue
        buildable_groups.append((group_key, unique_scenes, skipped_duplicates))

    group_total = len(buildable_groups)
    for group_index, (group_key, unique_scenes, skipped_duplicates) in enumerate(
        buildable_groups,
        start=1,
    ):
        output_uri = str(
            output_root / _cube_output_name(
                group_key=group_key,
                scenes=unique_scenes,
                stage_label=stage_label,
            )
        )

        def _group_progress(payload: dict[str, Any]) -> None:
            if progress_callback is None:
                return
            progress_callback(
                {
                    **payload,
                    "group_key": group_key,
                    "group_index": group_index,
                    "group_total": group_total,
                    "group_output_uri": output_uri,
                }
            )

        summary = build_time_cube(
            [scene.zarr_uri for scene in unique_scenes],
            output_uri,
            include_ancillary=include_ancillary,
            include_masks=resolved_include_masks,
            progress_callback=_group_progress,
        )
        cube_outputs.append(str(summary["zarr_uri"]))
        items.append(
            GroupedCubeItemRecord(
                summary=CubeBuildSummaryRecord(
                    zarr_uri=str(summary["zarr_uri"]),
                    cube_kind=str(summary["cube_kind"]),
                    source_scene_count=int(summary["source_scene_count"]),
                    source_zarr_uris=list(summary.get("source_zarr_uris") or []),
                    band_names=list(summary.get("band_names") or []),
                    shape=list(summary.get("shape") or []),
                    time_values=list(summary.get("time_values") or []),
                    scene_ids=list(summary.get("scene_ids") or []),
                    provider=summary.get("provider"),
                    collection=summary.get("collection"),
                    product_type=summary.get("product_type"),
                    data_family=summary.get("data_family"),
                    crs=summary.get("crs"),
                    transform=summary.get("transform"),
                    pixel_size=summary.get("pixel_size"),
                    dimensions=list(summary.get("dimensions") or ["time", "band", "y", "x"]),
                    ancillary_written=bool(summary.get("ancillary_written")),
                    ancillary_layer_names=list(summary.get("ancillary_layer_names") or []),
                    masks_written=bool(summary.get("masks_written")),
                    mask_layer_names=list(summary.get("mask_layer_names") or []),
                    quadkey_schema_version=summary.get("quadkey_schema_version"),
                    quadkey_coverage_mode=summary.get("quadkey_coverage_mode"),
                    quadkey_zoom_index=summary.get("quadkey_zoom_index"),
                    quadkeys_index=list(summary.get("quadkeys_index") or []),
                ),
                group_key=group_key,
                skipped_duplicate_scene_ids=skipped_duplicates,
            ).to_dict()
        )
        tiles_built.append(group_key)

    status = "written" if cube_outputs else "skipped"
    reason = "" if cube_outputs else "no_groups_with_multiple_times"
    return GroupedCubeSummaryRecord(
        status=status,
        reason=reason,
        cube_outputs=cube_outputs,
        items=items,
        tiles_built=tiles_built,
        tiles_skipped=tiles_skipped,
        stage_label=str(stage_label or "").strip() or None,
        date_range={
            "start_date": start_bound.isoformat() if start_bound else None,
            "end_date": end_bound.isoformat() if end_bound else None,
        },
    ).to_dict()


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
    mask_layers: dict[str, MaskLayer] = {}
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

    if "masks" in group:
        masks_group = group["masks"]
        for mask_name in sorted(masks_group.array_keys()):
            mask_array = masks_group[mask_name]
            if len(mask_array.shape) != 3:
                raise ConversionError(
                    "Source mask arrays must use the (time, y, x) layout: "
                    f"{source_zarr_uri}"
                )
            if int(mask_array.shape[0]) != 1:
                raise ConversionError(
                    "Cube v1 only supports source mask arrays with time=1: "
                    f"{source_zarr_uri}"
                )
            if tuple(mask_array.shape[1:]) != tuple(imagery.shape[2:]):
                raise ConversionError(
                    "Source mask grid does not match source imagery grid: "
                    f"{source_zarr_uri}"
                )
            mask_layers[mask_name] = MaskLayer(
                name=mask_name,
                dtype=str(mask_array.dtype),
                shape=tuple(int(value) for value in mask_array.shape),
                attrs=dict(mask_array.attrs),
            )

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
        mask_layers=mask_layers,
    )


def _validate_scene_compatibility(
    scenes: list[SourceScene],
    *,
    include_ancillary: bool,
    include_masks: bool,
) -> tuple[str, list[str]]:
    baseline = scenes[0]
    ancillary_mode = "stack" if include_ancillary and baseline.ancillary_layer_names else "skip"
    mask_layer_names = _validate_mask_compatibility(
        scenes,
        include_masks=include_masks,
    )

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

    return ancillary_mode, mask_layer_names


def _validate_mask_compatibility(
    scenes: list[SourceScene],
    *,
    include_masks: bool,
) -> list[str]:
    if not include_masks:
        return []

    baseline = scenes[0]
    if not baseline.mask_layers:
        raise ConversionError(
            f"Cube requested masks, but source Zarr has no masks group: {baseline.zarr_uri}"
        )

    mask_layer_names = sorted(baseline.mask_layers)
    for scene in scenes[1:]:
        scene_mask_names = sorted(scene.mask_layers)
        if scene_mask_names != mask_layer_names:
            raise ConversionError(
                "Source Zarr masks do not match the cube baseline: "
                f"{scene.zarr_uri}"
            )
        for mask_name in mask_layer_names:
            baseline_mask = baseline.mask_layers[mask_name]
            scene_mask = scene.mask_layers[mask_name]
            _ensure_same(
                scene_mask.dtype,
                baseline_mask.dtype,
                field_name=f"masks/{mask_name}.dtype",
                scene=scene,
            )
            _ensure_same(
                scene_mask.shape[1:],
                baseline_mask.shape[1:],
                field_name=f"masks/{mask_name}.shape",
                scene=scene,
            )
    return mask_layer_names


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


def _include_masks_for_stage(stage_label: str | None) -> bool:
    return str(stage_label or "").strip().lower() == "after_mask"


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
    compact_landsat = _compact_landsat_path_row(scene_id)
    if compact_landsat:
        return compact_landsat
    landsat_parts = str(scene_id or "").split("_")
    if len(landsat_parts) >= 3 and landsat_parts[2].isdigit():
        return landsat_parts[2]
    return str(scene_id or "scene")


def _compact_landsat_path_row(scene_id: str) -> str:
    value = str(scene_id or "").strip().upper()
    value = value.rsplit("/", 1)[-1]
    value = re.sub(r"\.(TAR|ZIP|TGZ|GZ)$", "", value)
    match = re.match(r"^L[A-Z]\d(?P<path>\d{3})(?P<row>\d{3})\d{7}[A-Z0-9]*$", value)
    if not match:
        return ""
    return f"{match.group('path')}{match.group('row')}"


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

#helpers for the multi tiles cube building 

#compute the spatial bounds of a scene fro, its raster transform and image size return (left, bottom, right, top)
def _scene_bounds(scene: SourceScene) -> tuple[float, float, float, float]:
    if not isinstance(scene.transform, (list, tuple)) or len(scene.transform) < 6:
        raise ConversionError(f"Scene has no usable transform: {scene.zarr_uri}")
    a, b, c, d, e, f = [float(v) for v in scene.transform[:6]]
    if b != 0.0 or d != 0.0:
        raise ConversionError(f"Rotated transforms are not supported yet: {scene.zarr_uri}")

    width = int(scene.imagery_shape[3])
    height = int(scene.imagery_shape[2])

    left = c
    right = c + a * width
    top = f
    bottom = f + e * height

    return (
        min(left, right),
        min(bottom, top),
        max(left, right),
        max(bottom, top),
    )

#To choose the most common  CRS 
def _choose_target_crs(scenes: list[SourceScene], explicit_crs: str | None) -> tuple[str, str]:
    if explicit_crs:
        return str(CRS.from_user_input(explicit_crs)), "explicit"

    values = [
        str(CRS.from_user_input(scene.crs))
        for scene in scenes
        if scene.crs is not None and str(scene.crs).strip()
    ]
    if not values:
        raise ConversionError("Could not determine target CRS from source scenes.")

    target_crs_value, _ = Counter(values).most_common(1)[0]
    return target_crs_value, "dominant_source_crs"

#To build one common output grid for all the scenes that is large enough to contain all input scenes after being expressed in the same CRS
def _build_mosaic_grid(
        scenes: list[SourceScene],
        *,
        target_crs: str,
        resolution_m: int,
) -> MosaicGrid:
    min_x = math.inf
    min_y = math.inf
    max_x = -math.inf
    max_y = -math.inf

    for scene in scenes:
        if not scene.crs:
            raise ConversionError(f"Scene has no CRS: {scene.zarr_uri}")
        left, bottom, right, top = _scene_bounds(scene)
        tx_left, tx_bottom, tx_right, tx_top = transform_bounds(
            scene.crs,
            target_crs,
            left,
            bottom,
            right,
            top,
            densify_pts=21,
        )

        min_x = min(min_x, tx_left)
        min_y = min(min_y, tx_bottom)
        max_x = max(max_x, tx_right)
        max_y = max(max_y, tx_top)

    res = float(resolution_m)
    snapped_min_x = math.floor(min_x / res) * res
    snapped_min_y = math.floor(min_y / res) * res
    snapped_max_x = math.ceil(max_x / res) * res
    snapped_max_y = math.ceil(max_y / res) * res

    width = int(round((snapped_max_x - snapped_min_x) / res))
    height = int(round((snapped_max_y - snapped_min_y) / res))
    if width <= 0 or height <= 0:
        raise ConversionError("Mosaic grid extent is empty.")

    transform = [res, 0.0, snapped_min_x, 0.0, -res, snapped_max_y]
    x_coords, y_coords = _derive_spatial_coords(transform, width=width, height=height)
    if x_coords is None or y_coords is None:
        raise ConversionError("Could not derive mosaic x/y coordinates.")

    return MosaicGrid(
        crs=target_crs,
        transform=transform,
        width=width,
        height=height,
        x_coords=x_coords,
        y_coords=y_coords,
    )


#To group scenes by day and return these grps by chronolpgical order 
def _group_scenes_by_utc_day(
    scenes: list[SourceScene],
    *,
    start_date: date | None,
    end_date: date | None,
) -> list[tuple[str, list[SourceScene]]]:
    grouped: dict[str, list[SourceScene]] = defaultdict(list)
    for scene in scenes:
        scene_day = _coerce_timestamp(scene.acquisition_time).date()
        if start_date and scene_day < start_date:
            continue
        if end_date and scene_day > end_date:
            continue
        grouped[scene_day.isoformat()].append(scene)

    return [(day, sorted(items, key=lambda item: item.sort_key)) for day, items in sorted(grouped.items())]

#A cloud score heler
def _least_cloud_score_from_ancillary(
    warped_ancillary: np.ndarray | None,
    ancillary_layer_names: list[str],
) -> np.ndarray | None:
    if warped_ancillary is None or not ancillary_layer_names:
        return None

    if "CLDPRB" in ancillary_layer_names:
        idx = ancillary_layer_names.index("CLDPRB")
        values = warped_ancillary[idx].astype(np.float32, copy=False)
        return np.where(np.isfinite(values), values, np.inf)

    if "SCL" in ancillary_layer_names:
        idx = ancillary_layer_names.index("SCL")
        scl = warped_ancillary[idx]
        clear = np.isin(scl, [4, 5, 6])  # vegetation, bare soil, water
        return np.where(clear, 0.0, 100.0).astype(np.float32, copy=False)

    return None


def _least_cloud_score(
    *,
    warped_cloud_probability: np.ndarray | None,
    warped_cloud_mask: np.ndarray | None,
    warped_ancillary: np.ndarray | None,
    ancillary_layer_names: list[str],
) -> np.ndarray | None:
    if warped_cloud_probability is not None:
        values = warped_cloud_probability.astype(np.float32, copy=False)
        return np.where(np.isfinite(values), values, np.inf)

    if warped_cloud_mask is not None:
        mask = warped_cloud_mask.astype(np.float32, copy=False)
        return np.where(np.isfinite(mask), np.where(mask > 0, 100.0, 0.0), np.inf)

    del warped_ancillary, ancillary_layer_names
    return None


def _warp_scene_stack(
    source_root: Any,
    scene: SourceScene,
    *,
    target_grid: MosaicGrid,
    include_ancillary: bool,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None, np.ndarray | None]:
    src_transform = Affine(*[float(v) for v in scene.transform[:6]])
    dst_transform = Affine(*[float(v) for v in target_grid.transform[:6]])

    imagery_src = np.asarray(source_root["imagery"][0], dtype=np.float32)
    imagery_dst = np.full(
        (imagery_src.shape[0], target_grid.height, target_grid.width),
        np.nan,
        dtype=np.float32,
    )

    for band_index in range(imagery_src.shape[0]):
        reproject(
            source=imagery_src[band_index],
            destination=imagery_dst[band_index],
            src_transform=src_transform,
            src_crs=scene.crs,
            dst_transform=dst_transform,
            dst_crs=target_grid.crs,
            src_nodata=np.nan,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
        )

    ancillary_dst: np.ndarray | None = None
    if include_ancillary and "ancillary" in source_root and scene.ancillary_layer_names:
        ancillary_src = np.asarray(source_root["ancillary"][0], dtype=np.float32)
        ancillary_dst = np.full(
            (ancillary_src.shape[0], target_grid.height, target_grid.width),
            np.nan,
            dtype=np.float32,
        )
        for layer_index, layer_name in enumerate(scene.ancillary_layer_names):
            layer_resampling = (
                Resampling.nearest
                if layer_name in {"SCL", "CLD", "SNW", "TCI"}
                else Resampling.bilinear
            )
            reproject(
                source=ancillary_src[layer_index],
                destination=ancillary_dst[layer_index],
                src_transform=src_transform,
                src_crs=scene.crs,
                dst_transform=dst_transform,
                dst_crs=target_grid.crs,
                src_nodata=np.nan,
                dst_nodata=np.nan,
                resampling=layer_resampling,
            )

    cloud_mask_dst: np.ndarray | None = None
    cloud_probability_dst: np.ndarray | None = None
    if "masks" in source_root:
        masks_group = source_root["masks"]

        if "cloud" in masks_group:
            cloud_src = np.asarray(masks_group["cloud"][0], dtype=np.float32)
            cloud_mask_dst = np.full(
                (target_grid.height, target_grid.width),
                np.nan,
                dtype=np.float32,
            )
            reproject(
                source=cloud_src,
                destination=cloud_mask_dst,
                src_transform=src_transform,
                src_crs=scene.crs,
                dst_transform=dst_transform,
                dst_crs=target_grid.crs,
                src_nodata=np.nan,
                dst_nodata=np.nan,
                resampling=Resampling.nearest,
            )

        if "cloud_probability" in masks_group:
            probability_src = np.asarray(masks_group["cloud_probability"][0], dtype=np.float32)
            cloud_probability_dst = np.full(
                (target_grid.height, target_grid.width),
                np.nan,
                dtype=np.float32,
            )
            reproject(
                source=probability_src,
                destination=cloud_probability_dst,
                src_transform=src_transform,
                src_crs=scene.crs,
                dst_transform=dst_transform,
                dst_crs=target_grid.crs,
                src_nodata=np.nan,
                dst_nodata=np.nan,
                resampling=Resampling.nearest,
            )

    return imagery_dst, ancillary_dst, cloud_mask_dst, cloud_probability_dst


def _iter_grid_windows(*, width: int, height: int, chunk_x: int, chunk_y: int) -> list[tuple[int, int, int, int]]:
    windows: list[tuple[int, int, int, int]] = []
    for row_off in range(0, height, chunk_y):
        win_h = min(chunk_y, height - row_off)
        for col_off in range(0, width, chunk_x):
            win_w = min(chunk_x, width - col_off)
            windows.append((row_off, col_off, win_h, win_w))
    return windows


def _scene_source_window_for_target_window(
    scene: SourceScene,
    *,
    target_grid: MosaicGrid,
    row_off: int,
    col_off: int,
    win_h: int,
    win_w: int,
    pad_pixels: int = 2,
) -> tuple[int, int, int, int] | None:
    src_transform = Affine(*[float(v) for v in scene.transform[:6]])
    dst_transform = Affine(*[float(v) for v in target_grid.transform[:6]])
    dst_window_transform = dst_transform * Affine.translation(col_off, row_off)

    left = dst_window_transform.c
    top = dst_window_transform.f
    right = left + dst_window_transform.a * win_w
    bottom = top + dst_window_transform.e * win_h

    min_x = min(left, right)
    max_x = max(left, right)
    min_y = min(bottom, top)
    max_y = max(bottom, top)

    src_min_x, src_min_y, src_max_x, src_max_y = transform_bounds(
        target_grid.crs,
        scene.crs,
        min_x,
        min_y,
        max_x,
        max_y,
        densify_pts=21,
    )

    inv_src = ~src_transform
    col_a, row_a = inv_src * (src_min_x, src_max_y)
    col_b, row_b = inv_src * (src_max_x, src_min_y)

    min_col = int(math.floor(min(col_a, col_b))) - pad_pixels
    max_col = int(math.ceil(max(col_a, col_b))) + pad_pixels
    min_row = int(math.floor(min(row_a, row_b))) - pad_pixels
    max_row = int(math.ceil(max(row_a, row_b))) + pad_pixels

    src_height = int(scene.imagery_shape[2])
    src_width = int(scene.imagery_shape[3])
    min_col = max(0, min_col)
    min_row = max(0, min_row)
    max_col = min(src_width, max_col)
    max_row = min(src_height, max_row)

    if min_col >= max_col or min_row >= max_row:
        return None
    return min_row, max_row, min_col, max_col


def _warp_scene_window(
    source_root: Any,
    scene: SourceScene,
    *,
    target_grid: MosaicGrid,
    row_off: int,
    col_off: int,
    win_h: int,
    win_w: int,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
    band_count = int(scene.imagery_shape[1])
    imagery_dst = np.full((band_count, win_h, win_w), np.nan, dtype=np.float32)

    src_window = _scene_source_window_for_target_window(
        scene,
        target_grid=target_grid,
        row_off=row_off,
        col_off=col_off,
        win_h=win_h,
        win_w=win_w,
    )
    if src_window is None:
        return imagery_dst, None, None

    min_row, max_row, min_col, max_col = src_window
    src_transform = Affine(*[float(v) for v in scene.transform[:6]]) * Affine.translation(min_col, min_row)
    dst_transform = Affine(*[float(v) for v in target_grid.transform[:6]]) * Affine.translation(col_off, row_off)

    for band_index in range(band_count):
        source_band = np.asarray(
            source_root["imagery"][0, band_index, min_row:max_row, min_col:max_col],
            dtype=np.float32,
        )
        reproject(
            source=source_band,
            destination=imagery_dst[band_index],
            src_transform=src_transform,
            src_crs=scene.crs,
            dst_transform=dst_transform,
            dst_crs=target_grid.crs,
            src_nodata=np.nan,
            dst_nodata=np.nan,
            resampling=Resampling.bilinear,
        )

    cloud_mask_dst: np.ndarray | None = None
    cloud_probability_dst: np.ndarray | None = None
    if "masks" in source_root:
        masks_group = source_root["masks"]
        if "cloud" in masks_group:
            cloud_src = np.asarray(
                masks_group["cloud"][0, min_row:max_row, min_col:max_col],
                dtype=np.float32,
            )
            cloud_mask_dst = np.full((win_h, win_w), np.nan, dtype=np.float32)
            reproject(
                source=cloud_src,
                destination=cloud_mask_dst,
                src_transform=src_transform,
                src_crs=scene.crs,
                dst_transform=dst_transform,
                dst_crs=target_grid.crs,
                src_nodata=np.nan,
                dst_nodata=np.nan,
                resampling=Resampling.nearest,
            )

        if "cloud_probability" in masks_group:
            probability_src = np.asarray(
                masks_group["cloud_probability"][0, min_row:max_row, min_col:max_col],
                dtype=np.float32,
            )
            cloud_probability_dst = np.full((win_h, win_w), np.nan, dtype=np.float32)
            reproject(
                source=probability_src,
                destination=cloud_probability_dst,
                src_transform=src_transform,
                src_crs=scene.crs,
                dst_transform=dst_transform,
                dst_crs=target_grid.crs,
                src_nodata=np.nan,
                dst_nodata=np.nan,
                resampling=Resampling.nearest,
            )

    return imagery_dst, cloud_mask_dst, cloud_probability_dst


def build_daily_mosaic_cube(
    source_zarr_uris: list[str],
    output_uri: str,
    *,
    include_ancillary: bool = True,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    target_crs: str | None = None,
    target_resolution_m: int = 10,
    overlap_policy: str = "least_cloud",
    progress_callback: CubeProgressCallback | None = None,
) -> dict[str, Any]:
    try:
        import zarr
        from numcodecs import Blosc
    except ImportError as exc:
        raise ConversionDependencyError(
            f"Cube-building dependencies are unavailable ({exc}). Ensure zarr and numcodecs are installed."
        ) from exc

    if not source_zarr_uris:
        raise ConversionError("At least one source Zarr store is required to build a cube.")

    scenes = [_load_source_scene(uri, include_ancillary=include_ancillary) for uri in source_zarr_uris]
    if not scenes:
        raise ConversionError("At least one source Zarr store is required to build a cube.")

    baseline = scenes[0]
    normalized_collection = str(baseline.collection or "").strip().upper()
    normalized_product = str(baseline.product_type or "").strip().upper()
    if "SENTINEL-2" not in normalized_collection and not normalized_product.startswith("S2"):
        raise ConversionError("Daily mosaic cube is currently supported only for Sentinel-2 scene Zarr inputs.")

    for scene in scenes[1:]:
        _ensure_same(scene.band_names, baseline.band_names, field_name="band_names", scene=scene)
        _ensure_same(scene.provider, baseline.provider, field_name="provider", scene=scene)
        _ensure_same(scene.collection, baseline.collection, field_name="collection", scene=scene)
        _ensure_same(scene.product_type, baseline.product_type, field_name="product_type", scene=scene)

    start_bound = _coerce_date_only(start_date)
    end_bound = _coerce_date_only(end_date)
    if start_bound and end_bound and end_bound < start_bound:
        raise ConversionError("Cube end date must be greater or equal to cube start date.")

    grouped_days = _group_scenes_by_utc_day(
        scenes,
        start_date=start_bound,
        end_date=end_bound,
    )
    if not grouped_days:
        raise ConversionError("No scenes matched the requested date range.")

    resolved_crs, crs_policy = _choose_target_crs(
        [scene for _, day_scenes in grouped_days for scene in day_scenes],
        target_crs,
    )
    mosaic_grid = _build_mosaic_grid(
        [scene for _, day_scenes in grouped_days for scene in day_scenes],
        target_crs=resolved_crs,
        resolution_m=target_resolution_m,
    )

    time_values = [f"{day}T00:00:00+00:00" for day, _ in grouped_days]
    band_names = list(baseline.band_names)
    time_count = len(grouped_days)
    band_count = len(band_names)
    scene_index_lookup = {scene.scene_id: index for index, scene in enumerate(scenes)}

    output_store, public_uri = _prepare_output_store(output_uri)
    root = zarr.open_group(output_store, mode="w", zarr_format=2)
    compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE)

    imagery = root.create_array(
        "imagery",
        shape=(time_count, band_count, mosaic_grid.height, mosaic_grid.width),
        chunks=(
            1,
            min(4, band_count),
            min(512, mosaic_grid.height),
            min(512, mosaic_grid.width),
        ),
        dtype=np.float32,
        compressor=compressor,
        fill_value=np.float32(np.nan),
    )
    best_score = root.create_array(
        "best_cloud_score",
        shape=(time_count, mosaic_grid.height, mosaic_grid.width),
        chunks=(1, min(512, mosaic_grid.height), min(512, mosaic_grid.width)),
        dtype=np.float32,
        compressor=compressor,
        fill_value=np.float32(np.inf),
    )
    scene_choice = root.create_array(
        "source_scene_index",
        shape=(time_count, mosaic_grid.height, mosaic_grid.width),
        chunks=(1, min(512, mosaic_grid.height), min(512, mosaic_grid.width)),
        dtype=np.int32,
        compressor=compressor,
        fill_value=-1,
    )

    root.create_array("time", data=_string_array(time_values), overwrite=True)
    root.create_array("band", data=_string_array(band_names), overwrite=True)
    root.create_array("x", data=mosaic_grid.x_coords, overwrite=True)
    root.create_array("y", data=mosaic_grid.y_coords, overwrite=True)
    root.create_array("source_scene_id", data=_string_array([scene.scene_id for scene in scenes]), overwrite=True)

    chunk_y = int(imagery.chunks[2]) if imagery.chunks and len(imagery.chunks) >= 4 else min(512, mosaic_grid.height)
    chunk_x = int(imagery.chunks[3]) if imagery.chunks and len(imagery.chunks) >= 4 else min(512, mosaic_grid.width)
    windows = _iter_grid_windows(
        width=mosaic_grid.width,
        height=mosaic_grid.height,
        chunk_x=max(1, chunk_x),
        chunk_y=max(1, chunk_y),
    )
    total_windows = len(windows) * max(1, time_count)
    processed_windows = 0

    root.attrs.update(
        {
            "cube_kind": "daily_mosaic",
            "build_status": "writing",
            "shape": [time_count, band_count, mosaic_grid.height, mosaic_grid.width],
        }
    )

    try:
        for time_index, (day_label, day_scenes) in enumerate(grouped_days):
            for row_off, col_off, win_h, win_w in windows:
                window_imagery = np.full((band_count, win_h, win_w), np.nan, dtype=np.float32)
                window_score = np.full((win_h, win_w), np.inf, dtype=np.float32)
                window_choice = np.full((win_h, win_w), -1, dtype=np.int32)
                window_rank = np.full((win_h, win_w), -1, dtype=np.int64)

                for scene in day_scenes:
                    source_store = _open_existing_output_store(scene.zarr_uri)
                    source_root = zarr.open_group(source_store, mode="r", zarr_format=2)

                    warped_imagery, warped_cloud_mask, warped_cloud_probability = _warp_scene_window(
                        source_root,
                        scene,
                        target_grid=mosaic_grid,
                        row_off=row_off,
                        col_off=col_off,
                        win_h=win_h,
                        win_w=win_w,
                    )

                    valid_mask = np.any(np.isfinite(warped_imagery), axis=0)
                    if not np.any(valid_mask):
                        continue

                    candidate_score = _least_cloud_score(
                        warped_cloud_probability=warped_cloud_probability,
                        warped_cloud_mask=warped_cloud_mask,
                        warped_ancillary=None,
                        ancillary_layer_names=[],
                    )
                    if overlap_policy == "least_cloud" and candidate_score is None:
                        raise ConversionError(
                            "least_cloud overlap policy requires OmniCloudMask outputs in the source Zarr. "
                            f"Missing masks/cloud_probability or masks/cloud for scene: {scene.scene_id}"
                        )
                    if candidate_score is None or overlap_policy != "least_cloud":
                        candidate_score = np.full((win_h, win_w), 0.0, dtype=np.float32)

                    scene_rank = int(_coerce_timestamp(scene.acquisition_time).timestamp())

                    replace_mask = valid_mask & (candidate_score < window_score)
                    tie_mask = valid_mask & np.isfinite(candidate_score) & (candidate_score == window_score)

                    if overlap_policy == "latest":
                        replace_mask |= tie_mask & (scene_rank > window_rank)
                    elif overlap_policy == "earliest":
                        replace_mask |= tie_mask & ((window_rank < 0) | (scene_rank < window_rank))
                    elif overlap_policy == "first_valid":
                        replace_mask |= tie_mask & (window_choice < 0)
                    else:
                        replace_mask |= tie_mask & (scene_rank > window_rank)

                    if np.any(replace_mask):
                        window_imagery[:, replace_mask] = warped_imagery[:, replace_mask]
                        window_score[replace_mask] = candidate_score[replace_mask]
                        window_choice[replace_mask] = scene_index_lookup[scene.scene_id]
                        window_rank[replace_mask] = scene_rank

                imagery[time_index, :, row_off : row_off + win_h, col_off : col_off + win_w] = window_imagery
                best_score[time_index, row_off : row_off + win_h, col_off : col_off + win_w] = window_score
                scene_choice[time_index, row_off : row_off + win_h, col_off : col_off + win_w] = window_choice

                processed_windows += 1
                if progress_callback is not None:
                    progress_callback(
                        {
                            "stage": "daily_mosaic",
                            "group_key": day_label,
                            "group_index": time_index + 1,
                            "group_total": time_count,
                            "window_row_offset": row_off,
                            "window_col_offset": col_off,
                            "window_height": win_h,
                            "window_width": win_w,
                            "window_index": processed_windows,
                            "window_total": total_windows,
                            "fraction": processed_windows / total_windows if total_windows else 1.0,
                        }
                    )
    except Exception as exc:
        root.attrs.update(
            {
                "build_status": "failed",
                "build_error": str(exc),
            }
        )
        raise

    root.attrs.update(
        {
            "zarr_format_version": ZARR_FORMAT_VERSION,
            "cube_kind": "daily_mosaic",
            "build_status": "written",
            "source_scene_count": len(scenes),
            "provider": baseline.provider,
            "collection": baseline.collection,
            "product_type": baseline.product_type,
            "data_family": baseline.data_family,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [time_count, band_count, mosaic_grid.height, mosaic_grid.width],
            "dtype": "float32",
            "band_names": list(band_names),
            "crs": mosaic_grid.crs,
            "transform": mosaic_grid.transform,
            "reference_band": baseline.reference_band,
            "reference_pixel_size": [float(target_resolution_m), float(target_resolution_m)],
            "time_start": time_values[0],
            "time_end": time_values[-1],
            "time_granularity": "daily",
            "mosaic_enabled": True,
            "mosaic_grid_policy": "union_extent",
            "mosaic_crs_policy": crs_policy,
            "mosaic_resolution_m": int(target_resolution_m),
            "mosaic_overlap_policy": str(overlap_policy),
            "mosaic_overlap_fallback_policy": "latest",
            "cloud_score_source_priority": [
                "masks/cloud_probability",
                "masks/cloud",
            ],
            "source_scene_ids": [scene.scene_id for scene in scenes],
        }
    )

    zarr.consolidate_metadata(output_store)

    return CubeBuildSummaryRecord(
        zarr_uri=public_uri,
        cube_kind="daily_mosaic",
        source_scene_count=len(scenes),
        source_zarr_uris=list(source_zarr_uris),
        band_names=list(band_names),
        shape=[time_count, band_count, mosaic_grid.height, mosaic_grid.width],
        time_values=list(time_values),
        scene_ids=[scene.scene_id for scene in scenes],
        provider=baseline.provider,
        collection=baseline.collection,
        product_type=baseline.product_type,
        data_family=baseline.data_family,
        crs=mosaic_grid.crs,
        transform=mosaic_grid.transform,
        pixel_size=[float(target_resolution_m), float(target_resolution_m)],
        dimensions=["time", "band", "y", "x"],
        ancillary_written=False,
        ancillary_layer_names=[],
        masks_written=True,
        mask_layer_names=["best_cloud_score", "source_scene_index"],
        time_granularity="daily",
        mosaic_overlap_policy=str(overlap_policy),
        mosaic_crs_policy=crs_policy,
        mosaic_resolution_m=int(target_resolution_m),
        nodata=None,
    ).to_dict()
