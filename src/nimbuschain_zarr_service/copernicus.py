from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

import numpy as np

from nimbuschain_zarr_service.config_loader import (
    get_copernicus_product_spec,
    supported_product_types,
    target_pixel_size_for,
)
from nimbuschain_zarr_service.core import (
    ConversionError,
    PreparedSource,
    TargetGrid,
    attach_layer_array,
    build_standard_dataset,
    load_aligned_raster_stack,
    prepare_source,
    stream_raster_product_to_zarr,
    summarize_dataset,
    write_dataset_to_zarr,
)
from nimbuschain_zarr_service.sentinel1_raw import (
    build_sentinel1_raw_dataset,
    convert_sentinel1_raw_to_zarr,
)


_S1_POLARIZATION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "VV": (re.compile(r"(?:^|[_-])vv(?:[_\.-]|$)", re.IGNORECASE),),
    "VH": (re.compile(r"(?:^|[_-])vh(?:[_\.-]|$)", re.IGNORECASE),),
    "HH": (re.compile(r"(?:^|[_-])hh(?:[_\.-]|$)", re.IGNORECASE),),
    "HV": (re.compile(r"(?:^|[_-])hv(?:[_\.-]|$)", re.IGNORECASE),),
}
_S2_SPECTRAL_CODES = (
    "B01",
    "B02",
    "B03",
    "B04",
    "B05",
    "B06",
    "B07",
    "B08",
    "B8A",
    "B09",
    "B10",
    "B11",
    "B12",
)
_S2_L2A_SPECTRAL_CODES = tuple(code for code in _S2_SPECTRAL_CODES if code != "B10")
_S2_CATEGORICAL_LAYER_TOKENS = {"SCL", "CLD", "SNW", "TCI"}
_S2_PREFERRED_IMAGERY_REFERENCE = ("B04", "B03", "B02", "B08")
_S2_DYNAMIC_TOKENS = (
    "AOT",
    "WVP",
    "SCL",
    "CLDPRB",
    "SNWPRB",
    "CLD",
    "SNW",
    "TCI",
    *_S2_SPECTRAL_CODES,
)
_ANCILLARY_PRIORITY = {
    "SCL": 0,
    "AOT": 1,
    "WVP": 2,
    "CLDPRB": 3,
    "SNWPRB": 4,
    "CLD": 5,
    "SNW": 6,
    "TCI": 7,
}
_LANDSAT_ANGLE_TOKENS = {"SAA", "SZA", "VAA", "VZA"}


def build_copernicus_dataset(
    *,
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    extracted = prepare_source(raw_uri, label="copernicus")
    try:
        resolved_scene_id = _resolve_scene_id(extracted, scene_id)
        data_family = _detect_family(extracted, collection=collection, scene_id=resolved_scene_id)
        if data_family == "optical":
            dataset, summary = _build_sentinel2_dataset(
                extracted,
                provider=provider,
                collection=collection,
                scene_id=resolved_scene_id,
                product_type=product_type,
            )
        elif data_family == "sar":
            dataset, summary = _build_sentinel1_dataset(
                extracted,
                provider=provider,
                collection=collection,
                scene_id=resolved_scene_id,
                product_type=product_type,
            )
        elif data_family == "swath":
            dataset, summary = _build_generic_netcdf_dataset(
                extracted,
                provider=provider,
                collection=collection,
                scene_id=resolved_scene_id,
            )
        else:
            raise ConversionError(f"Unsupported Copernicus family: {data_family}")
        summary["scene_id"] = resolved_scene_id
        if scene_id != resolved_scene_id:
            summary["requested_scene_id"] = scene_id
        summary["source_kind"] = extracted.source_kind
        summary["raw_path"] = str(extracted.raw_path)
        return dataset, summary
    finally:
        if extracted.cleanup is not None:
            extracted.cleanup.cleanup()


def convert_copernicus_to_zarr(
    *,
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
    output_uri: str,
    product_type: str | None = None,
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    extracted = prepare_source(raw_uri, label="copernicus")
    try:
        resolved_scene_id = _resolve_scene_id(extracted, scene_id)
        data_family = _detect_family(extracted, collection=collection, scene_id=resolved_scene_id)
        if data_family == "optical":
            return _convert_sentinel2_to_zarr(
                extracted,
                provider=provider,
                collection=collection,
                scene_id=resolved_scene_id,
                output_uri=output_uri,
                product_type=product_type,
                requested_scene_id=scene_id,
            )
        if data_family == "sar":
            s1_product_type = _s1_product_type(resolved_scene_id, requested=product_type)
            if s1_product_type == "RAW":
                written_uri, summary, dataset_summary = convert_sentinel1_raw_to_zarr(
                    root=extracted.root,
                    provider=provider,
                    collection=collection,
                    scene_id=resolved_scene_id,
                    output_uri=output_uri,
                )
                summary["scene_id"] = resolved_scene_id
                summary["source_kind"] = extracted.source_kind
                summary["raw_path"] = str(extracted.raw_path)
                if scene_id != resolved_scene_id:
                    summary["requested_scene_id"] = scene_id
                return written_uri, "sar", summary, dataset_summary

            dataset, summary = _build_sentinel1_dataset(
                extracted,
                provider=provider,
                collection=collection,
                scene_id=resolved_scene_id,
                product_type=product_type,
            )
            written_uri = write_dataset_to_zarr(dataset, output_uri)
            dataset_summary = summarize_dataset(
                dataset,
                data_family=str(summary.get("data_family", "unknown")),
                zarr_uri=written_uri,
            )
            return written_uri, str(summary.get("data_family", "unknown")), summary, dataset_summary

        dataset, summary = _build_generic_netcdf_dataset(
            extracted,
            provider=provider,
            collection=collection,
            scene_id=resolved_scene_id,
        )
        written_uri = write_dataset_to_zarr(dataset, output_uri)
        dataset_summary = summarize_dataset(
            dataset,
            data_family=str(summary.get("data_family", "unknown")),
            zarr_uri=written_uri,
        )
        return written_uri, str(summary.get("data_family", "unknown")), summary, dataset_summary
    finally:
        if extracted.cleanup is not None:
            extracted.cleanup.cleanup()


def _detect_family(extracted: PreparedSource, *, collection: str, scene_id: str) -> str:
    normalized_collection = collection.upper()
    normalized_scene = scene_id.upper()
    if normalized_collection.startswith("SENTINEL-1") or normalized_scene.startswith("S1"):
        return "sar"
    if normalized_collection.startswith("SENTINEL-2") or normalized_scene.startswith("S2"):
        return "optical"
    if any(path.suffix.lower() == ".nc" for path in extracted.root.rglob("*") if path.is_file()):
        return "swath"
    if any(path.suffix.lower() in {".jp2", ".tif", ".tiff"} for path in extracted.root.rglob("*") if path.is_file()):
        if any("measurement" in str(path.parent).lower() for path in extracted.root.rglob("*") if path.is_file()):
            return "sar"
        return "optical"
    raise ConversionError(
        "Unable to detect a supported Copernicus family from the bundle contents. "
        "Only Sentinel-1, Sentinel-2, and NetCDF swath products are supported."
    )


def _build_sentinel2_dataset(
    extracted: PreparedSource,
    *,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    imagery_paths, ancillary_paths, s2_product_type = _prepare_sentinel2_layers(
        extracted,
        scene_id=scene_id,
        product_type=product_type,
    )
    ordered_imagery = _ordered_s2_imagery_layers(s2_product_type, imagery_paths)
    ordered_ancillary = _ordered_ancillary_layers(ancillary_paths)
    reference_band = _pick_first_available(_S2_PREFERRED_IMAGERY_REFERENCE, ordered_imagery)
    target_pixel_size = target_pixel_size_for(provider, collection)
    imagery_stack = load_aligned_raster_stack(
        imagery_paths,
        ordered_bands=ordered_imagery,
        reference_band=reference_band,
        target_pixel_size=target_pixel_size,
    )
    acquisition = _extract_timestamp_from_scene_id(scene_id, prefix="S2")
    dataset = build_standard_dataset(
        arrays=imagery_stack["arrays"],
        band_names=imagery_stack["band_names"],
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "product_type": s2_product_type,
            "product_level": _s2_product_level(scene_id),
            "data_family": "optical",
            "crs": imagery_stack["crs"],
            "transform": imagery_stack["transform"],
            "reference_band": imagery_stack["reference_band"],
            "reference_pixel_size": imagery_stack["pixel_size"],
            "band_metadata": imagery_stack["band_metadata"],
        },
    )
    ancillary_metadata: dict[str, Any] = {}
    if ordered_ancillary:
        target_grid = TargetGrid(
            height=int(imagery_stack["height"]),
            width=int(imagery_stack["width"]),
            crs=imagery_stack["crs"],
            transform=imagery_stack["transform"],
            pixel_size=imagery_stack["pixel_size"],
            reference_band=imagery_stack["reference_band"],
        )
        ancillary_stack = load_aligned_raster_stack(
            ancillary_paths,
            ordered_bands=ordered_ancillary,
            reference_band=ordered_ancillary[0],
            categorical_bands=_S2_CATEGORICAL_LAYER_TOKENS.intersection(ordered_ancillary),
            target_pixel_size=target_pixel_size,
            target_grid=target_grid,
        )
        dataset = attach_layer_array(
            dataset,
            arrays=ancillary_stack["arrays"],
            layer_names=ancillary_stack["band_names"],
            acquisition_datetime=acquisition,
            variable_name="ancillary",
            coord_name="ancillary_layer",
        )
        ancillary_metadata = ancillary_stack["band_metadata"]

    summary = _summarize_optical_product(
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        product_id=scene_id,
        product_type=s2_product_type,
        product_level=_s2_product_level(scene_id),
        acquisition=acquisition,
        extracted=extracted,
        imagery_paths=imagery_paths,
        imagery_band_names=imagery_stack["band_names"],
        imagery_metadata=imagery_stack["band_metadata"],
        grid={
            "height": imagery_stack["height"],
            "width": imagery_stack["width"],
            "dtype": imagery_stack["dtype"],
            "crs": imagery_stack["crs"],
            "transform": imagery_stack["transform"],
            "pixel_size": imagery_stack["pixel_size"],
            "reference_band": imagery_stack["reference_band"],
        },
        target_pixel_size=target_pixel_size,
        ancillary_paths=ancillary_paths,
        ancillary_layer_names=ordered_ancillary,
        ancillary_metadata=ancillary_metadata,
    )
    return dataset, summary


def _convert_sentinel2_to_zarr(
    extracted: PreparedSource,
    *,
    provider: str,
    collection: str,
    scene_id: str,
    output_uri: str,
    product_type: str | None = None,
    requested_scene_id: str | None = None,
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    imagery_paths, ancillary_paths, s2_product_type = _prepare_sentinel2_layers(
        extracted,
        scene_id=scene_id,
        product_type=product_type,
    )
    ordered_imagery = _ordered_s2_imagery_layers(s2_product_type, imagery_paths)
    ordered_ancillary = _ordered_ancillary_layers(ancillary_paths)
    reference_band = _pick_first_available(_S2_PREFERRED_IMAGERY_REFERENCE, ordered_imagery)
    acquisition = _extract_timestamp_from_scene_id(scene_id, prefix="S2")
    target_pixel_size = target_pixel_size_for(provider, collection)
    metadata = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "product_id": scene_id,
        "product_type": s2_product_type,
        "product_level": _s2_product_level(scene_id),
        "data_family": "optical",
        "source_uri": str(extracted.raw_path),
    }
    written_uri, dataset_summary = stream_raster_product_to_zarr(
        imagery_band_paths=imagery_paths,
        imagery_layer_names=ordered_imagery,
        output_uri=output_uri,
        metadata=metadata,
        acquisition_datetime=acquisition,
        reference_band=reference_band,
        target_pixel_size=target_pixel_size,
        ancillary_band_paths=ancillary_paths,
        ancillary_layer_names=ordered_ancillary,
        ancillary_categorical_layers=_S2_CATEGORICAL_LAYER_TOKENS.intersection(ordered_ancillary),
    )
    summary = _summarize_optical_product(
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        product_id=scene_id,
        product_type=s2_product_type,
        product_level=_s2_product_level(scene_id),
        acquisition=acquisition,
        extracted=extracted,
        imagery_paths=imagery_paths,
        imagery_band_names=list(dataset_summary["band_names"]),
        imagery_metadata=dict(dataset_summary.get("band_metadata") or {}),
        grid={
            "height": int(dataset_summary["shape"][2]),
            "width": int(dataset_summary["shape"][3]),
            "dtype": str(dataset_summary.get("dtype") or "unknown"),
            "crs": dataset_summary.get("crs"),
            "transform": dataset_summary.get("transform"),
            "pixel_size": dataset_summary.get("pixel_size"),
            "reference_band": reference_band,
        },
        target_pixel_size=target_pixel_size,
        ancillary_paths=ancillary_paths,
        ancillary_layer_names=list(dataset_summary.get("ancillary_layer_names") or []),
        ancillary_metadata=dict(dataset_summary.get("ancillary_metadata") or {}),
    )
    if requested_scene_id and requested_scene_id != scene_id:
        summary["requested_scene_id"] = requested_scene_id
    return written_uri, "optical", summary, dataset_summary


def _build_sentinel1_dataset(
    extracted: PreparedSource,
    *,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    s1_product_type = _s1_product_type(scene_id, requested=product_type)
    if s1_product_type == "RAW":
        return build_sentinel1_raw_dataset(
            root=extracted.root,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
        )

    measurement_exts = _s1_measurement_exts(s1_product_type)
    raster_files = [
        path
        for path in extracted.root.rglob("*")
        if path.is_file() and path.suffix.lower() in measurement_exts
    ]
    if not raster_files:
        raise ConversionError(
            "No Sentinel-1 measurement rasters compatible with this product type were found in the SAFE product."
        )

    imagery_paths, ancillary_paths = _discover_s1_layers(raster_files)
    if not imagery_paths:
        raise ConversionError("No Sentinel-1 polarization rasters were detected.")

    ordered_imagery = [band for band in ("VV", "VH", "HH", "HV") if band in imagery_paths]
    ordered_ancillary = _ordered_ancillary_layers(ancillary_paths)
    target_pixel_size = target_pixel_size_for(provider, collection)
    reference_band = ordered_imagery[0]
    imagery_stack = load_aligned_raster_stack(
        imagery_paths,
        ordered_bands=ordered_imagery,
        reference_band=reference_band,
        target_pixel_size=target_pixel_size,
    )
    acquisition = _extract_timestamp_from_scene_id(scene_id, prefix="S1")
    dataset = build_standard_dataset(
        arrays=imagery_stack["arrays"].astype(np.float32),
        band_names=imagery_stack["band_names"],
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "product_type": s1_product_type,
            "product_mode": _s1_mode(scene_id),
            "product_token": _s1_product_token(scene_id),
            "data_family": "sar",
            "crs": imagery_stack["crs"],
            "transform": imagery_stack["transform"],
            "reference_band": imagery_stack["reference_band"],
            "reference_pixel_size": imagery_stack["pixel_size"],
            "band_metadata": imagery_stack["band_metadata"],
        },
    )
    ancillary_metadata: dict[str, Any] = {}
    if ordered_ancillary:
        target_grid = TargetGrid(
            height=int(imagery_stack["height"]),
            width=int(imagery_stack["width"]),
            crs=imagery_stack["crs"],
            transform=imagery_stack["transform"],
            pixel_size=imagery_stack["pixel_size"],
            reference_band=reference_band,
        )
        ancillary_stack = load_aligned_raster_stack(
            ancillary_paths,
            ordered_bands=ordered_ancillary,
            reference_band=ordered_ancillary[0],
            target_pixel_size=target_pixel_size,
            target_grid=target_grid,
        )
        dataset = attach_layer_array(
            dataset,
            arrays=ancillary_stack["arrays"].astype(np.float32),
            layer_names=ancillary_stack["band_names"],
            acquisition_datetime=acquisition,
            variable_name="ancillary",
            coord_name="ancillary_layer",
        )
        ancillary_metadata = ancillary_stack["band_metadata"]
        dataset.attrs.update(
            {
                "ancillary_layer_names": list(ancillary_stack["band_names"]),
                "ancillary_dimensions": ["time", "ancillary_layer", "y", "x"],
                "ancillary_shape": [
                    1,
                    len(ancillary_stack["band_names"]),
                    int(imagery_stack["height"]),
                    int(imagery_stack["width"]),
                ],
                "ancillary_metadata": ancillary_metadata,
            }
        )

    summary = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "data_family": "sar",
        "product_type": s1_product_type,
        "product_mode": _s1_mode(scene_id),
        "product_token": _s1_product_token(scene_id),
        "product_level": "sar",
        "product_id": scene_id,
        "normalized_band_order": imagery_stack["band_names"],
        "resolution_policy_meters": target_pixel_size,
        "band_sources": {
            band: str(path.relative_to(extracted.root)) for band, path in imagery_paths.items()
        },
        "band_resampling": {
            band: imagery_stack["band_metadata"][band]["resampled_to_reference"]
            for band in imagery_stack["band_names"]
        },
        "band_native_pixel_size": {
            band: imagery_stack["band_metadata"][band]["source_pixel_size"]
            for band in imagery_stack["band_names"]
        },
        "ancillary_layer_names": ordered_ancillary,
        "ancillary_sources": {
            layer: str(path.relative_to(extracted.root)) for layer, path in ancillary_paths.items()
        },
        "ancillary_resampling": {
            layer: ancillary_metadata.get(layer, {}).get("resampled_to_reference")
            for layer in ordered_ancillary
        },
        "ancillary_native_pixel_size": {
            layer: ancillary_metadata.get(layer, {}).get("source_pixel_size")
            for layer in ordered_ancillary
        },
        "acquisition_datetime": acquisition,
        "grid": {
            "height": imagery_stack["height"],
            "width": imagery_stack["width"],
            "dtype": imagery_stack["dtype"],
            "crs": imagery_stack["crs"],
            "transform": imagery_stack["transform"],
            "pixel_size": imagery_stack["pixel_size"],
            "reference_band": imagery_stack["reference_band"],
        },
        "validation": {
            "imagery_layer_count": len(imagery_stack["band_names"]),
            "ancillary_layer_count": len(ordered_ancillary),
        },
    }
    return dataset, summary


def _build_generic_netcdf_dataset(
    extracted: PreparedSource,
    *,
    provider: str,
    collection: str,
    scene_id: str,
) -> tuple[Any, dict[str, Any]]:
    try:
        import xarray as xr
    except ImportError as exc:
        raise ConversionError(f"xarray is required for NetCDF conversion ({exc}).") from exc

    nc_files = [path for path in extracted.root.rglob("*") if path.is_file() and path.suffix.lower() == ".nc"]
    if not nc_files:
        raise ConversionError("No NetCDF files were found in the Copernicus product.")

    selected_file = nc_files[0]
    try:
        raw_ds = xr.open_dataset(selected_file, engine="h5netcdf")
    except Exception:
        raw_ds = xr.open_dataset(selected_file)

    candidates: list[tuple[str, np.ndarray]] = []
    ref_shape: tuple[int, int] | None = None
    spatial_dims: list[str] | None = None
    for name, data_array in raw_ds.data_vars.items():
        if not np.issubdtype(data_array.dtype, np.number):
            continue
        reduced = data_array
        if reduced.ndim < 2:
            continue
        while reduced.ndim > 2:
            reduced = reduced.isel({reduced.dims[0]: 0})
        shape = tuple(int(v) for v in reduced.shape)
        if ref_shape is None:
            ref_shape = shape
            spatial_dims = [str(dim) for dim in reduced.dims]
        if shape != ref_shape:
            continue
        candidates.append((name, reduced.values))

    if not candidates or ref_shape is None or spatial_dims is None:
        raise ConversionError("No 2D numeric variables with a shared grid were found in the NetCDF product.")

    arrays = np.stack([values.astype(np.float32) for _, values in candidates], axis=0)
    acquisition = _extract_time_from_netcdf(raw_ds) or datetime.now(timezone.utc).isoformat()
    dataset = build_standard_dataset(
        arrays=arrays,
        band_names=[name for name, _ in candidates],
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "data_family": "swath",
            "crs": raw_ds.attrs.get("crs") or raw_ds.attrs.get("spatial_ref"),
            "transform": None,
            "source_netcdf": selected_file.name,
            "source_spatial_dims": spatial_dims,
        },
    )
    summary = {
        "data_family": "swath",
        "product_id": scene_id,
        "source_netcdf": str(selected_file.relative_to(extracted.root)),
        "normalized_band_order": [name for name, _ in candidates],
        "acquisition_datetime": acquisition,
        "grid": {
            "height": ref_shape[0],
            "width": ref_shape[1],
            "dtype": str(arrays.dtype),
            "crs": raw_ds.attrs.get("crs") or raw_ds.attrs.get("spatial_ref"),
            "transform": None,
            "pixel_size": None,
        },
    }
    raw_ds.close()
    return dataset, summary


def _prepare_sentinel2_layers(
    extracted: PreparedSource,
    *,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[dict[str, Path], dict[str, Path], str]:
    raster_files = [
        path
        for path in extracted.root.rglob("*")
        if path.is_file()
        and path.suffix.lower() in {".jp2", ".tif", ".tiff"}
        and "img_data" in str(path.parent).lower()
    ]
    if not raster_files:
        raster_files = [
            path
            for path in extracted.root.rglob("*")
            if path.is_file() and path.suffix.lower() in {".jp2", ".tif", ".tiff"}
        ]
    if not raster_files:
        raise ConversionError("No Sentinel-2 raster files were found in the SAFE product.")

    s2_product_type = _s2_product_type(scene_id, requested=product_type)
    s2_spec = get_copernicus_product_spec("SENTINEL-2", s2_product_type)
    if not s2_spec:
        raise ConversionError(
            "Unsupported Sentinel-2 productType for this project. "
            f"Expected one of: {', '.join(sorted(supported_product_types().get('SENTINEL-2', [])))}. "
            f"Got: {s2_product_type}."
        )

    imagery_paths, ancillary_paths = _discover_s2_layers(raster_files)
    expected_layers = list(_S2_SPECTRAL_CODES if s2_product_type == "S2MSI1C" else _S2_L2A_SPECTRAL_CODES)
    missing = [band for band in expected_layers if band not in imagery_paths]
    if missing:
        raise ConversionError(
            f"Missing required Sentinel-2 imagery layers for {s2_product_type}: {', '.join(missing)}"
        )
    return imagery_paths, ancillary_paths, s2_product_type


def _discover_s2_layers(raster_files: list[Path]) -> tuple[dict[str, Path], dict[str, Path]]:
    imagery_paths: dict[str, Path] = {}
    ancillary_paths: dict[str, Path] = {}
    for path in sorted(raster_files):
        layer_name = _s2_layer_name(path)
        if not layer_name:
            continue
        bucket = imagery_paths if layer_name in _S2_SPECTRAL_CODES else ancillary_paths
        existing = bucket.get(layer_name)
        if existing is None or _s2_resolution_rank(path) < _s2_resolution_rank(existing):
            bucket[layer_name] = path
    return imagery_paths, ancillary_paths


def _s2_layer_name(path: Path) -> str | None:
    stem = path.stem.upper()
    for token in _S2_DYNAMIC_TOKENS:
        if re.search(rf"(?:^|[_-]){re.escape(token)}(?:[_\.-]|$)", stem):
            return token
    return None


def _s2_resolution_rank(path: Path) -> int:
    stem = path.stem.lower()
    if "10m" in stem:
        return 10
    if "20m" in stem:
        return 20
    if "60m" in stem:
        return 60
    return 999


def _ordered_s2_imagery_layers(product_type: str, imagery_paths: dict[str, Path]) -> list[str]:
    expected = _S2_SPECTRAL_CODES if product_type == "S2MSI1C" else _S2_L2A_SPECTRAL_CODES
    return [band for band in expected if band in imagery_paths]


def _discover_s1_layers(raster_files: list[Path]) -> tuple[dict[str, Path], dict[str, Path]]:
    imagery_paths: dict[str, Path] = {}
    ancillary_paths: dict[str, Path] = {}
    for path in sorted(raster_files):
        polarization = _s1_polarization(path.name)
        if "measurement" in str(path.parent).lower() and polarization:
            imagery_paths[polarization] = path
            continue
        layer_name = _normalize_s1_layer_name(path)
        if not layer_name:
            continue
        if polarization and layer_name not in imagery_paths:
            imagery_paths[polarization] = path
            continue
        if layer_name not in ancillary_paths:
            ancillary_paths[layer_name] = path
    return imagery_paths, ancillary_paths


def _s1_polarization(name: str) -> str | None:
    for band_name, patterns in _S1_POLARIZATION_PATTERNS.items():
        if any(pattern.search(name) for pattern in patterns):
            return band_name
    return None


def _normalize_s1_layer_name(path: Path) -> str | None:
    stem = path.stem.upper()
    pol = _s1_polarization(path.name)
    if pol and "MEASUREMENT" in str(path.parent).upper():
        return pol
    tokens = [token for token in re.split(r"[^A-Z0-9]+", stem) if token and not token.startswith("S1")]
    if not tokens:
        return None
    priority = [token for token in tokens if token not in {"IW", "EW", "SM", "RAW", "GRD", "SLC", pol or ""}]
    if priority:
        return "_".join(priority[-3:])
    return "_".join(tokens[-3:])


def _ordered_ancillary_layers(ancillary_paths: dict[str, Path]) -> list[str]:
    return sorted(
        ancillary_paths,
        key=lambda value: (_ANCILLARY_PRIORITY.get(value, 100), value),
    )


def _pick_first_available(preferred: tuple[str, ...], available: list[str]) -> str:
    for candidate in preferred:
        if candidate in available:
            return candidate
    if not available:
        raise ConversionError("No imagery layers are available.")
    return available[0]


def _summarize_optical_product(
    *,
    provider: str,
    collection: str,
    scene_id: str,
    product_id: str,
    product_type: str,
    product_level: str,
    acquisition: str | None,
    extracted: PreparedSource,
    imagery_paths: dict[str, Path],
    imagery_band_names: list[str],
    imagery_metadata: dict[str, Any],
    grid: dict[str, Any],
    target_pixel_size: float | None,
    ancillary_paths: dict[str, Path],
    ancillary_layer_names: list[str],
    ancillary_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "product_id": product_id,
        "product_type": product_type,
        "product_level": product_level,
        "data_family": "optical",
        "source_kind": extracted.source_kind,
        "raw_path": str(extracted.raw_path),
        "normalized_band_order": imagery_band_names,
        "resolution_policy_meters": target_pixel_size,
        "band_sources": {
            band: str(path.relative_to(extracted.root)) for band, path in imagery_paths.items()
        },
        "band_resampling": {
            band: imagery_metadata.get(band, {}).get("resampled_to_reference")
            for band in imagery_band_names
        },
        "band_native_pixel_size": {
            band: imagery_metadata.get(band, {}).get("source_pixel_size")
            for band in imagery_band_names
        },
        "ancillary_layer_names": ancillary_layer_names,
        "ancillary_sources": {
            layer: str(path.relative_to(extracted.root)) for layer, path in ancillary_paths.items()
        },
        "ancillary_resampling": {
            layer: ancillary_metadata.get(layer, {}).get("resampled_to_reference")
            for layer in ancillary_layer_names
        },
        "ancillary_native_pixel_size": {
            layer: ancillary_metadata.get(layer, {}).get("source_pixel_size")
            for layer in ancillary_layer_names
        },
        "acquisition_datetime": acquisition,
        "grid": grid,
        "validation": {
            "imagery_layer_count": len(imagery_band_names),
            "ancillary_layer_count": len(ancillary_layer_names),
        },
    }


def _extract_timestamp_from_scene_id(scene_id: str, *, prefix: str) -> str | None:
    if prefix in {"S1", "S2"}:
        match = re.search(r"_(\d{8}T\d{6})_", scene_id)
        if match:
            return _to_iso(match.group(1), "%Y%m%dT%H%M%S")
    return None


def _extract_time_from_netcdf(dataset: Any) -> str | None:
    time_coord = dataset.coords.get("time")
    if time_coord is not None and time_coord.size:
        return str(np.asarray(time_coord.values).reshape(-1)[0])
    for name in ("time_coverage_start", "start_time", "datetime"):
        value = dataset.attrs.get(name)
        if value:
            return str(value)
    return None


def _to_iso(value: str, fmt: str) -> str | None:
    try:
        parsed = datetime.strptime(value, fmt)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc).isoformat()


def _s2_product_type(scene_id: str, *, requested: str | None = None) -> str:
    requested_upper = str(requested or "").strip().upper()
    if requested_upper:
        return requested_upper
    parts = str(scene_id).split("_")
    if len(parts) >= 2 and parts[1]:
        token = parts[1].upper()
        if token == "MSIL1C":
            return "S2MSI1C"
        if token == "MSIL2A":
            return "S2MSI2A"
        return token
    return "MSI"


def _s2_product_level(scene_id: str) -> str:
    product_type = _s2_product_type(scene_id).upper()
    if "MSI1C" in product_type:
        return "toa"
    if "MSI2A" in product_type:
        return "boa"
    return "unknown"


def _s1_product_type(scene_id: str, *, requested: str | None = None) -> str:
    requested_upper = str(requested or "").strip().upper()
    if requested_upper:
        return requested_upper
    token = _s1_product_token(scene_id)
    mode = _s1_mode(scene_id)
    token_upper = token.upper()
    mode_upper = mode.upper()
    if token_upper.startswith("GRD"):
        return "GRD"
    if token_upper.startswith("RAW"):
        return "RAW"
    if token_upper.startswith("SLC"):
        if mode_upper == "IW":
            return "IW_SLC__1S"
        return "SLC"
    return "UNKNOWN"


def _s1_measurement_exts(product_type: str) -> set[str]:
    spec = get_copernicus_product_spec("SENTINEL-1", product_type)
    if not spec:
        raise ConversionError(
            "Unsupported Sentinel-1 productType for this project. "
            f"Expected one of: {', '.join(sorted(supported_product_types().get('SENTINEL-1', [])))}. "
            f"Got: {product_type}."
        )
    return {
        f".{str(value).lower().lstrip('.')}"
        for value in (spec.get("measurement_exts") or ["tif", "tiff"])
    }


def _s1_mode(scene_id: str) -> str:
    parts = str(scene_id).split("_")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return "UNKNOWN"


def _s1_product_token(scene_id: str) -> str:
    parts = str(scene_id).split("_")
    if len(parts) >= 3 and parts[2]:
        return parts[2]
    return "UNKNOWN"


def _resolve_scene_id(extracted: PreparedSource, provided_scene_id: str) -> str:
    provided = str(provided_scene_id or "").strip()
    detected = _detect_scene_id_from_bundle(extracted)
    if detected:
        return detected
    return provided


def _detect_scene_id_from_bundle(extracted: PreparedSource) -> str:
    safe_dirs = sorted(
        [path for path in extracted.root.rglob("*.SAFE") if path.is_dir()],
        key=lambda path: (len(path.relative_to(extracted.root).parts), len(path.name)),
    )
    if safe_dirs:
        return safe_dirs[0].name.removesuffix(".SAFE")

    manifest = next(
        (
            path
            for path in extracted.root.rglob("*")
            if path.is_file() and path.name.lower() == "manifest.safe"
        ),
        None,
    )
    if manifest is not None:
        return manifest.parent.name.removesuffix(".SAFE")

    nc_file = next(
        (
            path
            for path in extracted.root.rglob("*")
            if path.is_file() and path.suffix.lower() == ".nc"
        ),
        None,
    )
    if nc_file is not None:
        return nc_file.stem

    return ""
