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
    build_standard_dataset,
    load_aligned_raster_stack,
    prepare_source,
    stream_raster_stack_to_zarr,
    summarize_dataset,
    write_dataset_to_zarr,
)
from nimbuschain_zarr_service.sentinel1_raw import (
    build_sentinel1_raw_dataset,
    convert_sentinel1_raw_to_zarr,
)


_S1_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "vv": (re.compile(r"(?:^|[_-])vv(?:[_\.-]|$)", re.IGNORECASE),),
    "vh": (re.compile(r"(?:^|[_-])vh(?:[_\.-]|$)", re.IGNORECASE),),
    "hh": (re.compile(r"(?:^|[_-])hh(?:[_\.-]|$)", re.IGNORECASE),),
    "hv": (re.compile(r"(?:^|[_-])hv(?:[_\.-]|$)", re.IGNORECASE),),
}
_S2_CANONICAL_BY_CODE = {
    "SCL": "scene_classification",
    "B01": "coastal",
    "B02": "blue",
    "B03": "green",
    "B04": "red",
    "B05": "rededge1",
    "B06": "rededge2",
    "B07": "rededge3",
    "B08": "nir",
    "B8A": "nir_narrow",
    "B09": "water_vapor",
    "B10": "cirrus",
    "B11": "swir1",
    "B12": "swir2",
}
_S2_REQUIRED_BANDS = ("blue", "green", "red", "nir")
_S2_CANONICAL_BANDS = (
    "coastal",
    "blue",
    "green",
    "red",
    "rededge1",
    "rededge2",
    "rededge3",
    "nir",
    "nir_narrow",
    "water_vapor",
    "cirrus",
    "swir1",
    "swir2",
    "scene_classification",
)


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
    if any(path.suffix.lower() in {".jp2", ".tif", ".tiff"} for path in extracted.root.rglob("*") if path.is_file()):
        if any("measurement" in str(path.parent).lower() for path in extracted.root.rglob("*") if path.is_file()):
            return "sar"
        return "optical"

    raise ConversionError(
        "Unable to detect a supported Copernicus family from the bundle contents. "
        "Only Sentinel-1 and Sentinel-2 products are supported."
    )


def _build_sentinel2_dataset(
    extracted: PreparedSource,
    *,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    raster_files = [
        path
        for path in extracted.root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".jp2", ".tif", ".tiff"}
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

    band_paths = _discover_s2_band_paths(
        raster_files,
        product_type=s2_product_type,
        ext=str(s2_spec.get("ext", "jp2")),
    )

    missing = [band for band in _S2_REQUIRED_BANDS if band not in band_paths]
    if missing:
        raise ConversionError(
            "Missing required Sentinel-2 optical bands: " + ", ".join(missing)
        )

    ordered_bands = _ordered_s2_bands(s2_spec, band_paths)
    target_pixel_size = target_pixel_size_for(provider, collection)
    stack = load_aligned_raster_stack(
        band_paths,
        ordered_bands=ordered_bands,
        reference_band="red",
        categorical_bands=set(
            _S2_CANONICAL_BY_CODE.get(code, code.lower())
            for code in list(s2_spec.get("categorical_bands") or [])
        ),
        target_pixel_size=target_pixel_size,
    )
    acquisition = _extract_timestamp_from_scene_id(scene_id, prefix="S2")
    dataset = build_standard_dataset(
        arrays=stack["arrays"],
        band_names=stack["band_names"],
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "data_family": "optical",
            "crs": stack["crs"],
            "transform": stack["transform"],
            "reference_band": stack["reference_band"],
            "reference_pixel_size": stack["pixel_size"],
            "band_metadata": stack["band_metadata"],
        },
    )
    summary = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "data_family": "optical",
        "product_type": s2_product_type,
        "product_level": _s2_product_level(scene_id),
        "product_id": scene_id,
        "canonical_bands": list(_S2_CANONICAL_BANDS),
        "required_core_bands": list(_S2_REQUIRED_BANDS),
        "normalized_band_order": stack["band_names"],
        "resolution_policy_meters": target_pixel_size,
        "band_sources": {
            band: str(path.relative_to(extracted.root)) for band, path in band_paths.items()
        },
        "band_resampling": {
            band: stack["band_metadata"][band]["resampled_to_reference"]
            for band in stack["band_names"]
        },
        "band_native_pixel_size": {
            band: stack["band_metadata"][band]["source_pixel_size"]
            for band in stack["band_names"]
        },
        "acquisition_datetime": acquisition,
        "grid": {
            "height": stack["height"],
            "width": stack["width"],
            "dtype": stack["dtype"],
            "crs": stack["crs"],
            "transform": stack["transform"],
            "pixel_size": stack["pixel_size"],
            "reference_band": stack["reference_band"],
        },
    }
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
    raster_files = [
        path
        for path in extracted.root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".jp2", ".tif", ".tiff"}
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

    band_paths = _discover_s2_band_paths(
        raster_files,
        product_type=s2_product_type,
        ext=str(s2_spec.get("ext", "jp2")),
    )
    missing = [band for band in _S2_REQUIRED_BANDS if band not in band_paths]
    if missing:
        raise ConversionError(
            "Missing required Sentinel-2 optical bands: " + ", ".join(missing)
        )

    ordered_bands = _ordered_s2_bands(s2_spec, band_paths)
    categorical_bands = {
        _S2_CANONICAL_BY_CODE[code]
        for code in list(s2_spec.get("categorical_bands") or [])
        if code in _S2_CANONICAL_BY_CODE
    }
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
    written_uri, dataset_summary = stream_raster_stack_to_zarr(
        band_paths=band_paths,
        ordered_bands=ordered_bands,
        output_uri=output_uri,
        metadata=metadata,
        acquisition_datetime=acquisition,
        reference_band="red",
        categorical_bands=categorical_bands,
        target_pixel_size=target_pixel_size,
    )
    band_metadata = dict(dataset_summary.get("band_metadata") or {})
    grid = {
        "height": int(dataset_summary["shape"][2]),
        "width": int(dataset_summary["shape"][3]),
        "dtype": str(dataset_summary.get("dtype") or "unknown"),
        "crs": dataset_summary.get("crs"),
        "transform": dataset_summary.get("transform"),
        "pixel_size": dataset_summary.get("pixel_size"),
        "reference_band": "red",
    }
    summary = {
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "product_id": scene_id,
        "product_type": s2_product_type,
        "product_level": _s2_product_level(scene_id),
        "data_family": "optical",
        "source_kind": extracted.source_kind,
        "raw_path": str(extracted.raw_path),
        "canonical_bands": list(_S2_CANONICAL_BANDS),
        "required_core_bands": list(_S2_REQUIRED_BANDS),
        "optional_bands_present": [band for band in _S2_CANONICAL_BANDS if band in band_paths and band not in _S2_REQUIRED_BANDS],
        "normalized_band_order": list(dataset_summary["band_names"]),
        "resolution_policy_meters": target_pixel_size,
        "band_sources": {
            band: str(path.relative_to(extracted.root)) for band, path in band_paths.items()
        },
        "band_resampling": {
            band: band_metadata.get(band, {}).get("resampled_to_reference")
            for band in dataset_summary["band_names"]
        },
        "band_native_pixel_size": {
            band: band_metadata.get(band, {}).get("source_pixel_size")
            for band in dataset_summary["band_names"]
        },
        "acquisition_datetime": acquisition,
        "grid": grid,
        "validation": {
            "same_grid": True,
            "missing_optional_bands": [
                band for band in _S2_CANONICAL_BANDS if band not in band_paths and band not in _S2_REQUIRED_BANDS
            ],
        },
    }
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
        dataset, summary = build_sentinel1_raw_dataset(
            root=extracted.root,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
        )
        return dataset, summary
    s1_spec = get_copernicus_product_spec("SENTINEL-1", s1_product_type)
    if not s1_spec:
        raise ConversionError(
            "Unsupported Sentinel-1 productType for this project. "
            f"Expected one of: {', '.join(sorted(supported_product_types().get('SENTINEL-1', [])))}. "
            f"Got: {s1_product_type}."
        )

    measurement_exts = {
        f".{str(value).lower().lstrip('.')}"
        for value in (s1_spec.get("measurement_exts") or ["tif", "tiff"])
    }
    raster_files = [
        path
        for path in extracted.root.rglob("*")
        if path.is_file()
        and path.suffix.lower() in measurement_exts
        and "measurement" in str(path.parent).lower()
    ]
    if not raster_files:
        raster_files = [
            path
            for path in extracted.root.rglob("*")
            if path.is_file() and path.suffix.lower() in measurement_exts
        ]
    if not raster_files:
        raise ConversionError(
            "No Sentinel-1 measurement rasters compatible with this product type were found in the SAFE product."
        )

    band_paths: dict[str, Path] = {}
    for band_name, patterns in _S1_PATTERNS.items():
        for pattern in patterns:
            match = next((path for path in raster_files if pattern.search(path.name)), None)
            if match is not None:
                band_paths[band_name] = match
                break

    if not band_paths:
        raise ConversionError("No Sentinel-1 polarization rasters were detected.")

    ordered_bands = [band for band in ("vv", "vh", "hh", "hv") if band in band_paths]
    target_pixel_size = target_pixel_size_for(provider, collection)
    stack = load_aligned_raster_stack(
        band_paths,
        ordered_bands=ordered_bands,
        reference_band=ordered_bands[0],
        target_pixel_size=target_pixel_size,
    )
    acquisition = _extract_timestamp_from_scene_id(scene_id, prefix="S1")
    dataset = build_standard_dataset(
        arrays=stack["arrays"].astype(np.float32),
        band_names=stack["band_names"],
        acquisition_datetime=acquisition,
        metadata={
            "provider": provider,
            "collection": collection,
            "scene_id": scene_id,
            "product_id": scene_id,
            "data_family": "sar",
            "crs": stack["crs"],
            "transform": stack["transform"],
            "reference_band": stack["reference_band"],
            "reference_pixel_size": stack["pixel_size"],
            "band_metadata": stack["band_metadata"],
        },
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
        "normalized_band_order": stack["band_names"],
        "resolution_policy_meters": target_pixel_size,
        "band_sources": {
            band: str(path.relative_to(extracted.root)) for band, path in band_paths.items()
        },
        "band_resampling": {
            band: stack["band_metadata"][band]["resampled_to_reference"]
            for band in stack["band_names"]
        },
        "band_native_pixel_size": {
            band: stack["band_metadata"][band]["source_pixel_size"]
            for band in stack["band_names"]
        },
        "acquisition_datetime": acquisition,
        "grid": {
            "height": stack["height"],
            "width": stack["width"],
            "dtype": stack["dtype"],
            "crs": stack["crs"],
            "transform": stack["transform"],
            "pixel_size": stack["pixel_size"],
            "reference_band": stack["reference_band"],
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

    nc_files = [
        path for path in extracted.root.rglob("*") if path.is_file() and path.suffix.lower() == ".nc"
    ]
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
        raise ConversionError(
            "No 2D numeric variables with a shared grid were found in the NetCDF product."
        )

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


def _discover_s2_band_paths(
    raster_files: list[Path],
    *,
    product_type: str,
    ext: str,
) -> dict[str, Path]:
    spec = get_copernicus_product_spec("SENTINEL-2", product_type)
    band_paths: dict[str, Path] = {}
    for bucket, band_codes in dict(spec.get("bands_map", {})).items():
        suffix = _s2_storage_suffix(str(bucket))
        for code in band_codes:
            canonical = _S2_CANONICAL_BY_CODE.get(str(code))
            if not canonical:
                continue
            patterns = _s2_band_patterns(str(code), suffix=suffix, ext=ext)
            for pattern in patterns:
                match = next((path for path in raster_files if pattern.search(path.name)), None)
                if match is not None:
                    band_paths[canonical] = match
                    break
            if canonical in band_paths:
                continue
    return band_paths


def _ordered_s2_bands(spec: dict[str, Any], band_paths: dict[str, Path]) -> list[str]:
    _ = spec
    return [band for band in _S2_CANONICAL_BANDS if band in band_paths]


def _s2_storage_suffix(bucket: str) -> str:
    normalized = str(bucket or "").strip()
    if not normalized or normalized.lower() == "empty":
        return ""
    match = re.search(r"(\d{2})m$", normalized, re.IGNORECASE)
    if match:
        return f"_{match.group(1)}m"
    return f"_{normalized}"


def _s2_band_patterns(code: str, *, suffix: str, ext: str) -> tuple[re.Pattern[str], ...]:
    escaped_ext = re.escape(str(ext).lstrip("."))
    patterns = []
    if suffix:
        patterns.append(re.compile(rf"_{re.escape(code)}{re.escape(suffix)}\.(?:{escaped_ext}|tif|tiff)$", re.IGNORECASE))
    else:
        patterns.append(re.compile(rf"_{re.escape(code)}_(?:10m|20m|60m)\.(?:{escaped_ext}|tif|tiff)$", re.IGNORECASE))
    patterns.append(re.compile(rf"_{re.escape(code)}\.(?:{escaped_ext}|tif|tiff)$", re.IGNORECASE))
    return tuple(patterns)


def _extract_timestamp_from_scene_id(scene_id: str, *, prefix: str) -> str | None:
    if prefix == "S2":
        match = re.search(r"_(\d{8}T\d{6})_", scene_id)
        if match:
            return _to_iso(match.group(1), "%Y%m%dT%H%M%S")
    if prefix == "S1":
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
    # Prefer explicit SAFE product folders.
    safe_dirs = sorted(
        [path for path in extracted.root.rglob("*.SAFE") if path.is_dir()],
        key=lambda path: (len(path.relative_to(extracted.root).parts), len(path.name)),
    )
    if safe_dirs:
        return safe_dirs[0].name.removesuffix(".SAFE")

    # Fallback to manifest parent.
    manifest = next(
        (
            path
            for path in extracted.root.rglob("*")
            if path.is_file() and path.name.lower() == "manifest.safe"
        ),
        None,
    )
    if manifest is not None:
        parent_name = manifest.parent.name
        return parent_name.removesuffix(".SAFE")

    # Fallback to first NetCDF stem for swath products.
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
