from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

from nimbuschain_zarr_service.config_loader import (
    get_landsat_product_spec,
    target_pixel_size_for,
    supported_product_types,
)
from nimbuschain_zarr_service.core import (
    ConversionDependencyError,
    ConversionError,
    build_standard_dataset,
    load_aligned_raster_stack,
    stream_raster_stack_to_zarr,
    prepare_source,
)
from nimbuschain_zarr_service.schema import CORE_BANDS


class LandsatNormalizationError(ConversionError):
    """Raised when a Landsat source cannot be normalized."""


class LandsatDependencyError(ConversionDependencyError):
    """Raised when a runtime dependency required for Landsat conversion is missing."""


LANDSAT_CANONICAL_BANDS = (
    "coastal",
    "blue",
    "green",
    "red",
    "pan",
    "nir",
    "cirrus",
    "swir1",
    "swir2",
    "thermal1",
    "thermal2",
)
LANDSAT_OPTIONAL_BANDS = tuple(
    band for band in LANDSAT_CANONICAL_BANDS if band not in CORE_BANDS
)
_LANDSAT_CANONICAL_BY_CODE = {
    "B1": "coastal",
    "B2": "blue",
    "B3": "green",
    "B4": "red",
    "B8": "pan",
    "B5": "nir",
    "B9": "cirrus",
    "B6": "swir1",
    "B7": "swir2",
    "B10": "thermal1",
    "B11": "thermal2",
}
_MTL_NAME_RE = re.compile(r"_MTL\.txt$", re.IGNORECASE)
_MTL_KV_RE = re.compile(r"^\s*([A-Z0-9_]+)\s=\s(.+?)\s*$")
_LANDSAT_PRODUCT_TYPE_RE = re.compile(r"_((?:L1|L2)[A-Z0-9]{2})_", re.IGNORECASE)
_LANDSAT_SPACECRAFT_RE = re.compile(r"^L[A-Z]?0?([0-9]{2})_")


def build_landsat_dataset(
    *,
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    extracted = prepare_source(raw_uri, label="landsat")
    try:
        files = _discover_files(extracted.root)
        mtl_values = _parse_mtl_file(files.get("mtl"))
        normalized_scene_id = str(
            mtl_values.get("LANDSAT_SCENE_ID")
            or mtl_values.get("LANDSAT_PRODUCT_ID")
            or scene_id
        )
        product_id = str(
            mtl_values.get("LANDSAT_PRODUCT_ID")
            or normalized_scene_id
        )
        resolved_product_type = _landsat_product_type(product_id, requested=product_type)
        _validate_landsat_collection_product_type(collection=collection, product_type=resolved_product_type)
        band_paths = _select_band_paths(
            files["tifs"],
            collection=collection,
            product_type=resolved_product_type,
        )
        ordered_bands = [band for band in LANDSAT_CANONICAL_BANDS if band in band_paths]
        target_pixel_size = target_pixel_size_for(provider, collection)
        stack = load_aligned_raster_stack(
            band_paths,
            ordered_bands=ordered_bands,
            reference_band="red",
            target_pixel_size=target_pixel_size,
        )
        satellite_code = _landsat_satellite_code(product_id)
        product_type_short = _landsat_product_type_short(satellite_code, resolved_product_type)
        product_level = _landsat_product_level(collection=collection, product_id=product_id)
        acquisition = _build_acquisition_datetime(mtl_values)
        dataset = build_standard_dataset(
            arrays=stack["arrays"],
            band_names=stack["band_names"],
            acquisition_datetime=acquisition,
            metadata={
                "provider": provider,
                "collection": collection,
                "scene_id": normalized_scene_id,
                "product_id": product_id,
                "product_type": resolved_product_type,
                "product_type_short": product_type_short,
                "satellite": satellite_code,
                "product_level": product_level,
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
            "scene_id": normalized_scene_id,
            "product_id": product_id,
            "product_type": resolved_product_type,
            "product_type_short": product_type_short,
            "satellite": satellite_code,
            "product_level": product_level,
            "data_family": "optical",
            "source_kind": extracted.source_kind,
            "raw_path": str(extracted.raw_path),
            "canonical_bands": list(LANDSAT_CANONICAL_BANDS),
            "required_core_bands": list(CORE_BANDS),
            "optional_bands_present": [
                band for band in LANDSAT_OPTIONAL_BANDS if band in band_paths
            ],
            "normalized_band_order": stack["band_names"],
            "resolution_policy_meters": target_pixel_size,
            "band_sources": {
                band: (
                    str(path.relative_to(extracted.root))
                    if extracted.cleanup is not None
                    else str(path)
                )
                for band, path in band_paths.items()
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
            "validation": {
                "same_grid": True,
                "missing_optional_bands": [
                    band for band in LANDSAT_OPTIONAL_BANDS if band not in band_paths
                ],
            },
        }
        if normalized_scene_id != scene_id:
            summary["requested_scene_id"] = scene_id
        return dataset, summary
    finally:
        if extracted.cleanup is not None:
            extracted.cleanup.cleanup()


def normalize_landsat_source(
    *,
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
    product_type: str | None = None,
) -> dict[str, Any]:
    _, summary = build_landsat_dataset(
        raw_uri=raw_uri,
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        product_type=product_type,
    )
    return summary


def convert_landsat_to_zarr(
    *,
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
    output_uri: str,
    product_type: str | None = None,
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    extracted = prepare_source(raw_uri, label="landsat")
    try:
        files = _discover_files(extracted.root)
        mtl_values = _parse_mtl_file(files.get("mtl"))
        normalized_scene_id = str(
            mtl_values.get("LANDSAT_SCENE_ID")
            or mtl_values.get("LANDSAT_PRODUCT_ID")
            or scene_id
        )
        product_id = str(
            mtl_values.get("LANDSAT_PRODUCT_ID")
            or normalized_scene_id
        )
        resolved_product_type = _landsat_product_type(product_id, requested=product_type)
        _validate_landsat_collection_product_type(collection=collection, product_type=resolved_product_type)
        band_paths = _select_band_paths(
            files["tifs"],
            collection=collection,
            product_type=resolved_product_type,
        )
        ordered_bands = [band for band in LANDSAT_CANONICAL_BANDS if band in band_paths]
        satellite_code = _landsat_satellite_code(product_id)
        product_type_short = _landsat_product_type_short(satellite_code, resolved_product_type)
        product_level = _landsat_product_level(collection=collection, product_id=product_id)
        acquisition = _build_acquisition_datetime(mtl_values)
        target_pixel_size = target_pixel_size_for(provider, collection)
        metadata = {
            "provider": provider,
            "collection": collection,
            "scene_id": normalized_scene_id,
            "product_id": product_id,
            "product_type": resolved_product_type,
            "product_type_short": product_type_short,
            "satellite": satellite_code,
            "product_level": product_level,
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
            target_pixel_size=target_pixel_size,
        )
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
            "scene_id": normalized_scene_id,
            "product_id": product_id,
            "product_type": resolved_product_type,
            "product_type_short": product_type_short,
            "satellite": satellite_code,
            "product_level": product_level,
            "data_family": "optical",
            "source_kind": extracted.source_kind,
            "raw_path": str(extracted.raw_path),
            "canonical_bands": list(LANDSAT_CANONICAL_BANDS),
            "required_core_bands": list(CORE_BANDS),
            "optional_bands_present": [
                band for band in LANDSAT_OPTIONAL_BANDS if band in band_paths
            ],
            "normalized_band_order": list(dataset_summary["band_names"]),
            "resolution_policy_meters": target_pixel_size,
            "band_sources": {
                band: (
                    str(path.relative_to(extracted.root))
                    if extracted.cleanup is not None
                    else str(path)
                )
                for band, path in band_paths.items()
            },
            "band_resampling": {},
            "band_native_pixel_size": {},
            "acquisition_datetime": acquisition,
            "grid": grid,
            "validation": {
                "same_grid": True,
                "missing_optional_bands": [
                    band for band in LANDSAT_OPTIONAL_BANDS if band not in band_paths
                ],
            },
        }
        # Extract per-band metadata from the consolidated store attrs written by the streaming path.
        band_metadata = dataset_summary.get("band_metadata")
        if isinstance(band_metadata, dict):
            summary["band_resampling"] = {
                band: band_metadata.get(band, {}).get("resampled_to_reference")
                for band in dataset_summary["band_names"]
            }
            summary["band_native_pixel_size"] = {
                band: band_metadata.get(band, {}).get("source_pixel_size")
                for band in dataset_summary["band_names"]
            }
        if normalized_scene_id != scene_id:
            summary["requested_scene_id"] = scene_id
        return written_uri, "optical", summary, dataset_summary
    finally:
        if extracted.cleanup is not None:
            extracted.cleanup.cleanup()


def _discover_files(root: Path) -> dict[str, Any]:
    tifs = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}
    ]
    if not tifs:
        raise LandsatNormalizationError(
            "No GeoTIFF bands were found in the Landsat source after extraction."
        )
    mtl = next(
        (path for path in root.rglob("*") if path.is_file() and _MTL_NAME_RE.search(path.name)),
        None,
    )
    return {"tifs": tifs, "mtl": mtl}


def _select_band_paths(
    tifs: list[Path],
    *,
    collection: str,
    product_type: str,
) -> dict[str, Path]:
    spec = get_landsat_product_spec(collection, product_type)
    if not spec:
        raise LandsatNormalizationError(
            f"Unsupported Landsat collection/product type: {collection} / {product_type}"
        )

    band_paths: dict[str, Path] = {}
    for band_code in list(spec.get("band_codes") or []):
        canonical_band = _LANDSAT_CANONICAL_BY_CODE.get(str(band_code))
        if not canonical_band:
            continue
        for pattern in _landsat_code_patterns(str(band_code)):
            match = next((path for path in tifs if pattern.search(path.name)), None)
            if match is not None:
                band_paths[canonical_band] = match
                break

    missing_core = [band for band in CORE_BANDS if band not in band_paths]
    if missing_core:
        raise LandsatNormalizationError(
            "Missing required Landsat reflective bands for normalization: "
            + ", ".join(missing_core)
        )
    return band_paths


def _landsat_code_patterns(code: str) -> tuple[re.Pattern[str], ...]:
    escaped = re.escape(code)
    return (
        re.compile(rf"_SR_{escaped}\.(?:TIF|TIFF)$", re.IGNORECASE),
        re.compile(rf"_ST_{escaped}\.(?:TIF|TIFF)$", re.IGNORECASE),
        re.compile(rf"_{escaped}\.(?:TIF|TIFF)$", re.IGNORECASE),
    )


def _parse_mtl_file(mtl_path: Path | None) -> dict[str, str]:
    if mtl_path is None:
        return {}
    values: dict[str, str] = {}
    for line in mtl_path.read_text(errors="replace").splitlines():
        match = _MTL_KV_RE.match(line)
        if not match:
            continue
        key, value = match.groups()
        values[key] = value.strip().strip('"')
    return values


def _build_acquisition_datetime(values: dict[str, str]) -> str | None:
    date_value = values.get("DATE_ACQUIRED")
    if not date_value:
        return None

    time_value = values.get("SCENE_CENTER_TIME", "00:00:00Z").strip('"').rstrip("Z")
    time_main = time_value.split(".")[0]
    try:
        parsed = datetime.fromisoformat(f"{date_value}T{time_main}")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc).isoformat()


def _landsat_product_type(product_id: str, *, requested: str | None = None) -> str:
    requested_upper = str(requested or "").strip().upper()
    if requested_upper:
        return requested_upper
    match = _LANDSAT_PRODUCT_TYPE_RE.search(str(product_id))
    if match:
        return match.group(1).upper()
    return "UNKNOWN"


def _landsat_product_level(*, collection: str, product_id: str) -> str:
    product_upper = str(product_id).upper()
    if "_L1" in product_upper:
        return "toa"
    if "_L2" in product_upper:
        return "boa"
    collection_lower = str(collection).lower()
    if collection_lower.endswith("_l1"):
        return "toa"
    if collection_lower.endswith("_l2"):
        return "boa"
    return "unknown"


def _landsat_satellite_code(product_id: str) -> str:
    match = _LANDSAT_SPACECRAFT_RE.match(str(product_id).upper())
    if match:
        return match.group(1)
    return "00"


def _landsat_product_type_short(satellite_code: str, product_type: str) -> str:
    sat = str(satellite_code or "").strip()
    ptype = str(product_type or "").strip().upper()
    if sat and sat != "00" and ptype != "UNKNOWN":
        return f"{int(sat)}{ptype}"
    return ptype


def _validate_landsat_collection_product_type(*, collection: str, product_type: str) -> None:
    ptype = str(product_type or "").upper()
    if ptype == "UNKNOWN":
        raise LandsatNormalizationError(
            "Unable to infer Landsat product type from product id. "
            f"Expected one of L1TP/L1GT/L1GS/L2SP/L2SR."
        )
    allowed = supported_product_types().get(str(collection).lower()) or supported_product_types().get(str(collection)) or []
    if ptype not in allowed:
        raise LandsatNormalizationError(
            f"Invalid Landsat product type for collection {collection}. "
            f"Expected one of: {', '.join(sorted(allowed))}. Got: {ptype}."
        )
