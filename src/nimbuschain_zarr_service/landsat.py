from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

from nimbuschain_zarr_service.config_loader import (
    supported_product_types,
    target_pixel_size_for,
)
from nimbuschain_zarr_service.core import (
    ConversionDependencyError,
    ConversionError,
    attach_layer_array,
    build_standard_dataset,
    load_aligned_raster_stack,
    stream_raster_product_to_zarr,
)
from nimbuschain_zarr_service.models import GridMetadataRecord, LandsatNormalizationSummaryRecord
from nimbuschain_zarr_service.storage_support import PreparedSource, TargetGrid, prepare_source


class LandsatNormalizationError(ConversionError):
    """Raised when a Landsat source cannot be normalized."""


class LandsatDependencyError(ConversionDependencyError):
    """Raised when a runtime dependency required for Landsat conversion is missing."""


_MTL_NAME_RE = re.compile(r"_MTL\.txt$", re.IGNORECASE)
_MTL_KV_RE = re.compile(r"^\s*([A-Z0-9_]+)\s=\s(.+?)\s*$")
_LANDSAT_PRODUCT_TYPE_RE = re.compile(r"_((?:L1|L2)[A-Z0-9]{2})_", re.IGNORECASE)
_LANDSAT_SPACECRAFT_RE = re.compile(r"^L[A-Z]?0?([0-9]{2})_")

_L1_IMAGERY_LAYERS = tuple(f"B{index}" for index in range(1, 12))
_L2_REFLECTANCE_LAYERS = tuple(f"SR_B{index}" for index in range(1, 8))
_L2_THERMAL_LAYERS = ("ST_B10",)
_L2_IMAGERY_LAYERS = _L2_REFLECTANCE_LAYERS + _L2_THERMAL_LAYERS
_L2SR_IMAGERY_LAYERS = _L2_REFLECTANCE_LAYERS
_LANDSAT_ANCILLARY_PRIORITY = {
    "QA_PIXEL": 0,
    "QA_RADSAT": 1,
    "SR_QA_AEROSOL": 2,
    "QA_AEROSOL": 3,
    "ST_QA": 4,
    "SAA": 5,
    "SZA": 6,
    "VAA": 7,
    "VZA": 8,
    "ST_CDIST": 9,
    "ST_DRAD": 10,
    "ST_URAD": 11,
    "ST_TRAD": 12,
    "ST_ATRAN": 13,
    "ST_EMIS": 14,
    "ST_EMSD": 15,
}
_LANDSAT_KNOWN_LAYER_TOKENS = tuple(
    sorted(
        {
            *_L1_IMAGERY_LAYERS,
            *_L2_IMAGERY_LAYERS,
            *_LANDSAT_ANCILLARY_PRIORITY,
        },
        key=len,
        reverse=True,
    )
)


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
        product_id = str(mtl_values.get("LANDSAT_PRODUCT_ID") or normalized_scene_id)
        resolved_product_type = _landsat_product_type(product_id, requested=product_type)
        _validate_landsat_collection_product_type(
            collection=collection,
            product_type=resolved_product_type,
        )
        imagery_paths, ancillary_paths = _discover_landsat_layers(
            files["tifs"],
            collection=collection,
            product_type=resolved_product_type,
        )
        ordered_imagery = _ordered_landsat_imagery_layers(
            collection=collection,
            product_type=resolved_product_type,
            imagery_paths=imagery_paths,
        )
        if not ordered_imagery:
            raise LandsatNormalizationError(
                "No Landsat imagery layers were detected in the extracted source."
            )

        reference_band = _landsat_reference_band(
            collection=collection,
            product_type=resolved_product_type,
            available=ordered_imagery,
        )
        target_pixel_size = target_pixel_size_for(provider, collection)
        radiometric_metadata = _landsat_radiometric_metadata(
            collection=collection,
            product_type=resolved_product_type,
            values=mtl_values,
        )
        imagery_stack = load_aligned_raster_stack(
            imagery_paths,
            ordered_bands=ordered_imagery,
            reference_band=reference_band,
            target_pixel_size=target_pixel_size,
        )
        acquisition = _build_acquisition_datetime(mtl_values)
        satellite_code = _landsat_satellite_code(product_id)
        product_type_short = _landsat_product_type_short(
            satellite_code,
            resolved_product_type,
        )
        product_level = _landsat_product_level(
            collection=collection,
            product_id=product_id,
        )
        dataset = build_standard_dataset(
        arrays=imagery_stack.arrays,
        band_names=imagery_stack.band_names,
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
                "crs": imagery_stack.crs,
                "transform": imagery_stack.transform,
                "reference_band": imagery_stack.reference_band,
                "reference_pixel_size": imagery_stack.pixel_size,
                "band_metadata": imagery_stack.band_metadata_dict,
                "radiometric_metadata": radiometric_metadata,
            },
        )
        ancillary_metadata: dict[str, Any] = {}
        ordered_ancillary = _ordered_landsat_ancillary_layers(ancillary_paths)
        if ordered_ancillary:
            target_grid = TargetGrid(
                height=int(imagery_stack.height),
                width=int(imagery_stack.width),
                crs=imagery_stack.crs,
                transform=imagery_stack.transform,
                pixel_size=imagery_stack.pixel_size,
                reference_band=imagery_stack.reference_band,
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
                arrays=ancillary_stack.arrays,
                layer_names=ancillary_stack.band_names,
                acquisition_datetime=acquisition,
                variable_name="ancillary",
                coord_name="ancillary_layer",
            )
            ancillary_metadata = ancillary_stack.band_metadata_dict

        grid = GridMetadataRecord(
            height=int(imagery_stack.height),
            width=int(imagery_stack.width),
            dtype=str(imagery_stack.dtype),
            crs=imagery_stack.crs,
            transform=imagery_stack.transform,
            pixel_size=imagery_stack.pixel_size,
            reference_band=imagery_stack.reference_band,
        )
        grid = GridMetadataRecord(
            height=int(dataset_summary.shape[2]),
            width=int(dataset_summary.shape[3]),
            dtype=str(dataset_summary.dtype or "unknown"),
            crs=dataset_summary.crs,
            transform=dataset_summary.transform,
            pixel_size=dataset_summary.pixel_size,
            reference_band=reference_band,
        )
        summary = _summarize_landsat_product(
            provider=provider,
            collection=collection,
            scene_id=normalized_scene_id,
            product_id=product_id,
            product_type=resolved_product_type,
            product_type_short=product_type_short,
            satellite_code=satellite_code,
            product_level=product_level,
            acquisition=acquisition,
            extracted=extracted,
            imagery_paths=imagery_paths,
            imagery_band_names=imagery_stack.band_names,
            imagery_metadata=imagery_stack.band_metadata_dict,
            grid=grid,
            target_pixel_size=target_pixel_size,
            radiometric_metadata=radiometric_metadata,
            ancillary_paths=ancillary_paths,
            ancillary_layer_names=ordered_ancillary,
            ancillary_metadata=ancillary_metadata,
        )
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
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
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
        product_id = str(mtl_values.get("LANDSAT_PRODUCT_ID") or normalized_scene_id)
        resolved_product_type = _landsat_product_type(product_id, requested=product_type)
        _validate_landsat_collection_product_type(
            collection=collection,
            product_type=resolved_product_type,
        )
        imagery_paths, ancillary_paths = _discover_landsat_layers(
            files["tifs"],
            collection=collection,
            product_type=resolved_product_type,
        )
        ordered_imagery = _ordered_landsat_imagery_layers(
            collection=collection,
            product_type=resolved_product_type,
            imagery_paths=imagery_paths,
        )
        if not ordered_imagery:
            raise LandsatNormalizationError(
                "No Landsat imagery layers were detected in the extracted source."
            )
        ordered_ancillary = _ordered_landsat_ancillary_layers(ancillary_paths)
        target_pixel_size = target_pixel_size_for(provider, collection)
        reference_band = _landsat_reference_band(
            collection=collection,
            product_type=resolved_product_type,
            available=ordered_imagery,
        )
        acquisition = _build_acquisition_datetime(mtl_values)
        satellite_code = _landsat_satellite_code(product_id)
        product_type_short = _landsat_product_type_short(
            satellite_code,
            resolved_product_type,
        )
        product_level = _landsat_product_level(
            collection=collection,
            product_id=product_id,
        )
        radiometric_metadata = _landsat_radiometric_metadata(
            collection=collection,
            product_type=resolved_product_type,
            values=mtl_values,
        )
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
            "radiometric_metadata": radiometric_metadata,
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
            progress_callback=progress_callback,
        )
        grid = GridMetadataRecord(
            height=int(dataset_summary.shape[2]),
            width=int(dataset_summary.shape[3]),
            dtype=str(dataset_summary.dtype or "unknown"),
            crs=dataset_summary.crs,
            transform=dataset_summary.transform,
            pixel_size=dataset_summary.pixel_size,
            reference_band=reference_band,
        )
        summary = _summarize_landsat_product(
            provider=provider,
            collection=collection,
            scene_id=normalized_scene_id,
            product_id=product_id,
            product_type=resolved_product_type,
            product_type_short=product_type_short,
            satellite_code=satellite_code,
            product_level=product_level,
            acquisition=acquisition,
            extracted=extracted,
            imagery_paths=imagery_paths,
            imagery_band_names=list(dataset_summary.band_names),
            imagery_metadata=dict(dataset_summary.band_metadata),
            grid=grid,
            target_pixel_size=target_pixel_size,
            radiometric_metadata=radiometric_metadata,
            ancillary_paths=ancillary_paths,
            ancillary_layer_names=list(dataset_summary.ancillary_layer_names or []),
            ancillary_metadata=dict(dataset_summary.ancillary_metadata or {}),
        )
        if normalized_scene_id != scene_id:
            summary["requested_scene_id"] = scene_id
        return written_uri, "optical", summary, dataset_summary.to_dict()
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
        (
            path
            for path in root.rglob("*")
            if path.is_file() and _MTL_NAME_RE.search(path.name)
        ),
        None,
    )
    return {"tifs": tifs, "mtl": mtl}


def _discover_landsat_layers(
    tifs: list[Path],
    *,
    collection: str,
    product_type: str,
) -> tuple[dict[str, Path], dict[str, Path]]:
    imagery_expected = _landsat_expected_imagery_layers(collection, product_type)
    imagery_paths: dict[str, Path] = {}
    ancillary_paths: dict[str, Path] = {}
    for path in sorted(tifs):
        layer_name = _landsat_layer_name(path)
        if not layer_name:
            continue
        if layer_name in imagery_expected:
            imagery_paths[layer_name] = path
        else:
            ancillary_paths[layer_name] = path

    missing_imagery = [layer for layer in imagery_expected if layer not in imagery_paths]
    if missing_imagery:
        raise LandsatNormalizationError(
            "Missing expected Landsat imagery layers for "
            f"{collection}/{product_type}: {', '.join(missing_imagery)}"
        )
    return imagery_paths, ancillary_paths


def _landsat_expected_imagery_layers(collection: str, product_type: str) -> tuple[str, ...]:
    collection_lower = str(collection).lower()
    product_upper = str(product_type).upper()
    if collection_lower.endswith("_l1"):
        return _L1_IMAGERY_LAYERS
    if product_upper == "L2SR":
        return _L2SR_IMAGERY_LAYERS
    return _L2_IMAGERY_LAYERS


def _ordered_landsat_imagery_layers(
    *,
    collection: str,
    product_type: str,
    imagery_paths: dict[str, Path],
) -> list[str]:
    expected = _landsat_expected_imagery_layers(collection, product_type)
    return [layer for layer in expected if layer in imagery_paths]


def _ordered_landsat_ancillary_layers(ancillary_paths: dict[str, Path]) -> list[str]:
    return sorted(
        ancillary_paths,
        key=lambda value: (_LANDSAT_ANCILLARY_PRIORITY.get(value, 100), value),
    )


def _landsat_layer_name(path: Path) -> str | None:
    stem = path.stem.upper()
    for token in _LANDSAT_KNOWN_LAYER_TOKENS:
        if re.search(rf"(?:^|_){re.escape(token)}(?:_|$)", stem):
            return token

    tokens = [token for token in re.split(r"[^A-Z0-9]+", stem) if token]
    for index, token in enumerate(tokens):
        if token.startswith("QA") or token in {"SAA", "SZA", "VAA", "VZA"} or token.startswith("ST"):
            return "_".join(tokens[index:])
    if len(tokens) >= 2:
        return "_".join(tokens[-2:])
    if tokens:
        return tokens[-1]
    return None


def _landsat_reference_band(
    *,
    collection: str,
    product_type: str,
    available: list[str],
) -> str:
    preferred = ("SR_B4", "B4", "SR_B3", "B3", "SR_B2", "B2")
    for candidate in preferred:
        if candidate in available:
            return candidate
    if not available:
        raise LandsatNormalizationError("No Landsat imagery layers are available.")
    return available[0]


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


def _landsat_radiometric_metadata(
    *,
    collection: str,
    product_type: str,
    values: dict[str, str],
) -> dict[str, Any]:
    collection_lower = str(collection).lower()
    payload: dict[str, Any] = {
        "collection": collection,
        "product_type": product_type,
        "sun_elevation": _as_float(values.get("SUN_ELEVATION")),
        "bands": {},
    }
    bands = payload["bands"]
    if collection_lower.endswith("_l1"):
        for index in range(1, 10):
            bands[f"B{index}"] = {
                "quantity": "toa_reflectance",
                "mult": _first_float(
                    values,
                    f"REFLECTANCE_MULT_BAND_{index}",
                    f"SURFACE_REFLECTANCE_MULT_BAND_{index}",
                    f"SR_MULT_BAND_{index}",
                )
                or 2.0e-5,
                "add": _first_float(
                    values,
                    f"REFLECTANCE_ADD_BAND_{index}",
                    f"SURFACE_REFLECTANCE_ADD_BAND_{index}",
                    f"SR_ADD_BAND_{index}",
                )
                or -0.1,
                "apply_sun_elevation": True,
            }
    else:
        for index in range(1, 8):
            bands[f"SR_B{index}"] = {
                "quantity": "surface_reflectance",
                "mult": _first_float(
                    values,
                    f"REFLECTANCE_MULT_BAND_{index}",
                    f"SURFACE_REFLECTANCE_MULT_BAND_{index}",
                    f"SR_MULT_BAND_{index}",
                )
                or 2.75e-5,
                "add": _first_float(
                    values,
                    f"REFLECTANCE_ADD_BAND_{index}",
                    f"SURFACE_REFLECTANCE_ADD_BAND_{index}",
                    f"SR_ADD_BAND_{index}",
                )
                or -0.2,
                "apply_sun_elevation": False,
            }
    return payload


def _first_float(values: dict[str, str], *keys: str) -> float | None:
    for key in keys:
        value = _as_float(values.get(key))
        if value is not None:
            return value
    return None


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip().strip('"'))
    except ValueError:
        return None


def _landsat_product_type(product_id: str, *, requested: str | None = None) -> str:
    requested_upper = str(requested or "").strip().upper()
    if re.fullmatch(r"[0-9]L[0-9A-Z]{3}", requested_upper):
        requested_upper = requested_upper[1:]
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
            "Expected one of L1TP/L1GT/L1GS/L2SP/L2SR."
        )
    allowed = (
        supported_product_types().get(str(collection).lower())
        or supported_product_types().get(str(collection))
        or []
    )
    if ptype not in allowed:
        raise LandsatNormalizationError(
            f"Invalid Landsat product type for collection {collection}. "
            f"Expected one of: {', '.join(sorted(allowed))}. Got: {ptype}."
        )


def _summarize_landsat_product(
    *,
    provider: str,
    collection: str,
    scene_id: str,
    product_id: str,
    product_type: str,
    product_type_short: str,
    satellite_code: str,
    product_level: str,
    acquisition: str | None,
    extracted: PreparedSource,
    imagery_paths: dict[str, Path],
    imagery_band_names: list[str],
    imagery_metadata: dict[str, Any],
    grid: GridMetadataRecord,
    target_pixel_size: float | None,
    radiometric_metadata: dict[str, Any],
    ancillary_paths: dict[str, Path],
    ancillary_layer_names: list[str],
    ancillary_metadata: dict[str, Any],
) -> dict[str, Any]:
    return LandsatNormalizationSummaryRecord(
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        product_id=product_id,
        product_type=product_type,
        product_type_short=product_type_short,
        satellite=satellite_code,
        product_level=product_level,
        data_family="optical",
        source_kind=extracted.source_kind,
        raw_path=str(extracted.raw_path),
        acquisition_datetime=acquisition,
        normalized_band_order=list(imagery_band_names),
        resolution_policy_meters=target_pixel_size,
        radiometric_metadata=dict(radiometric_metadata),
        band_sources={band: str(path.relative_to(extracted.root)) for band, path in imagery_paths.items()},
        band_resampling={
            band: imagery_metadata.get(band, {}).get("resampled_to_reference")
            for band in imagery_band_names
        },
        band_native_pixel_size={
            band: imagery_metadata.get(band, {}).get("source_pixel_size")
            for band in imagery_band_names
        },
        ancillary_layer_names=list(ancillary_layer_names),
        ancillary_sources={
            layer: str(path.relative_to(extracted.root)) for layer, path in ancillary_paths.items()
        },
        ancillary_resampling={
            layer: ancillary_metadata.get(layer, {}).get("resampled_to_reference")
            for layer in ancillary_layer_names
        },
        ancillary_native_pixel_size={
            layer: ancillary_metadata.get(layer, {}).get("source_pixel_size")
            for layer in ancillary_layer_names
        },
        grid=grid.to_dict(),
        validation={
            "imagery_layer_count": len(imagery_band_names),
            "ancillary_layer_count": len(ancillary_layer_names),
        },
    ).to_dict()
