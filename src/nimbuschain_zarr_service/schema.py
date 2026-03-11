from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


ZARR_FORMAT_VERSION = 1
CORE_BANDS = ("blue", "green", "red", "nir")
OPTICAL_EXTENDED_BANDS = (
    "coastal",
    "pan",
    "rededge1",
    "rededge2",
    "rededge3",
    "nir_narrow",
    "water_vapor",
    "cirrus",
    "swir1",
    "swir2",
    "thermal1",
    "thermal2",
    "scene_classification",
)
CANONICAL_BANDS = (
    "coastal",
    "blue",
    "green",
    "red",
    "pan",
    "rededge1",
    "rededge2",
    "rededge3",
    "nir",
    "nir_narrow",
    "water_vapor",
    "cirrus",
    "swir1",
    "swir2",
    "thermal1",
    "thermal2",
    "scene_classification",
)
DIMENSIONS = ("time", "band", "y", "x")
REQUIRED_METADATA_FIELDS = (
    "provider",
    "collection",
    "scene_id",
    "acquisition_datetime",
    "crs",
    "transform",
    "band_names",
    "zarr_format_version",
)


@dataclass(frozen=True)
class ChunkShape:
    time: int = 1
    band: int = 1
    y: int = 1024
    x: int = 1024


@dataclass(frozen=True)
class CompressionSpec:
    codec: str = "blosc"
    cname: str = "zstd"
    clevel: int = 5
    shuffle: str = "bitshuffle"


@dataclass(frozen=True)
class ZarrDataModelSpec:
    format_version: int
    layout: dict[str, str]
    dimensions: tuple[str, ...]
    canonical_bands: tuple[str, ...]
    required_metadata_fields: tuple[str, ...]
    default_chunks: ChunkShape
    default_compression: CompressionSpec
    single_scene_rule: str
    notes: tuple[str, ...]


def default_zarr_model() -> dict[str, Any]:
    spec = ZarrDataModelSpec(
        format_version=ZARR_FORMAT_VERSION,
        layout={
            "imagery": "imagery",
            "metadata": "metadata",
            "masks_root": "masks",
            "cloud_mask": "masks/cloud",
            "water_mask": "masks/water",
        },
        dimensions=DIMENSIONS,
        canonical_bands=CANONICAL_BANDS,
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        default_chunks=ChunkShape(),
        default_compression=CompressionSpec(),
        single_scene_rule="A single-scene conversion writes time=1.",
        notes=(
            "Both Copernicus and Landsat inputs must be normalized to canonical bands before writing.",
            "Mask services read and write within the same Zarr store.",
            "Raw products and Zarr outputs may be stored on the local filesystem or on OCI object storage.",
            "This schema is the baseline for v1 and may evolve under explicit format_version changes.",
        ),
    )
    payload = asdict(spec)
    payload["required_conversion_bands"] = list(CORE_BANDS)
    payload["optional_conversion_bands"] = list(OPTICAL_EXTENDED_BANDS)
    payload["required_mask_bands"] = {
        "omni_cloud_mask": ["red", "green", "nir"],
        "omni_water_mask": ["red", "green", "blue", "nir"],
    }
    payload["sensor_band_sets"] = {
        "sentinel-2": [
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
        ],
        "landsat-8-9": [
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
        ],
        "sentinel-1": ["vv", "vh", "hh", "hv"],
    }
    payload["resolution_policy"] = {
        "optical": (
            "Use a collection-specific target grid that preserves the intended multispectral geometry. "
            "Sentinel-2 is normalized to 10 m. Landsat is normalized to 30 m even when Level-1 includes "
            "the 15 m panchromatic band B8. Coarser bands are reprojected to that collection-specific target grid."
        ),
        "sentinel-2": {
            "reference_band": "red",
            "target_pixel_size_meters": 10,
        },
        "landsat-8-9": {
            "reference_band": "red",
            "target_pixel_size_meters": 30,
        },
        "sentinel-1": {
            "reference_band": "first_available_polarization",
            "target_pixel_size_meters": None,
        },
    }
    payload["sentinel1_runtime_notes"] = {
        "standard_products": "GRD, SLC, and IW_SLC__1S are handled as raster Sentinel-1 products.",
        "raw_products": (
            "RAW products are decoded in sample space through an optional Sentinel-1 Level-0 decoder. "
            "Their x/y axes are acquisition sample coordinates, not georeferenced map coordinates."
        ),
        "snap": (
            "SNAP support is optional at runtime and is exposed via the service health/schema endpoints. "
            "It is intended for Sentinel-1 preprocessing flows but is not required for the Python RAW decoder path."
        ),
    }
    payload["data_families"] = {
        "optical": {
            "dimensions": ["time", "band", "y", "x"],
            "notes": (
                "Sentinel-2 and Landsat are normalized to a sensor-aware optical band model "
                "that preserves all useful spectral bands instead of collapsing to RGB/NIR/SWIR only."
            ),
        },
        "sar": {
            "dimensions": ["time", "band", "y", "x"],
            "notes": "Sentinel-1 polarizations are stored on the band axis (vv, vh, hh, hv).",
        },
        "swath": {
            "dimensions": ["time", "band", "y", "x"],
            "notes": "Sentinel-3 / Sentinel-5P NetCDF variables are flattened into the band axis.",
        },
    }
    return payload
