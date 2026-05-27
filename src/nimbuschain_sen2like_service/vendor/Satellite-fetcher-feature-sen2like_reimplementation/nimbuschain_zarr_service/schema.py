from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


ZARR_FORMAT_VERSION = 1
DIMENSIONS = ("time", "band", "y", "x")
ANCILLARY_DIMENSIONS = ("time", "ancillary_layer", "y", "x")
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
    ancillary_dimensions: tuple[str, ...]
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
            "ancillary": "ancillary",
            "metadata": "metadata",
            "masks_root": "masks",
            "cloud_mask": "masks/cloud",
            "water_mask": "masks/water",
        },
        dimensions=DIMENSIONS,
        ancillary_dimensions=ANCILLARY_DIMENSIONS,
        required_metadata_fields=REQUIRED_METADATA_FIELDS,
        default_chunks=ChunkShape(),
        default_compression=CompressionSpec(),
        single_scene_rule="A single-scene conversion writes time=1.",
        notes=(
            "Physical imagery layers are preserved with their exact source layer names on the band axis.",
            "QA, mask, angle, aerosol, water vapour, cloud, snow, and other support rasters are written to the ancillary array.",
            "Mask services read and write within the same Zarr store.",
            "Raw products and Zarr outputs may be stored on the local filesystem or on OCI object storage.",
            "This schema is the baseline for v1 and may evolve under explicit format_version changes.",
        ),
    )
    payload = asdict(spec)
    payload["layer_policy"] = {
        "imagery": "Preserve every native physical imagery raster layer from the source product using exact source layer names.",
        "ancillary": "Preserve every native QA, mask, classification, angle, aerosol, and other support raster layer in a separate ancillary array.",
    }
    payload["required_mask_bands"] = {
        "omni_cloud_mask": [
            "sensor-specific imagery layers required by the model input configuration"
        ],
        "omni_water_mask": [
            "sensor-specific imagery layers required by the model input configuration"
        ],
    }
    payload["imagery_layer_expectations"] = {
        "sentinel-2-l1c": [
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
        ],
        "sentinel-2-l2a": [
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
            "B11",
            "B12",
        ],
        "landsat-8-9-l1": [
            "B1",
            "B2",
            "B3",
            "B4",
            "B5",
            "B6",
            "B7",
            "B8",
            "B9",
            "B10",
            "B11",
        ],
        "landsat-8-9-l2": [
            "SR_B1",
            "SR_B2",
            "SR_B3",
            "SR_B4",
            "SR_B5",
            "SR_B6",
            "SR_B7",
            "ST_B10",
        ],
        "sentinel-1": ["VV", "VH", "HH", "HV"],
    }
    payload["resolution_policy"] = {
        "optical": (
            "Use a collection-specific target grid that preserves the intended multispectral geometry. "
            "Sentinel-2 is normalized to 10 m. Landsat 8/9 Collection 2 products are normalized to 10 m. "
            "Coarser bands are reprojected to that collection-specific target grid."
        ),
        "sentinel-2": {
            "reference_band": "B04",
            "target_pixel_size_meters": 10,
        },
        "landsat-8-9": {
            "reference_band": "B4 or SR_B4",
            "target_pixel_size_meters": 10,
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
                "Sentinel-2 and Landsat preserve all native imagery layers using exact source layer names. "
                "Ancillary layers are stored in a separate array."
            ),
        },
        "sar": {
            "dimensions": ["time", "band", "y", "x"],
            "notes": (
                "Sentinel-1 polarizations are stored on the band axis using exact source polarization names "
                "(VV, VH, HH, HV). Raster ancillary layers are stored separately when present."
            ),
        },
        "swath": {
            "dimensions": ["time", "band", "y", "x"],
            "notes": "Sentinel-3 / Sentinel-5P NetCDF variables are flattened into the band axis.",
        },
    }
    return payload
