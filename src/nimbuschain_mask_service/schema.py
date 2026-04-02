from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

MASK_ROOT = "masks"
WATER_MASK_NAME = "water"
WATER_MASK_PATH = f"{MASK_ROOT}/water"
WATER_PROBABILITY_PATH = f"{MASK_ROOT}/water_probability"
CLOUD_MASK_NAME = "cloud"
CLOUD_MASK_PATH = f"{MASK_ROOT}/cloud"
CLOUD_PROBABILITY_PATH = f"{MASK_ROOT}/cloud_probability"
WATER_MASK_CLASSES = {"0": "non-water", "1": "water"}
CLOUD_MASK_CLASSES = {"0": "clear", "1": "cloud_or_shadow"}
MASK_DIMS = ("time", "y", "x")


@dataclass(frozen=True)
class BinaryMaskSpec:
    name: str
    path: str
    dimensions: tuple[str, ...]
    dtype: str
    values: dict[str, str]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class ProbabilityMaskSpec:
    name: str
    path: str
    dimensions: tuple[str, ...]
    dtype: str
    notes: tuple[str, ...]


def _integration_policy() -> dict[str, Any]:
    return {
        "public_api": "backend_only",
        "pipeline_stage": "manual_post_zarr",
        "storage_policy": "write masks directly into the selected existing zarr under masks/",
        "mask_contract_version": "v2",
    }


def default_mask_model() -> dict[str, Any]:
    from nimbuschain_mask_service.registry import registry_status

    water = BinaryMaskSpec(
        name=WATER_MASK_NAME,
        path=WATER_MASK_PATH,
        dimensions=MASK_DIMS,
        dtype="uint8",
        values=WATER_MASK_CLASSES,
        notes=(
            "Water masks are written directly into the selected existing Zarr store.",
            "Water-mask pixels use 0 for non-water and 1 for water.",
        ),
    )
    cloud = BinaryMaskSpec(
        name=CLOUD_MASK_NAME,
        path=CLOUD_MASK_PATH,
        dimensions=MASK_DIMS,
        dtype="uint8",
        values=CLOUD_MASK_CLASSES,
        notes=(
            "Cloud masks are written directly into the selected existing Zarr store.",
            "Cloud-mask pixels use 0 for clear and 1 for cloud or cloud shadow obstruction.",
        ),
    )
    cloud_probability = ProbabilityMaskSpec(
        name="cloud_probability",
        path=CLOUD_PROBABILITY_PATH,
        dimensions=MASK_DIMS,
        dtype="float32",
        notes=("Cloud probability values are stored in [0, 1].",),
    )
    water_probability = ProbabilityMaskSpec(
        name="water_probability",
        path=WATER_PROBABILITY_PATH,
        dimensions=MASK_DIMS,
        dtype="float32",
        notes=("Water probability values are stored in [0, 1] as a debug/quality layer.",),
    )
    return {
        "status": "ok",
        "integration_policy": _integration_policy(),
        "backends": registry_status(),
        "water": asdict(water),
        "cloud": asdict(cloud),
        "cloud_probability": asdict(cloud_probability),
        "water_probability": asdict(water_probability),
        "input_policy": {
            "sentinel-2": {
                "water": ["B04", "B03", "B02", "B08"],
                "cloud": ["B02", "B03", "B04", "B08", "B11", "B12"],
            },
            "landsat-l1": {
                "water": ["B4", "B3", "B2", "B5"],
                "cloud": ["B2", "B3", "B4", "B5", "B6", "B7"],
            },
            "landsat-l2": {
                "water": ["SR_B4", "SR_B3", "SR_B2", "SR_B5"],
                "cloud": ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"],
            },
        },
    }
