from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


WATER_MASK_PATH = "masks/water"
WATER_MASK_NAME = "water"
WATER_MASK_CLASSES = {
    "0": "non-water",
    "1": "water",
}
WATER_MASK_DIMS = ("time", "y", "x")


@dataclass(frozen=True)
class BinaryMaskSpec:
    name: str
    path: str
    dimensions: tuple[str, ...]
    dtype: str
    values: dict[str, str]
    notes: tuple[str, ...]


def default_mask_model() -> dict[str, Any]:
    spec = BinaryMaskSpec(
        name=WATER_MASK_NAME,
        path=WATER_MASK_PATH,
        dimensions=WATER_MASK_DIMS,
        dtype="uint8",
        values=WATER_MASK_CLASSES,
        notes=(
            "Binary masks are written into a derived masked Zarr copy, not into the source Zarr store.",
            "Water-mask pixels use 0 for non-water and 1 for water.",
            "Masking packages are internal runtime components and are not a second public orchestration API.",
        ),
    )
    payload = asdict(spec)
    payload["integration_policy"] = {
        "public_api": "backend_only",
        "pipeline_stage": "post_zarr_conversion",
        "storage_policy": "copy source zarr to a derived masked zarr, then write under masks/",
    }
    payload["input_policy"] = {
        "sentinel-2": ["B04", "B03", "B02", "B08"],
        "landsat-l1": ["B4", "B3", "B2", "B5"],
        "landsat-l2": ["SR_B4", "SR_B3", "SR_B2", "SR_B5"],
    }
    return payload
