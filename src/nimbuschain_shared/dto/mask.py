from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class MaskExecutionRequest:
    source_zarr_uri: str
    provider: str
    collection: str
    product_type: str | None
    scene_id: str
    acquisition_datetime: str | None
    dataset_summary: dict[str, Any] = field(default_factory=dict)
    mask_types: list[str] = field(default_factory=list)
    backend: str = "auto"
    threshold: float | None = None
    overwrite: bool = True
    inference_device: str | None = None
    include_shadows: bool = True
    water_backend: str = "auto"
    water_overwrite: bool = True
    water_inference_device: str | None = None
    fail_on_error: bool = False
