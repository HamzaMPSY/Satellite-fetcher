from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ZarrConversionRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    provider: str
    collection: str
    scene_id: str
    raw_uri: str
    output_uri: str
    product_type: str | None = None
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class GroupedCubeBuildRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    source_zarr_uris: list[str]
    output_dir: str
    include_ancillary: bool = True
    include_masks: bool | None = None
    start_date: str | None = None
    end_date: str | None = None
    stage_label: str | None = None
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class CubeBuildRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    source_zarr_uris: list[str]
    output_uri: str
    include_ancillary: bool = True
    include_masks: bool = False
    progress_callback: Callable[[dict[str, Any]], None] | None = None
