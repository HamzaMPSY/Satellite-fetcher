from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nimbuschain_fetch.pipeline.core import (
    FunctionStage,
    PipelineContext,
    PipelineStage,
    StageResult,
)
from nimbuschain_fetch.pipeline.sen2like import Sen2LikeStage, is_landsat_selection


@dataclass(frozen=True, slots=True)
class PipelineOptions:
    provider: str
    collection: str
    product_type: str | None = None
    mask_types: tuple[str, ...] = field(default_factory=tuple)
    cube_mode: str = "none"
    sen2like_service_url: str | None = None
    allow_sen2like_raw_fallback: bool = False

    @property
    def normalized_provider(self) -> str:
        return self.provider.strip().lower()

    @property
    def normalized_cube_mode(self) -> str:
        value = self.cube_mode.strip().lower() or "none"
        if value in {"none", "before_mask", "after_mask"}:
            return value
        raise ValueError("cube_mode must be one of: none, before_mask, after_mask.")

    @property
    def requires_sen2like(self) -> bool:
        return is_landsat_selection(
            provider=self.normalized_provider,
            collection=self.collection,
            product_type=self.product_type,
        )


def build_default_pipeline_stages(options: PipelineOptions) -> list[PipelineStage]:
    has_mask = bool(options.mask_types)
    zarr_depends_on = ("sen2like",) if options.requires_sen2like else ("fetch",)
    cube_depends_on = (
        ("mask",)
        if has_mask and options.normalized_cube_mode == "after_mask"
        else ("zarr",)
    )
    mask_depends_on = ("cube",) if options.normalized_cube_mode == "before_mask" else ("zarr",)

    stages: list[PipelineStage] = [
        FunctionStage(
            "fetch",
            _placeholder_stage("fetch"),
            skip_reason="fetch_stage_disabled",
        ),
    ]
    if options.requires_sen2like:
        stages.append(
            Sen2LikeStage(
                service_url=options.sen2like_service_url,
                allow_raw_fallback=options.allow_sen2like_raw_fallback,
            )
        )
    stages.append(
        FunctionStage(
            "zarr",
            _placeholder_stage("zarr"),
            depends_on=zarr_depends_on,
            skip_reason="zarr_stage_disabled",
        )
    )
    if has_mask:
        stages.append(
            FunctionStage(
                "mask",
                _placeholder_stage("mask"),
                depends_on=mask_depends_on,
                skip_reason="mask_stage_disabled",
            )
        )
    if options.normalized_cube_mode != "none":
        stages.append(
            FunctionStage(
                "cube",
                _placeholder_stage("cube"),
                depends_on=cube_depends_on,
                skip_reason="cube_stage_disabled",
            )
        )
    return stages


def _placeholder_stage(stage_name: str):
    def _run(context: PipelineContext) -> StageResult:
        metadata: dict[str, Any] = {
            "placeholder": True,
            "provider": context.provider,
            "collection": context.collection,
            "product_type": context.product_type,
            "message": (
                f"{stage_name} stage hook executed. "
                "Production job execution still uses NimbusFetcher."
            ),
        }
        return StageResult.succeeded_result(
            stage_name,
            outputs=[f"stage://{stage_name}/{context.job_id or 'manual'}"],
            metadata=metadata,
        )

    return _run
