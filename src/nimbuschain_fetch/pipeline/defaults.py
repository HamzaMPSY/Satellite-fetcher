from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nimbuschain_fetch.pipeline.core import FunctionStage, PipelineContext, StageResult


@dataclass(frozen=True, slots=True)
class PipelineOptions:
    provider: str
    collection: str
    product_type: str | None = None
    mask_types: tuple[str, ...] = field(default_factory=tuple)
    cube_mode: str = "none"

    @property
    def normalized_provider(self) -> str:
        return self.provider.strip().lower()

    @property
    def normalized_cube_mode(self) -> str:
        value = self.cube_mode.strip().lower() or "none"
        if value in {"none", "before_mask", "after_mask"}:
            return value
        raise ValueError("cube_mode must be one of: none, before_mask, after_mask.")


def build_default_pipeline_stages(options: PipelineOptions) -> list[FunctionStage]:
    cube_depends_on = ("mask",) if options.normalized_cube_mode == "after_mask" else ("zarr",)
    mask_depends_on = ("cube",) if options.normalized_cube_mode == "before_mask" else ("zarr",)

    return [
        FunctionStage(
            "fetch",
            _placeholder_stage("fetch"),
            skip_reason="fetch_stage_disabled",
        ),
        FunctionStage(
            "zarr",
            _placeholder_stage("zarr"),
            depends_on=("fetch",),
            skip_reason="zarr_stage_disabled",
        ),
        FunctionStage(
            "mask",
            _placeholder_stage("mask"),
            depends_on=mask_depends_on,
            condition=lambda _context: bool(options.mask_types),
            skip_reason="mask_types_empty",
        ),
        FunctionStage(
            "cube",
            _placeholder_stage("cube"),
            depends_on=cube_depends_on,
            condition=lambda _context: options.normalized_cube_mode != "none",
            skip_reason="cube_mode_none",
        ),
    ]


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
