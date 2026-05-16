from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nimbuschain_fetch.pipeline.core import PipelineContext, StageResult
from nimbuschain_fetch.pipeline.sen2like import Sen2LikeStage, is_landsat_context
from nimbuschain_shared.clients.mask import MaskServiceClient as _MaskServiceClient
from nimbuschain_shared.clients.zarr import ZarrServiceClient as _ZarrServiceClient


ZarrServiceClient: Any = _ZarrServiceClient
MaskServiceClient: Any = _MaskServiceClient


@dataclass(frozen=True, slots=True)
class PipelineRuntimeConfig:
    zarr_service_url: str | None = None
    mask_service_url: str | None = None
    zarr_output_dir: str = "./data/downloads/zarr/manual"
    zarr_output_uri: str | None = None
    cube_output_dir: str = "./data/downloads/zarr/cubes/manual"
    cube_output_uri: str | None = None
    cube_group_by_tile: bool = True
    cube_layout: str = "grouped_time"
    cube_target_crs: str | None = None
    cube_target_resolution_m: int = 10
    cube_overlap_policy: str = "least_cloud"
    include_masks_in_cube: bool = False
    include_ancillary: bool = True
    cube_start_date: str | None = None
    cube_end_date: str | None = None

    def resolved_zarr_service_url(self) -> str | None:
        return _first_non_empty(self.zarr_service_url, os.getenv("NIMBUS_ZARR_SERVICE_URL"))

    def resolved_mask_service_url(self) -> str | None:
        return _first_non_empty(self.mask_service_url, os.getenv("NIMBUS_MASK_SERVICE_URL"))


@dataclass(frozen=True, slots=True)
class ManualFetchStage:
    name: str = "fetch"
    depends_on: tuple[str, ...] = ()
    skip_reason: str = "raw_input_not_required"

    def should_run(self, context: PipelineContext) -> bool:
        return True

    def run(self, context: PipelineContext) -> StageResult:
        raw_outputs = _raw_inputs(context)
        context.set("raw_outputs", raw_outputs)
        if raw_outputs:
            return StageResult.succeeded_result(
                self.name,
                outputs=raw_outputs,
                metadata={
                    "runner": "manual_inputs",
                    "raw_output_count": len(raw_outputs),
                },
            )
        return StageResult.succeeded_result(
            self.name,
            outputs=[],
            metadata={
                "runner": "manual_inputs",
                "reason": "raw_input_not_required",
            },
        )


@dataclass(frozen=True, slots=True)
class ZarrStage:
    depends_on: tuple[str, ...]
    runtime: PipelineRuntimeConfig
    name: str = "zarr"

    def should_run(self, context: PipelineContext) -> bool:
        return True

    def run(self, context: PipelineContext) -> StageResult:
        existing_zarrs = _source_zarr_inputs(context)
        raw_outputs = _zarr_raw_inputs(context)
        if existing_zarrs and not raw_outputs:
            context.set("zarr_outputs", existing_zarrs)
            context.set("final_scene_uris", existing_zarrs)
            return StageResult.succeeded_result(
                self.name,
                outputs=existing_zarrs,
                metadata={
                    "runner": "existing_zarr_inputs",
                    "reason": "zarr_already_available",
                    "source_zarr_count": len(existing_zarrs),
                },
            )
        if not raw_outputs:
            return StageResult.skipped_result(
                self.name,
                reason="zarr_input_missing",
                metadata={"runner": "zarr_service"},
            )

        service_url = self.runtime.resolved_zarr_service_url()
        if not service_url:
            return StageResult.failed_result(
                self.name,
                error="NIMBUS_ZARR_SERVICE_URL or --zarr-service-url is required for real Zarr execution.",
                metadata={"runner": "zarr_service"},
            )

        client = ZarrServiceClient(service_url=service_url)
        converted_items: list[dict[str, Any]] = []
        outputs: list[str] = []
        try:
            for index, raw_uri in enumerate(raw_outputs, start=1):
                scene_id = _scene_id_from_uri(raw_uri)
                output_uri = self._output_uri(scene_id=scene_id, index=index, total=len(raw_outputs))
                provider, collection, product_type = _conversion_identity(context)
                written_uri, data_family, summary, dataset_summary = client.convert(
                    job_id=context.job_id or scene_id,
                    pipeline_id=context.job_id or scene_id,
                    trace_id=uuid.uuid4().hex,
                    provider=provider,
                    collection=collection,
                    product_type=product_type,
                    scene_id=scene_id,
                    raw_uri=raw_uri,
                    output_uri=output_uri,
                )
                item = {
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "zarr_uri": written_uri,
                    "data_family": data_family,
                    "summary": dict(summary or {}),
                    "dataset_summary": dict(dataset_summary or {}),
                }
                converted_items.append(item)
                outputs.append(str(written_uri))
        finally:
            client.close()

        context.set("zarr_outputs", outputs)
        context.set("final_scene_uris", outputs)
        context.set("zarr_items", converted_items)
        return StageResult.succeeded_result(
            self.name,
            outputs=outputs,
            metadata={
                "runner": "zarr_service",
                "service_url": service_url,
                "items": converted_items,
                "conversion_provider": _conversion_identity(context)[0],
            },
        )

    def _output_uri(self, *, scene_id: str, index: int, total: int) -> str:
        if total == 1 and self.runtime.zarr_output_uri:
            return str(self.runtime.zarr_output_uri).strip()
        output_dir = Path(str(self.runtime.zarr_output_dir or "./data/downloads/zarr/manual")).expanduser()
        return str((output_dir / f"{scene_id}.zarr").resolve())


@dataclass(frozen=True, slots=True)
class MaskStage:
    depends_on: tuple[str, ...]
    runtime: PipelineRuntimeConfig
    mask_types: tuple[str, ...] = field(default_factory=tuple)
    name: str = "mask"

    def should_run(self, context: PipelineContext) -> bool:
        return bool(self.mask_types)

    def run(self, context: PipelineContext) -> StageResult:
        source_zarrs = _zarr_outputs(context)
        if not source_zarrs:
            return StageResult.skipped_result(
                self.name,
                reason="mask_input_missing",
                metadata={"runner": "mask_service", "mask_types": list(self.mask_types)},
            )
        service_url = self.runtime.resolved_mask_service_url()
        if not service_url:
            return StageResult.failed_result(
                self.name,
                error="NIMBUS_MASK_SERVICE_URL or --mask-service-url is required when mask types are selected.",
                metadata={"runner": "mask_service", "mask_types": list(self.mask_types)},
            )

        client = MaskServiceClient(service_url=service_url)
        items: list[dict[str, Any]] = []
        final_scene_uris: list[str] = []
        outputs: list[str] = []
        try:
            for zarr_uri in source_zarrs:
                scene_id = _scene_id_from_uri(zarr_uri)
                dataset_summary = _dataset_summary_for_zarr(context, zarr_uri)
                result = client.apply_masks_to_zarr(
                    job_id=context.job_id or None,
                    zarr_uri=zarr_uri,
                    provider=context.provider,
                    collection=context.collection,
                    product_type=context.product_type,
                    scene_id=scene_id,
                    acquisition_datetime=dataset_summary.get("acquisition_datetime"),
                    dataset_summary=dataset_summary,
                    mask_types=list(self.mask_types),
                )
                masked_uri = str(result.get("masked_zarr_uri") or zarr_uri).strip()
                if masked_uri:
                    final_scene_uris.append(masked_uri)
                item_outputs = _unique_strings(
                    [
                        masked_uri,
                        *list(result.get("masked_zarr_outputs") or []),
                        *list(result.get("watermask_outputs") or []),
                        *list(result.get("cloudmask_outputs") or []),
                    ]
                )
                outputs.extend(item_outputs)
                items.append(
                    {
                        "source_zarr_uri": zarr_uri,
                        "masked_zarr_uri": masked_uri or zarr_uri,
                        "result": dict(result),
                    }
                )
        finally:
            client.close()

        final_scene_uris = _unique_strings(final_scene_uris) or source_zarrs
        outputs = _unique_strings(outputs)
        context.set("mask_outputs", outputs)
        context.set("final_scene_uris", final_scene_uris)
        return StageResult.succeeded_result(
            self.name,
            outputs=outputs,
            metadata={
                "runner": "mask_service",
                "service_url": service_url,
                "mask_types": list(self.mask_types),
                "items": items,
            },
        )


@dataclass(frozen=True, slots=True)
class CubeStage:
    depends_on: tuple[str, ...]
    runtime: PipelineRuntimeConfig
    cube_mode: str
    name: str = "cube"

    def should_run(self, context: PipelineContext) -> bool:
        return self.cube_mode != "none"

    def run(self, context: PipelineContext) -> StageResult:
        source_zarrs = (
            _zarr_outputs(context)
            if self.cube_mode == "before_mask"
            else _final_scene_uris(context)
        )
        if not source_zarrs:
            return StageResult.skipped_result(
                self.name,
                reason="cube_input_missing",
                metadata={"runner": "zarr_service", "cube_mode": self.cube_mode},
            )
        service_url = self.runtime.resolved_zarr_service_url()
        if not service_url:
            return StageResult.failed_result(
                self.name,
                error="NIMBUS_ZARR_SERVICE_URL or --zarr-service-url is required for cube execution.",
                metadata={"runner": "zarr_service", "cube_mode": self.cube_mode},
            )

        client = ZarrServiceClient(service_url=service_url)
        try:
            if self.runtime.cube_group_by_tile:
                summary = client.build_grouped_cubes(
                    job_id=context.job_id or "manual-grouped-cube-build",
                    pipeline_id=context.job_id or "manual-grouped-cube-build",
                    trace_id=uuid.uuid4().hex,
                    source_zarr_uris=source_zarrs,
                    output_dir=str(self.runtime.cube_output_dir),
                    include_ancillary=bool(self.runtime.include_ancillary),
                    include_masks=bool(self.runtime.include_masks_in_cube),
                    start_date=self.runtime.cube_start_date,
                    end_date=self.runtime.cube_end_date,
                    stage_label=self.cube_mode,
                    cube_layout=str(self.runtime.cube_layout or "grouped_time"),
                    target_crs=self.runtime.cube_target_crs,
                    target_resolution_m=int(self.runtime.cube_target_resolution_m or 10),
                    overlap_policy=str(self.runtime.cube_overlap_policy or "least_cloud"),
                )
            else:
                if not self.runtime.cube_output_uri:
                    return StageResult.failed_result(
                        self.name,
                        error="--cube-output-uri is required when --single-cube is used.",
                        metadata={"runner": "zarr_service", "cube_mode": self.cube_mode},
                    )
                summary = client.build_cube(
                    job_id=context.job_id or "manual-cube-build",
                    pipeline_id=context.job_id or "manual-cube-build",
                    trace_id=uuid.uuid4().hex,
                    source_zarr_uris=source_zarrs,
                    output_uri=str(self.runtime.cube_output_uri).strip(),
                    include_ancillary=bool(self.runtime.include_ancillary),
                    include_masks=bool(self.runtime.include_masks_in_cube),
                )
        finally:
            client.close()

        outputs = _unique_strings(
            [
                *list(summary.get("cube_outputs") or []),
                str(summary.get("cube_uri") or "").strip(),
                str(summary.get("output_uri") or "").strip(),
            ]
        )
        context.set("cube_outputs", outputs)
        return StageResult.succeeded_result(
            self.name,
            outputs=outputs,
            metadata={
                "runner": "zarr_service",
                "service_url": service_url,
                "cube_mode": self.cube_mode,
                "summary": dict(summary),
            },
        )


def build_runtime_pipeline_stages(
    *,
    requires_sen2like: bool,
    mask_types: tuple[str, ...],
    cube_mode: str,
    sen2like_service_url: str | None,
    allow_sen2like_raw_fallback: bool = False,
    runtime: PipelineRuntimeConfig,
) -> list[Any]:
    has_mask = bool(mask_types)
    normalized_cube_mode = str(cube_mode or "none").strip().lower() or "none"
    zarr_depends_on = ("sen2like",) if requires_sen2like else ("fetch",)
    cube_depends_on = (
        ("mask",)
        if has_mask and normalized_cube_mode == "after_mask"
        else ("zarr",)
    )
    mask_depends_on = ("cube",) if normalized_cube_mode == "before_mask" else ("zarr",)

    stages: list[Any] = [ManualFetchStage()]
    if requires_sen2like:
        stages.append(
            Sen2LikeStage(
                service_url=sen2like_service_url,
                allow_raw_fallback=allow_sen2like_raw_fallback,
            )
        )
    stages.append(ZarrStage(depends_on=zarr_depends_on, runtime=runtime))
    if has_mask:
        stages.append(MaskStage(depends_on=mask_depends_on, runtime=runtime, mask_types=mask_types))
    if normalized_cube_mode != "none":
        stages.append(
            CubeStage(
                depends_on=cube_depends_on,
                runtime=runtime,
                cube_mode=normalized_cube_mode,
            )
        )
    return stages


def _raw_inputs(context: PipelineContext) -> list[str]:
    values: list[str] = []
    for key in ("raw_uri", "landsat_path", "source_uri", "product_path"):
        raw_value = context.payload.get(key) or context.get(key)
        if raw_value:
            values.append(str(raw_value))
    for key in ("raw_uris", "raw_outputs", "sources"):
        raw_values = context.payload.get(key) or context.get(key)
        if isinstance(raw_values, (list, tuple, set)):
            values.extend(str(item) for item in raw_values)
    return _unique_strings(values)


def _source_zarr_inputs(context: PipelineContext) -> list[str]:
    values: list[str] = []
    for key in ("source_zarr_uri", "zarr_uri"):
        raw_value = context.payload.get(key) or context.get(key)
        if raw_value:
            values.append(str(raw_value))
    for key in ("source_zarr_uris", "zarr_outputs"):
        raw_values = context.payload.get(key) or context.get(key)
        if isinstance(raw_values, (list, tuple, set)):
            values.extend(str(item) for item in raw_values)
    return _unique_strings(values)


def _zarr_raw_inputs(context: PipelineContext) -> list[str]:
    for key in ("zarr_inputs", "sen2like_outputs", "raw_outputs"):
        values = context.get(key)
        if isinstance(values, (list, tuple, set)) and values:
            return _unique_strings([str(item) for item in values])
    sen2like_result = context.results.get("sen2like")
    if sen2like_result is not None and sen2like_result.outputs:
        return _unique_strings(sen2like_result.outputs)
    return _raw_inputs(context)


def _zarr_outputs(context: PipelineContext) -> list[str]:
    values = context.get("zarr_outputs")
    if isinstance(values, (list, tuple, set)) and values:
        return _unique_strings([str(item) for item in values])
    zarr_result = context.results.get("zarr")
    if zarr_result is not None and zarr_result.outputs:
        return _unique_strings(zarr_result.outputs)
    return _source_zarr_inputs(context)


def _final_scene_uris(context: PipelineContext) -> list[str]:
    values = context.get("final_scene_uris")
    if isinstance(values, (list, tuple, set)) and values:
        return _unique_strings([str(item) for item in values])
    return _zarr_outputs(context)


def _dataset_summary_for_zarr(context: PipelineContext, zarr_uri: str) -> dict[str, Any]:
    dataset_summary = context.payload.get("dataset_summary")
    if isinstance(dataset_summary, dict):
        return dict(dataset_summary)
    for item in list(context.get("zarr_items", []) or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("zarr_uri") or "") == zarr_uri:
            return dict(item.get("dataset_summary") or {})
    return {}


def _conversion_identity(context: PipelineContext) -> tuple[str, str, str | None]:
    sen2like_result = context.results.get("sen2like")
    sen2like_fallback_to_raw = bool(
        sen2like_result is not None
        and sen2like_result.metadata.get("fallback_to_raw")
    )
    sen2like_succeeded = bool(
        sen2like_result is not None
        and sen2like_result.outputs
        and is_landsat_context(context)
        and not sen2like_fallback_to_raw
    )
    if sen2like_succeeded:
        return "copernicus", "SENTINEL-2", "S2MSI2A"
    return context.provider, context.collection, context.product_type


def _scene_id_from_uri(raw_uri: str) -> str:
    name = Path(str(raw_uri).rstrip("/")).name
    for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff", ".zarr"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem or "scene"


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def _unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
