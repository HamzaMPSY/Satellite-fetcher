from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any

from nimbuschain_fetch.pipeline import PipelineContext, PipelineOrchestrator
from nimbuschain_fetch.pipeline.defaults import PipelineOptions, build_default_pipeline_stages
from nimbuschain_fetch.pipeline.runners import (
    PipelineRuntimeConfig,
    build_runtime_pipeline_stages,
)


def _json_default(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _mask_types(raw_value: str | None) -> tuple[str, ...]:
    normalized: list[str] = []
    for item in str(raw_value or "").split(","):
        value = item.strip().lower()
        if not value:
            continue
        if value not in {"water", "cloud"}:
            raise ValueError("--mask-types accepts only water, cloud.")
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or execute modular Nimbus pipeline stages."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--job-id", default="", help="Existing or manual job id.")
        command.add_argument("--provider", required=True, choices=["copernicus", "usgs"])
        command.add_argument("--collection", required=True)
        command.add_argument("--product-type", default=None)
        command.add_argument("--raw-uri", default=None)
        command.add_argument("--raw-uris", default="", help="Comma-separated raw paths.")
        command.add_argument("--source-zarr-uri", default=None)
        command.add_argument(
            "--source-zarr-uris",
            default="",
            help="Comma-separated existing Zarr stores for mask/cube stages.",
        )
        command.add_argument("--mask-types", default="")
        command.add_argument("--zarr-service-url", default=None)
        command.add_argument("--mask-service-url", default=None)
        command.add_argument("--zarr-output-dir", default=None)
        command.add_argument("--zarr-output-uri", default=None)
        command.add_argument("--cube-output-dir", default=None)
        command.add_argument("--cube-output-uri", default=None)
        command.add_argument("--single-cube", action="store_true")
        command.add_argument("--cube-layout", choices=["grouped_time", "daily_mosaic"], default="grouped_time")
        command.add_argument("--cube-target-crs", default=None)
        command.add_argument("--cube-target-resolution-m", type=int, default=10)
        command.add_argument(
            "--cube-overlap-policy",
            choices=["least_cloud", "latest", "earliest", "first_valid"],
            default="least_cloud",
        )
        command.add_argument("--include-masks-in-cube", action="store_true")
        command.add_argument("--skip-ancillary", action="store_true")
        command.add_argument("--cube-start-date", default=None)
        command.add_argument("--cube-end-date", default=None)
        command.add_argument("--sen2like-service-url", default=None)
        command.add_argument("--sen2like-working-dir", default=None)
        command.add_argument("--sen2like-workers", type=int, default=4)
        command.add_argument(
            "--execute",
            action="store_true",
            help="Use real microservice-backed stage runners instead of dry hooks.",
        )
        command.add_argument(
            "--cube-mode",
            default="none",
            choices=["none", "before_mask", "after_mask"],
        )

    plan_parser = subparsers.add_parser("plan", help="Print the stage execution plan.")
    add_common_options(plan_parser)
    plan_parser.add_argument("--target-stage", default=None)

    run_parser = subparsers.add_parser("run-stage", help="Run one stage and its dependencies.")
    add_common_options(run_parser)
    run_parser.add_argument("--stage", required=True)

    return parser


def _options_from_args(args: argparse.Namespace) -> PipelineOptions:
    return PipelineOptions(
        provider=str(args.provider),
        collection=str(args.collection),
        product_type=str(args.product_type).strip() or None if args.product_type else None,
        mask_types=_mask_types(args.mask_types),
        cube_mode=str(args.cube_mode),
        sen2like_service_url=(
            str(args.sen2like_service_url).strip() or None
            if args.sen2like_service_url
            else None
        ),
    )


def _context_from_args(args: argparse.Namespace, options: PipelineOptions) -> PipelineContext:
    raw_uris = _csv_values(args.raw_uris)
    if args.raw_uri:
        raw_uris.insert(0, str(args.raw_uri).strip())
    source_zarr_uris = _csv_values(args.source_zarr_uris)
    if args.source_zarr_uri:
        source_zarr_uris.insert(0, str(args.source_zarr_uri).strip())
    return PipelineContext(
        job_id=str(args.job_id or "").strip() or f"manual-{uuid.uuid4().hex[:12]}",
        provider=options.normalized_provider,
        collection=options.collection,
        product_type=options.product_type,
        payload={
            "provider": options.normalized_provider,
            "collection": options.collection,
            "product_type": options.product_type,
            "raw_uri": raw_uris[0] if raw_uris else None,
            "raw_uris": _unique_strings(raw_uris),
            "source_zarr_uri": source_zarr_uris[0] if source_zarr_uris else None,
            "source_zarr_uris": _unique_strings(source_zarr_uris),
            "mask_types": list(options.mask_types),
            "cube_mode": options.normalized_cube_mode,
            "sen2like_working_dir": (
                str(args.sen2like_working_dir).strip() or None
                if args.sen2like_working_dir
                else None
            ),
            "sen2like_workers": int(args.sen2like_workers),
        },
    )


def _runtime_config_from_args(args: argparse.Namespace) -> PipelineRuntimeConfig:
    return PipelineRuntimeConfig(
        zarr_service_url=_optional_arg(args.zarr_service_url),
        mask_service_url=_optional_arg(args.mask_service_url),
        zarr_output_dir=_optional_arg(args.zarr_output_dir) or "./data/downloads/zarr/manual",
        zarr_output_uri=_optional_arg(args.zarr_output_uri),
        cube_output_dir=_optional_arg(args.cube_output_dir) or "./data/downloads/zarr/cubes/manual",
        cube_output_uri=_optional_arg(args.cube_output_uri),
        cube_group_by_tile=not bool(args.single_cube),
        cube_layout=str(args.cube_layout or "grouped_time").strip(),
        cube_target_crs=_optional_arg(args.cube_target_crs),
        cube_target_resolution_m=int(args.cube_target_resolution_m),
        cube_overlap_policy=str(args.cube_overlap_policy or "least_cloud").strip(),
        include_masks_in_cube=bool(args.include_masks_in_cube),
        include_ancillary=not bool(args.skip_ancillary),
        cube_start_date=_optional_arg(args.cube_start_date),
        cube_end_date=_optional_arg(args.cube_end_date),
    )


def _uses_runtime_stages(args: argparse.Namespace) -> bool:
    return bool(
        args.execute
        or _optional_arg(args.zarr_service_url)
        or _optional_arg(args.mask_service_url)
        or _optional_arg(args.source_zarr_uri)
        or _csv_values(args.source_zarr_uris)
        or _optional_arg(args.zarr_output_uri)
        or _optional_arg(args.cube_output_uri)
    )


def _orchestrator(
    options: PipelineOptions,
    *,
    args: argparse.Namespace | None = None,
    runtime_stages: bool = False,
) -> PipelineOrchestrator:
    if runtime_stages and args is not None:
        return PipelineOrchestrator(
            build_runtime_pipeline_stages(
                requires_sen2like=options.requires_sen2like,
                mask_types=options.mask_types,
                cube_mode=options.normalized_cube_mode,
                sen2like_service_url=options.sen2like_service_url,
                runtime=_runtime_config_from_args(args),
            )
        )
    return PipelineOrchestrator(build_default_pipeline_stages(options))


def run_plan(args: argparse.Namespace) -> int:
    options = _options_from_args(args)
    context = _context_from_args(args, options)
    orchestrator = _orchestrator(options)
    payload = {
        "status": "planned",
        "job_id": context.job_id,
        "provider": options.normalized_provider,
        "collection": options.collection,
        "product_type": options.product_type,
        "mask_types": list(options.mask_types),
        "cube_mode": options.normalized_cube_mode,
        "stages": orchestrator.describe_plan(target_stage=args.target_stage),
    }
    print(json.dumps(payload, default=_json_default))
    return 0


def run_stage(args: argparse.Namespace) -> int:
    options = _options_from_args(args)
    context = _context_from_args(args, options)
    runtime_stages = _uses_runtime_stages(args)
    orchestrator = _orchestrator(options, args=args, runtime_stages=runtime_stages)
    results = orchestrator.run(context, target_stage=str(args.stage), raise_on_failure=False)
    has_failure = any(result.status.value == "failed" for result in results)
    payload = {
        "status": "failed" if has_failure else "completed",
        "job_id": context.job_id,
        "stage": str(args.stage),
        "execution_mode": "runtime" if runtime_stages else "dry",
        "results": [result.to_dict() for result in results],
    }
    stream = sys.stderr if has_failure else sys.stdout
    print(json.dumps(payload, default=_json_default), file=stream)
    return 1 if has_failure else 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            return run_plan(args)
        if args.command == "run-stage":
            return run_stage(args)
        parser.error(f"Unsupported command: {args.command}")
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 1
    return 1


def _csv_values(raw_value: str | None) -> list[str]:
    return [
        value
        for value in (str(item).strip() for item in str(raw_value or "").split(","))
        if value
    ]


def _optional_arg(raw_value: Any) -> str | None:
    value = str(raw_value or "").strip()
    return value or None


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


if __name__ == "__main__":
    sys.exit(main())
