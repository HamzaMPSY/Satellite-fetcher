from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any

from nimbuschain_fetch.pipeline import PipelineContext, PipelineOrchestrator
from nimbuschain_fetch.pipeline.defaults import PipelineOptions, build_default_pipeline_stages


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
        command.add_argument("--mask-types", default="")
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
    )


def _context_from_args(args: argparse.Namespace, options: PipelineOptions) -> PipelineContext:
    return PipelineContext(
        job_id=str(args.job_id or "").strip() or f"manual-{uuid.uuid4().hex[:12]}",
        provider=options.normalized_provider,
        collection=options.collection,
        product_type=options.product_type,
        payload={
            "provider": options.normalized_provider,
            "collection": options.collection,
            "product_type": options.product_type,
            "mask_types": list(options.mask_types),
            "cube_mode": options.normalized_cube_mode,
        },
    )


def _orchestrator(options: PipelineOptions) -> PipelineOrchestrator:
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
    orchestrator = _orchestrator(options)
    results = orchestrator.run(context, target_stage=str(args.stage), raise_on_failure=False)
    has_failure = any(result.status.value == "failed" for result in results)
    payload = {
        "status": "failed" if has_failure else "completed",
        "job_id": context.job_id,
        "stage": str(args.stage),
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


if __name__ == "__main__":
    sys.exit(main())
