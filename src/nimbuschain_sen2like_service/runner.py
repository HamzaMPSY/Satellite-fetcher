from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from nimbuschain_sen2like_service.models import (
    Sen2LikeNormalizeRequest,
    Sen2LikeNormalizeResponse,
    Sen2LikeProductOutput,
)


DEFAULT_VENDOR_SUBDIR = "sen2like-service/vendor/Satellite-fetcher-feature-sen2like_reimplementation"
DEFAULT_SPARK_LOCAL_DIRS = "/tmp/nimbus-sen2like-spark"


def resolve_vendor_root() -> Path:
    configured = str(os.getenv("NIMBUS_SEN2LIKE_VENDOR_DIR") or "").strip()
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[2] / DEFAULT_VENDOR_SUBDIR


def resolve_working_dir(requested: str | None = None) -> Path:
    configured = requested or os.getenv("NIMBUS_SEN2LIKE_WORK_DIR") or "/data/downloads/sen2like"
    return Path(configured).resolve()


def pipeline_path(vendor_root: Path | None = None) -> Path:
    return (vendor_root or resolve_vendor_root()) / "Pipeline.py"


def build_command(
    request: Sen2LikeNormalizeRequest,
    *,
    vendor_root: Path | None = None,
    working_dir: Path | None = None,
) -> list[str]:
    root = vendor_root or resolve_vendor_root()
    work_dir = working_dir or resolve_working_dir(request.working_dir)
    products = request.product_inputs()
    if not products:
        raise ValueError("At least one Landsat product path is required.")

    command = [
        sys.executable,
        str(pipeline_path(root)),
        *products,
        "--working-dir",
        str(work_dir),
        "--workers",
        str(request.workers),
        "--base-dir",
        str(root),
        "--cleanup-mode",
        request.cleanup_mode,
    ]
    if request.steps:
        command.extend(["--steps", *request.steps])
    if request.s2_path:
        command.extend(["--s2-path", request.s2_path])
    if request.no_resume:
        command.append("--no-resume")
    if request.no_routing:
        command.append("--no-routing")
    if request.router_fallback_ok:
        command.append("--router-fallback-ok")
    if request.exclude_water:
        command.append("--exclude-water")
    if request.cleanup_dry_run:
        command.append("--cleanup-dry-run")
    command.extend(request.extra_args)
    return command


def run_sen2like(request: Sen2LikeNormalizeRequest) -> Sen2LikeNormalizeResponse:
    vendor_root = resolve_vendor_root()
    work_dir = resolve_working_dir(request.working_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    command = build_command(request, vendor_root=vendor_root, working_dir=work_dir)
    env = os.environ.copy()
    env["LANDSAT_UPSAMPLING_BASE"] = str(vendor_root)
    env["PYTHONPATH"] = _prepend_path(str(vendor_root), env.get("PYTHONPATH"))
    spark_local_dirs = _prepare_spark_environment(env)
    if request.spark_master:
        env["SPARK_MASTER"] = request.spark_master

    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=str(vendor_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=request.timeout_seconds,
        check=False,
    )
    duration = time.perf_counter() - started
    outputs = [_product_output(product, work_dir) for product in request.product_inputs()]
    status = "succeeded" if completed.returncode == 0 else "failed"
    return Sen2LikeNormalizeResponse(
        status=status,
        job_id=request.job_id,
        pipeline_id=request.pipeline_id,
        trace_id=request.trace_id,
        products=request.product_inputs(),
        working_dir=str(work_dir),
        outputs=outputs,
        duration_seconds=duration,
        return_code=int(completed.returncode),
        command=command,
        stdout_tail=_tail(completed.stdout),
        stderr_tail=_tail(completed.stderr),
        metadata={
            "vendor_root": str(vendor_root),
            "pipeline_py": str(pipeline_path(vendor_root)),
            "spark_master": request.spark_master,
            "spark_local_dirs": spark_local_dirs,
            "pyspark_service": True,
        },
    )


def readiness_payload() -> dict[str, Any]:
    vendor_root = resolve_vendor_root()
    script = pipeline_path(vendor_root)
    return {
        "status": "ok" if script.exists() else "unavailable",
        "service": "nimbus-sen2like",
        "vendor_root": str(vendor_root),
        "pipeline_py": str(script),
        "pipeline_py_exists": script.exists(),
    }


def _product_output(product: str, working_dir: Path) -> Sen2LikeProductOutput:
    output_dir = working_dir / Path(product).name
    manifest_path = output_dir / "manifest.json"
    safe_dirs = sorted(output_dir.glob("*_L2F"))
    normalized_uri = str(safe_dirs[0]) if safe_dirs else (str(output_dir) if output_dir.exists() else None)
    return Sen2LikeProductOutput(
        product=product,
        output_dir=str(output_dir),
        manifest_path=str(manifest_path) if manifest_path.exists() else None,
        normalized_uri=normalized_uri,
        exists=output_dir.exists(),
    )


def _prepend_path(value: str, existing: str | None) -> str:
    if not existing:
        return value
    parts = [value, *[part for part in existing.split(os.pathsep) if part and part != value]]
    return os.pathsep.join(parts)


def _prepare_spark_environment(env: dict[str, str]) -> str:
    configured = (
        str(env.get("NIMBUS_SEN2LIKE_SPARK_DIR") or "").strip()
        or str(env.get("SPARK_LOCAL_DIRS") or "").strip()
        or DEFAULT_SPARK_LOCAL_DIRS
    )
    spark_dirs = _spark_dir_values(configured)
    for spark_dir in spark_dirs:
        path = Path(spark_dir)
        path.mkdir(parents=True, exist_ok=True)
        try:
            path.chmod(0o777)
        except OSError:
            pass
    normalized = ",".join(spark_dirs)
    env["SPARK_LOCAL_DIRS"] = normalized
    env.setdefault("TMPDIR", "/tmp")
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return normalized


def _spark_dir_values(configured: str) -> list[str]:
    raw_parts = [
        part.strip()
        for item in str(configured or DEFAULT_SPARK_LOCAL_DIRS).split(os.pathsep)
        for part in item.split(",")
        if part.strip()
    ]
    return raw_parts or [DEFAULT_SPARK_LOCAL_DIRS]


def _tail(value: str, *, limit: int = 6000) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]
