from __future__ import annotations

from dataclasses import dataclass
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import Any, Sequence

from nimbuschain_sen2like_service.models import (
    Sen2LikeNormalizeRequest,
    Sen2LikeNormalizeResponse,
    Sen2LikeProductOutput,
)


DEFAULT_VENDOR_SUBDIR = "sen2like-service/vendor/Satellite-fetcher-feature-sen2like_reimplementation"
DEFAULT_SPARK_LOCAL_DIRS = "/tmp/nimbus-sen2like-spark"
DEFAULT_TIMEOUT_SECONDS = 3600.0
DEFAULT_SPARK_DRIVER_MEMORY = "1g"
DEFAULT_SPARK_EXECUTOR_MEMORY = "1g"
DEFAULT_SPARK_PYTHON_WORKER_MEMORY = "256m"


@dataclass(frozen=True, slots=True)
class PreparedProduct:
    original: str
    command_input: str
    output_name: str
    extracted: bool
    input_issue: dict[str, str] | None = None


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
    product_inputs: Sequence[str] | None = None,
) -> list[str]:
    root = vendor_root or resolve_vendor_root()
    work_dir = working_dir or resolve_working_dir(request.working_dir)
    products = list(product_inputs) if product_inputs is not None else request.product_inputs()
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
    original_products = request.product_inputs()
    started = time.perf_counter()
    prepared_products = [
        _prepare_product_input(product, work_dir)
        for product in original_products
    ]
    input_issues = [
        issue
        for product in prepared_products
        if (issue := product.input_issue) is not None
    ]
    if input_issues:
        duration = time.perf_counter() - started
        return Sen2LikeNormalizeResponse(
            status="failed",
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            products=original_products,
            working_dir=str(work_dir),
            outputs=[_product_output(product, work_dir) for product in prepared_products],
            duration_seconds=duration,
            return_code=-1,
            command=[],
            stdout_tail="",
            stderr_tail="",
            metadata=_response_metadata(
                vendor_root=vendor_root,
                request=request,
                spark_local_dirs=None,
                prepared_products=prepared_products,
                input_issues=input_issues,
                output_issues=input_issues,
            ),
        )

    command = build_command(
        request,
        vendor_root=vendor_root,
        working_dir=work_dir,
        product_inputs=[product.command_input for product in prepared_products],
    )
    env = os.environ.copy()
    env["LANDSAT_UPSAMPLING_BASE"] = str(vendor_root)
    env["PYTHONPATH"] = _prepend_path(str(vendor_root), env.get("PYTHONPATH"))
    spark_local_dirs = _prepare_spark_environment(env)
    if request.spark_master:
        env["SPARK_MASTER"] = request.spark_master

    timeout_seconds = _resolve_timeout_seconds(request)
    try:
        completed = subprocess.run(
            command,
            cwd=str(vendor_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.perf_counter() - started
        outputs = [
            _product_output(product, work_dir)
            for product in prepared_products
        ]
        timeout_issue = {
            "code": "timeout",
            "message": (
                "Sen2Like exceeded its runtime timeout "
                f"({timeout_seconds:.0f}s) and was stopped."
            ),
        }
        return Sen2LikeNormalizeResponse(
            status="failed",
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            products=original_products,
            working_dir=str(work_dir),
            outputs=outputs,
            duration_seconds=duration,
            return_code=-124,
            command=command,
            stdout_tail=_tail(exc.stdout or ""),
            stderr_tail=_tail(exc.stderr or ""),
            metadata=_response_metadata(
                vendor_root=vendor_root,
                request=request,
                spark_local_dirs=spark_local_dirs,
                prepared_products=prepared_products,
                input_issues=[],
                output_issues=[timeout_issue],
            )
            | {"timeout_seconds": timeout_seconds},
        )
    duration = time.perf_counter() - started
    outputs = [
        _product_output(product, work_dir)
        for product in prepared_products
    ]
    output_issues = _collect_output_issues(outputs)
    status = "succeeded" if completed.returncode == 0 and not output_issues else "failed"
    return Sen2LikeNormalizeResponse(
        status=status,
        job_id=request.job_id,
        pipeline_id=request.pipeline_id,
        trace_id=request.trace_id,
        products=original_products,
        working_dir=str(work_dir),
        outputs=outputs,
        duration_seconds=duration,
        return_code=int(completed.returncode),
        command=command,
        stdout_tail=_tail(completed.stdout),
        stderr_tail=_tail(completed.stderr),
        metadata=_response_metadata(
            vendor_root=vendor_root,
            request=request,
            spark_local_dirs=spark_local_dirs,
            prepared_products=prepared_products,
            input_issues=[],
            output_issues=output_issues,
        )
        | {"timeout_seconds": timeout_seconds},
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


def _prepare_product_input(product: str, working_dir: Path) -> PreparedProduct:
    product_path = Path(product)
    if _looks_like_tar_product(product_path):
        output_name = _tar_output_name(product_path)
        input_issue = _tar_input_issue(product_path)
        if input_issue:
            return PreparedProduct(
                original=product,
                command_input=product,
                output_name=output_name,
                extracted=False,
                input_issue=input_issue,
            )
        extracted_dir = working_dir / "_inputs" / output_name
        _extract_tar_product(product_path, extracted_dir)
        return PreparedProduct(
            original=product,
            command_input=str(extracted_dir),
            output_name=output_name,
            extracted=True,
        )
    return PreparedProduct(
        original=product,
        command_input=product,
        output_name=Path(product).name,
        extracted=False,
    )


def _product_output(product: PreparedProduct, working_dir: Path) -> Sen2LikeProductOutput:
    output_dir = working_dir / product.output_name
    manifest_path = output_dir / "manifest.json"
    normalized_uri = _normalized_output_uri(output_dir)
    return Sen2LikeProductOutput(
        product=product.original,
        output_dir=str(output_dir),
        manifest_path=str(manifest_path) if manifest_path.exists() else None,
        normalized_uri=normalized_uri,
        exists=output_dir.exists(),
    )


def _looks_like_tar_product(path: Path) -> bool:
    return str(path.name).lower().endswith((".tar", ".tar.gz", ".tgz"))


def _tar_input_issue(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return {
            "product": str(path),
            "code": "input_missing",
            "message": f"Landsat tar input is missing before Sen2Like: {path}.",
        }
    if not path.is_file():
        return {
            "product": str(path),
            "code": "input_not_file",
            "message": f"Landsat tar input is not a file: {path}.",
        }
    if not _is_tar_product(path):
        return {
            "product": str(path),
            "code": "input_not_tar",
            "message": f"Landsat input is not a readable tar archive: {path}.",
        }
    return None


def _is_tar_product(path: Path) -> bool:
    try:
        return path.is_file() and tarfile.is_tarfile(path)
    except (OSError, tarfile.TarError):
        return False


def _tar_output_name(path: Path) -> str:
    name = path.name
    lowered = name.lower()
    for suffix in (".tar.gz", ".tgz", ".tar"):
        if lowered.endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


def _extract_tar_product(archive_path: Path, target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path) as archive:
        members = list(_safe_tar_members(archive, target_dir))
        archive.extractall(path=target_dir, members=members)


def _safe_tar_members(archive: tarfile.TarFile, target_dir: Path):
    target_root = target_dir.resolve()
    for member in archive.getmembers():
        if member.issym() or member.islnk():
            raise ValueError(f"Refusing to extract linked tar member: {member.name}")
        member_path = (target_root / member.name).resolve()
        try:
            member_path.relative_to(target_root)
        except ValueError as exc:
            raise ValueError(f"Refusing to extract unsafe tar member: {member.name}") from exc
        yield member


def _normalized_output_uri(output_dir: Path) -> str | None:
    candidates = [
        *sorted((output_dir / "SAFE").glob("*.SAFE")),
        *sorted(output_dir.glob("*.SAFE")),
        *sorted(output_dir.glob("*_L2F")),
    ]
    for candidate in candidates:
        if _valid_safe_output(candidate):
            return str(candidate)
    return None


def _valid_safe_output(path: Path) -> bool:
    if not path.is_dir():
        return False
    if path.suffix.upper() == ".SAFE" and not (path / "manifest.safe").exists():
        return False
    raster_files = [
        raster
        for raster in path.rglob("*")
        if raster.is_file() and raster.suffix.lower() in {".tif", ".tiff", ".jp2"}
    ]
    return bool(raster_files)


def _collect_output_issues(outputs: Sequence[Sen2LikeProductOutput]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for output in outputs:
        output_dir = Path(output.output_dir)
        product = output.product
        if not output.exists:
            issues.append(
                {
                    "product": product,
                    "code": "output_missing",
                    "message": f"No Sen2Like output directory was written for {Path(product).name}.",
                }
            )
            continue

        manifest_path = Path(output.manifest_path) if output.manifest_path else output_dir / "manifest.json"
        if not manifest_path.exists():
            issues.append(
                {
                    "product": product,
                    "code": "manifest_missing",
                    "message": f"Sen2Like manifest is missing for {Path(product).name}.",
                }
            )
        else:
            issues.extend(_manifest_issues(product=product, manifest_path=manifest_path))

        if not output.normalized_uri:
            issues.append(
                {
                    "product": product,
                    "code": "normalized_output_missing",
                    "message": (
                        f"Sen2Like did not produce a valid Sentinel-like SAFE output "
                        f"for {Path(product).name}."
                    ),
                }
            )
    return issues


def _manifest_issues(*, product: str, manifest_path: Path) -> list[dict[str, str]]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [
            {
                "product": product,
                "code": "manifest_unreadable",
                "message": f"Sen2Like manifest could not be read for {Path(product).name}: {exc}",
            }
        ]
    issues: list[dict[str, str]] = []
    steps = manifest.get("steps") if isinstance(manifest, dict) else None
    if not isinstance(steps, dict):
        return issues
    for step_name, payload in steps.items():
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status") or "").strip().lower() != "failed":
            continue
        error = str(payload.get("error") or "").strip()
        message = f"Sen2Like step {step_name} failed for {Path(product).name}"
        if error:
            message = f"{message}: {error}"
        issues.append(
            {
                "product": product,
                "code": "step_failed",
                "message": message,
            }
        )
    return issues


def _prepend_path(value: str, existing: str | None) -> str:
    if not existing:
        return value
    parts = [value, *[part for part in existing.split(os.pathsep) if part and part != value]]
    return os.pathsep.join(parts)


def _prepare_spark_environment(env: dict[str, str]) -> str:
    configured = (
        str(env.get("NIMBUS_SEN2LIKE_SPARK_DIR") or "").strip()
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
    env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    env.setdefault("SPARK_DRIVER_HOST", "127.0.0.1")
    driver_memory = (
        str(env.get("NIMBUS_SEN2LIKE_SPARK_DRIVER_MEMORY") or "").strip()
        or DEFAULT_SPARK_DRIVER_MEMORY
    )
    executor_memory = (
        str(env.get("NIMBUS_SEN2LIKE_SPARK_EXECUTOR_MEMORY") or "").strip()
        or DEFAULT_SPARK_EXECUTOR_MEMORY
    )
    python_worker_memory = (
        str(env.get("NIMBUS_SEN2LIKE_SPARK_PYTHON_WORKER_MEMORY") or "").strip()
        or DEFAULT_SPARK_PYTHON_WORKER_MEMORY
    )
    env.setdefault("SPARK_DRIVER_MEMORY", driver_memory)
    env.setdefault("PYSPARK_SUBMIT_ARGS", _default_pyspark_submit_args(
        driver_memory=driver_memory,
        executor_memory=executor_memory,
        python_worker_memory=python_worker_memory,
        spark_local_dirs=normalized,
    ))
    return normalized


def _default_pyspark_submit_args(
    *,
    driver_memory: str,
    executor_memory: str,
    python_worker_memory: str,
    spark_local_dirs: str,
) -> str:
    return (
        f"--driver-memory {driver_memory} "
        f"--conf spark.driver.memory={driver_memory} "
        f"--conf spark.executor.memory={executor_memory} "
        f"--conf spark.python.worker.memory={python_worker_memory} "
        "--conf spark.sql.shuffle.partitions=1 "
        f"--conf spark.local.dir={spark_local_dirs} "
        "pyspark-shell"
    )


def _resolve_timeout_seconds(request: Sen2LikeNormalizeRequest) -> float:
    if request.timeout_seconds:
        return float(request.timeout_seconds)
    configured = str(os.getenv("NIMBUS_SEN2LIKE_TIMEOUT_SECONDS") or "").strip()
    if configured:
        try:
            value = float(configured)
        except ValueError:
            return DEFAULT_TIMEOUT_SECONDS
        if value > 0:
            return value
    return DEFAULT_TIMEOUT_SECONDS


def _spark_dir_values(configured: str) -> list[str]:
    raw_parts = [
        part.strip()
        for item in str(configured or DEFAULT_SPARK_LOCAL_DIRS).split(os.pathsep)
        for part in item.split(",")
        if part.strip()
    ]
    return raw_parts or [DEFAULT_SPARK_LOCAL_DIRS]


def _response_metadata(
    *,
    vendor_root: Path,
    request: Sen2LikeNormalizeRequest,
    spark_local_dirs: str | None,
    prepared_products: Sequence[PreparedProduct],
    input_issues: Sequence[dict[str, str]],
    output_issues: Sequence[dict[str, str]],
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "vendor_root": str(vendor_root),
        "pipeline_py": str(pipeline_path(vendor_root)),
        "spark_master": request.spark_master,
        "spark_local_dirs": spark_local_dirs,
        "pyspark_service": True,
        "tar_inputs_supported": True,
        "tar_inputs_are_extracted_before_pyspark": True,
        "prepared_products": [
            {
                "original": product.original,
                "command_input": product.command_input,
                "output_name": product.output_name,
                "extracted": product.extracted,
                **({"input_issue": product.input_issue} if product.input_issue else {}),
            }
            for product in prepared_products
        ],
        "input_issues": list(input_issues),
        "output_issues": list(output_issues),
    }
    return metadata


def _tail(value: str | bytes, *, limit: int = 6000) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if len(value) <= limit:
        return value
    return value[-limit:]
