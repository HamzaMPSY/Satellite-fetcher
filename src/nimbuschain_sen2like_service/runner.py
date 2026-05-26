from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path
from typing import Any, Sequence

from nimbuschain_shared.contracts.sen2like import (
    Sen2LikeNormalizeRequest,
    Sen2LikeNormalizeResponse,
    Sen2LikeProductOutput,
)


DEFAULT_VENDOR_SUBDIR = (
    "src/nimbuschain_sen2like_service/vendor/"
    "Satellite-fetcher-feature-sen2like_reimplementation"
)
DEFAULT_SPARK_LOCAL_DIRS = "/tmp/nimbus-sen2like-spark"
DEFAULT_TIMEOUT_SECONDS = 3600.0
DEFAULT_SPARK_DRIVER_MEMORY = "1g"
DEFAULT_SPARK_EXECUTOR_MEMORY = "1g"
DEFAULT_SPARK_PYTHON_WORKER_MEMORY = "256m"
DEFAULT_PREPROCESS_TARGET_SHAPE = "native"
_ORIGINAL_SUBPROCESS_RUN = subprocess.run
_PROCESS_LOCK = threading.RLock()
_RUNNING_PROCESSES: dict[str, subprocess.Popen[str]] = {}
_CANCEL_REQUESTS: set[str] = set()
_MTL_KV_RE = re.compile(r'^\s*([A-Z0-9_]+)\s*=\s*"?([^"]*)"?\s*$')
_REQUIRED_SAFE_BANDS = frozenset({"B02", "B03", "B04", "B08", "B11", "B12"})


@dataclass(frozen=True, slots=True)
class PreparedProduct:
    original: str
    command_input: str
    output_name: str
    extracted: bool
    input_issue: dict[str, str] | None = None
    preprocess: dict[str, Any] | None = None


class Sen2LikeJobCancelled(RuntimeError):
    """Raised when a Sen2Like subprocess is cancelled by the API."""


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
    _clear_cancel(request.job_id)
    vendor_root = resolve_vendor_root()
    work_dir = resolve_working_dir(request.working_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    original_products = request.product_inputs()
    started = time.perf_counter()
    prepare_started = time.perf_counter()
    preprocess_target_shape = _resolve_preprocess_target_shape(request)
    prepared_products = _prepare_product_inputs(
        original_products,
        work_dir,
        workers=int(request.workers),
        preprocess_target_shape=preprocess_target_shape,
    )
    preparation_duration = time.perf_counter() - prepare_started
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
                preparation_duration_seconds=preparation_duration,
                pipeline_duration_seconds=0.0,
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
    pipeline_started = time.perf_counter()
    subprocess_attempts: list[dict[str, Any]] = []
    try:
        completed = _run_pipeline_with_safe_retry(
            command,
            cwd=str(vendor_root),
            env=env,
            timeout=timeout_seconds,
            job_id=request.job_id,
            attempts=subprocess_attempts,
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.perf_counter() - started
        pipeline_duration = time.perf_counter() - pipeline_started
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
                preparation_duration_seconds=preparation_duration,
                pipeline_duration_seconds=pipeline_duration,
                subprocess_attempts=subprocess_attempts,
            )
            | {"timeout_seconds": timeout_seconds},
        )
    except Sen2LikeJobCancelled as exc:
        duration = time.perf_counter() - started
        pipeline_duration = time.perf_counter() - pipeline_started
        outputs = [
            _product_output(product, work_dir)
            for product in prepared_products
        ]
        cancel_issue = {
            "code": "cancelled",
            "message": "Sen2Like was cancelled before the normalization subprocess finished.",
        }
        return Sen2LikeNormalizeResponse(
            status="cancelled",
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            products=original_products,
            working_dir=str(work_dir),
            outputs=outputs,
            duration_seconds=duration,
            return_code=-15,
            command=command,
            stdout_tail="",
            stderr_tail=str(exc),
            metadata=_response_metadata(
                vendor_root=vendor_root,
                request=request,
                spark_local_dirs=spark_local_dirs,
                prepared_products=prepared_products,
                input_issues=[],
                output_issues=[cancel_issue],
                preparation_duration_seconds=preparation_duration,
                pipeline_duration_seconds=pipeline_duration,
                subprocess_attempts=subprocess_attempts,
            )
            | {"timeout_seconds": timeout_seconds, "cancelled": True},
        )
    duration = time.perf_counter() - started
    pipeline_duration = time.perf_counter() - pipeline_started
    outputs = [
        _product_output(product, work_dir)
        for product in prepared_products
    ]
    output_issues = _collect_output_issues(outputs)
    if _should_retry_sen2like_after_output_issues(
        output_issues=output_issues,
        env=env,
        attempts=subprocess_attempts,
    ):
        safe_env = dict(env)
        safe_env["NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"] = "false"
        safe_env["NIMBUS_SEN2LIKE_BAND_WORKERS"] = "1"
        safe_command = _command_with_workers(command, workers=1)
        safe_started = time.perf_counter()
        try:
            completed = _run_pipeline_subprocess(
                safe_command,
                cwd=str(vendor_root),
                env=safe_env,
                timeout=timeout_seconds,
                job_id=request.job_id,
            )
        except subprocess.TimeoutExpired as exc:
            duration = time.perf_counter() - started
            pipeline_duration = time.perf_counter() - pipeline_started
            timeout_issue = {
                "code": "timeout",
                "message": (
                    "Sen2Like safe retry exceeded its runtime timeout "
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
                outputs=[_product_output(product, work_dir) for product in prepared_products],
                duration_seconds=duration,
                return_code=-124,
                command=safe_command,
                stdout_tail=_tail(exc.stdout or ""),
                stderr_tail=_tail(exc.stderr or ""),
                metadata=_response_metadata(
                    vendor_root=vendor_root,
                    request=request,
                    spark_local_dirs=spark_local_dirs,
                    prepared_products=prepared_products,
                    input_issues=[],
                    output_issues=[timeout_issue],
                    preparation_duration_seconds=preparation_duration,
                    pipeline_duration_seconds=pipeline_duration,
                    subprocess_attempts=subprocess_attempts,
                )
                | {"timeout_seconds": timeout_seconds},
            )
        attempts_summary = _subprocess_attempt_summary(
            mode="safe_serial_after_incomplete_manifest",
            completed=completed,
            env=safe_env,
            duration_seconds=time.perf_counter() - safe_started,
        )
        attempts_summary["trigger"] = "output_issues"
        attempts_summary["trigger_codes"] = sorted(
            {
                str(issue.get("code") or "").strip()
                for issue in output_issues
                if isinstance(issue, dict)
            }
        )
        subprocess_attempts.append(attempts_summary)
        command = safe_command
        outputs = [
            _product_output(product, work_dir)
            for product in prepared_products
        ]
        output_issues = _collect_output_issues(outputs)
        duration = time.perf_counter() - started
        pipeline_duration = time.perf_counter() - pipeline_started
    status = "succeeded" if completed.returncode == 0 and not output_issues else "failed"
    direct_zarr_metadata: dict[str, Any] | None = None
    if status == "succeeded":
        outputs, direct_zarr_metadata = _attach_direct_zarr_outputs(
            outputs,
            request=request,
            job_id=request.job_id,
        )
        duration = time.perf_counter() - started
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
            preparation_duration_seconds=preparation_duration,
            pipeline_duration_seconds=pipeline_duration,
            subprocess_attempts=subprocess_attempts,
            direct_zarr_metadata=direct_zarr_metadata,
        )
        | {"timeout_seconds": timeout_seconds},
    )


def readiness_payload() -> dict[str, Any]:
    vendor_root = resolve_vendor_root()
    script = pipeline_path(vendor_root)
    sixs_path = _sixs_executable_path()
    ready = script.exists() and sixs_path is not None
    return {
        "status": "ok" if ready else "unavailable",
        "service": "nimbus-sen2like",
        "vendor_root": str(vendor_root),
        "pipeline_py": str(script),
        "pipeline_py_exists": script.exists(),
        "sixs_executable": sixs_path,
        "sixs_executable_exists": sixs_path is not None,
    }


def cancel_sen2like_job(job_id: str | None) -> bool:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return False
    with _PROCESS_LOCK:
        _CANCEL_REQUESTS.add(normalized_job_id)
        process = _RUNNING_PROCESSES.get(normalized_job_id)
    if process is None:
        return True
    _terminate_process(process)
    return True


def _clear_cancel(job_id: str | None) -> None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return
    with _PROCESS_LOCK:
        _CANCEL_REQUESTS.discard(normalized_job_id)


def _is_cancel_requested(job_id: str | None) -> bool:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return False
    with _PROCESS_LOCK:
        return normalized_job_id in _CANCEL_REQUESTS


def _run_pipeline_subprocess(
    command: list[str],
    *,
    cwd: str,
    env: dict[str, str],
    timeout: float,
    job_id: str | None,
) -> subprocess.CompletedProcess[str]:
    if _is_cancel_requested(job_id):
        raise Sen2LikeJobCancelled("Sen2Like cancellation requested before subprocess start.")

    if subprocess.run is not _ORIGINAL_SUBPROCESS_RUN:
        return subprocess.run(
            command,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    normalized_job_id = str(job_id or "").strip()
    if normalized_job_id:
        with _PROCESS_LOCK:
            _RUNNING_PROCESSES[normalized_job_id] = process
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_process(process)
        stdout, stderr = _communicate_after_stop(process)
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=stdout,
            stderr=stderr,
        ) from exc
    finally:
        if normalized_job_id:
            with _PROCESS_LOCK:
                _RUNNING_PROCESSES.pop(normalized_job_id, None)

    if _is_cancel_requested(job_id):
        raise Sen2LikeJobCancelled("Sen2Like cancellation requested.")
    return subprocess.CompletedProcess(
        args=command,
        returncode=int(process.returncode or 0),
        stdout=stdout or "",
        stderr=stderr or "",
    )


def _run_pipeline_with_safe_retry(
    command: list[str],
    *,
    cwd: str,
    env: dict[str, str],
    timeout: float,
    job_id: str | None,
    attempts: list[dict[str, Any]],
) -> subprocess.CompletedProcess[str]:
    first_env = dict(env)
    started = time.perf_counter()
    completed = _run_pipeline_subprocess(
        command,
        cwd=cwd,
        env=first_env,
        timeout=timeout,
        job_id=job_id,
    )
    attempts.append(_subprocess_attempt_summary(
        mode="parallel_products_and_bands",
        completed=completed,
        env=first_env,
        duration_seconds=time.perf_counter() - started,
    ))
    if not _should_retry_sen2like_safe(completed=completed, env=first_env):
        return completed

    safe_env = dict(env)
    safe_env["NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"] = "false"
    safe_started = time.perf_counter()
    safe_completed = _run_pipeline_subprocess(
        command,
        cwd=cwd,
        env=safe_env,
        timeout=timeout,
        job_id=job_id,
    )
    attempts.append(_subprocess_attempt_summary(
        mode="safe_product_parallel_only",
        completed=safe_completed,
        env=safe_env,
        duration_seconds=time.perf_counter() - safe_started,
    ))
    return safe_completed


def _subprocess_attempt_summary(
    *,
    mode: str,
    completed: subprocess.CompletedProcess[str],
    env: dict[str, str],
    duration_seconds: float,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "return_code": int(completed.returncode),
        "duration_seconds": float(duration_seconds),
        "nested_band_parallelism": _env_flag_value(
            env.get("NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"),
            default=True,
        ),
        "band_workers": _env_int_value(
            env.get("NIMBUS_SEN2LIKE_BAND_WORKERS"),
            default=2,
            minimum=1,
            maximum=32,
        ),
        "stdout_tail": _tail(completed.stdout or "", limit=1200),
        "stderr_tail": _tail(completed.stderr or "", limit=1200),
    }


def _should_retry_sen2like_safe(
    *,
    completed: subprocess.CompletedProcess[str],
    env: dict[str, str],
) -> bool:
    if not _env_flag_value(env.get("NIMBUS_SEN2LIKE_SAFE_RETRY"), default=True):
        return False
    if int(completed.returncode) == 0:
        return False
    if not _env_flag_value(env.get("NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"), default=True):
        return False
    detail = f"{completed.stdout or ''}\n{completed.stderr or ''}".lower()
    if int(completed.returncode) in {-9, 137}:
        return True
    return any(
        token in detail
        for token in (
            "killed",
            "out of memory",
            "cannot allocate memory",
            "resource temporarily unavailable",
            "too many open files",
            "no space left on device",
        )
    )


def _should_retry_sen2like_after_output_issues(
    *,
    output_issues: Sequence[dict[str, str]],
    env: dict[str, str],
    attempts: Sequence[dict[str, Any]],
) -> bool:
    if not output_issues:
        return False
    if not _env_flag_value(env.get("NIMBUS_SEN2LIKE_SAFE_RETRY"), default=True):
        return False
    if any(str(attempt.get("mode") or "").startswith("safe_") for attempt in attempts):
        return False
    if not _env_flag_value(env.get("NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"), default=True):
        return False
    codes = {
        str(issue.get("code") or "").strip().lower()
        for issue in output_issues
        if isinstance(issue, dict)
    }
    return "step_incomplete" in codes


def _command_with_workers(command: Sequence[str], *, workers: int) -> list[str]:
    updated = list(command)
    try:
        index = updated.index("--workers")
    except ValueError:
        return updated + ["--workers", str(int(workers))]
    if index + 1 < len(updated):
        updated[index + 1] = str(int(workers))
    else:
        updated.append(str(int(workers)))
    return updated


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _communicate_after_stop(process: subprocess.Popen[str]) -> tuple[str, str]:
    try:
        stdout, stderr = process.communicate(timeout=5)
    except Exception:
        return "", ""
    return stdout or "", stderr or ""


def _sixs_executable_path() -> str | None:
    for executable in ("sixs", "sixsV1.1", "sixs.exe", "sixsV1.1.exe"):
        if path := shutil.which(executable):
            return path
    return None


def _prepare_product_input(
    product: str,
    working_dir: Path,
    *,
    preprocess_target_shape: tuple[int, int] | None,
) -> PreparedProduct:
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
        preprocess = _preprocess_extracted_product(
            extracted_dir,
            target_shape=preprocess_target_shape,
        )
        return PreparedProduct(
            original=product,
            command_input=str(extracted_dir),
            output_name=output_name,
            extracted=True,
            preprocess=preprocess,
        )
    return PreparedProduct(
        original=product,
        command_input=product,
        output_name=Path(product).name,
        extracted=False,
        preprocess=_preprocess_disabled_metadata(preprocess_target_shape, reason="not_extracted"),
    )


def _prepare_product_inputs(
    products: Sequence[str],
    working_dir: Path,
    *,
    workers: int,
    preprocess_target_shape: tuple[int, int] | None,
) -> list[PreparedProduct]:
    product_list = list(products)
    if len(product_list) <= 1:
        return [
            _prepare_product_input(
                product,
                working_dir,
                preprocess_target_shape=preprocess_target_shape,
            )
            for product in product_list
        ]

    effective_workers = min(max(1, int(workers)), len(product_list), 4)
    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        return list(pool.map(
            lambda product: _prepare_product_input(
                product,
                working_dir,
                preprocess_target_shape=preprocess_target_shape,
            ),
            product_list,
        ))


def _product_output(product: PreparedProduct, working_dir: Path) -> Sen2LikeProductOutput:
    output_dir = working_dir / product.output_name
    manifest_path = output_dir / "manifest.json"
    expected_timestamp = _expected_acquisition_timestamp(product.command_input)
    normalized_uri = _normalized_output_uri(
        output_dir,
        expected_acquisition_timestamp=expected_timestamp,
    )
    return Sen2LikeProductOutput(
        product=product.original,
        output_dir=str(output_dir),
        manifest_path=str(manifest_path) if manifest_path.exists() else None,
        normalized_uri=normalized_uri,
        exists=output_dir.exists(),
    )


def _attach_direct_zarr_outputs(
    outputs: Sequence[Sen2LikeProductOutput],
    *,
    request: Sen2LikeNormalizeRequest,
    job_id: str | None,
) -> tuple[list[Sen2LikeProductOutput], dict[str, Any]]:
    enabled = _direct_zarr_enabled(request)
    output_root = _direct_zarr_output_root(request)
    metadata: dict[str, Any] = {
        "direct_zarr_requested": enabled,
        "direct_zarr_output_dir": str(output_root),
        "direct_zarr_status": "skipped",
        "direct_zarr_outputs": [],
        "direct_zarr_items": [],
        "direct_zarr_issues": [],
        "direct_zarr_duration_seconds": 0.0,
    }
    if not enabled:
        metadata["direct_zarr_reason"] = "disabled"
        return list(outputs), metadata

    started = time.perf_counter()
    updated_outputs: list[Sen2LikeProductOutput] = []
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    eligible_count = 0
    try:
        from nimbuschain_zarr_service.service import ZarrConversionService
    except Exception as exc:
        for output in outputs:
            updated_outputs.append(output.model_copy(update={"zarr_error": str(exc)}))
        metadata.update(
            {
                "direct_zarr_status": "failed",
                "direct_zarr_issues": [
                    {
                        "code": "zarr_runtime_unavailable",
                        "message": f"Direct Sen2Like Zarr conversion is unavailable: {exc}",
                    }
                ],
                "direct_zarr_duration_seconds": time.perf_counter() - started,
            }
        )
        return updated_outputs, metadata

    converter = ZarrConversionService()
    for index, output in enumerate(outputs, start=1):
        if _is_cancel_requested(job_id):
            raise Sen2LikeJobCancelled("Sen2Like cancellation requested before direct Zarr conversion.")
        normalized_uri = str(output.normalized_uri or "").strip()
        if not normalized_uri:
            updated_outputs.append(output)
            continue
        eligible_count += 1
        scene_id = _scene_id_from_normalized_uri(normalized_uri)
        output_uri = str(output_root / f"{_safe_output_stem(scene_id)}.zarr")
        item_started = time.perf_counter()
        try:
            written_uri, data_family, summary, dataset_summary = converter.convert(
                provider="copernicus",
                collection="SENTINEL-2",
                product_type="S2MSI2A",
                scene_id=scene_id,
                raw_uri=normalized_uri,
                output_uri=output_uri,
            )
        except Exception as exc:
            issue = {
                "product": output.product,
                "normalized_uri": normalized_uri,
                "scene_id": scene_id,
                "zarr_uri": output_uri,
                "code": "direct_zarr_conversion_failed",
                "message": str(exc),
            }
            issues.append(issue)
            updated_outputs.append(output.model_copy(update={"zarr_uri": output_uri, "zarr_error": str(exc)}))
            continue

        item = {
            "product": output.product,
            "normalized_uri": normalized_uri,
            "scene_id": scene_id,
            "zarr_uri": written_uri,
            "data_family": data_family,
            "summary": dict(summary or {}),
            "dataset_summary": dict(dataset_summary or {}),
            "duration_seconds": time.perf_counter() - item_started,
            "index": index,
        }
        items.append(item)
        updated_outputs.append(
            output.model_copy(
                update={
                    "zarr_uri": written_uri,
                    "zarr_exists": True,
                    "zarr_data_family": data_family,
                    "zarr_summary": dict(summary or {}),
                    "zarr_dataset_summary": dict(dataset_summary or {}),
                    "zarr_error": None,
                }
            )
        )

    if not eligible_count:
        status = "skipped"
        reason = "no_normalized_outputs"
    elif len(items) == eligible_count:
        status = "written"
        reason = ""
    elif items:
        status = "partial"
        reason = "some_outputs_failed"
    else:
        status = "failed"
        reason = "all_outputs_failed"
    metadata.update(
        {
            "direct_zarr_status": status,
            "direct_zarr_reason": reason,
            "direct_zarr_outputs": [str(item["zarr_uri"]) for item in items],
            "direct_zarr_items": items,
            "direct_zarr_issues": issues,
            "direct_zarr_output_count": len(items),
            "direct_zarr_duration_seconds": time.perf_counter() - started,
        }
    )
    return updated_outputs, metadata


def _direct_zarr_enabled(request: Sen2LikeNormalizeRequest) -> bool:
    if request.direct_zarr is not None:
        return bool(request.direct_zarr)
    return _env_flag_value(os.getenv("NIMBUS_SEN2LIKE_DIRECT_ZARR"), default=False)


def _direct_zarr_output_root(request: Sen2LikeNormalizeRequest) -> Path:
    configured = str(
        request.zarr_output_dir
        or os.getenv("NIMBUS_SEN2LIKE_ZARR_DIR")
        or ""
    ).strip()
    if configured:
        return Path(configured).expanduser()
    data_root = Path(str(os.getenv("NIMBUS_DATA_DIR") or "/data/downloads")).expanduser()
    return data_root / "zarr"


def _scene_id_from_normalized_uri(normalized_uri: str) -> str:
    name = Path(str(normalized_uri).rstrip("/")).name
    if name.upper().endswith(".SAFE"):
        return name[:-5]
    return Path(name).stem or name or "scene"


def _safe_output_stem(scene_id: str) -> str:
    safe_scene = "".join(
        ch if ch.isalnum() or ch in "._-" else "_"
        for ch in str(scene_id or "scene")
    ).strip("._-")
    return safe_scene or "scene"


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


def _resolve_preprocess_target_shape(
    request: Sen2LikeNormalizeRequest,
) -> tuple[int, int] | None:
    raw = (
        str(request.preprocess_target_shape or "").strip()
        or str(os.getenv("NIMBUS_SEN2LIKE_PREPROCESS_TARGET_SHAPE") or "").strip()
        or str(os.getenv("NIMBUS_ZARR_TARGET_SHAPE") or "").strip()
        or DEFAULT_PREPROCESS_TARGET_SHAPE
    )
    return _parse_target_shape(raw)


def _parse_target_shape(raw: str | None) -> tuple[int, int] | None:
    text = str(raw or DEFAULT_PREPROCESS_TARGET_SHAPE).strip().lower()
    if text in {"", "native", "none", "off", "false", "0"}:
        return None
    normalized = text.replace(" ", "").replace(",", "x").replace(":", "x")
    parts = [part for part in normalized.split("x") if part]
    if len(parts) == 1:
        try:
            side = int(parts[0])
        except ValueError:
            return None
        side = max(1, side)
        return side, side
    try:
        height = int(parts[0])
        width = int(parts[1])
    except (IndexError, ValueError):
        return None
    return max(1, height), max(1, width)


def _preprocess_disabled_metadata(
    target_shape: tuple[int, int] | None,
    *,
    reason: str,
) -> dict[str, Any]:
    return {
        "enabled": bool(target_shape),
        "applied": False,
        "reason": reason,
        "target_shape": [int(target_shape[0]), int(target_shape[1])] if target_shape else None,
    }


def _preprocess_extracted_product(
    scene_dir: Path,
    *,
    target_shape: tuple[int, int] | None,
) -> dict[str, Any]:
    if target_shape is None:
        return _preprocess_disabled_metadata(target_shape, reason="disabled")

    started = time.perf_counter()
    tif_paths = sorted(
        path
        for path in scene_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}
    )
    if not tif_paths:
        return {
            "enabled": True,
            "applied": False,
            "reason": "no_tif_inputs",
            "target_shape": [int(target_shape[0]), int(target_shape[1])],
            "files_total": 0,
            "files_resampled": 0,
            "duration_seconds": time.perf_counter() - started,
        }

    bytes_before = _sum_file_sizes(tif_paths)
    workers = _env_int_value(
        os.getenv("NIMBUS_SEN2LIKE_PREPROCESS_WORKERS"),
        default=2,
        minimum=1,
        maximum=16,
    )
    effective_workers = min(int(workers), len(tif_paths))

    def _process(path: Path) -> dict[str, Any]:
        return _resample_raster_to_shape(path, target_shape=target_shape)

    if effective_workers <= 1:
        results = [_process(path) for path in tif_paths]
    else:
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            results = list(pool.map(_process, tif_paths))

    _patch_mtl_dimensions(scene_dir, target_shape=target_shape)
    bytes_after = _sum_file_sizes(tif_paths)
    files_resampled = sum(1 for item in results if item.get("resampled"))
    failures = [item for item in results if item.get("error")]
    if failures:
        first = failures[0]
        raise RuntimeError(
            "Sen2Like input preprocessing failed for "
            f"{Path(str(first.get('path') or '')).name}: {first.get('error')}"
        )
    return {
        "enabled": True,
        "applied": bool(files_resampled),
        "target_shape": [int(target_shape[0]), int(target_shape[1])],
        "files_total": len(tif_paths),
        "files_resampled": files_resampled,
        "workers": effective_workers,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "duration_seconds": time.perf_counter() - started,
    }


def _sum_file_sizes(paths: Sequence[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += int(path.stat().st_size)
        except OSError:
            continue
    return total


def _resample_raster_to_shape(
    path: Path,
    *,
    target_shape: tuple[int, int],
) -> dict[str, Any]:
    try:
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.transform import from_bounds
    except ImportError as exc:
        return {"path": str(path), "resampled": False, "error": f"rasterio unavailable: {exc}"}

    height, width = int(target_shape[0]), int(target_shape[1])
    tmp_path = path.with_name(f"{path.name}.nimbus-preprocess.tmp")
    try:
        with rasterio.open(path) as src:
            original_shape = [int(src.height), int(src.width)]
            if src.height == height and src.width == width:
                return {
                    "path": str(path),
                    "resampled": False,
                    "original_shape": original_shape,
                    "target_shape": [height, width],
                }

            bounds = src.bounds
            transform = from_bounds(
                bounds.left,
                bounds.bottom,
                bounds.right,
                bounds.top,
                width,
                height,
            )
            profile = src.profile.copy()
            for key in ("blockxsize", "blockysize"):
                profile.pop(key, None)
            profile.update(
                driver="GTiff",
                height=height,
                width=width,
                transform=transform,
                tiled=False,
                compress="deflate",
                bigtiff="IF_SAFER",
            )
            resampling = (
                Resampling.nearest
                if _is_nearest_resampling_raster(path)
                else Resampling.average
            )
            with rasterio.open(tmp_path, "w", **profile) as dst:
                for band_index in range(1, src.count + 1):
                    data = src.read(
                        band_index,
                        out_shape=(height, width),
                        resampling=resampling,
                    )
                    dst.write(data, band_index)
                    description = src.descriptions[band_index - 1] if src.descriptions else None
                    if description:
                        dst.set_band_description(band_index, description)
                    band_tags = src.tags(band_index)
                    if band_tags:
                        dst.update_tags(band_index, **band_tags)
                tags = src.tags()
                if tags:
                    dst.update_tags(**tags)
        tmp_path.replace(path)
        return {
            "path": str(path),
            "resampled": True,
            "original_shape": original_shape,
            "target_shape": [height, width],
        }
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        return {"path": str(path), "resampled": False, "error": str(exc)}


def _is_nearest_resampling_raster(path: Path) -> bool:
    name = path.name.upper()
    return any(
        token in name
        for token in (
            "QA_PIXEL",
            "QA_RADSAT",
            "QA_AEROSOL",
            "PIXELQA",
            "QUALITY",
            "MASK",
        )
    )


def _patch_mtl_dimensions(
    scene_dir: Path,
    *,
    target_shape: tuple[int, int],
) -> None:
    height, width = int(target_shape[0]), int(target_shape[1])
    dimension_keys = {
        "REFLECTIVE_LINES": str(height),
        "REFLECTIVE_SAMPLES": str(width),
        "THERMAL_LINES": str(height),
        "THERMAL_SAMPLES": str(width),
        "PANCHROMATIC_LINES": str(height),
        "PANCHROMATIC_SAMPLES": str(width),
    }
    for mtl_path in sorted(scene_dir.glob("*_MTL.txt")) + sorted(scene_dir.glob("**/*_MTL.txt")):
        try:
            lines = mtl_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        changed = False
        patched: list[str] = []
        for line in lines:
            match = _MTL_KV_RE.match(line)
            if not match:
                patched.append(line)
                continue
            key, _value = match.groups()
            replacement = dimension_keys.get(key)
            if replacement is None:
                patched.append(line)
                continue
            indent = line[: len(line) - len(line.lstrip())]
            patched.append(f"{indent}{key} = {replacement}")
            changed = True
        if changed:
            mtl_path.write_text("\n".join(patched) + "\n", encoding="utf-8")


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


def _normalized_output_uri(
    output_dir: Path,
    *,
    expected_acquisition_timestamp: str | None = None,
) -> str | None:
    candidates = [
        *sorted((output_dir / "SAFE").glob("*.SAFE")),
        *sorted(output_dir.glob("*.SAFE")),
        *sorted(output_dir.glob("*_L2F")),
    ]
    for candidate in candidates:
        if _valid_safe_output(
            candidate,
            expected_acquisition_timestamp=expected_acquisition_timestamp,
        ):
            return str(candidate)
    return None


def _valid_safe_output(
    path: Path,
    *,
    expected_acquisition_timestamp: str | None = None,
) -> bool:
    if not path.is_dir():
        return False
    if path.suffix.upper() != ".SAFE":
        return any(
            raster.is_file() and raster.suffix.lower() in {".tif", ".tiff", ".jp2"}
            for raster in path.rglob("*")
        )
    if expected_acquisition_timestamp and f"_{expected_acquisition_timestamp}_" not in path.name:
        return False
    if not (path / "manifest.safe").exists():
        return False
    if not any(path.glob("MTD_MSI*.xml")):
        return False
    granule_dirs = [item for item in (path / "GRANULE").glob("*") if item.is_dir()]
    if not granule_dirs:
        return False
    granule_dir = granule_dirs[0]
    if not (granule_dir / "MTD_TL.xml").exists():
        return False
    img_dir = granule_dir / "IMG_DATA" / "RESOLUTION_10M"
    if not img_dir.exists():
        return False
    return _REQUIRED_SAFE_BANDS.issubset(_safe_band_ids(img_dir))


def _safe_band_ids(img_dir: Path) -> set[str]:
    found: set[str] = set()
    for raster in img_dir.glob("*_10m.TIF"):
        for part in raster.stem.split("_"):
            candidate = part.upper()
            if candidate in _REQUIRED_SAFE_BANDS:
                found.add(candidate)
    return found


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
        status = str(payload.get("status") or "").strip().lower()
        if status in {"running", "queued", "pending", "invalidated"}:
            issues.append(
                {
                    "product": product,
                    "code": "step_incomplete",
                    "message": (
                        f"Sen2Like step {step_name} did not finish for "
                        f"{Path(product).name} (status={status})."
                    ),
                }
            )
            continue
        if status != "failed":
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


def _expected_acquisition_timestamp(product_input: str) -> str | None:
    path = Path(str(product_input))
    values = _read_landsat_mtl_values(path)
    date_value = _normalize_landsat_mtl_date(values.get("DATE_ACQUIRED"))
    if not date_value:
        return None
    return _build_landsat_acquisition_timestamp(
        date_value,
        values.get("SCENE_CENTER_TIME"),
    )


def _read_landsat_mtl_values(path: Path) -> dict[str, str]:
    if path.is_dir():
        for mtl_path in sorted(path.glob("*_MTL.txt")) + sorted(path.glob("**/*_MTL.txt")):
            values = _read_mtl_text(mtl_path)
            if values:
                return values
        return {}
    if _looks_like_tar_product(path) and path.exists():
        try:
            with tarfile.open(path) as archive:
                for member in archive.getmembers():
                    if not member.isfile() or not member.name.endswith("_MTL.txt"):
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    return _parse_mtl_lines(
                        extracted.read().decode("utf-8", errors="ignore").splitlines()
                    )
        except (OSError, tarfile.TarError):
            return {}
    if path.is_file() and path.name.endswith("_MTL.txt"):
        return _read_mtl_text(path)
    return {}


def _read_mtl_text(path: Path) -> dict[str, str]:
    try:
        return _parse_mtl_lines(path.read_text(encoding="utf-8", errors="ignore").splitlines())
    except OSError:
        return {}


def _parse_mtl_lines(lines: Sequence[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        match = _MTL_KV_RE.match(line)
        if not match:
            continue
        key, value = match.groups()
        values[key] = value.strip().strip('"')
    return values


def _normalize_landsat_mtl_date(value: str | None) -> str:
    text = str(value or "").strip().strip('"')
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return "".join(match.groups())
    if re.fullmatch(r"\d{8}", text):
        return text
    return ""


def _build_landsat_acquisition_timestamp(
    acq_date: str,
    scene_center_time: str | None,
) -> str:
    match = re.search(r"(\d{2}):(\d{2}):(\d{2})", str(scene_center_time or ""))
    if not match:
        return f"{acq_date}T000000"
    return f"{acq_date}T{match.group(1)}{match.group(2)}{match.group(3)}"


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


def _env_flag_value(value: str | None, *, default: bool) -> bool:
    if value is None or not str(value).strip():
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_int_value(value: str | None, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value) if value is not None and str(value).strip() else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), min(int(parsed), int(maximum)))


def _spark_dir_values(configured: str) -> list[str]:
    raw_parts = [
        part.strip()
        for item in str(configured or DEFAULT_SPARK_LOCAL_DIRS).split(os.pathsep)
        for part in item.split(",")
        if part.strip()
    ]
    return raw_parts or [DEFAULT_SPARK_LOCAL_DIRS]


def _sen2like_execution_mode(*, product_count: int, worker_count: int) -> str:
    if product_count <= 1:
        return "single_product_parallel_steps" if worker_count > 1 else "single_product"
    if worker_count > 1:
        return "parallel_multi_product"
    return "batched_multi_product_single_worker"


def _response_metadata(
    *,
    vendor_root: Path,
    request: Sen2LikeNormalizeRequest,
    spark_local_dirs: str | None,
    prepared_products: Sequence[PreparedProduct],
    input_issues: Sequence[dict[str, str]],
    output_issues: Sequence[dict[str, str]],
    preparation_duration_seconds: float | None = None,
    pipeline_duration_seconds: float | None = None,
    subprocess_attempts: Sequence[dict[str, Any]] | None = None,
    direct_zarr_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    product_count = len(prepared_products)
    worker_count = int(request.workers)
    metadata: dict[str, Any] = {
        "vendor_root": str(vendor_root),
        "pipeline_py": str(pipeline_path(vendor_root)),
        "spark_master": request.spark_master,
        "spark_local_dirs": spark_local_dirs,
        "pyspark_service": True,
        "execution_mode": _sen2like_execution_mode(
            product_count=product_count,
            worker_count=worker_count,
        ),
        "product_count": product_count,
        "workers": worker_count,
        "product_parallelism": product_count > 1 and worker_count > 1,
        "tile_parallelism": worker_count > 1,
        "band_parallelism": worker_count > 1,
        "nested_band_parallelism": _env_flag_value(
            os.getenv("NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"),
            default=True,
        ),
        "band_workers": _env_int_value(
            os.getenv("NIMBUS_SEN2LIKE_BAND_WORKERS"),
            default=2,
            minimum=1,
            maximum=32,
        ),
        "safe_retry_enabled": _env_flag_value(
            os.getenv("NIMBUS_SEN2LIKE_SAFE_RETRY"),
            default=True,
        ),
        "subprocess_attempts": [dict(item) for item in list(subprocess_attempts or [])],
        "tar_inputs_supported": True,
        "tar_inputs_are_extracted_before_pyspark": True,
        "preparation_duration_seconds": preparation_duration_seconds,
        "pipeline_duration_seconds": pipeline_duration_seconds,
        "direct_zarr_requested": _direct_zarr_enabled(request),
        "direct_zarr_output_dir": str(_direct_zarr_output_root(request)),
        "direct_zarr_status": "not_started",
        "direct_zarr_outputs": [],
        "direct_zarr_items": [],
        "direct_zarr_issues": [],
        "prepared_products": [
            {
                "original": product.original,
                "command_input": product.command_input,
                "output_name": product.output_name,
                "extracted": product.extracted,
                "preprocess": dict(product.preprocess or {}),
                **({"input_issue": product.input_issue} if product.input_issue else {}),
            }
            for product in prepared_products
        ],
        "input_issues": list(input_issues),
        "output_issues": list(output_issues),
    }
    if direct_zarr_metadata:
        metadata.update(dict(direct_zarr_metadata))
    return metadata


def _tail(value: str | bytes, *, limit: int = 6000) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if len(value) <= limit:
        return value
    return value[-limit:]
