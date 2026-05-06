from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class StageCliInvocation:
    command: list[str]
    return_code: int
    stdout: str
    stderr: str
    payload: dict[str, Any]
    error: str | None = None


def build_stage_cli_command(
    *,
    action: str,
    provider: str,
    collection: str,
    product_type: str | None = None,
    job_id: str | None = None,
    mask_types: list[str] | tuple[str, ...] | None = None,
    cube_mode: str = "none",
    target_stage: str | None = None,
    run_stage: str | None = None,
    raw_uri: str | None = None,
    sen2like_service_url: str | None = None,
    sen2like_working_dir: str | None = None,
    sen2like_workers: int | None = None,
    python_executable: str | None = None,
) -> list[str]:
    if action not in {"plan", "run-stage"}:
        raise ValueError("action must be either 'plan' or 'run-stage'.")
    if action == "run-stage" and not run_stage:
        raise ValueError("run_stage is required when action='run-stage'.")

    command = [
        python_executable or sys.executable,
        "-m",
        "nimbuschain_fetch.stage_cli",
        action,
        "--provider",
        provider,
        "--collection",
        collection,
        "--cube-mode",
        cube_mode,
    ]
    if sen2like_workers is not None:
        command.extend(["--sen2like-workers", str(int(sen2like_workers))])
    if job_id:
        command.extend(["--job-id", job_id])
    if product_type:
        command.extend(["--product-type", product_type])
    normalized_masks = [item for item in list(mask_types or []) if item]
    if normalized_masks:
        command.extend(["--mask-types", ",".join(normalized_masks)])
    if raw_uri:
        command.extend(["--raw-uri", raw_uri])
    if sen2like_service_url:
        command.extend(["--sen2like-service-url", sen2like_service_url])
    if sen2like_working_dir:
        command.extend(["--sen2like-working-dir", sen2like_working_dir])
    if action == "plan" and target_stage:
        command.extend(["--target-stage", target_stage])
    if action == "run-stage":
        command.extend(["--stage", str(run_stage)])
    return command


def run_stage_cli(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: int = 120,
    extra_env: dict[str, str] | None = None,
) -> StageCliInvocation:
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    src_dir = str(cwd / "src")
    if src_dir not in current_pythonpath.split(os.pathsep):
        env["PYTHONPATH"] = os.pathsep.join([src_dir, current_pythonpath]) if current_pythonpath else src_dir
    if extra_env:
        env.update(extra_env)

    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return StageCliInvocation(
            command=command,
            return_code=124,
            stdout=str(exc.stdout or ""),
            stderr=str(exc.stderr or ""),
            payload={"status": "failed", "error": f"CLI timed out after {timeout_seconds}s"},
            error=f"CLI timed out after {timeout_seconds}s",
        )

    payload, error = parse_stage_cli_payload(completed.stdout, completed.stderr)
    return StageCliInvocation(
        command=command,
        return_code=int(completed.returncode),
        stdout=completed.stdout,
        stderr=completed.stderr,
        payload=payload,
        error=error,
    )


def parse_stage_cli_payload(stdout: str, stderr: str) -> tuple[dict[str, Any], str | None]:
    raw = stdout.strip() or stderr.strip()
    if not raw:
        return {}, "CLI returned no output."
    last_line = raw.splitlines()[-1]
    try:
        payload = json.loads(last_line)
    except json.JSONDecodeError as exc:
        return {}, f"CLI returned non-JSON output: {exc}"
    if not isinstance(payload, dict):
        return {}, "CLI JSON payload is not an object."
    return payload, None
