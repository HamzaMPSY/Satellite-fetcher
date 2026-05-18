from __future__ import annotations

import os
from typing import Any

from nimbuschain_shared import runtime as _shared_runtime

importlib = _shared_runtime.importlib


def normalize_device_name(value: str | None) -> str:
    return _shared_runtime.normalize_device_name(value)


def resolve_inference_device(*, explicit: str | None, env_var: str) -> str:
    explicit_value = normalize_device_name(explicit)
    if explicit_value != "auto":
        return _validated_device_choice(explicit_value)
    env_value = normalize_device_name(os.getenv(env_var))
    if env_value != "auto":
        return _validated_device_choice(env_value)
    auto_value = _auto_detect_device()
    return auto_value or "cpu"


def runtime_device_status(*, explicit: str | None, env_var: str) -> dict[str, Any]:
    requested_explicit = normalize_device_name(explicit)
    requested_env = normalize_device_name(os.getenv(env_var))
    available = _available_devices()
    auto_detected = _auto_detect_device() or "cpu"
    resolved = resolve_inference_device(explicit=explicit, env_var=env_var)
    return {
        "explicit": requested_explicit,
        "env": requested_env,
        "auto_detected": auto_detected,
        "resolved": resolved,
        "available": available,
    }


def parallel_worker_count(
    *,
    device: str | None,
    env_var: str,
    cpu_default: int,
    gpu_default: int = 1,
    hard_limit: int = 8,
) -> int:
    return _shared_runtime.parallel_worker_count(
        device=device,
        env_var=env_var,
        cpu_default=cpu_default,
        gpu_default=gpu_default,
        hard_limit=hard_limit,
    )


def batch_size_for_device(
    *,
    device: str | None,
    env_var: str,
    cpu_default: int = 1,
    gpu_default: int = 2,
    hard_limit: int = 16,
) -> int:
    return _shared_runtime.batch_size_for_device(
        device=device,
        env_var=env_var,
        cpu_default=cpu_default,
        gpu_default=gpu_default,
        hard_limit=hard_limit,
    )


def _auto_detect_device() -> str | None:
    available = _available_devices()
    if available["cuda"]:
        return "cuda"
    if available["mps"]:
        return "mps"
    return "cpu"


def _validated_device_choice(value: str) -> str:
    normalized = normalize_device_name(value)
    if normalized in {"auto", "cpu"}:
        return "cpu" if normalized == "auto" else normalized
    available = _available_devices()
    if available.get(normalized):
        return normalized
    return _auto_detect_device() or "cpu"


def _available_devices() -> dict[str, bool]:
    return _shared_runtime._available_devices()


__all__ = [
    "batch_size_for_device",
    "normalize_device_name",
    "parallel_worker_count",
    "resolve_inference_device",
    "runtime_device_status",
    "importlib",
    "_available_devices",
]
