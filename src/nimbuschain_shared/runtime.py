from __future__ import annotations

import importlib
import os
from typing import Any


def normalize_device_name(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"", "auto"}:
        return "auto"
    if normalized in {"cuda", "gpu", "nvidia"}:
        return "cuda"
    if normalized in {"mps", "metal"}:
        return "mps"
    if normalized in {"cpu"}:
        return "cpu"
    return normalized


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
    raw = str(os.getenv(env_var) or "").strip()
    try:
        configured = int(raw) if raw else None
    except ValueError:
        configured = None
    default_value = gpu_default if normalize_device_name(device) in {"cuda", "mps"} else cpu_default
    value = configured if configured is not None else default_value
    return max(1, min(int(value), hard_limit))


def batch_size_for_device(
    *,
    device: str | None,
    env_var: str,
    cpu_default: int = 1,
    gpu_default: int = 2,
    hard_limit: int = 16,
) -> int:
    raw = str(os.getenv(env_var) or "").strip()
    try:
        configured = int(raw) if raw else None
    except ValueError:
        configured = None
    default_value = gpu_default if normalize_device_name(device) in {"cuda", "mps"} else cpu_default
    value = configured if configured is not None else default_value
    return max(1, min(int(value), hard_limit))


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
    try:
        torch = importlib.import_module("torch")
    except Exception:
        return {"cpu": True, "cuda": False, "mps": False}

    cuda_available = False
    try:
        cuda_available = bool(getattr(torch.cuda, "is_available", lambda: False)())
    except Exception:
        cuda_available = False

    mps_available = False
    try:
        mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
        if mps_backend is not None:
            mps_available = bool(getattr(mps_backend, "is_available", lambda: False)())
    except Exception:
        mps_available = False
    return {"cpu": True, "cuda": cuda_available, "mps": mps_available}
