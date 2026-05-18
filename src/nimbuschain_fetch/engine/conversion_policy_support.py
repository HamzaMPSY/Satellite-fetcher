from __future__ import annotations

import os
from typing import Any

from nimbuschain_shared.runtime import normalize_device_name, resolve_inference_device


class FetcherConversionPolicySupport:
    """Parallelism and conversion-policy helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def scene_parallelism_target_from_download(
        self,
        *,
        pipeline_metadata: dict[str, Any] | None,
        total: int,
    ) -> int:
        if total <= 1:
            return 1
        metadata = dict(pipeline_metadata or {})
        selected_accounts = int(metadata.get("account_pool_selected_accounts", 0) or 0)
        account_labels: set[str] = set()
        for item in list(metadata.get("account_pool_assignments") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        for item in list(dict(metadata.get("download_telemetry") or {}).get("accounts") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        if account_labels:
            selected_accounts = max(selected_accounts, len(account_labels))
        if selected_accounts <= 1:
            selected_accounts = min(total, 4)
        return max(
            1,
            min(
                int(selected_accounts or 1),
                max(1, total),
                max(1, int(self._rt.settings.nimbus_max_jobs or 1)),
                4,
            ),
        )

    @staticmethod
    def zarr_convert_max_workers(
        *,
        total: int,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
        os_module: Any = os,
    ) -> int:
        raw = str(os_module.getenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        cpu_budget = max(1, min(4, max(1, int((os_module.cpu_count() or 2) / 2))))
        default_value = min(max(1, int(preferred_parallelism or 1)), cpu_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))

    @staticmethod
    def integrated_mask_max_workers(
        *,
        total: int,
        inference_device: str | None,
        water_inference_device: str | None,
        remote_runtime: dict[str, Any] | None = None,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
        os_module: Any = os,
        resolve_inference_device_fn: Any = resolve_inference_device,
        normalize_device_name_fn: Any = normalize_device_name,
    ) -> int:
        raw = str(os_module.getenv("NIMBUS_MASK_SCENE_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        resolved_cloud = resolve_inference_device_fn(
            explicit=inference_device,
            env_var="NIMBUS_CLOUDMASK_DEVICE",
        )
        resolved_water = resolve_inference_device_fn(
            explicit=water_inference_device,
            env_var="NIMBUS_WATERMASK_DEVICE",
        )
        runtime_payload = dict(remote_runtime or {})
        remote_cloud = normalize_device_name_fn(
            dict(runtime_payload.get("cloud") or {}).get("resolved")
        )
        remote_water = normalize_device_name_fn(
            dict(runtime_payload.get("water") or {}).get("resolved")
        )
        if remote_cloud not in {"", "auto"}:
            resolved_cloud = remote_cloud
        if remote_water not in {"", "auto"}:
            resolved_water = remote_water
        remote_service = remote_runtime is not None
        has_accelerator = any(device in {"cuda", "mps"} for device in {resolved_cloud, resolved_water})
        cpu_budget = max(1, min(4, max(1, int((os_module.cpu_count() or 2) / 2))))
        if remote_service:
            heuristic_budget = min(cpu_budget, 2 if has_accelerator else 1)
        else:
            heuristic_budget = min(cpu_budget, 3 if has_accelerator else 2)
        default_target = (
            max(1, int(preferred_parallelism or 1))
            if preferred_parallelism is not None
            else (2 if total > 1 and has_accelerator else 1)
        )
        default_value = min(default_target, heuristic_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))
