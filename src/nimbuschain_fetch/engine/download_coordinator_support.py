from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nimbuschain_fetch.download.coordinator import DownloadBatchResult, DownloadCoordinator


class FetcherDownloadCoordinatorSupport:
    """Download-coordinator lifecycle, capability, and status helpers."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def placeholder_status(self, *, status: str = "not_initialized") -> dict[str, Any]:
        configured_accounts = [
            {
                "account_label": str(item.get("label") or "primary").strip() or "primary",
                "active_downloads": 0,
                "cooldown_seconds": 0.0,
                "max_concurrent_downloads": int(self._rt.settings.nimbus_copernicus_account_pool_concurrency),
            }
            for item in self._rt.settings.copernicus_account_pool_accounts
        ]
        return {
            "status": status,
            "started": False,
            "closed": False,
            "timestamp": self._rt._now_iso(),
            "db_path": str(self._rt.settings.download_coordinator_db_path),
            "limits": {
                "job": dict(self._rt.settings.provider_job_limits_map),
                "control_plane": dict(self._rt.settings.provider_control_plane_limits_map),
                "data_plane": dict(self._rt.settings.provider_data_plane_limits_map),
            },
            "machine": {
                "active_downloads": 0,
                "active_download_limit": int(self._rt.settings.nimbus_download_global_limit),
                "disk_path": str(self._rt.settings.nimbus_data_dir),
                "disk_free_bytes": None,
                "min_free_bytes": int(self._rt.settings.nimbus_download_min_free_bytes or 0),
                "bandwidth_limit_bps": (
                    int(self._rt.settings.nimbus_download_global_max_bps)
                    if self._rt.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "providers": {
                "copernicus": {
                    "job_limit": int(self._rt.settings.provider_job_limits_map.get("copernicus", 1)),
                    "control_plane_limit": int(self._rt.settings.provider_control_plane_limits_map.get("copernicus", 1)),
                    "data_plane_limit": int(self._rt.settings.provider_data_plane_limits_map.get("copernicus", 1)),
                    "active_downloads": 0,
                    "pending_tasks": 0,
                    "counts": {
                        "queued": 0,
                        "preparing": 0,
                        "ready": 0,
                        "downloading": 0,
                        "done": 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                    "accounts_configured": len(configured_accounts),
                    "accounts": configured_accounts,
                },
                "usgs": {
                    "job_limit": int(self._rt.settings.provider_job_limits_map.get("usgs", 1)),
                    "control_plane_limit": int(self._rt.settings.provider_control_plane_limits_map.get("usgs", 1)),
                    "data_plane_limit": int(self._rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "active_prepares": 0,
                    "active_downloads": 0,
                    "adaptive_window_current": min(
                        2,
                        int(self._rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_peak": min(
                        2,
                        int(self._rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_max": int(self._rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "success_streak": 0,
                    "cooldown_seconds": 0.0,
                    "pending_tasks": 0,
                    "counts": {
                        "queued": 0,
                        "preparing": 0,
                        "ready": 0,
                        "downloading": 0,
                        "done": 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                },
            },
            "jobs": {
                "pending_tasks_total": 0,
                "pending_jobs_total": 0,
                "pending_by_job": [],
            },
            "tasks": {
                "active": [],
                "recent_terminal": [],
            },
        }

    def local_report(self) -> dict[str, Any]:
        if self._rt._download_coordinator is None:
            return self.placeholder_status()
        return self._rt._download_coordinator.snapshot()

    @staticmethod
    def wrap_reports(
        *,
        reports: list[dict[str, Any]],
        source: str,
        summary: dict[str, Any],
        timestamp: str,
    ) -> dict[str, Any]:
        return {
            "status": str(summary.get("status") or ("unavailable" if not reports else "unknown")),
            "source": source,
            "timestamp": timestamp,
            "workers_reporting": len(reports),
            "summary": summary,
            "workers": reports,
        }

    def get_status(self) -> dict[str, Any]:
        if self._rt._execution_enabled or self._rt._download_coordinator is not None:
            local_report = {
                "worker_id": self._rt._worker_id,
                "hostname": self._rt._worker_hostname,
                "pid": self._rt._worker_pid,
                "runtime_role": self._rt._runtime_role,
                "execution_enabled": self._rt._execution_enabled,
                "last_seen_at": self._rt._now_iso(),
                "snapshot": self.local_report(),
            }
            return self.wrap_reports(
                reports=[local_report],
                source="local_worker",
                summary=dict(local_report["snapshot"]),
                timestamp=self._rt._now_iso(),
            )

        stale_after = max(5, int(self._rt.settings.nimbus_worker_stale_seconds))
        now = datetime.now(timezone.utc)
        worker_reports: list[dict[str, Any]] = []
        for worker in self._rt.store.list_workers():
            last_seen = self._rt._parse_iso(worker.get("last_seen_at"))
            if last_seen is None:
                continue
            age_seconds = max(0.0, (now - last_seen).total_seconds())
            if age_seconds > stale_after or not bool(worker.get("execution_enabled", False)):
                continue
            metadata = dict(worker.get("metadata") or {})
            snapshot = metadata.get("download_coordinator")
            if not isinstance(snapshot, dict):
                continue
            worker_reports.append(
                {
                    "worker_id": str(worker.get("worker_id") or "").strip(),
                    "hostname": str(worker.get("hostname") or "").strip(),
                    "pid": worker.get("pid"),
                    "runtime_role": str(worker.get("runtime_role") or "").strip(),
                    "execution_enabled": bool(worker.get("execution_enabled", False)),
                    "last_seen_at": worker.get("last_seen_at"),
                    "snapshot": snapshot,
                }
            )

        if worker_reports:
            return self.wrap_reports(
                reports=worker_reports,
                source="worker_heartbeats",
                summary=dict(worker_reports[0]["snapshot"]),
                timestamp=self._rt._now_iso(),
            )

        placeholder = self.placeholder_status(status="unavailable")
        return self.wrap_reports(
            reports=[],
            source="worker_heartbeats",
            summary=placeholder,
            timestamp=self._rt._now_iso(),
        )

    def instance(
        self,
        *,
        coordinator_cls: type[DownloadCoordinator] = DownloadCoordinator,
    ) -> DownloadCoordinator:
        if self._rt._download_coordinator is None:
            self._rt._download_coordinator = coordinator_cls(self._rt.settings)
        return self._rt._download_coordinator

    @staticmethod
    def supports(provider_name: str, provider: Any) -> bool:
        _ = provider_name
        return callable(getattr(provider, "download_with_coordinator", None))

    def download_with_coordinator(
        self,
        *,
        job_id: str,
        provider_name: str,
        provider: Any,
        collection: str,
        product_ids: list[str],
        output_dir: Path,
        progress_callback: Any,
        retry_callback: Any,
        cancel_checker: Any,
        download_strategy: str,
        coordinator_cls: type[DownloadCoordinator] = DownloadCoordinator,
    ) -> DownloadBatchResult:
        coordinator = self.instance(coordinator_cls=coordinator_cls)
        result = coordinator.download_products(
            job_id=job_id,
            provider_name=provider_name,
            provider=provider,
            collection=collection,
            product_ids=product_ids,
            output_dir=str(output_dir),
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            cancel_checker=cancel_checker,
            download_strategy=download_strategy,
        )
        setattr(provider, "last_download_metadata", dict(result.metadata or {}))
        return result
