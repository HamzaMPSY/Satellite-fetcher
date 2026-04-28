from __future__ import annotations

from datetime import datetime, timezone
import threading
from typing import Any

from nimbuschain_fetch.jobs.store import JobListFilters
from nimbuschain_fetch.models import JobState


class FetcherWorkerRuntimeSupport:
    """Worker heartbeat and runtime status helpers for the fetcher facade."""

    @staticmethod
    def start_worker_heartbeat_thread(rt: Any) -> None:
        if rt._heartbeat_thread is not None and rt._heartbeat_thread.is_alive():
            return
        rt._heartbeat_stop_event.clear()
        rt._heartbeat_thread = threading.Thread(
            target=rt._worker_heartbeat_loop,
            name="nimbus-worker-heartbeat",
            daemon=True,
        )
        rt._heartbeat_thread.start()

    @staticmethod
    def stop_worker_heartbeat_thread(rt: Any) -> None:
        thread = rt._heartbeat_thread
        if thread is None:
            return
        rt._heartbeat_stop_event.set()
        thread.join(timeout=2.0)
        rt._heartbeat_thread = None

    @staticmethod
    def worker_heartbeat_loop(rt: Any) -> None:
        interval_seconds = max(1.0, float(rt.settings.nimbus_worker_heartbeat_seconds))
        while not rt._heartbeat_stop_event.wait(interval_seconds):
            try:
                rt._publish_worker_heartbeat()
            except Exception:
                continue

    @staticmethod
    def download_coordinator_placeholder_status(rt: Any, *, status: str = "not_initialized") -> dict[str, Any]:
        configured_accounts = [
            {
                "account_label": str(item.get("label") or "primary").strip() or "primary",
                "active_downloads": 0,
                "cooldown_seconds": 0.0,
                "max_concurrent_downloads": int(rt.settings.nimbus_copernicus_account_pool_concurrency),
            }
            for item in rt.settings.copernicus_account_pool_accounts
        ]
        return {
            "status": status,
            "started": False,
            "closed": False,
            "timestamp": rt._now_iso(),
            "db_path": str(rt.settings.download_coordinator_db_path),
            "limits": {
                "job": dict(rt.settings.provider_job_limits_map),
                "control_plane": dict(rt.settings.provider_control_plane_limits_map),
                "data_plane": dict(rt.settings.provider_data_plane_limits_map),
            },
            "machine": {
                "active_downloads": 0,
                "active_download_limit": int(rt.settings.nimbus_download_global_limit),
                "disk_path": str(rt.settings.nimbus_data_dir),
                "disk_free_bytes": None,
                "min_free_bytes": int(rt.settings.nimbus_download_min_free_bytes or 0),
                "bandwidth_limit_bps": (
                    int(rt.settings.nimbus_download_global_max_bps)
                    if rt.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "providers": {
                "copernicus": {
                    "job_limit": int(rt.settings.provider_job_limits_map.get("copernicus", 1)),
                    "control_plane_limit": int(rt.settings.provider_control_plane_limits_map.get("copernicus", 1)),
                    "data_plane_limit": int(rt.settings.provider_data_plane_limits_map.get("copernicus", 1)),
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
                    "job_limit": int(rt.settings.provider_job_limits_map.get("usgs", 1)),
                    "control_plane_limit": int(rt.settings.provider_control_plane_limits_map.get("usgs", 1)),
                    "data_plane_limit": int(rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "active_prepares": 0,
                    "active_downloads": 0,
                    "adaptive_window_current": min(
                        2,
                        int(rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_peak": min(
                        2,
                        int(rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    ),
                    "adaptive_window_max": int(rt.settings.provider_data_plane_limits_map.get("usgs", 1)),
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

    @staticmethod
    def local_download_coordinator_report(rt: Any) -> dict[str, Any]:
        if rt._download_coordinator is None:
            return FetcherWorkerRuntimeSupport.download_coordinator_placeholder_status(rt)
        return rt._download_coordinator.snapshot()

    @staticmethod
    def wrap_download_coordinator_reports(
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

    @staticmethod
    def get_download_coordinator_status(rt: Any) -> dict[str, Any]:
        if rt._execution_enabled or rt._download_coordinator is not None:
            local_report = {
                "worker_id": rt._worker_id,
                "hostname": rt._worker_hostname,
                "pid": rt._worker_pid,
                "runtime_role": rt._runtime_role,
                "execution_enabled": rt._execution_enabled,
                "last_seen_at": rt._now_iso(),
                "snapshot": rt._local_download_coordinator_report(),
            }
            return FetcherWorkerRuntimeSupport.wrap_download_coordinator_reports(
                reports=[local_report],
                source="local_worker",
                summary=dict(local_report["snapshot"]),
                timestamp=rt._now_iso(),
            )

        stale_after = max(5, int(rt.settings.nimbus_worker_stale_seconds))
        now = datetime.now(timezone.utc)
        worker_reports: list[dict[str, Any]] = []
        for worker in rt.store.list_workers():
            last_seen = rt._parse_iso(worker.get("last_seen_at"))
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
            return FetcherWorkerRuntimeSupport.wrap_download_coordinator_reports(
                reports=worker_reports,
                source="worker_heartbeats",
                summary=dict(worker_reports[0]["snapshot"]),
                timestamp=rt._now_iso(),
            )

        placeholder = rt._download_coordinator_placeholder_status(status="unavailable")
        return FetcherWorkerRuntimeSupport.wrap_download_coordinator_reports(
            reports=[],
            source="worker_heartbeats",
            summary=placeholder,
            timestamp=rt._now_iso(),
        )

    @staticmethod
    def get_worker_status(rt: Any) -> dict[str, Any]:
        stale_after = max(5, int(rt.settings.nimbus_worker_stale_seconds))
        pruned_workers = int(rt.store.prune_stale_workers(stale_after))
        workers = list(rt.store.list_workers())
        now = datetime.now(timezone.utc)
        alive_workers: list[dict[str, Any]] = []
        stale_workers: list[dict[str, Any]] = []

        running_rows, running_total = rt.store.list_jobs(
            JobListFilters(
                states=(JobState.running.value, JobState.cancel_requested.value),
                page=1,
                page_size=max(1000, rt.settings.nimbus_max_jobs * 200),
            )
        )
        queued_rows, queued_total = rt.store.list_jobs(
            JobListFilters(
                state=JobState.queued.value,
                page=1,
                page_size=max(1000, rt.settings.nimbus_max_jobs * 200),
            )
        )
        running_by_provider: dict[str, int] = {}
        queued_by_provider: dict[str, int] = {}
        configured_provider_limits = {
            str(name).strip().lower(): max(1, int(limit))
            for name, limit in rt.settings.provider_job_limits_map.items()
        }

        running_by_worker: dict[str, int] = {}
        cancel_requested_by_worker: dict[str, int] = {}
        for row in running_rows:
            worker_id = str(row.get("worker_id") or "").strip()
            provider_name = str(row.get("provider") or "").strip().lower()
            if provider_name:
                running_by_provider[provider_name] = running_by_provider.get(provider_name, 0) + 1
            if not worker_id:
                continue
            state = str(row.get("state") or "").strip().lower()
            if state == JobState.cancel_requested.value:
                cancel_requested_by_worker[worker_id] = cancel_requested_by_worker.get(worker_id, 0) + 1
            else:
                running_by_worker[worker_id] = running_by_worker.get(worker_id, 0) + 1
        for row in queued_rows:
            provider_name = str(row.get("provider") or "").strip().lower()
            if provider_name:
                queued_by_provider[provider_name] = queued_by_provider.get(provider_name, 0) + 1

        worker_payloads: list[dict[str, Any]] = []
        capacity_total = 0
        capacity_used = 0
        provider_capacity_total: dict[str, int] = {}

        for worker in workers:
            last_seen = rt._parse_iso(worker.get("last_seen_at"))
            age_seconds = None
            is_alive = False
            if last_seen is not None:
                age_seconds = max(0.0, (now - last_seen).total_seconds())
                is_alive = age_seconds <= stale_after
            worker_id = str(worker.get("worker_id") or "")
            running_count = int(running_by_worker.get(worker_id, worker.get("active_running_jobs", 0) or 0))
            cancel_requested_count = int(
                cancel_requested_by_worker.get(
                    worker_id,
                    worker.get("active_cancel_requested_jobs", 0) or 0,
                )
            )
            max_concurrent = max(1, int(worker.get("max_concurrent_jobs", 1) or 1))
            worker_capacity_used = running_count + cancel_requested_count
            capacity_total += max_concurrent
            capacity_used += min(max_concurrent, worker_capacity_used)
            item = {
                **worker,
                "status": "alive" if is_alive else "stale",
                "age_seconds": age_seconds,
                "active_running_jobs": running_count,
                "active_cancel_requested_jobs": cancel_requested_count,
                "available_slots": max(0, max_concurrent - worker_capacity_used),
            }
            worker_payloads.append(item)
            if is_alive:
                alive_workers.append(item)
                provider_limits = dict(item.get("provider_limits") or {})
                provider_names = set(configured_provider_limits) | {
                    str(name).strip().lower() for name in provider_limits.keys() if str(name).strip()
                }
                for provider_name in provider_names:
                    limit = provider_limits.get(
                        provider_name,
                        configured_provider_limits.get(provider_name, 1),
                    )
                    provider_capacity_total[provider_name] = (
                        provider_capacity_total.get(provider_name, 0) + max(1, int(limit or 1))
                    )
            else:
                stale_workers.append(item)

        capacity_available = max(0, capacity_total - capacity_used)
        can_accept_work = bool(alive_workers) and capacity_available > 0
        ready = bool(alive_workers)
        status = "ready" if ready else "not_ready"
        if ready and not can_accept_work:
            status = "saturated"

        provider_names = (
            set(configured_provider_limits)
            | set(running_by_provider)
            | set(queued_by_provider)
            | set(provider_capacity_total)
        )
        provider_capacity: dict[str, dict[str, int | bool]] = {}
        for provider_name in sorted(provider_names):
            total_limit = max(
                0,
                int(
                    provider_capacity_total.get(
                        provider_name,
                        configured_provider_limits.get(provider_name, 0),
                    )
                ),
            )
            running_count = int(running_by_provider.get(provider_name, 0))
            queued_count = int(queued_by_provider.get(provider_name, 0))
            available = max(0, total_limit - running_count)
            provider_capacity[provider_name] = {
                "limit_total": total_limit,
                "running": running_count,
                "queued": queued_count,
                "available": available,
                "blocked_by_limit": queued_count > 0 and available <= 0,
            }

        return {
            "status": status,
            "ready": ready,
            "timestamp": now.isoformat(),
            "worker_stale_seconds": stale_after,
            "workers_pruned": pruned_workers,
            "workers_alive": len(alive_workers),
            "workers_stale": len(stale_workers),
            "workers_total": len(worker_payloads),
            "queued_jobs": int(queued_total),
            "running_jobs": int(running_total),
            "capacity_total": capacity_total,
            "capacity_used": capacity_used,
            "capacity_available": capacity_available,
            "can_accept_work": can_accept_work,
            "provider_job_limits": configured_provider_limits,
            "provider_control_plane_limits": rt.settings.provider_control_plane_limits_map,
            "provider_data_plane_limits": rt.settings.provider_data_plane_limits_map,
            "download_guardrails": {
                "global_active_limit": int(rt.settings.nimbus_download_global_limit),
                "min_free_bytes": int(rt.settings.nimbus_download_min_free_bytes or 0),
                "global_max_bps": (
                    int(rt.settings.nimbus_download_global_max_bps)
                    if rt.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "provider_capacity": provider_capacity,
            "workers": worker_payloads,
        }

    @staticmethod
    def publish_worker_heartbeat(rt: Any) -> dict[str, Any] | None:
        if not rt._execution_enabled:
            return None
        running_rows, running_total = rt.store.list_jobs(
            JobListFilters(
                states=(JobState.running.value,),
                worker_id=rt._worker_id,
                page=1,
                page_size=max(1, rt.settings.nimbus_max_jobs * 2),
            )
        )
        cancel_rows, cancel_total = rt.store.list_jobs(
            JobListFilters(
                states=(JobState.cancel_requested.value,),
                worker_id=rt._worker_id,
                page=1,
                page_size=max(1, rt.settings.nimbus_max_jobs * 2),
            )
        )
        queued_rows, queued_total = rt.store.list_jobs(
            JobListFilters(
                state=JobState.queued.value,
                page=1,
                page_size=1,
            )
        )
        _ = running_rows, cancel_rows, queued_rows
        download_coordinator_report = rt._local_download_coordinator_report()
        return rt.store.upsert_worker_heartbeat(
            rt._worker_id,
            {
                "runtime_role": rt._runtime_role,
                "execution_enabled": rt._execution_enabled,
                "max_concurrent_jobs": rt.settings.nimbus_max_jobs,
                "queue_poll_seconds": rt.settings.nimbus_queue_poll_seconds,
                "heartbeat_interval_seconds": rt.settings.nimbus_worker_heartbeat_seconds,
                "provider_limits": rt.settings.provider_job_limits_map,
                "hostname": rt._worker_hostname,
                "pid": rt._worker_pid,
                "active_running_jobs": running_total,
                "active_cancel_requested_jobs": cancel_total,
                "queue_backlog": queued_total,
                "started_at": rt._worker_started_at,
                "last_seen_at": rt._now_iso(),
                "metadata": {
                    "runtime_role": rt._runtime_role,
                    "executor_present": rt._executor is not None,
                    "provider_job_limits": rt.settings.provider_job_limits_map,
                    "provider_control_plane_limits": rt.settings.provider_control_plane_limits_map,
                    "provider_data_plane_limits": rt.settings.provider_data_plane_limits_map,
                    "download_guardrails": {
                        "global_active_limit": int(rt.settings.nimbus_download_global_limit),
                        "min_free_bytes": int(rt.settings.nimbus_download_min_free_bytes or 0),
                        "global_max_bps": (
                            int(rt.settings.nimbus_download_global_max_bps)
                            if rt.settings.nimbus_download_global_max_bps
                            else None
                        ),
                    },
                    "download_coordinator": download_coordinator_report,
                },
            },
        )
