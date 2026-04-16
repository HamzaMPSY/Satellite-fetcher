from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from nimbuschain_fetch.download.download_manager import (
    CancelChecker,
    DownloadCancelled,
    DownloadManager,
    ProgressCallback,
    RetryCallback,
)
from nimbuschain_fetch.providers.copernicus import CopernicusProvider
from nimbuschain_fetch.providers.usgs import UsgsProvider
from nimbuschain_fetch.settings import Settings


TASK_STATUS_QUEUED = "queued"
TASK_STATUS_PREPARING = "preparing"
TASK_STATUS_READY = "ready"
TASK_STATUS_DOWNLOADING = "downloading"
TASK_STATUS_DONE = "done"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"

TERMINAL_TASK_STATUSES = {
    TASK_STATUS_DONE,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
}
ACTIVE_COORDINATOR_TASK_STATUSES = (
    TASK_STATUS_QUEUED,
    TASK_STATUS_PREPARING,
    TASK_STATUS_READY,
    TASK_STATUS_DOWNLOADING,
)


@dataclass(slots=True)
class DownloadBatchResult:
    paths: list[str]
    metadata: dict[str, Any]


@dataclass(slots=True)
class _TaskRuntimeContext:
    progress_callback: ProgressCallback | None
    retry_callback: RetryCallback | None
    cancel_checker: CancelChecker | None


class GlobalBandwidthLimiter:
    def __init__(self, rate_bps: int):
        self._rate_bps = max(1, int(rate_bps))
        self._lock = threading.Lock()
        self._next_slot_at = 0.0

    async def acquire(self, byte_count: int) -> None:
        amount = max(0, int(byte_count))
        if amount <= 0:
            return

        now = time.monotonic()
        with self._lock:
            start_at = max(now, self._next_slot_at)
            self._next_slot_at = start_at + (amount / float(self._rate_bps))
        delay = max(0.0, start_at - now)
        if delay > 0:
            await asyncio.sleep(delay)


class DownloadCoordinatorStore:
    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS download_tasks (
                    task_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    collection TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    output_dir TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_url TEXT,
                    file_name TEXT,
                    output_path TEXT,
                    account_label TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    retry_after TEXT,
                    bytes_downloaded INTEGER NOT NULL DEFAULT 0,
                    bytes_total INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    UNIQUE(provider, job_id, product_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_download_tasks_provider_status_updated
                ON download_tasks(provider, status, updated_at)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_download_tasks_job_provider
                ON download_tasks(job_id, provider, updated_at)
                """
            )
            self._conn.commit()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _row_to_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "task_id": row["task_id"],
            "provider": row["provider"],
            "job_id": row["job_id"],
            "collection": row["collection"],
            "product_id": row["product_id"],
            "output_dir": row["output_dir"],
            "status": row["status"],
            "source_url": row["source_url"],
            "file_name": row["file_name"],
            "output_path": row["output_path"],
            "account_label": row["account_label"],
            "attempts": int(row["attempts"] or 0),
            "retry_after": row["retry_after"],
            "bytes_downloaded": int(row["bytes_downloaded"] or 0),
            "bytes_total": int(row["bytes_total"] or 0),
            "error_text": row["error_text"],
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
        }

    def reset_inflight_tasks(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE download_tasks
                SET status = CASE
                        WHEN provider = 'usgs' AND COALESCE(source_url, '') <> '' THEN ?
                        ELSE ?
                    END,
                    account_label = NULL,
                    retry_after = NULL,
                    error_text = NULL,
                    updated_at = ?,
                    started_at = NULL,
                    finished_at = NULL
                WHERE status IN (?, ?)
                """,
                (
                    TASK_STATUS_READY,
                    TASK_STATUS_QUEUED,
                    self._utc_now(),
                    TASK_STATUS_PREPARING,
                    TASK_STATUS_DOWNLOADING,
                ),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.ProgrammingError:
                pass

    def ensure_task(
        self,
        *,
        task_id: str,
        provider: str,
        job_id: str,
        collection: str,
        product_id: str,
        output_dir: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        now = self._utc_now()
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT * FROM download_tasks
                WHERE provider = ? AND job_id = ? AND product_id = ?
                """,
                (provider, job_id, product_id),
            ).fetchone()

            if existing is None:
                self._conn.execute(
                    """
                    INSERT INTO download_tasks(
                        task_id, provider, job_id, collection, product_id, output_dir,
                        status, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        provider,
                        job_id,
                        collection,
                        product_id,
                        output_dir,
                        TASK_STATUS_QUEUED,
                        json.dumps(metadata or {}),
                        now,
                        now,
                    ),
                )
                self._conn.commit()
                row = self._conn.execute(
                    "SELECT * FROM download_tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                return self._row_to_task(row) or {}

            row = self._row_to_task(existing) or {}
            merged_metadata = {
                **dict(row.get("metadata") or {}),
                **dict(metadata or {}),
            }
            should_requeue = False
            if row.get("status") in {TASK_STATUS_FAILED, TASK_STATUS_CANCELLED}:
                should_requeue = True
            if row.get("status") == TASK_STATUS_DONE:
                path_value = str(row.get("output_path") or "").strip()
                if not path_value or not Path(path_value).exists():
                    should_requeue = True

            if should_requeue:
                self._conn.execute(
                    """
                    UPDATE download_tasks
                    SET collection = ?,
                        output_dir = ?,
                        status = ?,
                        source_url = NULL,
                        file_name = NULL,
                        output_path = NULL,
                        account_label = NULL,
                        retry_after = NULL,
                        bytes_downloaded = 0,
                        bytes_total = 0,
                        error_text = NULL,
                        metadata_json = ?,
                        updated_at = ?,
                        started_at = NULL,
                        finished_at = NULL
                    WHERE task_id = ?
                    """,
                    (
                        collection,
                        output_dir,
                        TASK_STATUS_QUEUED,
                        json.dumps(merged_metadata),
                        now,
                        row["task_id"],
                    ),
                )
            else:
                self._conn.execute(
                    """
                    UPDATE download_tasks
                    SET collection = ?,
                        output_dir = ?,
                        metadata_json = ?,
                        updated_at = ?
                    WHERE task_id = ?
                    """,
                    (
                        collection,
                        output_dir,
                        json.dumps(merged_metadata),
                        now,
                        row["task_id"],
                    ),
                )
            self._conn.commit()
            refreshed = self._conn.execute(
                "SELECT * FROM download_tasks WHERE task_id = ?",
                (row["task_id"],),
            ).fetchone()
            return self._row_to_task(refreshed) or {}

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM download_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row)

    def list_tasks(self, task_ids: list[str]) -> list[dict[str, Any]]:
        if not task_ids:
            return []
        placeholders = ", ".join("?" for _ in task_ids)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM download_tasks
                WHERE task_id IN ({placeholders})
                ORDER BY created_at ASC
                """,
                task_ids,
            ).fetchall()
        return [self._row_to_task(row) for row in rows if row]

    def list_job_tasks(self, *, provider: str, job_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM download_tasks
                WHERE provider = ? AND job_id = ?
                ORDER BY created_at ASC
                """,
                (provider, job_id),
            ).fetchall()
        return [self._row_to_task(row) for row in rows if row]

    def list_candidate_tasks(
        self,
        *,
        provider: str,
        statuses: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ", ".join("?" for _ in statuses)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM download_tasks
                WHERE provider = ?
                  AND status IN ({placeholders})
                ORDER BY created_at ASC, updated_at ASC
                """,
                [provider, *statuses],
            ).fetchall()
        return [self._row_to_task(row) for row in rows if row]

    def list_tasks_by_statuses(
        self,
        statuses: tuple[str, ...],
        *,
        limit: int = 100,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ", ".join("?" for _ in statuses)
        order_direction = "DESC" if descending else "ASC"
        query_limit = max(1, int(limit))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM download_tasks
                WHERE status IN ({placeholders})
                ORDER BY updated_at {order_direction}, created_at {order_direction}
                LIMIT ?
                """,
                [*statuses, query_limit],
            ).fetchall()
        return [self._row_to_task(row) for row in rows if row]

    def count_tasks_by_provider_status(self) -> dict[str, dict[str, int]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT provider, status, COUNT(*) AS count_value
                FROM download_tasks
                GROUP BY provider, status
                ORDER BY provider ASC, status ASC
                """
            ).fetchall()

        summary: dict[str, dict[str, int]] = defaultdict(dict)
        for row in rows:
            provider = str(row["provider"] or "").strip().lower()
            status = str(row["status"] or "").strip().lower()
            if not provider or not status:
                continue
            summary[provider][status] = int(row["count_value"] or 0)
        return {provider: dict(counts) for provider, counts in summary.items()}

    def summarize_pending_jobs(self, *, limit: int = 25) -> list[dict[str, Any]]:
        query_limit = max(1, int(limit))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT provider,
                       job_id,
                       SUM(CASE WHEN status = '{TASK_STATUS_QUEUED}' THEN 1 ELSE 0 END) AS queued_count,
                       SUM(CASE WHEN status = '{TASK_STATUS_PREPARING}' THEN 1 ELSE 0 END) AS preparing_count,
                       SUM(CASE WHEN status = '{TASK_STATUS_READY}' THEN 1 ELSE 0 END) AS ready_count,
                       SUM(CASE WHEN status = '{TASK_STATUS_DOWNLOADING}' THEN 1 ELSE 0 END) AS downloading_count,
                       COUNT(*) AS pending_count,
                       MAX(updated_at) AS updated_at
                FROM download_tasks
                WHERE status IN (?, ?, ?, ?)
                GROUP BY provider, job_id
                ORDER BY pending_count DESC, updated_at DESC
                LIMIT ?
                """,
                [
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_PREPARING,
                    TASK_STATUS_READY,
                    TASK_STATUS_DOWNLOADING,
                    query_limit,
                ],
            ).fetchall()

        pending_jobs: list[dict[str, Any]] = []
        for row in rows:
            pending_jobs.append(
                {
                    "provider": str(row["provider"] or "").strip().lower(),
                    "job_id": str(row["job_id"] or "").strip(),
                    "pending_tasks": int(row["pending_count"] or 0),
                    "queued": int(row["queued_count"] or 0),
                    "preparing": int(row["preparing_count"] or 0),
                    "ready": int(row["ready_count"] or 0),
                    "downloading": int(row["downloading_count"] or 0),
                    "updated_at": row["updated_at"],
                }
            )
        return pending_jobs

    def update_task(self, task_id: str, **fields: Any) -> dict[str, Any]:
        if not fields:
            current = self.get_task(task_id)
            return current or {}
        normalized: dict[str, Any] = {}
        for key, value in fields.items():
            if key == "metadata":
                normalized["metadata_json"] = json.dumps(value or {})
            else:
                normalized[key] = value
        normalized["updated_at"] = self._utc_now()
        assignments = ", ".join(f"{name} = ?" for name in normalized)
        params = [*normalized.values(), task_id]
        with self._lock:
            self._conn.execute(
                f"UPDATE download_tasks SET {assignments} WHERE task_id = ?",
                params,
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM download_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return self._row_to_task(row) or {}

    def cancel_pending_tasks(self, task_ids: list[str]) -> None:
        if not task_ids:
            return
        placeholders = ", ".join("?" for _ in task_ids)
        now = self._utc_now()
        with self._lock:
            self._conn.execute(
                f"""
                UPDATE download_tasks
                SET status = ?,
                    error_text = ?,
                    updated_at = ?,
                    finished_at = ?
                WHERE task_id IN ({placeholders})
                  AND status NOT IN (?, ?, ?)
                """,
                [
                    TASK_STATUS_CANCELLED,
                    "Download cancelled by job cancellation.",
                    now,
                    now,
                    *task_ids,
                    TASK_STATUS_DONE,
                    TASK_STATUS_FAILED,
                    TASK_STATUS_CANCELLED,
                ],
            )
            self._conn.commit()


class DownloadCoordinator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = DownloadCoordinatorStore(settings.download_coordinator_db_path)
        self.store.reset_inflight_tasks()

        self._condition = threading.Condition()
        self._runtime_contexts: dict[str, _TaskRuntimeContext] = {}
        self._job_round_robin: dict[str, deque[str]] = defaultdict(deque)
        self._copernicus_account_round_robin: deque[str] = deque()
        self._closed = False
        self._started = False
        self._scheduler_thread: threading.Thread | None = None

        self._active_downloads_global = 0
        self._copernicus_active_total = 0
        self._copernicus_active_by_account: dict[str, int] = defaultdict(int)
        self._copernicus_account_cooldown_until: dict[str, float] = defaultdict(float)
        self._usgs_active_prepares = 0
        self._usgs_active_downloads = 0
        self._usgs_cooldown_until = 0.0
        self._usgs_window_max = max(1, int(self.settings.provider_data_plane_limits_map.get("usgs", 1)))
        self._usgs_window_current = min(max(1, self._usgs_window_max), 2)
        self._usgs_window_peak = self._usgs_window_current
        self._usgs_success_streak = 0

        self._bandwidth_limiter: GlobalBandwidthLimiter | None = None
        if self.settings.nimbus_download_global_max_bps:
            self._bandwidth_limiter = GlobalBandwidthLimiter(
                int(self.settings.nimbus_download_global_max_bps)
            )

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _task_id_for(provider: str, job_id: str, product_id: str) -> str:
        raw = f"{provider}:{job_id}:{product_id}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_iso(value: str | None) -> datetime | None:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)

    def _retry_due(self, task: dict[str, Any]) -> bool:
        retry_after = self._parse_iso(str(task.get("retry_after") or "").strip())
        if retry_after is None:
            return True
        return retry_after <= datetime.now(timezone.utc)

    @staticmethod
    def _seconds_until_monotonic(deadline: float | None) -> float:
        if deadline is None:
            return 0.0
        return max(0.0, float(deadline) - time.monotonic())

    def _safe_disk_free_bytes(self) -> int | None:
        try:
            target = Path(self.settings.nimbus_data_dir)
            target.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(target)
            return int(usage.free)
        except Exception:
            return None

    @staticmethod
    def _task_snapshot_payload(task: dict[str, Any]) -> dict[str, Any]:
        return {
            "task_id": str(task.get("task_id") or "").strip(),
            "provider": str(task.get("provider") or "").strip().lower(),
            "job_id": str(task.get("job_id") or "").strip(),
            "collection": str(task.get("collection") or "").strip(),
            "product_id": str(task.get("product_id") or "").strip(),
            "status": str(task.get("status") or "").strip().lower(),
            "file_name": str(task.get("file_name") or "").strip(),
            "account_label": str(task.get("account_label") or "").strip(),
            "attempts": int(task.get("attempts") or 0),
            "bytes_downloaded": int(task.get("bytes_downloaded") or 0),
            "bytes_total": int(task.get("bytes_total") or 0),
            "retry_after": task.get("retry_after"),
            "updated_at": task.get("updated_at"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "error_text": str(task.get("error_text") or "").strip(),
        }

    @classmethod
    def _task_window_for_rows(cls, rows: list[dict[str, Any]]) -> tuple[str | None, str | None, float | None]:
        started_values = [
            parsed
            for row in rows
            if (parsed := cls._parse_iso(str(row.get("started_at") or "").strip())) is not None
        ]
        finished_values = [
            parsed
            for row in rows
            if (parsed := cls._parse_iso(str(row.get("finished_at") or "").strip())) is not None
        ]
        if not started_values:
            return None, None, None
        started_at = min(started_values)
        finished_at = max(finished_values) if finished_values else None
        duration_seconds = (
            max(0.0, (finished_at - started_at).total_seconds())
            if finished_at is not None
            else None
        )
        return (
            started_at.isoformat(),
            finished_at.isoformat() if finished_at is not None else None,
            round(duration_seconds, 3) if duration_seconds is not None else None,
        )

    def snapshot(
        self,
        *,
        active_task_limit: int = 60,
        terminal_task_limit: int = 20,
        pending_job_limit: int = 20,
    ) -> dict[str, Any]:
        status_counts = self.store.count_tasks_by_provider_status()
        active_tasks = [
            self._task_snapshot_payload(task)
            for task in self.store.list_tasks_by_statuses(
                ACTIVE_COORDINATOR_TASK_STATUSES,
                limit=active_task_limit,
                descending=False,
            )
        ]
        recent_terminal_tasks = [
            self._task_snapshot_payload(task)
            for task in self.store.list_tasks_by_statuses(
                (TASK_STATUS_FAILED, TASK_STATUS_CANCELLED, TASK_STATUS_DONE),
                limit=terminal_task_limit,
                descending=True,
            )
        ]
        pending_jobs = self.store.summarize_pending_jobs(limit=pending_job_limit)

        counts_by_provider: dict[str, dict[str, int]] = {}
        for provider_name in ("copernicus", "usgs"):
            provider_counts = dict(status_counts.get(provider_name) or {})
            for status_name in (
                TASK_STATUS_QUEUED,
                TASK_STATUS_PREPARING,
                TASK_STATUS_READY,
                TASK_STATUS_DOWNLOADING,
                TASK_STATUS_DONE,
                TASK_STATUS_FAILED,
                TASK_STATUS_CANCELLED,
            ):
                provider_counts.setdefault(status_name, 0)
            counts_by_provider[provider_name] = provider_counts

        with self._condition:
            started = bool(self._started)
            closed = bool(self._closed)
            active_downloads_global = int(self._active_downloads_global)
            copernicus_active_total = int(self._copernicus_active_total)
            copernicus_active_by_account = dict(self._copernicus_active_by_account)
            copernicus_account_cooldown_until = dict(self._copernicus_account_cooldown_until)
            usgs_active_prepares = int(self._usgs_active_prepares)
            usgs_active_downloads = int(self._usgs_active_downloads)
            usgs_cooldown_seconds = self._seconds_until_monotonic(self._usgs_cooldown_until)
            usgs_window_current = int(self._usgs_window_current)
            usgs_window_peak = int(self._usgs_window_peak)
            usgs_window_max = int(self._usgs_window_max)
            usgs_success_streak = int(self._usgs_success_streak)

        pending_tasks_total = sum(int(item.get("pending_tasks") or 0) for item in pending_jobs)
        if closed:
            coordinator_status = "closed"
        elif active_downloads_global > 0 or usgs_active_prepares > 0 or pending_tasks_total > 0:
            coordinator_status = "active"
        elif started:
            coordinator_status = "idle"
        else:
            coordinator_status = "not_started"

        per_account_limit = max(1, int(self.settings.nimbus_copernicus_account_pool_concurrency or 4))
        configured_accounts = list(self.settings.copernicus_account_pool_accounts)
        account_labels = {
            str(item.get("label") or "").strip() or "primary"
            for item in configured_accounts
        }
        account_labels.update(
            label
            for label in copernicus_active_by_account
            if str(label).strip()
        )
        account_labels.update(
            label
            for label in copernicus_account_cooldown_until
            if str(label).strip()
        )
        account_rows = []
        for label in sorted(account_labels):
            account_rows.append(
                {
                    "account_label": label,
                    "active_downloads": int(copernicus_active_by_account.get(label, 0) or 0),
                    "cooldown_seconds": round(
                        self._seconds_until_monotonic(copernicus_account_cooldown_until.get(label)),
                        1,
                    ),
                    "max_concurrent_downloads": per_account_limit,
                }
            )

        return {
            "status": coordinator_status,
            "started": started,
            "closed": closed,
            "timestamp": self._utc_now(),
            "db_path": str(self.settings.download_coordinator_db_path),
            "limits": {
                "job": dict(self.settings.provider_job_limits_map),
                "control_plane": dict(self.settings.provider_control_plane_limits_map),
                "data_plane": dict(self.settings.provider_data_plane_limits_map),
            },
            "machine": {
                "active_downloads": active_downloads_global,
                "active_download_limit": int(self.settings.nimbus_download_global_limit),
                "disk_path": str(self.settings.nimbus_data_dir),
                "disk_free_bytes": self._safe_disk_free_bytes(),
                "min_free_bytes": int(self.settings.nimbus_download_min_free_bytes or 0),
                "bandwidth_limit_bps": (
                    int(self.settings.nimbus_download_global_max_bps)
                    if self.settings.nimbus_download_global_max_bps
                    else None
                ),
            },
            "providers": {
                "copernicus": {
                    "job_limit": int(self.settings.provider_job_limits_map.get("copernicus", 1)),
                    "control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("copernicus", 1)),
                    "data_plane_limit": int(self.settings.provider_data_plane_limits_map.get("copernicus", 1)),
                    "active_downloads": copernicus_active_total,
                    "pending_tasks": sum(
                        counts_by_provider["copernicus"].get(status_name, 0)
                        for status_name in ACTIVE_COORDINATOR_TASK_STATUSES
                    ),
                    "counts": counts_by_provider["copernicus"],
                    "accounts_configured": int(len(configured_accounts)),
                    "accounts": account_rows,
                },
                "usgs": {
                    "job_limit": int(self.settings.provider_job_limits_map.get("usgs", 1)),
                    "control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("usgs", 1)),
                    "data_plane_limit": int(self.settings.provider_data_plane_limits_map.get("usgs", 1)),
                    "active_prepares": usgs_active_prepares,
                    "active_downloads": usgs_active_downloads,
                    "adaptive_window_current": usgs_window_current,
                    "adaptive_window_peak": usgs_window_peak,
                    "adaptive_window_max": usgs_window_max,
                    "success_streak": usgs_success_streak,
                    "cooldown_seconds": round(usgs_cooldown_seconds, 1),
                    "pending_tasks": sum(
                        counts_by_provider["usgs"].get(status_name, 0)
                        for status_name in ACTIVE_COORDINATOR_TASK_STATUSES
                    ),
                    "counts": counts_by_provider["usgs"],
                },
            },
            "jobs": {
                "pending_tasks_total": pending_tasks_total,
                "pending_jobs_total": len(pending_jobs),
                "pending_by_job": pending_jobs,
            },
            "tasks": {
                "active": active_tasks,
                "recent_terminal": recent_terminal_tasks,
            },
        }

    def _ensure_started(self) -> None:
        with self._condition:
            if self._started:
                return
            self._started = True
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                name="nimbus-download-coordinator",
                daemon=True,
            )
            self._scheduler_thread.start()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._scheduler_thread is not None:
            self._scheduler_thread.join(timeout=2.0)
            self._scheduler_thread = None
        self.store.close()
        self._started = False

    def download_products(
        self,
        *,
        job_id: str,
        provider_name: str,
        provider: CopernicusProvider | UsgsProvider,
        collection: str,
        product_ids: list[str],
        output_dir: str,
        progress_callback: ProgressCallback | None,
        retry_callback: RetryCallback | None,
        cancel_checker: CancelChecker | None,
        download_strategy: str = "default",
    ) -> DownloadBatchResult:
        if not product_ids:
            return DownloadBatchResult(paths=[], metadata={})

        self._ensure_started()

        metadata = self._build_submission_metadata(
            provider_name=provider_name,
            provider=provider,
            product_ids=product_ids,
            download_strategy=download_strategy,
        )
        task_ids: list[str] = []
        for product_id in product_ids:
            task_id = self._task_id_for(provider_name, job_id, str(product_id))
            task = self.store.ensure_task(
                task_id=task_id,
                provider=provider_name,
                job_id=job_id,
                collection=str(collection or "").strip(),
                product_id=str(product_id),
                output_dir=str(output_dir),
                metadata=metadata,
            )
            task_ids.append(str(task.get("task_id") or task_id))

        runtime_context = _TaskRuntimeContext(
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            cancel_checker=cancel_checker,
        )
        with self._condition:
            for task_id in task_ids:
                self._runtime_contexts[task_id] = runtime_context
            self._condition.notify_all()

        try:
            return self._wait_for_batch(
                provider_name=provider_name,
                provider=provider,
                product_ids=product_ids,
                task_ids=task_ids,
                cancel_checker=cancel_checker,
            )
        finally:
            with self._condition:
                for task_id in task_ids:
                    self._runtime_contexts.pop(task_id, None)
                self._condition.notify_all()

    def _build_submission_metadata(
        self,
        *,
        provider_name: str,
        provider: CopernicusProvider | UsgsProvider,
        product_ids: list[str],
        download_strategy: str,
    ) -> dict[str, Any]:
        if provider_name == "copernicus":
            metadata = dict(provider.plan_download_metadata(len(product_ids)))
            metadata["download_strategy"] = str(download_strategy or "default").strip().lower() or "default"
            metadata["control_plane_limit"] = int(self.settings.provider_control_plane_limits_map.get("copernicus", 2))
            metadata["data_plane_limit"] = int(self.settings.provider_data_plane_limits_map.get("copernicus", 32))
            metadata["global_download_limit"] = int(self.settings.nimbus_download_global_limit)
            return metadata

        return {
            "download_strategy": "adaptive_local",
            "control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("usgs", 1)),
            "data_plane_limit": int(self.settings.provider_data_plane_limits_map.get("usgs", self._usgs_window_max)),
            "data_plane_initial": int(min(2, self._usgs_window_max)),
            "global_download_limit": int(self.settings.nimbus_download_global_limit),
        }

    def _wait_for_batch(
        self,
        *,
        provider_name: str,
        provider: CopernicusProvider | UsgsProvider,
        product_ids: list[str],
        task_ids: list[str],
        cancel_checker: CancelChecker | None,
    ) -> DownloadBatchResult:
        while True:
            if cancel_checker is not None and cancel_checker():
                self.store.cancel_pending_tasks(task_ids)
                raise DownloadCancelled("Download batch cancelled.")

            rows = self.store.list_tasks(task_ids)
            by_product_id = {str(row.get("product_id") or ""): row for row in rows}
            if rows and all(str(row.get("status") or "") in TERMINAL_TASK_STATUSES for row in rows):
                paths = [
                    str((by_product_id.get(str(product_id)) or {}).get("output_path") or "").strip()
                    for product_id in product_ids
                    if str((by_product_id.get(str(product_id)) or {}).get("status") or "") == TASK_STATUS_DONE
                ]
                failures = [
                    row
                    for row in rows
                    if str(row.get("status") or "") == TASK_STATUS_FAILED
                ]
                metadata = self._final_metadata_for_batch(
                    provider_name=provider_name,
                    provider=provider,
                    rows=rows,
                )
                if not paths and failures:
                    detail = " | ".join(
                        str(row.get("error_text") or row.get("product_id") or "").strip()
                        for row in failures[:3]
                    )
                    raise RuntimeError(
                        f"All {provider_name} downloads failed. Causes: {detail or 'unknown'}"
                    )
                return DownloadBatchResult(paths=[path for path in paths if path], metadata=metadata)

            with self._condition:
                self._condition.wait(timeout=0.2)

    def _final_metadata_for_batch(
        self,
        *,
        provider_name: str,
        provider: CopernicusProvider | UsgsProvider,
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not rows:
            return {}

        base = dict((rows[0].get("metadata") or {}))
        download_started_at, download_finished_at, download_window_seconds = self._task_window_for_rows(rows)
        if download_started_at:
            base["download_started_at"] = download_started_at
        if download_finished_at:
            base["download_finished_at"] = download_finished_at
        if download_window_seconds is not None:
            base["download_window_seconds"] = download_window_seconds
        base["download_tasks_total"] = len(rows)
        base["download_bytes_total"] = sum(max(0, int(row.get("bytes_total") or 0)) for row in rows)
        base["download_bytes_downloaded"] = sum(max(0, int(row.get("bytes_downloaded") or 0)) for row in rows)
        if provider_name == "copernicus":
            failures = [
                f"{row.get('account_label') or 'unknown'}: {row.get('error_text') or 'failed'}"
                for row in rows
                if str(row.get("status") or "") == TASK_STATUS_FAILED
            ]
            account_assignments: dict[str, int] = defaultdict(int)
            for row in rows:
                label = str(row.get("account_label") or "").strip()
                if label:
                    account_assignments[label] += 1
            base["account_pool_assignments"] = [
                {"account_label": label, "product_count": count}
                for label, count in sorted(account_assignments.items())
            ]
            base["account_pool_selected_accounts"] = len(base["account_pool_assignments"])
            if base["account_pool_selected_accounts"] > 1:
                base.pop("account_pool_fallback_reason", None)
            base["account_pool_failures"] = failures
            return base

        base.update(
            {
                "download_strategy": "adaptive_local",
                "usgs_control_plane_limit": int(self.settings.provider_control_plane_limits_map.get("usgs", 1)),
                "usgs_data_plane_limit": int(self._usgs_window_max),
                "usgs_adaptive_window_final": int(self._usgs_window_current),
                "usgs_adaptive_window_peak": int(self._usgs_window_peak),
                "usgs_preparing_count": sum(
                    1 for row in rows if str(row.get("status") or "") == TASK_STATUS_PREPARING
                ),
            }
        )
        return base

    def _scheduler_loop(self) -> None:
        while True:
            with self._condition:
                if self._closed:
                    return

            launched = False
            launched = self._try_launch_copernicus_task() or launched
            launched = self._try_launch_usgs_download_task() or launched
            launched = self._try_launch_usgs_prepare_task() or launched

            if launched:
                continue

            with self._condition:
                if self._closed:
                    return
                self._condition.wait(timeout=0.5)

    def _disk_available_for_task(self, task: dict[str, Any]) -> bool:
        minimum_free = max(0, int(self.settings.nimbus_download_min_free_bytes or 0))
        if minimum_free <= 0:
            return True
        target = Path(str(task.get("output_dir") or self.settings.nimbus_data_dir))
        target.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(target)
        return int(usage.free) >= minimum_free

    def _global_download_capacity_available(self) -> bool:
        with self._condition:
            return self._active_downloads_global < int(self.settings.nimbus_download_global_limit)

    def _pick_round_robin_task(self, queue_key: str, tasks: list[dict[str, Any]]) -> dict[str, Any] | None:
        due_tasks = [task for task in tasks if self._retry_due(task)]
        if not due_tasks:
            return None
        by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for task in due_tasks:
            by_job[str(task.get("job_id") or "")].append(task)

        queue = self._job_round_robin[queue_key]
        existing_jobs = set(by_job)
        filtered_jobs = [job_id for job_id in queue if job_id in existing_jobs]
        queue.clear()
        queue.extend(filtered_jobs)
        for job_id in sorted(existing_jobs):
            if job_id not in queue:
                queue.append(job_id)

        for _ in range(len(queue)):
            job_id = queue[0]
            queue.rotate(-1)
            candidates = sorted(
                by_job.get(job_id, []),
                key=lambda item: str(item.get("created_at") or ""),
            )
            if candidates:
                return candidates[0]

        fallback = sorted(due_tasks, key=lambda item: str(item.get("created_at") or ""))
        return fallback[0] if fallback else None

    def _sync_copernicus_account_round_robin(self) -> None:
        labels = [
            str(item.get("label") or "primary").strip() or "primary"
            for item in self.settings.copernicus_account_pool_accounts
        ]
        if not labels:
            return

        queue = self._copernicus_account_round_robin
        label_set = set(labels)
        filtered = [label for label in queue if label in label_set]
        queue.clear()
        queue.extend(filtered)
        for label in labels:
            if label not in queue:
                queue.append(label)

    def _copernicus_accounts_for_task(self, task: dict[str, Any]) -> list[dict[str, str]]:
        metadata = dict(task.get("metadata") or {})
        strategy = str(metadata.get("download_strategy") or "default").strip().lower()
        accounts = list(self.settings.copernicus_account_pool_accounts)
        primary = [
            item
            for item in accounts
            if str(item.get("label") or "").strip() == "primary"
        ]
        if strategy != "copernicus_account_pool":
            return primary[:1]

        if not accounts:
            return primary[:1]
        return accounts

    def _select_copernicus_account(self, task: dict[str, Any]) -> dict[str, str] | None:
        accounts = self._copernicus_accounts_for_task(task)
        if not accounts:
            return None

        per_account_limit = max(1, int(self.settings.nimbus_copernicus_account_pool_concurrency or 4))
        provider_limit = max(1, int(self.settings.provider_data_plane_limits_map.get("copernicus", 32)))
        if self._copernicus_active_total >= provider_limit:
            return None

        self._sync_copernicus_account_round_robin()
        now = time.monotonic()
        available_by_label: dict[str, dict[str, str]] = {}
        for account in accounts:
            label = str(account.get("label") or "").strip() or "primary"
            if self._copernicus_account_cooldown_until.get(label, 0.0) > now:
                continue
            if self._copernicus_active_by_account[label] >= per_account_limit:
                continue
            available_by_label[label] = account
        if not available_by_label:
            return None

        queue = self._copernicus_account_round_robin
        for _ in range(len(queue)):
            label = queue[0]
            queue.rotate(-1)
            selected = available_by_label.get(label)
            if selected is not None:
                return selected

        for account in accounts:
            label = str(account.get("label") or "").strip() or "primary"
            if label in available_by_label:
                return available_by_label[label]
        return None

    def _try_launch_copernicus_task(self) -> bool:
        if not self._global_download_capacity_available():
            return False

        tasks = self.store.list_candidate_tasks(provider="copernicus", statuses=(TASK_STATUS_QUEUED,))
        task = self._pick_round_robin_task("copernicus", tasks)
        if task is None or not self._disk_available_for_task(task):
            return False
        account = self._select_copernicus_account(task)
        if account is None:
            return False

        label = str(account.get("label") or "primary").strip() or "primary"
        self._active_downloads_global += 1
        self._copernicus_active_total += 1
        self._copernicus_active_by_account[label] += 1
        self.store.update_task(
            task["task_id"],
            status=TASK_STATUS_DOWNLOADING,
            account_label=label,
            attempts=int(task.get("attempts") or 0) + 1,
            started_at=self._utc_now(),
            finished_at=None,
            error_text=None,
        )
        threading.Thread(
            target=self._run_copernicus_task,
            args=(task["task_id"], account),
            name=f"copernicus-download-{label}",
            daemon=True,
        ).start()
        return True

    def _try_launch_usgs_prepare_task(self) -> bool:
        control_limit = max(1, int(self.settings.provider_control_plane_limits_map.get("usgs", 1)))
        if self._usgs_active_prepares >= control_limit:
            return False
        if self._usgs_cooldown_until > time.monotonic():
            return False

        tasks = self.store.list_candidate_tasks(
            provider="usgs",
            statuses=(TASK_STATUS_QUEUED, TASK_STATUS_PREPARING),
        )
        tasks = [
            task
            for task in tasks
            if not str(task.get("source_url") or "").strip()
        ]
        task = self._pick_round_robin_task("usgs.prepare", tasks)
        if task is None:
            return False

        self._usgs_active_prepares += 1
        self.store.update_task(
            task["task_id"],
            status=TASK_STATUS_PREPARING,
            attempts=int(task.get("attempts") or 0) + 1,
            started_at=task.get("started_at") or self._utc_now(),
            finished_at=None,
            error_text=None,
        )
        threading.Thread(
            target=self._run_usgs_prepare_task,
            args=(task["task_id"],),
            name="usgs-prepare",
            daemon=True,
        ).start()
        return True

    def _try_launch_usgs_download_task(self) -> bool:
        if not self._global_download_capacity_available():
            return False
        if self._usgs_cooldown_until > time.monotonic():
            return False
        if self._usgs_active_downloads >= self._usgs_window_current:
            return False

        tasks = self.store.list_candidate_tasks(
            provider="usgs",
            statuses=(TASK_STATUS_READY, TASK_STATUS_QUEUED),
        )
        tasks = [
            task
            for task in tasks
            if str(task.get("source_url") or "").strip()
        ]
        task = self._pick_round_robin_task("usgs.download", tasks)
        if task is None or not self._disk_available_for_task(task):
            return False

        self._active_downloads_global += 1
        self._usgs_active_downloads += 1
        self.store.update_task(
            task["task_id"],
            status=TASK_STATUS_DOWNLOADING,
            account_label="primary",
            attempts=int(task.get("attempts") or 0) + 1,
            started_at=self._utc_now(),
            finished_at=None,
            error_text=None,
        )
        threading.Thread(
            target=self._run_usgs_download_task,
            args=(task["task_id"],),
            name="usgs-download",
            daemon=True,
        ).start()
        return True

    def _runtime_context_for_task(self, task_id: str) -> _TaskRuntimeContext:
        with self._condition:
            return self._runtime_contexts.get(task_id) or _TaskRuntimeContext(None, None, None)

    def _preserve_cancelled_task(self, task_id: str) -> bool:
        task = self.store.get_task(task_id) or {}
        if str(task.get("status") or "") != TASK_STATUS_CANCELLED:
            return False
        if not str(task.get("finished_at") or "").strip():
            self.store.update_task(task_id, finished_at=self._utc_now())
        return True

    def _notify_task_update(self) -> None:
        with self._condition:
            self._condition.notify_all()

    @staticmethod
    def _context_payload(task: dict[str, Any]) -> dict[str, Any]:
        file_name = str(task.get("file_name") or "").strip()
        account_label = str(task.get("account_label") or "primary").strip() or "primary"
        return {
            "account_label": account_label,
            "product_id": str(task.get("product_id") or "").strip(),
            "file_name": file_name,
            "provider": str(task.get("provider") or "").strip(),
        }

    @staticmethod
    def _retry_deadline(seconds: float | None, *, default_seconds: float) -> str:
        delay = max(0.0, float(seconds if seconds is not None else default_seconds))
        return (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()

    def _handle_progress_callback(
        self,
        *,
        task_id: str,
        file_name: str,
        delta: int,
        downloaded: int,
        total: int | None,
        context: dict[str, Any] | None,
    ) -> None:
        task = self.store.get_task(task_id) or {}
        merged_context = {
            **self._context_payload(task),
            **dict(context or {}),
        }
        updated = self.store.update_task(
            task_id,
            file_name=str(file_name or merged_context.get("file_name") or "").strip(),
            bytes_downloaded=max(0, int(downloaded or 0)),
            bytes_total=max(0, int(total or 0)) if total is not None else int(task.get("bytes_total") or 0),
        )
        runtime = self._runtime_context_for_task(task_id)
        if runtime.progress_callback is not None:
            runtime.progress_callback(
                str(file_name or updated.get("file_name") or "").strip(),
                int(delta or 0),
                max(0, int(downloaded or 0)),
                total,
                merged_context,
            )
        self._notify_task_update()

    def _handle_retry_callback(
        self,
        *,
        task_id: str,
        file_name: str,
        attempt: int,
        reason: str,
        retry_after: float | None,
        context: dict[str, Any] | None,
    ) -> None:
        task = self.store.get_task(task_id) or {}
        merged_context = {
            **self._context_payload(task),
            **dict(context or {}),
        }
        retry_deadline = self._retry_deadline(retry_after, default_seconds=10.0)
        self.store.update_task(
            task_id,
            retry_after=retry_deadline,
            error_text=str(reason or "").strip(),
        )

        provider_name = str(task.get("provider") or "").strip().lower()
        normalized_reason = str(reason or "").strip().lower()
        if provider_name == "copernicus" and normalized_reason == "http_429":
            label = str(merged_context.get("account_label") or "primary").strip() or "primary"
            with self._condition:
                self._copernicus_account_cooldown_until[label] = time.monotonic() + max(
                    float(retry_after or 0.0),
                    5.0,
                )
        if provider_name == "usgs" and normalized_reason in {"http_429", "http_500", "http_502", "http_503", "http_504"}:
            with self._condition:
                self._usgs_window_current = max(1, self._usgs_window_current // 2 or 1)
                self._usgs_cooldown_until = time.monotonic() + max(float(retry_after or 0.0), 5.0)
                self._usgs_success_streak = 0

        runtime = self._runtime_context_for_task(task_id)
        if runtime.retry_callback is not None:
            runtime.retry_callback(
                str(file_name or merged_context.get("file_name") or "").strip(),
                int(attempt or 0),
                str(reason or "").strip(),
                retry_after,
                merged_context,
            )
        self._notify_task_update()

    def _build_single_download_manager(
        self,
        *,
        provider_name: str,
        task_id: str,
    ) -> DownloadManager:
        runtime = self._runtime_context_for_task(task_id)
        progress_callback: ProgressCallback | None = lambda file_name, delta, downloaded, total, context=None: self._handle_progress_callback(
            task_id=task_id,
            file_name=file_name,
            delta=delta,
            downloaded=downloaded,
            total=total,
            context=context,
        )
        retry_callback: RetryCallback | None = lambda file_name, attempt, reason, retry_after, context=None: self._handle_retry_callback(
            task_id=task_id,
            file_name=file_name,
            attempt=attempt,
            reason=reason,
            retry_after=retry_after,
            context=context,
        )

        if provider_name == "copernicus":
            return DownloadManager(
                max_concurrent=1,
                max_retries=5,
                initial_delay=2.0,
                backoff_factor=1.5,
                max_retry_delay=120.0,
                connect_timeout=30.0,
                chunk_size=128 * 1024,
                max_connections=4,
                max_connections_per_host=1,
                progress_callback=progress_callback,
                cancel_checker=runtime.cancel_checker,
                retry_callback=retry_callback,
                bandwidth_limiter=self._bandwidth_limiter,
            )
        return DownloadManager(
            max_concurrent=1,
            max_retries=5,
            initial_delay=2.0,
            backoff_factor=1.5,
            max_retry_delay=120.0,
            connect_timeout=30.0,
            chunk_size=128 * 1024,
            max_connections=4,
            max_connections_per_host=1,
            progress_callback=progress_callback,
            cancel_checker=runtime.cancel_checker,
            retry_callback=retry_callback,
            bandwidth_limiter=self._bandwidth_limiter,
        )

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        return any(
            token in text
            for token in ("429", "500", "502", "503", "504", "timeout", "temporarily unavailable")
        )

    def _run_copernicus_task(self, task_id: str, account: dict[str, str]) -> None:
        task = self.store.get_task(task_id) or {}
        label = str(account.get("label") or "primary").strip() or "primary"
        try:
            manager = self._build_single_download_manager(provider_name="copernicus", task_id=task_id)
            provider = CopernicusProvider(
                self.settings,
                manager,
                username=str(account.get("username") or "").strip(),
                password=str(account.get("password") or "").strip(),
                account_label=label,
                download_strategy="default",
            )
            paths = provider._download_products_single_account(
                [str(task.get("product_id") or "").strip()],
                str(task.get("output_dir") or ""),
            )
            path = str(paths[0] if paths else "").strip()
            if not path:
                raise RuntimeError("Copernicus download did not return a local path.")
            if self._preserve_cancelled_task(task_id):
                return
            self.store.update_task(
                task_id,
                status=TASK_STATUS_DONE,
                output_path=path,
                retry_after=None,
                error_text=None,
                finished_at=self._utc_now(),
            )
        except DownloadCancelled:
            if self._preserve_cancelled_task(task_id):
                return
            self.store.update_task(
                task_id,
                status=TASK_STATUS_CANCELLED,
                error_text="Download cancelled.",
                finished_at=self._utc_now(),
            )
        except Exception as exc:
            if self._preserve_cancelled_task(task_id):
                return
            retryable = self._is_retryable_error(exc)
            if retryable:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_QUEUED,
                    retry_after=self._retry_deadline(None, default_seconds=15.0),
                    error_text=str(exc),
                    account_label=None,
                )
            else:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_FAILED,
                    error_text=str(exc),
                    finished_at=self._utc_now(),
                )
        finally:
            with self._condition:
                self._active_downloads_global = max(0, self._active_downloads_global - 1)
                self._copernicus_active_total = max(0, self._copernicus_active_total - 1)
                self._copernicus_active_by_account[label] = max(
                    0,
                    self._copernicus_active_by_account.get(label, 0) - 1,
                )
            self._notify_task_update()

    def _run_usgs_prepare_task(self, task_id: str) -> None:
        task = self.store.get_task(task_id) or {}
        try:
            manager = self._build_single_download_manager(provider_name="usgs", task_id=task_id)
            provider = UsgsProvider(self.settings, manager)
            provider.dataset = str(task.get("collection") or "").strip()
            prepared = provider.prepare_download_product(str(task.get("product_id") or "").strip())
            status = str(prepared.get("status") or "").strip().lower()
            if self._preserve_cancelled_task(task_id):
                return
            if status == TASK_STATUS_READY:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_READY,
                    source_url=str(prepared.get("url") or "").strip() or None,
                    file_name=str(prepared.get("file_name") or "").strip() or None,
                    retry_after=None,
                    error_text=None,
                )
            elif status == TASK_STATUS_PREPARING:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_PREPARING,
                    retry_after=self._retry_deadline(
                        float(prepared.get("retry_after_seconds") or 30.0),
                        default_seconds=30.0,
                    ),
                    error_text=str(prepared.get("reason") or "").strip() or None,
                )
            else:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_FAILED,
                    error_text=str(prepared.get("reason") or "USGS prepare failed.").strip(),
                    finished_at=self._utc_now(),
                )
        except DownloadCancelled:
            if self._preserve_cancelled_task(task_id):
                return
            self.store.update_task(
                task_id,
                status=TASK_STATUS_CANCELLED,
                error_text="Download cancelled.",
                finished_at=self._utc_now(),
            )
        except Exception as exc:
            if self._preserve_cancelled_task(task_id):
                return
            if self._is_retryable_error(exc):
                with self._condition:
                    self._usgs_cooldown_until = time.monotonic() + 10.0
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_PREPARING,
                    retry_after=self._retry_deadline(None, default_seconds=20.0),
                    error_text=str(exc),
                )
            else:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_FAILED,
                    error_text=str(exc),
                    finished_at=self._utc_now(),
                )
        finally:
            with self._condition:
                self._usgs_active_prepares = max(0, self._usgs_active_prepares - 1)
            self._notify_task_update()

    def _run_usgs_download_task(self, task_id: str) -> None:
        task = self.store.get_task(task_id) or {}
        had_retryable_signal = False
        try:
            manager = self._build_single_download_manager(provider_name="usgs", task_id=task_id)
            provider = UsgsProvider(self.settings, manager)
            provider.dataset = str(task.get("collection") or "").strip()
            prepared = {
                "entity_id": str(task.get("product_id") or "").strip(),
                "url": str(task.get("source_url") or "").strip(),
                "file_name": str(task.get("file_name") or "").strip(),
            }
            paths = provider.download_prepared_product(prepared, str(task.get("output_dir") or ""))
            path = str(paths[0] if paths else "").strip()
            if not path:
                raise RuntimeError("USGS download did not return a local path.")
            if self._preserve_cancelled_task(task_id):
                return
            self.store.update_task(
                task_id,
                status=TASK_STATUS_DONE,
                output_path=path,
                retry_after=None,
                error_text=None,
                finished_at=self._utc_now(),
            )
            with self._condition:
                self._usgs_success_streak += 1
                if (
                    self._usgs_success_streak >= max(1, self._usgs_window_current)
                    and self._usgs_window_current < self._usgs_window_max
                ):
                    self._usgs_window_current += 1
                    self._usgs_window_peak = max(self._usgs_window_peak, self._usgs_window_current)
                    self._usgs_success_streak = 0
        except DownloadCancelled:
            if self._preserve_cancelled_task(task_id):
                return
            self.store.update_task(
                task_id,
                status=TASK_STATUS_CANCELLED,
                error_text="Download cancelled.",
                finished_at=self._utc_now(),
            )
        except Exception as exc:
            if self._preserve_cancelled_task(task_id):
                return
            had_retryable_signal = self._is_retryable_error(exc)
            if had_retryable_signal:
                with self._condition:
                    self._usgs_window_current = max(1, self._usgs_window_current // 2 or 1)
                    self._usgs_success_streak = 0
                    self._usgs_cooldown_until = time.monotonic() + 10.0
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_READY,
                    retry_after=self._retry_deadline(None, default_seconds=15.0),
                    error_text=str(exc),
                )
            else:
                self.store.update_task(
                    task_id,
                    status=TASK_STATUS_FAILED,
                    error_text=str(exc),
                    finished_at=self._utc_now(),
                )
        finally:
            _ = had_retryable_signal
            with self._condition:
                self._active_downloads_global = max(0, self._active_downloads_global - 1)
                self._usgs_active_downloads = max(0, self._usgs_active_downloads - 1)
            self._notify_task_update()
