from __future__ import annotations

import json
import sqlite3
import threading
from datetime import timedelta
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nimbuschain_fetch.jobs.store import JobListFilters


class SQLiteJobStore:
    """SQLite-backed store for jobs, events and results."""

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
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    collection TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    progress REAL NOT NULL,
                    bytes_downloaded INTEGER NOT NULL,
                    bytes_total INTEGER NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    errors_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
                CREATE INDEX IF NOT EXISTS idx_jobs_provider ON jobs(provider);
                CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);

                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_events_job_id ON job_events(job_id);
                CREATE INDEX IF NOT EXISTS idx_events_timestamp ON job_events(timestamp);

                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT PRIMARY KEY,
                    result_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );
                """
            )
            self._conn.commit()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "job_id": row["job_id"],
            "job_type": row["job_type"],
            "provider": row["provider"],
            "collection": row["collection"],
            "request": json.loads(row["request_json"]),
            "state": row["state"],
            "progress": float(row["progress"]),
            "bytes_downloaded": int(row["bytes_downloaded"]),
            "bytes_total": int(row["bytes_total"]),
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "errors": json.loads(row["errors_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_job(
        self,
        job_id: str,
        job_type: str,
        provider: str,
        collection: str,
        request_payload: dict[str, Any],
    ) -> None:
        now = self._utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO jobs(
                    job_id, job_type, provider, collection, request_json, state,
                    progress, bytes_downloaded, bytes_total, started_at, finished_at,
                    errors_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_type,
                    provider,
                    collection,
                    json.dumps(request_payload),
                    "queued",
                    0.0,
                    0,
                    0,
                    None,
                    None,
                    "[]",
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        if not row:
            return None
        return self._row_to_job(row)

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return

        normalized: dict[str, Any] = {}
        for key, value in fields.items():
            if key == "errors":
                normalized["errors_json"] = json.dumps(value)
            elif key == "request":
                normalized["request_json"] = json.dumps(value)
            else:
                normalized[key] = value

        normalized["updated_at"] = self._utc_now()
        assignments = ", ".join(f"{key} = ?" for key in normalized)
        params = list(normalized.values()) + [job_id]

        with self._lock:
            self._conn.execute(f"UPDATE jobs SET {assignments} WHERE job_id = ?", params)
            self._conn.commit()

    def list_jobs(self, filters: JobListFilters) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []

        if filters.state:
            where.append("state = ?")
            params.append(filters.state)
        if filters.provider:
            where.append("provider = ?")
            params.append(filters.provider)
        if filters.date_from:
            where.append("created_at >= ?")
            params.append(filters.date_from.isoformat())
        if filters.date_to:
            where.append("created_at <= ?")
            params.append(filters.date_to.isoformat())

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size

        with self._lock:
            total_row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs {where_sql}", params
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT * FROM jobs
                {where_sql}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()

        total = int(total_row["n"]) if total_row else 0
        return [self._row_to_job(row) for row in rows], total

    def append_event(
        self,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> int:
        ts = (timestamp or datetime.now(timezone.utc)).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO job_events(job_id, type, timestamp, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, event_type, ts, json.dumps(payload)),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def list_events(
        self,
        job_id: str | None,
        since_id: int | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if since_id is not None:
            clauses.append("id > ?")
            params.append(since_id)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, job_id, type, timestamp, payload_json
                FROM job_events
                {where_sql}
                ORDER BY id ASC
                LIMIT ?
                """,
                [*params, max(1, min(1000, limit))],
            ).fetchall()

        return [
            {
                "id": int(row["id"]),
                "job_id": row["job_id"],
                "type": row["type"],
                "timestamp": row["timestamp"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def set_result(self, job_id: str, result_payload: dict[str, Any]) -> None:
        now = self._utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO job_results(job_id, result_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    result_json = excluded.result_json,
                    updated_at = excluded.updated_at
                """,
                (job_id, json.dumps(result_payload), now),
            )
            self._conn.commit()

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT result_json FROM job_results WHERE job_id = ?", (job_id,)
            ).fetchone()
        if not row:
            return None
        return json.loads(row["result_json"])

    def requeue_incomplete_jobs(self) -> list[str]:
        """Requeue jobs left in running/cancel_requested states after restart."""

        now = self._utc_now()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT job_id FROM jobs
                WHERE state IN ('running', 'cancel_requested')
                """
            ).fetchall()
            job_ids = [str(row["job_id"]) for row in rows]

            if job_ids:
                self._conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'queued', updated_at = ?
                    WHERE state IN ('running', 'cancel_requested')
                    """,
                    (now,),
                )

                for jid in job_ids:
                    self._conn.execute(
                        """
                        INSERT INTO job_events(job_id, type, timestamp, payload_json)
                        VALUES (?, 'job.requeued_after_restart', ?, ?)
                        """,
                        (jid, now, json.dumps({"reason": "service_restart"})),
                    )

            self._conn.commit()
        return job_ids

    def claim_job_for_execution(self, job_id: str, worker_id: str) -> bool:
        _ = worker_id
        now = self._utc_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE jobs
                SET state = 'running', started_at = COALESCE(started_at, ?), updated_at = ?
                WHERE job_id = ? AND state = 'queued'
                """,
                (now, now, job_id),
            )
            claimed = cursor.rowcount > 0
            if claimed:
                self._conn.commit()
            else:
                self._conn.rollback()
        return claimed

    def requeue_stale_running_jobs(self, stale_after_seconds: int) -> list[str]:
        now_dt = datetime.now(timezone.utc)
        stale_before = (now_dt - timedelta(seconds=max(1, int(stale_after_seconds)))).isoformat()
        now_iso = now_dt.isoformat()

        with self._lock:
            rows = self._conn.execute(
                """
                SELECT job_id FROM jobs
                WHERE state IN ('running', 'cancel_requested')
                  AND updated_at < ?
                """,
                (stale_before,),
            ).fetchall()
            job_ids = [str(row["job_id"]) for row in rows]
            if not job_ids:
                return []

            self._conn.execute(
                """
                UPDATE jobs
                SET state = 'queued', updated_at = ?
                WHERE state IN ('running', 'cancel_requested')
                  AND updated_at < ?
                """,
                (now_iso, stale_before),
            )
            for jid in job_ids:
                self._conn.execute(
                    """
                    INSERT INTO job_events(job_id, type, timestamp, payload_json)
                    VALUES (?, 'job.requeued_stale', ?, ?)
                    """,
                    (jid, now_iso, json.dumps({"reason": "stale_running_timeout"})),
                )
            self._conn.commit()
        return job_ids
