from __future__ import annotations

import json
import sqlite3
import threading
from datetime import timedelta
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nimbuschain_fetch.jobs.store import ArtifactListFilters, JobListFilters


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
                    product_type TEXT,
                    tile_id TEXT,
                    request_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    pipeline_state TEXT NOT NULL DEFAULT 'queued',
                    pipeline_step TEXT,
                    pipeline_progress REAL,
                    pipeline_metadata_json TEXT NOT NULL DEFAULT '{}',
                    conversion_metadata_json TEXT NOT NULL DEFAULT '{}',
                    raw_outputs_json TEXT NOT NULL DEFAULT '[]',
                    zarr_outputs_json TEXT NOT NULL DEFAULT '[]',
                    progress REAL NOT NULL,
                    bytes_downloaded INTEGER NOT NULL,
                    bytes_total INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_retry_at TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    errors_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
                CREATE INDEX IF NOT EXISTS idx_jobs_provider ON jobs(provider);
                CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_provider_state_created
                ON jobs(provider, state, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_provider_created
                ON jobs(provider, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_collection_updated
                ON jobs(collection, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_product_updated
                ON jobs(product_type, updated_at DESC);

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

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    artifact_type TEXT NOT NULL,
                    artifact_uri TEXT NOT NULL UNIQUE,
                    provider TEXT,
                    collection TEXT,
                    scene_id TEXT,
                    source_uri TEXT,
                    created_by_job_id TEXT,
                    source_job_id TEXT,
                    data_family TEXT,
                    band_names_json TEXT NOT NULL,
                    dimensions_json TEXT NOT NULL,
                    shape_json TEXT NOT NULL,
                    size_bytes INTEGER,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_artifacts_type_updated
                ON artifacts(artifact_type, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_artifacts_provider_updated
                ON artifacts(provider, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_artifacts_collection_updated
                ON artifacts(collection, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_artifacts_scene_updated
                ON artifacts(scene_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    runtime_role TEXT NOT NULL,
                    execution_enabled INTEGER NOT NULL,
                    max_concurrent_jobs INTEGER NOT NULL,
                    queue_poll_seconds REAL NOT NULL,
                    heartbeat_interval_seconds REAL NOT NULL,
                    provider_limits_json TEXT NOT NULL,
                    hostname TEXT,
                    pid INTEGER,
                    active_running_jobs INTEGER NOT NULL,
                    active_cancel_requested_jobs INTEGER NOT NULL,
                    queue_backlog INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_workers_last_seen
                ON workers(last_seen_at DESC);
                """
            )
            self._ensure_column("jobs", "product_type", "TEXT")
            self._ensure_column("jobs", "tile_id", "TEXT")
            self._ensure_column("jobs", "worker_id", "TEXT")
            self._ensure_column("jobs", "retry_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("jobs", "last_retry_at", "TEXT")
            self._ensure_column("jobs", "pipeline_state", "TEXT NOT NULL DEFAULT 'queued'")
            self._ensure_column("jobs", "pipeline_step", "TEXT")
            self._ensure_column("jobs", "pipeline_progress", "REAL")
            self._ensure_column("jobs", "pipeline_metadata_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("jobs", "conversion_metadata_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("jobs", "raw_outputs_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("jobs", "zarr_outputs_json", "TEXT NOT NULL DEFAULT '[]'")
            self._conn.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        self._conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
        request = json.loads(row["request_json"])
        return {
            "job_id": row["job_id"],
            "job_type": row["job_type"],
            "provider": row["provider"],
            "collection": row["collection"],
            "product_type": (row["product_type"] if "product_type" in row.keys() else None) or request.get("product_type"),
            "tile_id": (row["tile_id"] if "tile_id" in row.keys() else None) or request.get("tile_id"),
            "worker_id": (row["worker_id"] if "worker_id" in row.keys() else None),
            "request": request,
            "state": row["state"],
            "pipeline_state": row["pipeline_state"] if "pipeline_state" in row.keys() else "queued",
            "pipeline_step": row["pipeline_step"] if "pipeline_step" in row.keys() else None,
            "pipeline_progress": float(row["pipeline_progress"]) if "pipeline_progress" in row.keys() and row["pipeline_progress"] is not None else None,
            "pipeline_metadata": json.loads(row["pipeline_metadata_json"]) if "pipeline_metadata_json" in row.keys() and row["pipeline_metadata_json"] else {},
            "conversion_metadata": json.loads(row["conversion_metadata_json"]) if "conversion_metadata_json" in row.keys() and row["conversion_metadata_json"] else {},
            "raw_outputs": json.loads(row["raw_outputs_json"]) if "raw_outputs_json" in row.keys() and row["raw_outputs_json"] else [],
            "zarr_outputs": json.loads(row["zarr_outputs_json"]) if "zarr_outputs_json" in row.keys() and row["zarr_outputs_json"] else [],
            "progress": float(row["progress"]),
            "bytes_downloaded": int(row["bytes_downloaded"]),
            "bytes_total": int(row["bytes_total"]),
            "retry_count": int(row["retry_count"]) if "retry_count" in row.keys() and row["retry_count"] is not None else 0,
            "last_retry_at": row["last_retry_at"] if "last_retry_at" in row.keys() else None,
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
        product_type = request_payload.get("product_type")
        tile_id = request_payload.get("tile_id")
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO jobs(
                    job_id, job_type, provider, collection, product_type, tile_id, request_json, state,
                    pipeline_state, pipeline_step, pipeline_progress, pipeline_metadata_json,
                    conversion_metadata_json, raw_outputs_json, zarr_outputs_json,
                    progress, bytes_downloaded, bytes_total, retry_count, last_retry_at, started_at, finished_at,
                    errors_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_type,
                    provider,
                    collection,
                    product_type,
                    tile_id,
                    json.dumps(request_payload),
                    "queued",
                    "queued",
                    "queued",
                    0.0,
                    "{}",
                    "{}",
                    "[]",
                    "[]",
                    0.0,
                    0,
                    0,
                    0,
                    None,
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
            elif key in {"pipeline_metadata", "conversion_metadata"}:
                normalized[f"{key}_json"] = json.dumps(value or {})
            elif key in {"raw_outputs", "zarr_outputs"}:
                normalized[f"{key}_json"] = json.dumps(list(value or []))
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
        if filters.states:
            placeholders = ", ".join("?" for _ in filters.states)
            where.append(f"state IN ({placeholders})")
            params.extend(filters.states)
        if filters.provider:
            where.append("provider = ?")
            params.append(filters.provider)
        if filters.collection:
            where.append("collection = ?")
            params.append(filters.collection)
        if filters.product_type:
            where.append("COALESCE(product_type, json_extract(request_json, '$.product_type')) = ?")
            params.append(filters.product_type)
        if filters.worker_id:
            where.append("worker_id = ?")
            params.append(filters.worker_id)
        if filters.job_id_query:
            where.append("job_id LIKE ?")
            params.append(f"%{filters.job_id_query}%")
        if filters.date_from:
            where.append("created_at >= ?")
            params.append(filters.date_from.isoformat())
        if filters.date_to:
            where.append("created_at <= ?")
            params.append(filters.date_to.isoformat())
        if filters.updated_from:
            where.append("updated_at >= ?")
            params.append(filters.updated_from.isoformat())
        if filters.updated_to:
            where.append("updated_at <= ?")
            params.append(filters.updated_to.isoformat())

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size
        sort_by = filters.sort_by if filters.sort_by in {"created_at", "updated_at", "started_at", "finished_at"} else "updated_at"
        sort_dir = "DESC" if filters.sort_desc else "ASC"

        with self._lock:
            total_row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs {where_sql}", params
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT * FROM jobs
                {where_sql}
                ORDER BY {sort_by} {sort_dir}, created_at DESC
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

    @staticmethod
    def _row_to_artifact(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "artifact_id": row["artifact_id"],
            "artifact_type": row["artifact_type"],
            "artifact_uri": row["artifact_uri"],
            "provider": row["provider"],
            "collection": row["collection"],
            "scene_id": row["scene_id"],
            "source_uri": row["source_uri"],
            "created_by_job_id": row["created_by_job_id"],
            "source_job_id": row["source_job_id"],
            "data_family": row["data_family"],
            "band_names": json.loads(row["band_names_json"]),
            "dimensions": json.loads(row["dimensions_json"]),
            "shape": json.loads(row["shape_json"]),
            "size_bytes": row["size_bytes"],
            "metadata": json.loads(row["metadata_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def upsert_artifact(self, artifact_payload: dict[str, Any]) -> dict[str, Any]:
        now = self._utc_now()
        payload = {
            "artifact_id": artifact_payload["artifact_id"],
            "artifact_type": artifact_payload["artifact_type"],
            "artifact_uri": artifact_payload["artifact_uri"],
            "provider": artifact_payload.get("provider"),
            "collection": artifact_payload.get("collection"),
            "scene_id": artifact_payload.get("scene_id"),
            "source_uri": artifact_payload.get("source_uri"),
            "created_by_job_id": artifact_payload.get("created_by_job_id"),
            "source_job_id": artifact_payload.get("source_job_id"),
            "data_family": artifact_payload.get("data_family"),
            "band_names_json": json.dumps(artifact_payload.get("band_names", [])),
            "dimensions_json": json.dumps(artifact_payload.get("dimensions", [])),
            "shape_json": json.dumps(artifact_payload.get("shape", [])),
            "size_bytes": artifact_payload.get("size_bytes"),
            "metadata_json": json.dumps(artifact_payload.get("metadata", {})),
        }
        with self._lock:
            existing = self._conn.execute(
                "SELECT created_at FROM artifacts WHERE artifact_uri = ?",
                (payload["artifact_uri"],),
            ).fetchone()
            created_at = str(existing["created_at"]) if existing else now
            self._conn.execute(
                """
                INSERT INTO artifacts(
                    artifact_id, artifact_type, artifact_uri, provider, collection, scene_id,
                    source_uri, created_by_job_id, source_job_id, data_family, band_names_json,
                    dimensions_json, shape_json, size_bytes, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_uri) DO UPDATE SET
                    artifact_id = excluded.artifact_id,
                    artifact_type = excluded.artifact_type,
                    provider = excluded.provider,
                    collection = excluded.collection,
                    scene_id = excluded.scene_id,
                    source_uri = excluded.source_uri,
                    created_by_job_id = excluded.created_by_job_id,
                    source_job_id = excluded.source_job_id,
                    data_family = excluded.data_family,
                    band_names_json = excluded.band_names_json,
                    dimensions_json = excluded.dimensions_json,
                    shape_json = excluded.shape_json,
                    size_bytes = excluded.size_bytes,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["artifact_id"],
                    payload["artifact_type"],
                    payload["artifact_uri"],
                    payload["provider"],
                    payload["collection"],
                    payload["scene_id"],
                    payload["source_uri"],
                    payload["created_by_job_id"],
                    payload["source_job_id"],
                    payload["data_family"],
                    payload["band_names_json"],
                    payload["dimensions_json"],
                    payload["shape_json"],
                    payload["size_bytes"],
                    payload["metadata_json"],
                    created_at,
                    now,
                ),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM artifacts WHERE artifact_uri = ?",
                (payload["artifact_uri"],),
            ).fetchone()
        return self._row_to_artifact(row)

    def list_artifacts(self, filters: ArtifactListFilters) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []
        if filters.artifact_type:
            where.append("artifact_type = ?")
            params.append(filters.artifact_type)
        if filters.provider:
            where.append("provider = ?")
            params.append(filters.provider)
        if filters.collection:
            where.append("collection = ?")
            params.append(filters.collection)
        if filters.scene_id:
            where.append("scene_id = ?")
            params.append(filters.scene_id)
        if filters.job_id:
            where.append("(created_by_job_id = ? OR source_job_id = ?)")
            params.extend([filters.job_id, filters.job_id])
        if filters.uri_query:
            where.append("(artifact_uri LIKE ? OR source_uri LIKE ?)")
            like_value = f"%{filters.uri_query}%"
            params.extend([like_value, like_value])
        if filters.date_from:
            where.append("updated_at >= ?")
            params.append(filters.date_from.isoformat())
        if filters.date_to:
            where.append("updated_at <= ?")
            params.append(filters.date_to.isoformat())

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size
        with self._lock:
            total_row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM artifacts {where_sql}", params
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT * FROM artifacts
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()
        total = int(total_row["n"]) if total_row else 0
        return [self._row_to_artifact(row) for row in rows], total

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
        now = self._utc_now()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE jobs
                SET state = 'running',
                    started_at = COALESCE(started_at, ?),
                    updated_at = ?,
                    worker_id = ?
                WHERE job_id = ? AND state = 'queued'
                """,
                (now, now, worker_id, job_id),
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

    @staticmethod
    def _row_to_worker(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "worker_id": row["worker_id"],
            "runtime_role": row["runtime_role"],
            "execution_enabled": bool(int(row["execution_enabled"])),
            "max_concurrent_jobs": int(row["max_concurrent_jobs"]),
            "queue_poll_seconds": float(row["queue_poll_seconds"]),
            "heartbeat_interval_seconds": float(row["heartbeat_interval_seconds"]),
            "provider_limits": json.loads(row["provider_limits_json"]),
            "hostname": row["hostname"],
            "pid": row["pid"],
            "active_running_jobs": int(row["active_running_jobs"]),
            "active_cancel_requested_jobs": int(row["active_cancel_requested_jobs"]),
            "queue_backlog": int(row["queue_backlog"]),
            "metadata": json.loads(row["metadata_json"]),
            "started_at": row["started_at"],
            "last_seen_at": row["last_seen_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def upsert_worker_heartbeat(self, worker_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = self._utc_now()
        record = {
            "worker_id": worker_id,
            "runtime_role": str(payload.get("runtime_role") or "worker"),
            "execution_enabled": 1 if bool(payload.get("execution_enabled", True)) else 0,
            "max_concurrent_jobs": max(1, int(payload.get("max_concurrent_jobs", 1) or 1)),
            "queue_poll_seconds": float(payload.get("queue_poll_seconds", 1.0) or 1.0),
            "heartbeat_interval_seconds": float(payload.get("heartbeat_interval_seconds", 5.0) or 5.0),
            "provider_limits_json": json.dumps(payload.get("provider_limits") or {}),
            "hostname": payload.get("hostname"),
            "pid": payload.get("pid"),
            "active_running_jobs": max(0, int(payload.get("active_running_jobs", 0) or 0)),
            "active_cancel_requested_jobs": max(0, int(payload.get("active_cancel_requested_jobs", 0) or 0)),
            "queue_backlog": max(0, int(payload.get("queue_backlog", 0) or 0)),
            "metadata_json": json.dumps(payload.get("metadata") or {}),
            "started_at": str(payload.get("started_at") or now),
            "last_seen_at": str(payload.get("last_seen_at") or now),
            "created_at": now,
            "updated_at": now,
        }
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO workers(
                    worker_id, runtime_role, execution_enabled, max_concurrent_jobs,
                    queue_poll_seconds, heartbeat_interval_seconds, provider_limits_json,
                    hostname, pid, active_running_jobs, active_cancel_requested_jobs,
                    queue_backlog, metadata_json, started_at, last_seen_at, created_at, updated_at
                ) VALUES (
                    :worker_id, :runtime_role, :execution_enabled, :max_concurrent_jobs,
                    :queue_poll_seconds, :heartbeat_interval_seconds, :provider_limits_json,
                    :hostname, :pid, :active_running_jobs, :active_cancel_requested_jobs,
                    :queue_backlog, :metadata_json, :started_at, :last_seen_at, :created_at, :updated_at
                )
                ON CONFLICT(worker_id) DO UPDATE SET
                    runtime_role = excluded.runtime_role,
                    execution_enabled = excluded.execution_enabled,
                    max_concurrent_jobs = excluded.max_concurrent_jobs,
                    queue_poll_seconds = excluded.queue_poll_seconds,
                    heartbeat_interval_seconds = excluded.heartbeat_interval_seconds,
                    provider_limits_json = excluded.provider_limits_json,
                    hostname = excluded.hostname,
                    pid = excluded.pid,
                    active_running_jobs = excluded.active_running_jobs,
                    active_cancel_requested_jobs = excluded.active_cancel_requested_jobs,
                    queue_backlog = excluded.queue_backlog,
                    metadata_json = excluded.metadata_json,
                    started_at = COALESCE(workers.started_at, excluded.started_at),
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                record,
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"Failed to upsert worker heartbeat for '{worker_id}'.")
        return self._row_to_worker(row)

    def list_workers(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM workers ORDER BY last_seen_at DESC"
            ).fetchall()
        return [self._row_to_worker(row) for row in rows]

    def prune_stale_workers(self, stale_after_seconds: int) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(1, int(stale_after_seconds)))
        ).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM workers WHERE last_seen_at < ?",
                (cutoff,),
            )
            self._conn.commit()
        return int(cursor.rowcount or 0)
