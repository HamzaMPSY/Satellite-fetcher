from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from nimbuschain_fetch.jobs.store import JobListFilters

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover - import guarded for optional runtime
    ASCENDING = 1
    DESCENDING = -1
    MongoClient = None
    ReturnDocument = None
    PyMongoError = Exception


class MongoJobStore:
    """MongoDB-backed store for jobs, events and results."""

    def __init__(self, *, uri: str, db_name: str):
        if MongoClient is None or ReturnDocument is None:
            raise RuntimeError(
                "pymongo is required for MongoDB backend. Install dependencies first."
            )

        self._uri = uri
        self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self._db = self._client[db_name]
        self._jobs = self._db.jobs
        self._events = self._db.job_events
        self._results = self._db.job_results
        self._counters = self._db.counters
        self._wait_until_ready(timeout_seconds=60)
        self._init_schema()

    def _wait_until_ready(self, timeout_seconds: int = 60) -> None:
        deadline = time.monotonic() + max(1, timeout_seconds)
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self._client.admin.command("ping")
                return
            except PyMongoError as exc:  # pragma: no cover - depends on runtime env
                last_error = exc
                time.sleep(1.0)

        raise RuntimeError(
            f"MongoDB is not reachable at '{self._uri}' after {timeout_seconds}s. "
            f"Last error: {last_error}"
        )

    def _init_schema(self) -> None:
        self._jobs.create_index([("job_id", ASCENDING)], unique=True)
        self._jobs.create_index([("state", ASCENDING)])
        self._jobs.create_index([("provider", ASCENDING)])
        self._jobs.create_index([("created_at", DESCENDING)])

        self._events.create_index([("event_id", ASCENDING)], unique=True)
        self._events.create_index([("job_id", ASCENDING), ("event_id", ASCENDING)])
        self._events.create_index([("timestamp", DESCENDING)])

        self._results.create_index([("job_id", ASCENDING)], unique=True)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_job(doc: dict[str, Any] | None) -> dict[str, Any] | None:
        if not doc:
            return None
        doc = dict(doc)
        doc.pop("_id", None)
        if "errors" not in doc:
            doc["errors"] = []
        return doc

    def _next_event_id(self) -> int:
        row = self._counters.find_one_and_update(
            {"_id": "job_events"},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(row.get("seq", 1))

    def create_job(
        self,
        job_id: str,
        job_type: str,
        provider: str,
        collection: str,
        request_payload: dict[str, Any],
    ) -> None:
        now = self._utc_now()
        self._jobs.insert_one(
            {
                "job_id": job_id,
                "job_type": job_type,
                "provider": provider,
                "collection": collection,
                "request": request_payload,
                "state": "queued",
                "progress": 0.0,
                "bytes_downloaded": 0,
                "bytes_total": 0,
                "started_at": None,
                "finished_at": None,
                "errors": [],
                "created_at": now,
                "updated_at": now,
            }
        )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self._jobs.find_one({"job_id": job_id})
        return self._normalize_job(row)

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = self._utc_now()
        self._jobs.update_one({"job_id": job_id}, {"$set": fields})

    def list_jobs(self, filters: JobListFilters) -> tuple[list[dict[str, Any]], int]:
        query: dict[str, Any] = {}
        if filters.state:
            query["state"] = filters.state
        if filters.provider:
            query["provider"] = filters.provider

        created_range: dict[str, Any] = {}
        if filters.date_from:
            created_range["$gte"] = filters.date_from.isoformat()
        if filters.date_to:
            created_range["$lte"] = filters.date_to.isoformat()
        if created_range:
            query["created_at"] = created_range

        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size

        total = self._jobs.count_documents(query)
        rows = (
            self._jobs.find(query)
            .sort("created_at", DESCENDING)
            .skip(offset)
            .limit(page_size)
        )
        return [self._normalize_job(row) for row in rows if row], int(total)

    def append_event(
        self,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> int:
        ts = (timestamp or datetime.now(timezone.utc)).isoformat()
        event_id = self._next_event_id()
        self._events.insert_one(
            {
                "event_id": event_id,
                "job_id": job_id,
                "type": event_type,
                "timestamp": ts,
                "payload": payload,
            }
        )
        return event_id

    def list_events(
        self,
        job_id: str | None,
        since_id: int | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if job_id:
            query["job_id"] = job_id
        if since_id is not None:
            query["event_id"] = {"$gt": since_id}

        rows = self._events.find(query).sort("event_id", ASCENDING).limit(max(1, min(1000, limit)))
        return [
            {
                "id": int(row["event_id"]),
                "job_id": row["job_id"],
                "type": row["type"],
                "timestamp": row["timestamp"],
                "payload": row.get("payload", {}),
            }
            for row in rows
        ]

    def set_result(self, job_id: str, result_payload: dict[str, Any]) -> None:
        self._results.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "job_id": job_id,
                    "result": result_payload,
                    "updated_at": self._utc_now(),
                }
            },
            upsert=True,
        )

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        row = self._results.find_one({"job_id": job_id})
        if not row:
            return None
        return row.get("result")

    def requeue_incomplete_jobs(self) -> list[str]:
        rows = list(
            self._jobs.find(
                {"state": {"$in": ["running", "cancel_requested"]}},
                {"job_id": 1},
            )
        )
        job_ids = [str(row["job_id"]) for row in rows]
        if not job_ids:
            return []

        now = self._utc_now()
        self._jobs.update_many(
            {"job_id": {"$in": job_ids}},
            {"$set": {"state": "queued", "updated_at": now}},
        )
        for jid in job_ids:
            self.append_event(
                jid,
                "job.requeued_after_restart",
                {"reason": "service_restart"},
            )
        return job_ids

    def claim_job_for_execution(self, job_id: str, worker_id: str) -> bool:
        now = self._utc_now()
        result = self._jobs.update_one(
            {"job_id": job_id, "state": "queued"},
            {
                "$set": {
                    "state": "running",
                    "started_at": now,
                    "updated_at": now,
                    "worker_id": worker_id,
                }
            },
        )
        return result.modified_count > 0

    def requeue_stale_running_jobs(self, stale_after_seconds: int) -> list[str]:
        stale_before = (
            datetime.now(timezone.utc) - timedelta(seconds=max(1, int(stale_after_seconds)))
        ).isoformat()
        rows = list(
            self._jobs.find(
                {
                    "state": {"$in": ["running", "cancel_requested"]},
                    "updated_at": {"$lt": stale_before},
                },
                {"job_id": 1},
            )
        )
        job_ids = [str(row["job_id"]) for row in rows]
        if not job_ids:
            return []

        now = self._utc_now()
        self._jobs.update_many(
            {"job_id": {"$in": job_ids}},
            {"$set": {"state": "queued", "updated_at": now}},
        )
        for jid in job_ids:
            self.append_event(
                jid,
                "job.requeued_stale",
                {"reason": "stale_running_timeout"},
            )
        return job_ids
