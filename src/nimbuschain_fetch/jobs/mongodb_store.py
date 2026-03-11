from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from nimbuschain_fetch.jobs.store import ArtifactListFilters, JobListFilters

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
        self._artifacts = self._db.artifacts
        self._workers = self._db.workers
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
        self._jobs.create_index([("collection", ASCENDING)])
        self._jobs.create_index([("product_type", ASCENDING)])
        self._jobs.create_index([("created_at", DESCENDING)])
        self._jobs.create_index([("updated_at", DESCENDING)])
        self._jobs.create_index([("provider", ASCENDING), ("state", ASCENDING), ("created_at", DESCENDING)])
        self._jobs.create_index([("provider", ASCENDING), ("created_at", DESCENDING)])

        self._events.create_index([("event_id", ASCENDING)], unique=True)
        self._events.create_index([("job_id", ASCENDING), ("event_id", ASCENDING)])
        self._events.create_index([("timestamp", DESCENDING)])

        self._results.create_index([("job_id", ASCENDING)], unique=True)
        self._artifacts.create_index([("artifact_id", ASCENDING)], unique=True)
        self._artifacts.create_index([("artifact_uri", ASCENDING)], unique=True)
        self._artifacts.create_index([("artifact_type", ASCENDING), ("updated_at", DESCENDING)])
        self._artifacts.create_index([("provider", ASCENDING), ("updated_at", DESCENDING)])
        self._artifacts.create_index([("collection", ASCENDING), ("updated_at", DESCENDING)])
        self._artifacts.create_index([("scene_id", ASCENDING), ("updated_at", DESCENDING)])
        self._workers.create_index([("worker_id", ASCENDING)], unique=True)
        self._workers.create_index([("last_seen_at", DESCENDING)])

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
        request = dict(doc.get("request") or {})
        doc["product_type"] = doc.get("product_type") or request.get("product_type")
        doc["tile_id"] = doc.get("tile_id") or request.get("tile_id")
        return doc

    @staticmethod
    def _normalize_worker(doc: dict[str, Any] | None) -> dict[str, Any] | None:
        if not doc:
            return None
        doc = dict(doc)
        doc.pop("_id", None)
        doc["execution_enabled"] = bool(doc.get("execution_enabled", False))
        doc["max_concurrent_jobs"] = int(doc.get("max_concurrent_jobs", 0) or 0)
        doc["queue_poll_seconds"] = float(doc.get("queue_poll_seconds", 0.0) or 0.0)
        doc["heartbeat_interval_seconds"] = float(doc.get("heartbeat_interval_seconds", 0.0) or 0.0)
        doc["active_running_jobs"] = int(doc.get("active_running_jobs", 0) or 0)
        doc["active_cancel_requested_jobs"] = int(doc.get("active_cancel_requested_jobs", 0) or 0)
        doc["queue_backlog"] = int(doc.get("queue_backlog", 0) or 0)
        doc["provider_limits"] = dict(doc.get("provider_limits") or {})
        doc["metadata"] = dict(doc.get("metadata") or {})
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
                "product_type": request_payload.get("product_type"),
                "tile_id": request_payload.get("tile_id"),
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
        if filters.states:
            query["state"] = {"$in": list(filters.states)}
        if filters.provider:
            query["provider"] = filters.provider
        if filters.collection:
            query["collection"] = filters.collection
        if filters.product_type:
            query.setdefault("$and", [])
            query["$and"].append(
                {
                    "$or": [
                        {"product_type": filters.product_type},
                        {"request.product_type": filters.product_type},
                    ]
                }
            )
        if filters.worker_id:
            query["worker_id"] = filters.worker_id
        if filters.job_id_query:
            query["job_id"] = {"$regex": filters.job_id_query, "$options": "i"}

        created_range: dict[str, Any] = {}
        if filters.date_from:
            created_range["$gte"] = filters.date_from.isoformat()
        if filters.date_to:
            created_range["$lte"] = filters.date_to.isoformat()
        if created_range:
            query["created_at"] = created_range

        updated_range: dict[str, Any] = {}
        if filters.updated_from:
            updated_range["$gte"] = filters.updated_from.isoformat()
        if filters.updated_to:
            updated_range["$lte"] = filters.updated_to.isoformat()
        if updated_range:
            query["updated_at"] = updated_range

        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size
        sort_field = filters.sort_by if filters.sort_by in {"created_at", "updated_at", "started_at", "finished_at"} else "updated_at"
        sort_dir = DESCENDING if filters.sort_desc else ASCENDING

        total = self._jobs.count_documents(query)
        rows = (
            self._jobs.find(query)
            .sort(sort_field, sort_dir)
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
            "band_names": list(artifact_payload.get("band_names", [])),
            "dimensions": list(artifact_payload.get("dimensions", [])),
            "shape": list(artifact_payload.get("shape", [])),
            "size_bytes": artifact_payload.get("size_bytes"),
            "metadata": dict(artifact_payload.get("metadata", {})),
        }
        existing = self._artifacts.find_one({"artifact_uri": payload["artifact_uri"]}, {"created_at": 1})
        created_at = str(existing.get("created_at")) if existing else now
        self._artifacts.update_one(
            {"artifact_uri": payload["artifact_uri"]},
            {
                "$set": {
                    **payload,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": created_at,
                },
            },
            upsert=True,
        )
        row = self._artifacts.find_one({"artifact_uri": payload["artifact_uri"]})
        return self._normalize_artifact(row)

    @staticmethod
    def _normalize_artifact(doc: dict[str, Any] | None) -> dict[str, Any] | None:
        if not doc:
            return None
        doc = dict(doc)
        doc.pop("_id", None)
        doc.setdefault("band_names", [])
        doc.setdefault("dimensions", [])
        doc.setdefault("shape", [])
        doc.setdefault("metadata", {})
        return doc

    def list_artifacts(self, filters: ArtifactListFilters) -> tuple[list[dict[str, Any]], int]:
        query: dict[str, Any] = {}
        if filters.artifact_type:
            query["artifact_type"] = filters.artifact_type
        if filters.provider:
            query["provider"] = filters.provider
        if filters.collection:
            query["collection"] = filters.collection
        if filters.scene_id:
            query["scene_id"] = filters.scene_id
        if filters.job_id:
            query["$or"] = [
                {"created_by_job_id": filters.job_id},
                {"source_job_id": filters.job_id},
            ]
        if filters.uri_query:
            regex_query = {"$regex": filters.uri_query, "$options": "i"}
            query["$and"] = query.get("$and", [])
            query["$and"].append(
                {
                    "$or": [
                        {"artifact_uri": regex_query},
                        {"source_uri": regex_query},
                    ]
                }
            )
        updated_range: dict[str, Any] = {}
        if filters.date_from:
            updated_range["$gte"] = filters.date_from.isoformat()
        if filters.date_to:
            updated_range["$lte"] = filters.date_to.isoformat()
        if updated_range:
            query["updated_at"] = updated_range

        page = max(1, filters.page)
        page_size = max(1, min(200, filters.page_size))
        offset = (page - 1) * page_size
        total = self._artifacts.count_documents(query)
        rows = (
            self._artifacts.find(query)
            .sort("updated_at", DESCENDING)
            .skip(offset)
            .limit(page_size)
        )
        return [self._normalize_artifact(row) for row in rows if row], int(total)

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

    def upsert_worker_heartbeat(self, worker_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = self._utc_now()
        document = {
            "worker_id": worker_id,
            "runtime_role": str(payload.get("runtime_role") or "worker"),
            "execution_enabled": bool(payload.get("execution_enabled", True)),
            "max_concurrent_jobs": max(1, int(payload.get("max_concurrent_jobs", 1) or 1)),
            "queue_poll_seconds": float(payload.get("queue_poll_seconds", 1.0) or 1.0),
            "heartbeat_interval_seconds": float(payload.get("heartbeat_interval_seconds", 5.0) or 5.0),
            "provider_limits": dict(payload.get("provider_limits") or {}),
            "hostname": payload.get("hostname"),
            "pid": payload.get("pid"),
            "active_running_jobs": max(0, int(payload.get("active_running_jobs", 0) or 0)),
            "active_cancel_requested_jobs": max(0, int(payload.get("active_cancel_requested_jobs", 0) or 0)),
            "queue_backlog": max(0, int(payload.get("queue_backlog", 0) or 0)),
            "metadata": dict(payload.get("metadata") or {}),
            "started_at": str(payload.get("started_at") or now),
            "last_seen_at": str(payload.get("last_seen_at") or now),
            "updated_at": now,
        }
        self._workers.update_one(
            {"worker_id": worker_id},
            {
                "$set": document,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return self._normalize_worker(self._workers.find_one({"worker_id": worker_id})) or {}

    def list_workers(self) -> list[dict[str, Any]]:
        rows = self._workers.find({}).sort("last_seen_at", DESCENDING)
        return [self._normalize_worker(row) for row in rows if row]

    def prune_stale_workers(self, stale_after_seconds: int) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(1, int(stale_after_seconds)))
        ).isoformat()
        result = self._workers.delete_many({"last_seen_at": {"$lt": cutoff}})
        return int(result.deleted_count or 0)
