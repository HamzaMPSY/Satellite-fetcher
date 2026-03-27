from __future__ import annotations

from dataclasses import dataclass

from nimbuschain_fetch.jobs.mongodb_store import MongoJobStore


@dataclass
class _FakeUpdateResult:
    modified_count: int = 1


class _FakeJobsCollection:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, list[dict]]] = []

    def update_one(self, query, update):
        self.calls.append((query, update))
        return _FakeUpdateResult()


def test_claim_job_for_execution_keeps_existing_started_at_in_mongo_update_pipeline() -> None:
    store = MongoJobStore.__new__(MongoJobStore)
    store._jobs = _FakeJobsCollection()
    store._utc_now = lambda: "2026-03-19T12:00:00+00:00"  # type: ignore[method-assign]

    claimed = MongoJobStore.claim_job_for_execution(store, "job-123", "worker-1")

    assert claimed is True
    assert len(store._jobs.calls) == 1
    query, update = store._jobs.calls[0]
    assert query == {"job_id": "job-123", "state": "queued"}
    assert isinstance(update, list)
    stage = update[0]["$set"]
    assert stage["state"] == "running"
    assert stage["started_at"] == {"$ifNull": ["$started_at", "2026-03-19T12:00:00+00:00"]}
    assert stage["worker_id"] == "worker-1"
