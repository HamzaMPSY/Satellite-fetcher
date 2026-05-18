from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from nimbuschain_fetch.domain.records import (
    ArtifactRowRecord,
    JobEventRecord,
    JobResultRecord,
    JobRowRecord,
    WorkerHeartbeatRecord,
)


@dataclass(slots=True)
class JobListFilters:
    state: str | None = None
    states: tuple[str, ...] = ()
    provider: str | None = None
    collection: str | None = None
    product_type: str | None = None
    worker_id: str | None = None
    job_id_query: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    updated_from: datetime | None = None
    updated_to: datetime | None = None
    sort_by: str = "updated_at"
    sort_desc: bool = True
    page: int = 1
    page_size: int = 20


@dataclass(slots=True)
class ArtifactListFilters:
    artifact_type: str | None = None
    provider: str | None = None
    collection: str | None = None
    scene_id: str | None = None
    job_id: str | None = None
    uri_query: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    page: int = 1
    page_size: int = 20


class JobStore(Protocol):
    def create_job(
        self,
        job_id: str,
        job_type: str,
        provider: str,
        collection: str,
        request_payload: dict[str, Any],
    ) -> None:
        ...

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        ...

    def get_job_record(self, job_id: str) -> JobRowRecord | None:
        ...

    def update_job(self, job_id: str, **fields: Any) -> None:
        ...

    def list_jobs(self, filters: JobListFilters) -> tuple[list[dict[str, Any]], int]:
        ...

    def list_job_records(self, filters: JobListFilters) -> tuple[list[JobRowRecord], int]:
        ...

    def append_event(
        self,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> int:
        ...

    def list_events(
        self,
        job_id: str | None,
        since_id: int | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        ...

    def list_event_records(
        self,
        job_id: str | None,
        since_id: int | None,
        limit: int = 200,
    ) -> list[JobEventRecord]:
        ...

    def set_result(self, job_id: str, result_payload: dict[str, Any]) -> None:
        ...

    def set_result_record(self, result: JobResultRecord) -> None:
        ...

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        ...

    def get_result_record(self, job_id: str) -> JobResultRecord | None:
        ...

    def upsert_artifact(self, artifact_payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def upsert_artifact_record(self, artifact: ArtifactRowRecord) -> ArtifactRowRecord:
        ...

    def list_artifacts(self, filters: ArtifactListFilters) -> tuple[list[dict[str, Any]], int]:
        ...

    def list_artifact_records(self, filters: ArtifactListFilters) -> tuple[list[ArtifactRowRecord], int]:
        ...

    def requeue_incomplete_jobs(self) -> list[str]:
        ...

    def claim_job_for_execution(self, job_id: str, worker_id: str) -> bool:
        ...

    def requeue_stale_running_jobs(self, stale_after_seconds: int) -> list[str]:
        ...

    def upsert_worker_heartbeat(self, worker_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def upsert_worker_heartbeat_record(
        self,
        worker_id: str,
        payload: dict[str, Any],
    ) -> WorkerHeartbeatRecord:
        ...

    def list_workers(self) -> list[dict[str, Any]]:
        ...

    def list_worker_records(self) -> list[WorkerHeartbeatRecord]:
        ...

    def prune_stale_workers(self, stale_after_seconds: int) -> int:
        ...

    def clear_workers(self) -> int:
        ...
