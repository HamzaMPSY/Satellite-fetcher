from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(slots=True)
class JobListFilters:
    state: str | None = None
    provider: str | None = None
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

    def update_job(self, job_id: str, **fields: Any) -> None:
        ...

    def list_jobs(self, filters: JobListFilters) -> tuple[list[dict[str, Any]], int]:
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

    def set_result(self, job_id: str, result_payload: dict[str, Any]) -> None:
        ...

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        ...

    def requeue_incomplete_jobs(self) -> list[str]:
        ...

    def claim_job_for_execution(self, job_id: str, worker_id: str) -> bool:
        ...

    def requeue_stale_running_jobs(self, stale_after_seconds: int) -> list[str]:
        ...
