from __future__ import annotations

from typing import Any

from nimbuschain_fetch.domain.records import JobResultRecord, JobRowRecord
from nimbuschain_fetch.jobs.store import JobStore


class FetcherStoreRecordSupport:
    """Typed store boundary helpers for the fetcher facade."""

    def __init__(
        self,
        *,
        store: JobStore,
        normalize_historical_job_row: Any,
        normalize_job_row: Any,
        normalize_result_payload: Any,
    ) -> None:
        self._store = store
        self._normalize_historical_job_row = normalize_historical_job_row
        self._normalize_job_row = normalize_job_row
        self._normalize_result_payload = normalize_result_payload

    def get_job_row_record(self, job_id: str) -> JobRowRecord | None:
        record = self._store.get_job_record(job_id)
        if record is not None:
            return JobRowRecord.from_row(
                self._normalize_job_row(
                    self._normalize_historical_job_row(record.to_row())
                )
            )
        raw_row = self._store.get_job(job_id)
        if raw_row is None:
            return None
        return JobRowRecord.from_row(
            self._normalize_job_row(
                self._normalize_historical_job_row(raw_row)
            )
        )

    def get_job_row_payload(self, job_id: str) -> dict[str, Any]:
        record = self.get_job_row_record(job_id)
        return record.to_row() if record is not None else {}

    def get_result_record(self, job_id: str) -> JobResultRecord | None:
        if hasattr(self._store, "get_result_record"):
            record = self._store.get_result_record(job_id)
            if record is not None:
                return JobResultRecord.from_row(
                    job_id,
                    self._normalize_result_payload(record.to_row()),
                )
        raw_result = self._store.get_result(job_id)
        if raw_result is None:
            return None
        return JobResultRecord.from_row(
            job_id,
            self._normalize_result_payload(raw_result),
        )

    def get_result_payload(self, job_id: str) -> dict[str, Any]:
        record = self.get_result_record(job_id)
        return record.to_row() if record is not None else {}

    def set_result_record(self, result: JobResultRecord) -> None:
        if hasattr(self._store, "set_result_record"):
            self._store.set_result_record(result)
            return
        self._store.set_result(result.job_id, result.to_row())
