from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import Any

import requests
from anyio.from_thread import BlockingPortal, start_blocking_portal
from pydantic import TypeAdapter

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import (
    BatchJobCreateRequest,
    BatchJobCreatedResponse,
    JobCreateRequest,
    JobEvent,
    JobListResponse,
    JobResultResponse,
    JobStatusResponse,
)
from nimbuschain_fetch.settings import get_settings


class NimbusFetcherClient(AbstractContextManager["NimbusFetcherClient"]):
    """Unified client supporting direct mode and service mode."""

    def __init__(
        self,
        *,
        mode: str = "direct",
        service_url: str | None = None,
        api_key: str | None = None,
        fetcher: NimbusFetcher | None = None,
    ):
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"direct", "service"}:
            raise ValueError("mode must be 'direct' or 'service'.")

        self.mode = normalized_mode
        self.service_url = (service_url or "http://127.0.0.1:8000").rstrip("/")
        self.api_key = api_key

        self._request_adapter = TypeAdapter(JobCreateRequest)
        self._batch_adapter = TypeAdapter(BatchJobCreateRequest)

        self._session: requests.Session | None = None
        self._portal: BlockingPortal | None = None
        self._portal_cm = None
        self._fetcher: NimbusFetcher | None = None

        if self.mode == "service":
            self._session = requests.Session()
            if self.api_key:
                self._session.headers.update({"X-API-Key": self.api_key})
        else:
            self._portal_cm = start_blocking_portal()
            self._portal = self._portal_cm.__enter__()
            if fetcher is not None:
                self._fetcher = fetcher
            else:
                settings = get_settings().model_copy(update={"nimbus_runtime_role": "all"})
                self._fetcher = NimbusFetcher(settings=settings)
            self._portal.call(self._fetcher.start)

    def close(self) -> None:
        if self.mode == "direct":
            if self._portal and self._fetcher:
                self._portal.call(self._fetcher.stop)
            if self._portal_cm:
                self._portal_cm.__exit__(None, None, None)
            self._portal = None
            self._portal_cm = None
            self._fetcher = None
        else:
            if self._session:
                self._session.close()
            self._session = None

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit_job(self, request: JobCreateRequest | dict[str, Any]) -> str:
        req = self._request_adapter.validate_python(request)
        if self.mode == "direct":
            assert self._portal and self._fetcher
            return str(self._portal.call(self._fetcher.submit_job, req))

        assert self._session is not None
        response = self._session.post(
            f"{self.service_url}/v1/jobs",
            json=req.model_dump(mode="json"),
            timeout=60,
        )
        response.raise_for_status()
        return str(response.json()["job_id"])

    def submit_batch(self, jobs: BatchJobCreateRequest | dict[str, Any]) -> list[str]:
        batch = self._batch_adapter.validate_python(jobs)
        if self.mode == "direct":
            assert self._portal and self._fetcher
            return list(self._portal.call(self._fetcher.submit_batch, batch))

        assert self._session is not None
        response = self._session.post(
            f"{self.service_url}/v1/jobs/batch",
            json=batch.model_dump(mode="json"),
            timeout=60,
        )
        response.raise_for_status()
        return BatchJobCreatedResponse.model_validate(response.json()).job_ids

    def get_job(self, job_id: str) -> JobStatusResponse:
        if self.mode == "direct":
            assert self._portal and self._fetcher
            return self._portal.call(self._fetcher.get_job, job_id)

        assert self._session is not None
        response = self._session.get(f"{self.service_url}/v1/jobs/{job_id}", timeout=30)
        response.raise_for_status()
        return JobStatusResponse.model_validate(response.json())

    def cancel_job(self, job_id: str) -> bool:
        if self.mode == "direct":
            assert self._portal and self._fetcher
            return bool(self._portal.call(self._fetcher.cancel_job, job_id))

        assert self._session is not None
        response = self._session.delete(f"{self.service_url}/v1/jobs/{job_id}", timeout=30)
        response.raise_for_status()
        payload = response.json()
        return bool(payload.get("cancel_requested", False))

    def get_result(self, job_id: str) -> JobResultResponse:
        if self.mode == "direct":
            assert self._portal and self._fetcher
            return self._portal.call(self._fetcher.get_result, job_id)

        assert self._session is not None
        response = self._session.get(f"{self.service_url}/v1/jobs/{job_id}/result", timeout=30)
        response.raise_for_status()
        return JobResultResponse.model_validate(response.json())

    def list_jobs(
        self,
        *,
        state: str | None = None,
        provider: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> JobListResponse:
        if self.mode == "direct":
            assert self._portal and self._fetcher
            from datetime import datetime

            parsed_from = datetime.fromisoformat(date_from) if date_from else None
            parsed_to = datetime.fromisoformat(date_to) if date_to else None
            return self._portal.call(
                self._fetcher.list_jobs,
                state=state,
                provider=provider,
                date_from=parsed_from,
                date_to=parsed_to,
                page=page,
                page_size=page_size,
            )

        assert self._session is not None
        response = self._session.get(
            f"{self.service_url}/v1/jobs",
            params={
                "state": state,
                "provider": provider,
                "date_from": date_from,
                "date_to": date_to,
                "page": page,
                "page_size": page_size,
            },
            timeout=30,
        )
        response.raise_for_status()
        return JobListResponse.model_validate(response.json())

    def stream_events(
        self,
        *,
        job_id: str | None = None,
        since: int | None = None,
        poll_interval: float = 0.5,
    ) -> Iterator[JobEvent]:
        if self.mode == "direct":
            assert self._portal and self._fetcher
            cursor = since
            while True:
                rows = self._portal.call(self._fetcher.store.list_events, job_id, cursor, 200)
                if rows:
                    for row in rows:
                        cursor = int(row["id"])
                        yield JobEvent.model_validate(
                            {
                                "id": row["id"],
                                "job_id": row["job_id"],
                                "type": row["type"],
                                "timestamp": row["timestamp"],
                                "payload": row["payload"],
                            }
                        )
                    continue
                time.sleep(max(0.2, poll_interval))

        assert self._session is not None
        response = self._session.get(
            f"{self.service_url}/v1/events",
            params={"job_id": job_id, "since": since},
            stream=True,
            timeout=120,
        )
        response.raise_for_status()

        event_type = "message"
        event_id: int | None = None
        data_parts: list[str] = []

        for raw_line in response.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if not line:
                if data_parts:
                    payload = json.loads("\n".join(data_parts))
                    payload.setdefault("type", event_type)
                    payload.setdefault("id", event_id)
                    yield JobEvent.model_validate(payload)
                event_type = "message"
                event_id = None
                data_parts = []
                continue

            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip()
            elif line.startswith("id:"):
                value = line.split(":", 1)[1].strip()
                event_id = int(value) if value.isdigit() else None
            elif line.startswith("data:"):
                data_parts.append(line.split(":", 1)[1].strip())
