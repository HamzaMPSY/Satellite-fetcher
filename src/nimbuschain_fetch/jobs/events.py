from __future__ import annotations

from datetime import datetime, timezone
from time import monotonic
from typing import AsyncIterator

import anyio

from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import JobEvent


async def stream_events(
    store: JobStore,
    *,
    job_id: str | None = None,
    since_id: int | None = None,
    heartbeat_seconds: float = 10.0,
    poll_interval: float = 0.5,
) -> AsyncIterator[JobEvent]:
    """Poll persisted events and yield heartbeat events while idle."""

    cursor = since_id
    heartbeat_deadline = monotonic() + max(1.0, heartbeat_seconds)

    while True:
        rows = store.list_events(job_id=job_id, since_id=cursor, limit=200)
        if rows:
            for row in rows:
                cursor = row["id"]
                yield JobEvent(
                    id=row["id"],
                    job_id=row["job_id"],
                    type=row["type"],
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    payload=row["payload"],
                )
            heartbeat_deadline = monotonic() + max(1.0, heartbeat_seconds)
            continue

        if monotonic() >= heartbeat_deadline:
            yield JobEvent(
                id=None,
                job_id=job_id or "_all",
                type="heartbeat",
                timestamp=datetime.now(timezone.utc),
                payload={},
            )
            heartbeat_deadline = monotonic() + max(1.0, heartbeat_seconds)

        await anyio.sleep(max(0.1, poll_interval))
