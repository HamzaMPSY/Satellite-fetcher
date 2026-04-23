from __future__ import annotations

import json

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch_service.dependencies import get_fetcher

router = APIRouter(prefix="/v1", tags=["events"])


@router.get("/events")
async def events(
    job_id: str | None = None,
    since: int | None = None,
    fetcher: NimbusFetcher = Depends(get_fetcher),
) -> StreamingResponse:
    async def event_stream():
        async for event in fetcher.stream_events(job_id=job_id, since=since):
            payload = event.model_dump(mode="json")
            event_id = payload.get("id")
            if event_id is not None:
                yield f"id: {event_id}\n"
            yield f"event: {payload['type']}\n"
            yield f"data: {json.dumps(payload)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
