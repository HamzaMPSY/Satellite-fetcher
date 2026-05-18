from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from nimbuschain_mask_service.models import ProgressEvent, ProgressRecord, StageEventPayload


_LOCK = threading.RLock()
_PROGRESS: dict[str, dict[str, Any]] = {}
_MAX_HISTORY = 256


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_progress(
    job_id: str,
    *,
    stage_name: str,
    payload: StageEventPayload | dict[str, Any] | None = None,
    status: str = "running",
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return {}
    with _LOCK:
        previous = dict(_PROGRESS.get(normalized_job_id) or {})
        event = ProgressEvent(
            job_id=normalized_job_id,
            stage_name=str(stage_name or "").strip(),
            payload=(
                payload.to_dict()
                if isinstance(payload, StageEventPayload)
                else dict(payload or {})
            ),
            status=str(status or "running").strip().lower() or "running",
            updated_at=_now_iso(),
            sequence=int(previous.get("sequence") or 0) + 1,
        )
        history: list[ProgressEvent] = []
        for item in list(previous.get("history") or []):
            if isinstance(item, dict):
                history.append(
                    ProgressEvent(
                        job_id=str(item.get("job_id") or normalized_job_id),
                        stage_name=str(item.get("stage_name") or ""),
                        payload=dict(item.get("payload") or {}),
                        status=str(item.get("status") or "running"),
                        updated_at=str(item.get("updated_at") or _now_iso()),
                        sequence=int(item.get("sequence") or 0),
                    )
                )
        history.append(event)
        if len(history) > _MAX_HISTORY:
            history = history[-_MAX_HISTORY:]
        record = ProgressRecord(
            job_id=event.job_id,
            stage_name=event.stage_name,
            payload=dict(event.payload),
            status=event.status,
            updated_at=event.updated_at,
            sequence=event.sequence,
            history=history,
        )
        _PROGRESS[normalized_job_id] = record.to_dict()
        return record.to_dict()


def get_progress(job_id: str) -> dict[str, Any] | None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return None
    with _LOCK:
        record = _PROGRESS.get(normalized_job_id)
        if not record:
            return None
        snapshot = ProgressRecord(
            job_id=str(record.get("job_id") or normalized_job_id),
            stage_name=str(record.get("stage_name") or ""),
            payload=dict(record.get("payload") or {}),
            status=str(record.get("status") or "running"),
            updated_at=str(record.get("updated_at") or _now_iso()),
            sequence=int(record.get("sequence") or 0),
            history=[
                ProgressEvent(
                    job_id=str(item.get("job_id") or normalized_job_id),
                    stage_name=str(item.get("stage_name") or ""),
                    payload=dict(item.get("payload") or {}),
                    status=str(item.get("status") or "running"),
                    updated_at=str(item.get("updated_at") or _now_iso()),
                    sequence=int(item.get("sequence") or 0),
                )
                for item in list(record.get("history") or [])
                if isinstance(item, dict)
            ],
        )
        return snapshot.to_dict()


def clear_progress(job_id: str) -> None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return
    with _LOCK:
        _PROGRESS.pop(normalized_job_id, None)
