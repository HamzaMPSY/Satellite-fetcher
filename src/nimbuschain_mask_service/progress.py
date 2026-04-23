from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any


_LOCK = threading.RLock()
_PROGRESS: dict[str, dict[str, Any]] = {}
_MAX_HISTORY = 256


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_progress(
    job_id: str,
    *,
    stage_name: str,
    payload: dict[str, Any] | None = None,
    status: str = "running",
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return {}
    with _LOCK:
        previous = dict(_PROGRESS.get(normalized_job_id) or {})
        event = {
            "job_id": normalized_job_id,
            "stage_name": str(stage_name or "").strip(),
            "payload": dict(payload or {}),
            "status": str(status or "running").strip().lower() or "running",
            "updated_at": _now_iso(),
            "sequence": int(previous.get("sequence") or 0) + 1,
        }
        history: list[dict[str, Any]] = []
        for item in list(previous.get("history") or []):
            if isinstance(item, dict):
                history.append(dict(item))
        history.append(dict(event))
        if len(history) > _MAX_HISTORY:
            history = history[-_MAX_HISTORY:]
        record = {
            **event,
            "history": history,
        }
        _PROGRESS[normalized_job_id] = record
        return dict(record)


def get_progress(job_id: str) -> dict[str, Any] | None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return None
    with _LOCK:
        record = _PROGRESS.get(normalized_job_id)
        if not record:
            return None
        snapshot = dict(record)
        snapshot["history"] = [
            dict(item)
            for item in list(record.get("history") or [])
            if isinstance(item, dict)
        ]
        return snapshot


def clear_progress(job_id: str) -> None:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return
    with _LOCK:
        _PROGRESS.pop(normalized_job_id, None)
