from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


PIPELINE_TIMELINE_VERSION = 1

_STEP_DEFINITIONS: dict[str, dict[str, Any]] = {
    "queued": {
        "label": "Queued",
        "group": "queue",
        "group_label": "Queued",
        "order": 0,
        "badge": "WAIT",
        "kind": "queued",
    },
    "resume_after_restart": {
        "label": "Resume After Restart",
        "group": "queue",
        "group_label": "Queued",
        "order": 1,
        "badge": "WAIT",
        "kind": "running",
    },
    "searching": {
        "label": "Search Catalog",
        "group": "search",
        "group_label": "Search",
        "order": 10,
        "badge": "SRCH",
        "kind": "running",
    },
    "downloading": {
        "label": "Download Files",
        "group": "download",
        "group_label": "Download",
        "order": 20,
        "badge": "DL",
        "kind": "running",
    },
    "downloaded": {
        "label": "Download Complete",
        "group": "download",
        "group_label": "Download",
        "order": 21,
        "badge": "DL",
        "kind": "done",
    },
    "zarr_queued": {
        "label": "Conversion Queued",
        "group": "convert",
        "group_label": "Convert",
        "order": 30,
        "badge": "CNV",
        "kind": "queued",
    },
    "writing_chunks": {
        "label": "Write Zarr Chunks",
        "group": "convert",
        "group_label": "Convert",
        "order": 31,
        "badge": "CNV",
        "kind": "running",
    },
    "registering_artifact": {
        "label": "Register Zarr Artifact",
        "group": "convert",
        "group_label": "Convert",
        "order": 32,
        "badge": "CNV",
        "kind": "running",
    },
    "zarr_written": {
        "label": "Zarr Ready",
        "group": "ready",
        "group_label": "Ready",
        "order": 40,
        "badge": "RDY",
        "kind": "done",
    },
    "resolving_source_zarr": {
        "label": "Resolve Source Zarr",
        "group": "resolve",
        "group_label": "Resolve",
        "order": 50,
        "badge": "INIT",
        "kind": "running",
    },
    "copying_source_zarr": {
        "label": "Copy Source Zarr",
        "group": "resolve",
        "group_label": "Resolve",
        "order": 51,
        "badge": "INIT",
        "kind": "running",
    },
    "running_cloud_inference": {
        "label": "Cloud Masking",
        "group": "cloud",
        "group_label": "Cloud",
        "order": 60,
        "badge": "CLD",
        "kind": "running",
    },
    "cloud_failed": {
        "label": "Cloud Failed",
        "group": "cloud",
        "group_label": "Cloud",
        "order": 69,
        "badge": "FAIL",
        "kind": "failed",
    },
    "running_water_inference": {
        "label": "Water Masking",
        "group": "water",
        "group_label": "Water",
        "order": 70,
        "badge": "WTR",
        "kind": "running",
    },
    "water_failed": {
        "label": "Water Failed",
        "group": "water",
        "group_label": "Water",
        "order": 79,
        "badge": "FAIL",
        "kind": "failed",
    },
    "writing_mask_artifacts": {
        "label": "Write Mask Artifacts",
        "group": "mask_finalize",
        "group_label": "Finalize",
        "order": 80,
        "badge": "MASK",
        "kind": "running",
    },
    "writing_masked_zarr": {
        "label": "Write Masked Zarr",
        "group": "mask_finalize",
        "group_label": "Finalize",
        "order": 81,
        "badge": "MASK",
        "kind": "running",
    },
    "registering_artifacts": {
        "label": "Register Artifacts",
        "group": "mask_finalize",
        "group_label": "Finalize",
        "order": 82,
        "badge": "MASK",
        "kind": "running",
    },
    "masked_zarr_written": {
        "label": "Pipeline Ready",
        "group": "ready",
        "group_label": "Ready",
        "order": 90,
        "badge": "RDY",
        "kind": "done",
    },
    "zarr_failed": {
        "label": "Zarr Failed",
        "group": "convert",
        "group_label": "Convert",
        "order": 95,
        "badge": "FAIL",
        "kind": "failed",
    },
    "failed": {
        "label": "Failed",
        "group": "terminal",
        "group_label": "Failed",
        "order": 100,
        "badge": "FAIL",
        "kind": "failed",
    },
    "cancelled": {
        "label": "Cancelled",
        "group": "terminal",
        "group_label": "Cancelled",
        "order": 101,
        "badge": "STOP",
        "kind": "cancelled",
    },
}

_RUNNING_STATUSES = {"running", "queued"}
_TERMINAL_STEP_STATUSES = {"done", "failed", "cancelled"}
_STATE_STEP_RULES: dict[str, dict[str, Any]] = {
    "queued": {"allowed": {"queued", "resume_after_restart"}, "default": "queued"},
    "searching": {"allowed": {"searching"}, "default": "searching"},
    "downloading": {"allowed": {"downloading"}, "default": "downloading"},
    "downloaded": {"allowed": {"downloaded"}, "default": "downloaded"},
    "zarr_queued": {"allowed": {"zarr_queued"}, "default": "zarr_queued"},
    "zarr_converting": {
        "allowed": {"writing_chunks", "registering_artifact"},
        "default": "writing_chunks",
    },
    "zarr_written": {"allowed": {"zarr_written"}, "default": "zarr_written"},
    "resolving_source_zarr": {
        "allowed": {"resolving_source_zarr", "copying_source_zarr"},
        "default": "resolving_source_zarr",
    },
    "copying_source_zarr": {
        "allowed": {"copying_source_zarr"},
        "default": "copying_source_zarr",
    },
    "running_cloud_inference": {
        "allowed": {"running_cloud_inference"},
        "default": "running_cloud_inference",
    },
    "running_water_inference": {
        "allowed": {"running_water_inference"},
        "default": "running_water_inference",
    },
    "writing_mask_artifacts": {
        "allowed": {"writing_mask_artifacts"},
        "default": "writing_mask_artifacts",
    },
    "writing_masked_zarr": {
        "allowed": {"writing_masked_zarr"},
        "default": "writing_masked_zarr",
    },
    "registering_artifacts": {
        "allowed": {"registering_artifacts"},
        "default": "registering_artifacts",
    },
    "masked_zarr_written": {
        "allowed": {"masked_zarr_written"},
        "default": "masked_zarr_written",
    },
    "zarr_failed": {"allowed": {"zarr_failed"}, "default": "zarr_failed"},
    "failed": {
        "allowed": {"failed", "cloud_failed", "water_failed"},
        "default": "failed",
    },
    "cancelled": {"allowed": {"cancelled"}, "default": "cancelled"},
}


def _normalize_mask_types(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    for value in list(values or []):
        candidate = str(value or "").strip().lower()
        if candidate not in {"water", "cloud"}:
            continue
        if candidate not in normalized:
            normalized.append(candidate)
    return normalized


def _parse_iso(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_pipeline_step_key(
    pipeline_step: str | None,
    pipeline_state: str | None,
) -> str:
    normalized_step = str(pipeline_step or "").strip().lower()
    normalized_state = str(pipeline_state or "").strip().lower()

    if normalized_state:
        rule = dict(_STATE_STEP_RULES.get(normalized_state) or {})
        allowed = {
            str(item).strip().lower()
            for item in list(rule.get("allowed") or [])
            if str(item).strip()
        }
        default_step = str(rule.get("default") or normalized_state).strip().lower() or normalized_state
        if not normalized_step:
            return default_step
        if allowed and normalized_step not in allowed:
            return default_step
        return normalized_step

    return normalized_step or "queued"


def _duration_seconds(
    started_at: str | datetime | None,
    finished_at: str | datetime | None,
) -> float | None:
    start_dt = _parse_iso(started_at)
    end_dt = _parse_iso(finished_at)
    if start_dt is None or end_dt is None:
        return None
    return max(0.0, (end_dt - start_dt).total_seconds())


def _step_definition(step_key: str, pipeline_state: str | None = None) -> dict[str, Any]:
    normalized_key = str(step_key or "").strip().lower()
    if normalized_key in _STEP_DEFINITIONS:
        return dict(_STEP_DEFINITIONS[normalized_key])

    fallback_key = str(pipeline_state or "").strip().lower()
    if fallback_key in _STEP_DEFINITIONS:
        fallback = dict(_STEP_DEFINITIONS[fallback_key])
        fallback["label"] = normalized_key.replace("_", " ").title() or fallback["label"]
        return fallback

    return {
        "label": normalized_key.replace("_", " ").title() or "Step",
        "group": "other",
        "group_label": "Other",
        "order": 999,
        "badge": "STEP",
        "kind": "running",
    }


def _terminal_status_for_step(
    *,
    job_state: str | None,
    step_key: str,
) -> str:
    normalized_step = str(step_key or "").strip().lower()

    step_kind = str(_step_definition(normalized_step).get("kind") or "running").strip().lower()
    if step_kind == "cancelled":
        return "cancelled"
    if step_kind == "failed":
        return "failed"
    if step_kind == "done":
        return "done"
    if step_kind == "queued":
        return "queued"
    return "running"


def _find_active_step(steps: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in reversed(steps):
        if str(entry.get("status") or "").strip().lower() in _RUNNING_STATUSES:
            return entry
    return None


def _find_latest_step(steps: list[dict[str, Any]], *, step_key: str) -> dict[str, Any] | None:
    normalized_key = str(step_key or "").strip().lower()
    latest_terminal: dict[str, Any] | None = None
    for entry in reversed(steps):
        if str(entry.get("key") or "").strip().lower() != normalized_key:
            continue
        status = str(entry.get("status") or "").strip().lower()
        if status not in _TERMINAL_STEP_STATUSES:
            return entry
        if latest_terminal is None:
            latest_terminal = entry
    return latest_terminal


def _sorted_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _sort_key(entry: dict[str, Any]) -> tuple[int, float, int]:
        order = int(entry.get("order", 999) or 999)
        started_at = _parse_iso(entry.get("started_at"))
        started_ts = started_at.timestamp() if started_at is not None else 0.0
        step_index = int(entry.get("index", 0) or 0)
        return (order, started_ts, step_index)

    return sorted(steps, key=_sort_key)


def _stage_specs(*, job_kind: str | None, mask_types: list[str]) -> list[dict[str, str]]:
    normalized_kind = str(job_kind or "").strip().lower()
    specs: list[dict[str, str]] = []
    if normalized_kind == "mask":
        specs.append({"key": "resolve", "label": "Resolve", "badge": "INIT"})
    else:
        specs.extend(
            [
                {"key": "search", "label": "Search", "badge": "SRCH"},
                {"key": "download", "label": "Download", "badge": "DL"},
                {"key": "convert", "label": "Convert", "badge": "CNV"},
            ]
        )
    if "cloud" in mask_types:
        specs.append({"key": "cloud", "label": "Cloud", "badge": "CLD"})
    if "water" in mask_types:
        specs.append({"key": "water", "label": "Water", "badge": "WTR"})
    specs.append({"key": "ready", "label": "Ready", "badge": "RDY"})
    return specs


def _stage_key_for_step(
    step_key: str,
    *,
    job_kind: str | None,
    mask_types: list[str],
) -> str | None:
    normalized_step = str(step_key or "").strip().lower()
    if normalized_step == "zarr_written":
        # When masking continues after conversion, the Zarr-ready marker is the
        # completion of the convert stage, not the final pipeline-ready stage.
        return "convert" if mask_types else "ready"

    definition = _step_definition(step_key)
    group = str(definition.get("group") or "").strip().lower()

    if group == "mask_finalize":
        if "water" in mask_types:
            return "water"
        if "cloud" in mask_types:
            return "cloud"
        return "ready"
    if group == "terminal":
        return None
    if str(job_kind or "").strip().lower() == "mask" and group == "queue":
        return "resolve"
    return group or None


def _build_stage_rows(
    *,
    steps: list[dict[str, Any]],
    job_kind: str | None,
    mask_types: list[str],
) -> list[dict[str, Any]]:
    specs = _stage_specs(job_kind=job_kind, mask_types=mask_types)
    stage_rows: list[dict[str, Any]] = []
    steps_by_stage: dict[str, list[dict[str, Any]]] = {spec["key"]: [] for spec in specs}
    for step in steps:
        stage_key = _stage_key_for_step(
            str(step.get("key") or ""),
            job_kind=job_kind,
            mask_types=mask_types,
        )
        if stage_key and stage_key in steps_by_stage:
            steps_by_stage[stage_key].append(step)

    for stage_index, spec in enumerate(specs):
        members = steps_by_stage.get(spec["key"], [])
        started_at = members[0].get("started_at") if members else None
        finished_at = members[-1].get("finished_at") if members else None
        status = "pending"
        if members:
            statuses = [str(item.get("status") or "").strip().lower() for item in members]
            if any(item == "failed" for item in statuses):
                status = "failed"
            elif any(item == "cancelled" for item in statuses):
                status = "cancelled"
            elif any(item == "running" for item in statuses):
                status = "running"
            elif any(item == "queued" for item in statuses):
                status = "queued"
            elif any(item == "done" for item in statuses):
                status = "done"
        duration = sum(
            float(item.get("duration_seconds") or 0.0)
            for item in members
            if item.get("duration_seconds") is not None
        )
        detail_label = str(members[-1].get("label") or spec["label"]) if members else spec["label"]
        stage_rows.append(
            {
                "key": spec["key"],
                "label": spec["label"],
                "badge": spec["badge"],
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": duration if members else None,
                "detail_label": detail_label,
                "step_count": len(members),
                "index": stage_index,
            }
        )

    return stage_rows


def _current_stage_from_rows(stages: list[dict[str, Any]]) -> dict[str, Any] | None:
    current_stage = next(
        (stage for stage in stages if str(stage.get("status") or "").strip().lower() in _RUNNING_STATUSES),
        None,
    )
    if current_stage is not None:
        return current_stage
    return next(
        (stage for stage in reversed(stages) if str(stage.get("status") or "").strip().lower() in _TERMINAL_STEP_STATUSES),
        None,
    )


def _materialize_steps(steps: list[dict[str, Any]], *, now_iso: str) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    for entry in _sorted_steps(steps):
        snapshot = dict(entry)
        status = str(snapshot.get("status") or "").strip().lower()
        started_at = snapshot.get("started_at")
        finished_at = snapshot.get("finished_at")
        if finished_at is None and status in _RUNNING_STATUSES:
            finished_at = now_iso
        snapshot["duration_seconds"] = _duration_seconds(started_at, finished_at)
        materialized.append(snapshot)
    return materialized


def refresh_pipeline_timeline(
    existing_timeline: dict[str, Any] | None,
    *,
    timestamp: str | datetime | None = None,
    job_state: str | None = None,
    pipeline_state: str | None = None,
    pipeline_step: str | None = None,
    job_kind: str | None = None,
    mask_types: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    timeline = dict(existing_timeline or {})
    raw_steps = [dict(item) for item in list(timeline.get("steps") or []) if isinstance(item, dict)]
    timestamp_iso = (_parse_iso(timestamp) or datetime.now(timezone.utc)).isoformat()
    materialized_steps = _materialize_steps(raw_steps, now_iso=timestamp_iso)
    active_materialized = _find_active_step(materialized_steps)

    normalized_mask_types = _normalize_mask_types(
        mask_types
        if mask_types is not None
        else list(timeline.get("mask_types") or [])
    )
    if not normalized_mask_types:
        discovered: list[str] = []
        discovered_step_keys = {
            str(item.get("key") or "").strip().lower()
            for item in materialized_steps
        }
        if {"running_cloud_inference", "cloud_failed"} & discovered_step_keys:
            discovered.append("cloud")
        if {"running_water_inference", "water_failed"} & discovered_step_keys:
            discovered.append("water")
        normalized_mask_types = discovered

    effective_job_kind = str(job_kind or timeline.get("job_kind") or "").strip().lower() or None
    effective_job_state = str(job_state or timeline.get("job_state") or "").strip().lower() or None
    effective_pipeline_state = str(pipeline_state or timeline.get("pipeline_state") or "").strip().lower() or None
    effective_pipeline_step = resolve_pipeline_step_key(
        pipeline_step or timeline.get("pipeline_step"),
        effective_pipeline_state,
    )
    stages = _build_stage_rows(
        steps=materialized_steps,
        job_kind=effective_job_kind,
        mask_types=normalized_mask_types,
    )
    current_stage = _current_stage_from_rows(stages)

    return {
        "version": PIPELINE_TIMELINE_VERSION,
        "job_kind": effective_job_kind,
        "mask_types": normalized_mask_types,
        "updated_at": timestamp_iso,
        "terminal": effective_job_state in {"succeeded", "failed", "cancelled"},
        "job_state": effective_job_state,
        "pipeline_state": effective_pipeline_state,
        "pipeline_step": effective_pipeline_step,
        "current_step": str(active_materialized.get("key") or effective_pipeline_step) if active_materialized is not None else effective_pipeline_step,
        "current_step_label": str(active_materialized.get("label") or _step_definition(effective_pipeline_step, effective_pipeline_state).get("label") or effective_pipeline_step) if active_materialized is not None else str(_step_definition(effective_pipeline_step, effective_pipeline_state).get("label") or effective_pipeline_step),
        "current_stage": str(current_stage.get("key") or "") if current_stage is not None else None,
        "current_stage_label": str(current_stage.get("label") or "") if current_stage is not None else None,
        "steps": materialized_steps,
        "stages": stages,
    }


def advance_pipeline_timeline(
    existing_timeline: dict[str, Any] | None,
    *,
    job_state: str | None,
    pipeline_state: str | None,
    pipeline_step: str | None,
    pipeline_progress: float | None,
    timestamp: str | datetime | None = None,
    job_kind: str | None = None,
    mask_types: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    normalized_mask_types = _normalize_mask_types(mask_types)
    timeline = dict(existing_timeline or {})
    raw_steps = [dict(item) for item in list(timeline.get("steps") or []) if isinstance(item, dict)]
    timestamp_iso = (_parse_iso(timestamp) or datetime.now(timezone.utc)).isoformat()
    step_key = resolve_pipeline_step_key(pipeline_step, pipeline_state)
    step_definition = _step_definition(step_key, pipeline_state)
    incoming_status = _terminal_status_for_step(job_state=job_state, step_key=step_key)

    active_step = _find_active_step(raw_steps)
    if active_step is not None:
        active_key = str(active_step.get("key") or "").strip().lower()
        if active_key != step_key:
            active_status = str(active_step.get("status") or "").strip().lower()
            incoming_kind = str(step_definition.get("kind") or "running").strip().lower()
            if active_status in _RUNNING_STATUSES:
                active_step["status"] = (
                    incoming_status
                    if incoming_status in {"failed", "cancelled"} and incoming_kind in {"failed", "cancelled"}
                    else "done"
                )
                active_step["finished_at"] = timestamp_iso
                active_step["duration_seconds"] = _duration_seconds(
                    active_step.get("started_at"),
                    active_step.get("finished_at"),
                )

    current_step = _find_latest_step(raw_steps, step_key=step_key)

    if current_step is None or (
        current_step is not None
        and str(current_step.get("status") or "").strip().lower() in _TERMINAL_STEP_STATUSES
        and incoming_status not in _TERMINAL_STEP_STATUSES
    ):
        current_step = {
            "key": step_key,
            "label": step_definition["label"],
            "group": step_definition["group"],
            "group_label": step_definition["group_label"],
            "order": int(step_definition.get("order", 999) or 999),
            "badge": step_definition["badge"],
            "index": len(raw_steps),
            "attempts": 1,
            "started_at": timestamp_iso,
            "finished_at": None,
            "status": incoming_status,
            "pipeline_state": str(pipeline_state or "").strip().lower() or None,
            "progress_end": pipeline_progress,
        }
        raw_steps.append(current_step)
    else:
        current_step["label"] = step_definition["label"]
        current_step["group"] = step_definition["group"]
        current_step["group_label"] = step_definition["group_label"]
        current_step["order"] = int(step_definition.get("order", 999) or 999)
        current_step["badge"] = step_definition["badge"]
        current_step["pipeline_state"] = str(pipeline_state or "").strip().lower() or None
        if current_step.get("started_at") is None:
            current_step["started_at"] = timestamp_iso
        if pipeline_progress is not None:
            current_step["progress_end"] = pipeline_progress
        current_step["status"] = incoming_status

    if incoming_status in _TERMINAL_STEP_STATUSES:
        current_step["finished_at"] = timestamp_iso
    elif incoming_status in _RUNNING_STATUSES:
        current_step["finished_at"] = None
    current_step["duration_seconds"] = _duration_seconds(
        current_step.get("started_at"),
        current_step.get("finished_at"),
    )

    materialized_steps = _materialize_steps(raw_steps, now_iso=timestamp_iso)
    active_materialized = _find_active_step(materialized_steps)
    if not normalized_mask_types:
        discovered: list[str] = []
        discovered_step_keys = {str(item.get("key") or "").strip().lower() for item in materialized_steps}
        if {"running_cloud_inference", "cloud_failed"} & discovered_step_keys:
            discovered.append("cloud")
        if {"running_water_inference", "water_failed"} & discovered_step_keys:
            discovered.append("water")
        normalized_mask_types = discovered

    stages = _build_stage_rows(
        steps=materialized_steps,
        job_kind=job_kind,
        mask_types=normalized_mask_types,
    )
    current_stage = _current_stage_from_rows(stages)

    return {
        "version": PIPELINE_TIMELINE_VERSION,
        "job_kind": str(job_kind or "").strip().lower() or None,
        "mask_types": normalized_mask_types,
        "updated_at": timestamp_iso,
        "terminal": str(job_state or "").strip().lower() in {"succeeded", "failed", "cancelled"},
        "job_state": str(job_state or "").strip().lower() or None,
        "pipeline_state": str(pipeline_state or "").strip().lower() or None,
        "pipeline_step": step_key,
        "current_step": str(active_materialized.get("key") or step_key) if active_materialized is not None else step_key,
        "current_step_label": str(active_materialized.get("label") or step_definition["label"]) if active_materialized is not None else step_definition["label"],
        "current_stage": str(current_stage.get("key") or "") if current_stage is not None else None,
        "current_stage_label": str(current_stage.get("label") or "") if current_stage is not None else None,
        "steps": materialized_steps,
        "stages": stages,
    }


__all__ = [
    "PIPELINE_TIMELINE_VERSION",
    "advance_pipeline_timeline",
    "refresh_pipeline_timeline",
    "resolve_pipeline_step_key",
]
