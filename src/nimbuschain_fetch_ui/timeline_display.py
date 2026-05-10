from __future__ import annotations

from typing import Any


DISPLAY_STAGE_KEY_ALIASES = {
    "search": "fetch",
    "download": "fetch",
    "convert": "zarr",
}

STAGE_DISPLAY_DEFAULTS = {
    "fetch": ("Fetch", "FETCH", "Provider fetch"),
    "sen2like": ("Sen2Like", "S2L", "Landsat normalization"),
    "zarr": ("Zarr", "ZARR", "Zarr conversion"),
    "mask": ("Mask", "MASK", "Integrated masking"),
    "cube": ("Cube", "CUBE", "Cube builder"),
}

STAGE_RESULT_STATUS_ALIASES = {
    "succeeded": "done",
    "skipped": "skipped",
    "failed": "failed",
    "running": "running",
    "pending": "pending",
}


def display_stage_key(stage_key: str | None) -> str:
    normalized = str(stage_key or "").strip().lower()
    return DISPLAY_STAGE_KEY_ALIASES.get(normalized, normalized)


def display_pipeline_stages(
    item: dict[str, Any],
    timeline: dict[str, Any],
    stages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    del timeline
    orchestrated_stages = _display_stages_from_stage_results(item)
    if orchestrated_stages:
        return orchestrated_stages

    display_stages: list[dict[str, Any]] = []
    fetch_buffer: list[dict[str, Any]] = []

    def _flush_fetch() -> None:
        if fetch_buffer:
            display_stages.append(_combine_fetch_stage(fetch_buffer))
            fetch_buffer.clear()

    for stage in stages:
        stage_key = str(stage.get("key") or "").strip().lower()
        if stage_key in {"search", "download"}:
            fetch_buffer.append(dict(stage))
            continue
        _flush_fetch()
        display_stages.append(_retitle_stage(dict(stage)))
    _flush_fetch()

    if _requires_sen2like(item) and not any(
        str(stage.get("key") or "").strip().lower() == "sen2like"
        for stage in display_stages
    ):
        insert_at = next(
            (
                index + 1
                for index, stage in enumerate(display_stages)
                if str(stage.get("key") or "").strip().lower() == "fetch"
            ),
            0,
        )
        display_stages.insert(insert_at, _synthetic_sen2like_stage(display_stages))

    for index, stage in enumerate(display_stages):
        stage["index"] = index
    return display_stages


def _display_stages_from_stage_results(item: dict[str, Any]) -> list[dict[str, Any]]:
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    stage_results = [
        dict(stage)
        for stage in list(pipeline_metadata.get("stage_results") or [])
        if isinstance(stage, dict) and str(stage.get("name") or "").strip()
    ]
    if not stage_results:
        return []

    results_by_name = {
        str(stage.get("name") or "").strip().lower(): stage
        for stage in stage_results
    }
    ordered_names: list[str] = []
    for plan_stage in list(pipeline_metadata.get("stage_plan") or []):
        if not isinstance(plan_stage, dict):
            continue
        name = str(plan_stage.get("name") or "").strip().lower()
        if name and name not in ordered_names:
            ordered_names.append(name)
    for result_stage in stage_results:
        name = str(result_stage.get("name") or "").strip().lower()
        if name and name not in ordered_names:
            ordered_names.append(name)

    display_stages = [
        _stage_result_to_display_stage(name, results_by_name.get(name) or {}, index)
        for index, name in enumerate(ordered_names)
    ]
    return [stage for stage in display_stages if stage.get("key")]


def _stage_result_to_display_stage(
    name: str,
    result: dict[str, Any],
    index: int,
) -> dict[str, Any]:
    label, badge, default_detail = STAGE_DISPLAY_DEFAULTS.get(
        name,
        (name.replace("_", " ").title(), name[:4].upper() or "STEP", name.replace("_", " ").title()),
    )
    status = STAGE_RESULT_STATUS_ALIASES.get(
        str(result.get("status") or "pending").strip().lower(),
        str(result.get("status") or "pending").strip().lower() or "pending",
    )
    outputs = [
        str(output)
        for output in list(result.get("outputs") or [])
        if str(output).strip()
    ]
    metadata = dict(result.get("metadata") or {})
    reason = str(metadata.get("reason") or "").strip()
    error = str(result.get("error") or "").strip()
    if error:
        detail_label = error
    elif status == "skipped" and reason:
        detail_label = f"Skipped: {reason.replace('_', ' ')}"
    elif outputs:
        detail_label = f"{default_detail} · {len(outputs)} output{'s' if len(outputs) != 1 else ''}"
    else:
        detail_label = default_detail

    return {
        "key": name,
        "label": label,
        "badge": badge,
        "status": status,
        "started_at": result.get("started_at"),
        "finished_at": result.get("finished_at"),
        "duration_seconds": result.get("duration_seconds"),
        "detail_label": detail_label,
        "outputs": outputs,
        "metadata": metadata,
        "index": index,
    }


def _requires_sen2like(item: dict[str, Any]) -> bool:
    request_payload = dict(item.get("request") or {})
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    provider = str(
        item.get("provider")
        or request_payload.get("provider")
        or pipeline_metadata.get("provider")
        or ""
    ).strip().lower()
    collection = str(
        item.get("collection")
        or request_payload.get("collection")
        or pipeline_metadata.get("collection")
        or ""
    ).strip().lower()
    product_type = str(
        item.get("product_type")
        or request_payload.get("product_type")
        or pipeline_metadata.get("product_type")
        or ""
    ).strip().lower()
    return provider == "usgs" and ("landsat" in collection or product_type.startswith("l"))


def _combined_status(stages: list[dict[str, Any]]) -> str:
    statuses = [str(stage.get("status") or "pending").strip().lower() for stage in stages]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "cancelled" for status in statuses):
        return "cancelled"
    if any(status == "running" for status in statuses):
        return "running"
    if any(status == "queued" for status in statuses):
        return "queued"
    if stages and all(status == "done" for status in statuses):
        return "done"
    if any(status == "done" for status in statuses):
        return "running"
    return "pending"


def _combine_fetch_stage(fetch_stages: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(fetch_stages, key=lambda stage: int(stage.get("index", 0) or 0))
    status = _combined_status(ordered)
    started_at = next((stage.get("started_at") for stage in ordered if stage.get("started_at")), None)
    finished_at = None
    if status in {"done", "failed", "cancelled"}:
        finished_at = next(
            (stage.get("finished_at") for stage in reversed(ordered) if stage.get("finished_at")),
            None,
        )
    duration_values = [
        float(stage.get("duration_seconds") or 0.0)
        for stage in ordered
        if stage.get("duration_seconds") is not None
    ]
    active_member = next(
        (
            stage
            for stage in ordered
            if str(stage.get("status") or "").strip().lower() in {"running", "queued"}
        ),
        None,
    )
    last_member = next((stage for stage in reversed(ordered) if stage), {})
    if status == "done":
        detail_label = "Search catalog + download files"
    elif active_member is not None:
        detail_label = str(active_member.get("detail_label") or active_member.get("label") or "Fetch")
    elif status == "pending":
        detail_label = "Waiting for provider fetch"
    else:
        detail_label = str(last_member.get("detail_label") or last_member.get("label") or "Fetch")
    return {
        "key": "fetch",
        "label": "Fetch",
        "badge": "FETCH",
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": sum(duration_values) if duration_values else None,
        "detail_label": detail_label,
        "step_count": sum(int(stage.get("step_count", 1) or 1) for stage in ordered),
        "index": int(ordered[0].get("index", 0) or 0) if ordered else 0,
    }


def _retitle_stage(stage: dict[str, Any]) -> dict[str, Any]:
    display = dict(stage)
    key = str(display.get("key") or "").strip().lower()
    if key == "convert":
        display["key"] = "zarr"
        display["label"] = "Zarr"
        display["badge"] = "ZARR"
        if str(display.get("detail_label") or "").strip().lower() in {"convert", "conversion queued"}:
            display["detail_label"] = "Zarr conversion"
    elif key == "sen2like":
        display["label"] = "Sen2Like"
        display["badge"] = "S2L"
        if not display.get("detail_label"):
            display["detail_label"] = "Landsat normalization"
    elif key == "cloud":
        display["label"] = "Cloud Mask"
        display["badge"] = "CLD"
        if str(display.get("detail_label") or "").strip().lower() == "cloud":
            display["detail_label"] = "Cloud mask"
    elif key == "water":
        display["label"] = "Water Mask"
        display["badge"] = "WTR"
        if str(display.get("detail_label") or "").strip().lower() == "water":
            display["detail_label"] = "Water mask"
    return display


def _synthetic_sen2like_stage(display_stages: list[dict[str, Any]]) -> dict[str, Any]:
    stages_by_key = {
        str(stage.get("key") or "").strip().lower(): stage
        for stage in display_stages
        if isinstance(stage, dict)
    }
    fetch_status = str((stages_by_key.get("fetch") or {}).get("status") or "pending").strip().lower()
    downstream_statuses = [
        str((stages_by_key.get(key) or {}).get("status") or "pending").strip().lower()
        for key in ("zarr", "cube", "cloud", "water", "ready")
    ]
    if any(status in {"done", "running", "queued", "failed", "cancelled"} for status in downstream_statuses):
        status = "done"
        detail_label = "Landsat normalized before Zarr"
        duration_seconds: float | None = 0.0
    elif fetch_status == "done":
        status = "queued"
        detail_label = "Waiting to normalize Landsat"
        duration_seconds = None
    else:
        status = "pending"
        detail_label = "Landsat-only normalization"
        duration_seconds = None
    fetch_stage = stages_by_key.get("fetch") or {}
    reference_time = fetch_stage.get("finished_at") if status in {"done", "queued"} else None
    return {
        "key": "sen2like",
        "label": "Sen2Like",
        "badge": "S2L",
        "status": status,
        "started_at": reference_time if status == "done" else None,
        "finished_at": reference_time if status == "done" else None,
        "duration_seconds": duration_seconds,
        "detail_label": detail_label,
        "step_count": 0,
        "index": int(fetch_stage.get("index", 0) or 0) + 1,
        "synthetic": True,
    }
