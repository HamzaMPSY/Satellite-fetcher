from __future__ import annotations

import ast
import html
from typing import Any

from nimbuschain_shared import error_codes


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
        display_stages.append(_retitle_stage(dict(stage), item))
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
        display_stages.insert(insert_at, _synthetic_sen2like_stage(display_stages, item))

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
        _stage_result_to_display_stage(item, name, results_by_name.get(name) or {}, index)
        for index, name in enumerate(ordered_names)
    ]
    return [stage for stage in display_stages if stage.get("key")]


def _stage_result_to_display_stage(
    item: dict[str, Any],
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
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    pipeline_state = str(item.get("pipeline_state") or "").strip().lower()
    job_state = str(item.get("state") or "").strip().lower()
    cube_status = str(pipeline_metadata.get("cube_status") or "").strip().lower()
    mask_status = str(pipeline_metadata.get("mask_status") or "").strip().lower()
    outputs = [
        str(output)
        for output in list(result.get("outputs") or [])
        if str(output).strip()
    ]
    metadata = dict(result.get("metadata") or {})
    if name == "sen2like":
        metadata.setdefault("sen2like_status", pipeline_metadata.get("sen2like_status"))
        metadata.setdefault("fallback_reason", pipeline_metadata.get("sen2like_fallback_reason"))
        metadata.setdefault("fallback_message", pipeline_metadata.get("sen2like_fallback_message"))
        metadata.setdefault("zarr_input_source", pipeline_metadata.get("zarr_input_source"))
    elif name == "zarr":
        metadata.setdefault("zarr_input_source", pipeline_metadata.get("zarr_input_source"))
        metadata.setdefault("zarr_input_outputs", pipeline_metadata.get("zarr_input_outputs"))
        metadata.setdefault("sen2like_status", pipeline_metadata.get("sen2like_status"))
        metadata.setdefault("fallback_reason", pipeline_metadata.get("sen2like_fallback_reason"))
        if _zarr_uses_raw_fallback(metadata):
            metadata["raw_fallback"] = True
            metadata.setdefault("status_label", "degraded")
    elif name == "cube":
        metadata.setdefault("cube_status", pipeline_metadata.get("cube_status"))
        metadata.setdefault("cube_reason", pipeline_metadata.get("cube_reason"))
        metadata.setdefault("cube_error_code", pipeline_metadata.get("cube_error_code"))
        metadata.setdefault("cube_tiles_skipped", pipeline_metadata.get("cube_tiles_skipped"))
        metadata.setdefault("cube_diagnostics", pipeline_metadata.get("cube_diagnostics"))
    elif name == "mask":
        metadata.setdefault("mask_status", pipeline_metadata.get("mask_status"))
        metadata.setdefault("mask_types", pipeline_metadata.get("mask_types"))
        metadata.setdefault("mask_total_scenes", pipeline_metadata.get("mask_total_scenes"))
        metadata.setdefault("mask_completed_scenes", pipeline_metadata.get("mask_completed_scenes"))
        metadata.setdefault("mask_mode", pipeline_metadata.get("mask_mode"))
    reason = str(metadata.get("reason") or "").strip()
    error = str(result.get("error") or "").strip()
    if name == "sen2like" and _sen2like_used_raw_fallback(metadata):
        status = "done"
        error = ""
        reason = ""
        metadata["status_label"] = "degraded"
        metadata["raw_fallback"] = True
        if not outputs:
            outputs = [
                str(output)
                for output in list(pipeline_metadata.get("zarr_input_outputs") or item.get("raw_outputs") or [])
                if str(output).strip()
            ]
    elif name == "cube" and cube_status == "skipped":
        status = "skipped"
        error = ""
        metadata["status_label"] = "not built"
        if not reason:
            reason = str(metadata.get("cube_reason") or "").strip() or _cube_skip_reason_label(
                result,
                pipeline_metadata,
            )
    elif name == "mask" and (
        mask_status == "written"
        or (job_state == "succeeded" and pipeline_state == "masked_zarr_written")
    ):
        status = "done"
        error = ""
        reason = ""
        if not outputs:
            outputs = [
                str(output)
                for output in list(item.get("zarr_outputs") or pipeline_metadata.get("zarr_outputs") or [])
                if str(output).strip()
            ]
    if error:
        detail_label = _stage_error_summary(error)
    elif name == "sen2like" and status == "done" and _sen2like_used_raw_fallback(metadata):
        detail_label = _sen2like_fallback_label(metadata)
    elif name == "cube" and status == "skipped":
        detail_label = _cube_skip_reason_label(result, pipeline_metadata)
    elif name == "mask" and status == "done" and (
        str(metadata.get("mask_status") or "").strip().lower() == "written"
        or mask_status == "written"
    ):
        detail_label = _mask_written_label(metadata, outputs)
    elif name == "zarr" and status == "done" and _zarr_uses_raw_fallback(metadata):
        detail_label = _zarr_raw_fallback_label(metadata, outputs, default_detail)
    elif status == "skipped" and reason:
        detail_label = f"Skipped: {_generic_skip_reason_label(reason)}"
    elif outputs:
        detail_label = f"{default_detail} · {len(outputs)} output{'s' if len(outputs) != 1 else ''}"
    else:
        detail_label = default_detail

    duration_seconds = _stage_result_duration_seconds(
        name=name,
        result=result,
        pipeline_metadata=pipeline_metadata,
    )

    return {
        "key": name,
        "label": label,
        "badge": badge,
        "status": status,
        "started_at": result.get("started_at"),
        "finished_at": result.get("finished_at"),
        "duration_seconds": duration_seconds,
        "detail_label": detail_label,
        "outputs": outputs,
        "metadata": metadata,
        "index": index,
    }


def _stage_result_duration_seconds(
    *,
    name: str,
    result: dict[str, Any],
    pipeline_metadata: dict[str, Any],
) -> Any:
    explicit = _positive_float(result.get("duration_seconds"))
    candidates: list[float] = []
    if explicit is not None:
        candidates.append(explicit)
    if name == "fetch":
        download_telemetry = dict(pipeline_metadata.get("download_telemetry") or {})
        for value in (
            pipeline_metadata.get("download_window_seconds"),
            download_telemetry.get("duration_seconds"),
        ):
            duration = _positive_float(value)
            if duration is not None:
                candidates.append(duration)
    elif name == "sen2like":
        sen2like_response = dict(pipeline_metadata.get("sen2like_response") or {})
        duration = _positive_float(sen2like_response.get("duration_seconds"))
        if duration is not None:
            candidates.append(duration)
    if candidates:
        return max(candidates)
    return result.get("duration_seconds")


def _sen2like_used_raw_fallback(metadata: dict[str, Any]) -> bool:
    return bool(metadata.get("fallback_to_raw")) or (
        str(metadata.get("sen2like_status") or "").strip().lower() == "raw_fallback"
    )


def _sen2like_fallback_label(metadata: dict[str, Any]) -> str:
    reason = str(metadata.get("fallback_reason") or "").strip().lower()
    if reason == "sen2like_resource_exhausted":
        return "Degraded: raw Landsat inputs used because Sen2Like did not write outputs"
    if reason:
        return f"Degraded: raw Landsat inputs used after Sen2Like failure ({reason.replace('_', ' ')})"
    return "Degraded: raw Landsat inputs used after Sen2Like failure"


def _zarr_uses_raw_fallback(metadata: dict[str, Any]) -> bool:
    source = str(metadata.get("zarr_input_source") or "").strip().lower()
    return source == "raw" and _sen2like_used_raw_fallback(metadata)


def _zarr_raw_fallback_label(
    metadata: dict[str, Any],
    outputs: list[str],
    default_detail: str,
) -> str:
    output_count = len(outputs)
    suffix = f" · {output_count} output{'s' if output_count != 1 else ''}" if output_count else ""
    reason = str(metadata.get("fallback_reason") or "").strip().lower()
    if reason == "sen2like_resource_exhausted":
        return f"{default_detail} used raw Landsat fallback{suffix}"
    return f"{default_detail} used raw Landsat fallback{suffix}"


def _mask_written_label(metadata: dict[str, Any], outputs: list[str]) -> str:
    mask_types = [
        str(mask).strip().title()
        for mask in list(metadata.get("mask_types") or [])
        if str(mask).strip()
    ]
    prefix = f"{' + '.join(mask_types)} masks" if mask_types else "Masks"
    total = _positive_int(metadata.get("mask_total_scenes"))
    completed = _positive_int(metadata.get("mask_completed_scenes"))
    scene_count = completed or total or len(outputs)
    if total > 0 and completed > 0:
        scene_word = "scene" if total == 1 else "scenes"
        prefix_text = "OK: " if completed >= total else ""
        return f"{prefix_text}{prefix} written in-place on {completed}/{total} {scene_word}"
    if scene_count > 0:
        scene_word = "scene" if scene_count == 1 else "scenes"
        return f"OK: {prefix} written in-place on {scene_count} {scene_word}"
    return f"{prefix} written in-place"


def _cube_skip_reason_label(
    result: dict[str, Any],
    pipeline_metadata: dict[str, Any] | None = None,
) -> str:
    metadata = dict(result.get("metadata") or {})
    pipeline_metadata = dict(pipeline_metadata or {})
    reason = str(
        metadata.get("reason")
        or metadata.get("cube_reason")
        or pipeline_metadata.get("cube_reason")
        or result.get("error")
        or ""
    ).strip()
    lowered = reason.lower()
    skipped_groups = [
        dict(item)
        for item in list(
            metadata.get("cube_tiles_skipped")
            or pipeline_metadata.get("cube_tiles_skipped")
            or []
        )
        if isinstance(item, dict)
    ]
    skipped_group_keys = [
        _format_cube_group_key(str(item.get("group_key") or "").strip())
        for item in skipped_groups
        if str(item.get("group_key") or "").strip()
    ]
    error_code = _cube_error_code(result, pipeline_metadata)
    if len(set(skipped_group_keys)) > 1 and all(
        len(list(item.get("candidate_scene_ids") or [])) < 2 for item in skipped_groups
    ):
        return _with_error_code(
            error_code,
            "Cube not built because scenes are split across tile groups "
            f"({_compact_group_keys(skipped_group_keys)}); select at least two dates for the same tile/path-row",
        )
    if lowered in {"no_groups_with_multiple_times", "fewer_than_two_unique_times"}:
        return _with_error_code(
            error_code,
            "Cube not built because each group has fewer than two acquisition times; select at least two dates for the same tile/path-row",
        )
    if "fewer_than_two_unique_times" in lowered:
        return _with_error_code(
            error_code,
            "Cube not built because a group has fewer than two acquisition times; select at least two dates for the same tile/path-row",
        )
    if "no_groups_with_multiple_times" in lowered:
        return _with_error_code(
            error_code,
            "Cube not built because each group has fewer than two acquisition times; select at least two dates for the same tile/path-row",
        )
    if "daily mosaic cube" in lowered and "sentinel-2" in lowered:
        return _with_error_code(
            error_code,
            "Cube not built because daily mosaic cubes currently require Sentinel-2 scene inputs",
        )
    if lowered == "cube_input_missing":
        return _with_error_code(error_code, "Cube not built because no cube inputs were available")
    if lowered:
        return _with_error_code(error_code, _compact_error_text(reason, limit=140))
    return _with_error_code(error_code, "optional cube stage not required for these outputs")


def _cube_error_code(
    result: dict[str, Any],
    pipeline_metadata: dict[str, Any] | None = None,
) -> str:
    metadata = dict(result.get("metadata") or {})
    pipeline_metadata = dict(pipeline_metadata or {})
    explicit = str(
        metadata.get("error_code")
        or metadata.get("cube_error_code")
        or pipeline_metadata.get("cube_error_code")
        or ""
    ).strip()
    if explicit:
        return explicit

    skipped_groups = [
        dict(item)
        for item in list(
            metadata.get("cube_tiles_skipped")
            or pipeline_metadata.get("cube_tiles_skipped")
            or []
        )
        if isinstance(item, dict)
    ]
    skipped_group_keys = {
        str(item.get("group_key") or "").strip()
        for item in skipped_groups
        if str(item.get("group_key") or "").strip()
    }
    skipped_reasons = {
        str(item.get("reason") or "").strip().lower()
        for item in skipped_groups
        if str(item.get("reason") or "").strip()
    }
    if len(skipped_group_keys) > 1 and skipped_reasons <= {"fewer_than_two_unique_times"}:
        return error_codes.CUBE_SPLIT_ACROSS_TILE_GROUPS

    reason = str(
        metadata.get("reason")
        or metadata.get("cube_reason")
        or pipeline_metadata.get("cube_reason")
        or result.get("error")
        or ""
    ).strip().lower()
    if reason == "no_source_zarrs":
        return error_codes.CUBE_NO_SOURCE_ZARRS
    if reason == "no_scenes_in_date_range":
        return error_codes.CUBE_NO_SCENES_IN_DATE_RANGE
    if reason in {"no_groups_with_multiple_times", "fewer_than_two_unique_times"}:
        return error_codes.CUBE_NO_GROUPS_WITH_MULTIPLE_TIMES
    if "daily mosaic cube" in reason and "sentinel-2" in reason:
        return error_codes.CUBE_UNSUPPORTED_LAYOUT_FOR_INPUTS
    if reason == "cube_input_missing":
        return error_codes.CUBE_INPUT_MISSING
    return ""


def _with_error_code(error_code: str, message: str) -> str:
    code = str(error_code or "").strip()
    text = str(message or "").strip()
    return f"{code}: {text}" if code else text


def _generic_skip_reason_label(reason: str) -> str:
    normalized = str(reason or "").strip()
    if normalized == "sen2like_runtime_not_routed_yet":
        return "Sen2Like runtime is not routed yet"
    if normalized == "dependency_not_succeeded":
        return "dependency did not succeed"
    return normalized.replace("_", " ")


def _positive_int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def _string_list(value: Any) -> list[str]:
    return [str(item) for item in list(value or []) if str(item).strip()]


def _format_cube_group_key(value: str) -> str:
    normalized = str(value or "").strip()
    if len(normalized) == 5 and normalized[:2].isdigit():
        return f"T{normalized}"
    return normalized


def _compact_group_keys(values: list[str]) -> str:
    unique = sorted({str(value).strip() for value in values if str(value).strip()})
    if len(unique) <= 3:
        return ", ".join(unique)
    return ", ".join(unique[:3]) + f", +{len(unique) - 3} more"


def _stage_error_summary(error: str) -> str:
    text = html.unescape(str(error or "").strip())
    if not text:
        return "Stage failed."

    structured = _extract_structured_error(text)
    if structured:
        return _structured_error_summary(structured)

    lowered = text.lower()
    if "failed to create a temp directory" in lowered:
        return "Spark could not create its temporary directory."
    if "inappropriate content detected" in lowered:
        return "Provider rejected the search query."
    if "permission denied" in lowered:
        return "Stage could not write to a required directory."
    if "no such file or directory" in lowered:
        return "Stage could not find an input file or runtime path."
    return _compact_error_text(text)


def _extract_structured_error(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        value = ast.literal_eval(text[start : end + 1])
    except (SyntaxError, ValueError):
        return {}
    return dict(value) if isinstance(value, dict) else {}


def _structured_error_summary(payload: dict[str, Any]) -> str:
    stderr_tail = str(payload.get("stderr_tail") or "").strip()
    lowered = stderr_tail.lower()
    if "failed to create a temp directory" in lowered:
        return "Spark could not create its temporary directory."
    if "permission denied" in lowered:
        return "Sen2Like could not write to a required directory."
    if "no such file or directory" in lowered:
        return "Sen2Like could not find an input file or runtime path."

    status = str(payload.get("status") or "").strip().lower()
    return_code = payload.get("return_code")
    duration = payload.get("duration_seconds")
    if status == "failed" or return_code not in (None, 0, "0"):
        bits = ["Sen2Like subprocess failed"]
        if return_code is not None:
            bits.append(f"exit code {return_code}")
        if duration is not None:
            try:
                bits.append(f"{float(duration):.1f}s")
            except (TypeError, ValueError):
                pass
        return f"{bits[0]} ({', '.join(bits[1:])})." if len(bits) > 1 else f"{bits[0]}."
    return _compact_error_text(str(payload.get("error") or payload.get("detail") or "Stage failed."))


def _compact_error_text(text: str, *, limit: int = 170) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 1)].rstrip() + "..."


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


def _retitle_stage(stage: dict[str, Any], item: dict[str, Any] | None = None) -> dict[str, Any]:
    display = dict(stage)
    item = dict(item or {})
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    key = str(display.get("key") or "").strip().lower()
    if key == "convert":
        display["key"] = "zarr"
        display["label"] = "Zarr"
        display["badge"] = "ZARR"
        zarr_outputs = _string_list(item.get("zarr_outputs") or pipeline_metadata.get("zarr_outputs"))
        conversion_metadata = dict(item.get("conversion_metadata") or {})
        duration_from_metadata = _positive_float(
            conversion_metadata.get("duration_seconds")
            or pipeline_metadata.get("zarr_duration_seconds")
        )
        if zarr_outputs:
            display["outputs"] = zarr_outputs
            output_count = len(zarr_outputs)
            display["detail_label"] = (
                f"Zarr conversion · {output_count} output{'s' if output_count != 1 else ''}"
            )
            if duration_from_metadata is not None:
                display["duration_seconds"] = duration_from_metadata
            if str(display.get("status") or "").strip().lower() in {"pending", ""}:
                display["status"] = "done"
            _clear_zero_step_timing(display)
        if str(display.get("detail_label") or "").strip().lower() in {"convert", "conversion queued"}:
            display["detail_label"] = "Zarr conversion"
    elif key == "cube":
        _apply_cube_metadata_to_legacy_stage(display, item, pipeline_metadata)
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


def _apply_cube_metadata_to_legacy_stage(
    display: dict[str, Any],
    item: dict[str, Any],
    pipeline_metadata: dict[str, Any],
) -> None:
    cube_status = str(pipeline_metadata.get("cube_status") or "").strip().lower()
    if not cube_status:
        return

    metadata = dict(display.get("metadata") or {})
    metadata.setdefault("cube_status", cube_status)
    metadata.setdefault("cube_reason", pipeline_metadata.get("cube_reason"))
    metadata.setdefault("cube_error_code", pipeline_metadata.get("cube_error_code"))
    metadata.setdefault("cube_tiles_skipped", pipeline_metadata.get("cube_tiles_skipped"))
    metadata.setdefault("cube_diagnostics", pipeline_metadata.get("cube_diagnostics"))
    display["metadata"] = metadata

    cube_outputs = _string_list(item.get("cube_outputs") or pipeline_metadata.get("cube_outputs"))
    if cube_outputs:
        display["outputs"] = cube_outputs

    if cube_status == "skipped":
        display["status"] = "skipped"
        display["detail_label"] = _cube_skip_reason_label({"metadata": metadata}, pipeline_metadata)
        _clear_zero_step_timing(display)
    elif cube_status in {"written", "succeeded"}:
        display["status"] = "done"
        if cube_outputs:
            output_count = len(cube_outputs)
            display["detail_label"] = f"Cube · {output_count} output{'s' if output_count != 1 else ''}"
    elif cube_status == "failed":
        display["status"] = "failed"
        reason = str(pipeline_metadata.get("cube_reason") or "").strip()
        if reason:
            display["detail_label"] = _compact_error_text(reason, limit=140)


def _clear_zero_step_timing(display: dict[str, Any]) -> None:
    try:
        step_count = int(display.get("step_count") or 0)
    except (TypeError, ValueError):
        step_count = 0
    if step_count != 0:
        return
    try:
        duration = float(display.get("duration_seconds") or 0.0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration > 0.0:
        return
    display["duration_seconds"] = None
    display["started_at"] = None
    display["finished_at"] = None


def _synthetic_sen2like_stage(
    display_stages: list[dict[str, Any]],
    item: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = dict(item or {})
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
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
    sen2like_outputs = _string_list(pipeline_metadata.get("sen2like_outputs"))
    sen2like_response = dict(pipeline_metadata.get("sen2like_response") or {})
    duration_from_metadata = _positive_float(sen2like_response.get("duration_seconds"))
    if any(status in {"done", "running", "queued", "failed", "cancelled"} for status in downstream_statuses):
        status = "done"
        detail_label = "Landsat normalized before Zarr"
        if sen2like_outputs:
            output_count = len(sen2like_outputs)
            detail_label += f" · {output_count} output{'s' if output_count != 1 else ''}"
        duration_seconds: float | None = duration_from_metadata
    elif fetch_status == "done":
        status = "queued"
        detail_label = "Waiting to normalize Landsat"
        duration_seconds = None
    else:
        status = "pending"
        detail_label = "Landsat-only normalization"
        duration_seconds = None
    fetch_stage = stages_by_key.get("fetch") or {}
    reference_time = (
        fetch_stage.get("finished_at")
        if status in {"done", "queued"} and duration_from_metadata is None
        else None
    )
    return {
        "key": "sen2like",
        "label": "Sen2Like",
        "badge": "S2L",
        "status": status,
        "started_at": reference_time if status == "done" else None,
        "finished_at": reference_time if status == "done" else None,
        "duration_seconds": duration_seconds,
        "detail_label": detail_label,
        "outputs": sen2like_outputs,
        "step_count": 0,
        "index": int(fetch_stage.get("index", 0) or 0) + 1,
        "synthetic": True,
    }
