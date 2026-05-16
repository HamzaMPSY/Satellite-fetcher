from __future__ import annotations

import html
import os
import uuid
from typing import Any

import streamlit as st

from nimbuschain_fetch.pipeline import (
    PipelineOptions,
    PipelineOrchestrator,
    build_default_pipeline_stages,
    is_landsat_selection,
)
from nimbuschain_fetch_ui.constants import PRODUCT_TYPES, PROJECT_ROOT, PROVIDER_CLI_MAP, PROVIDERS
from nimbuschain_fetch_ui.orchestrator_cli import build_stage_cli_command, run_stage_cli


STAGE_LABELS = {
    "fetch": ("Fetch", "Provider search and raw product download"),
    "sen2like": ("Sen2Like", "Landsat normalization to Sentinel-2-like layout"),
    "zarr": ("Zarr", "Scene conversion to analysis-ready Zarr"),
    "mask": ("Mask", "Cloud and water mask execution"),
    "cube": ("Cube", "Time-series cube build"),
}
STATUS_CLASS = {
    "pending": "is-pending",
    "running": "is-running",
    "succeeded": "is-succeeded",
    "skipped": "is-skipped",
    "failed": "is-failed",
}


def render_orchestrator_tab(
    *,
    provider_label: str,
    collection: str,
    product_type: str,
) -> None:
    st.markdown(
        '<div class="nimbus-section-title"><span>Pipeline Orchestrator</span><strong>Visual CLI</strong></div>',
        unsafe_allow_html=True,
    )

    defaults = _defaults(provider_label, collection, product_type)
    provider_options = list(PROVIDERS.keys())
    selected_provider_label = st.session_state.get(
        "orch_provider_label",
        defaults["provider_label"],
    )
    if selected_provider_label not in provider_options:
        selected_provider_label = provider_options[0]
        st.session_state["orch_provider_label"] = selected_provider_label

    c1, c2, c3, c4 = st.columns([1.1, 1.35, 1.15, 1.1])
    with c1:
        selected_provider_label = st.selectbox(
            "Provider",
            provider_options,
            index=provider_options.index(selected_provider_label),
            key="orch_provider_label",
        )
    collection_options = PROVIDERS.get(selected_provider_label, [])
    selected_collection = st.session_state.get("orch_collection", defaults["collection"])
    if selected_collection not in collection_options and collection_options:
        selected_collection = collection_options[0]
        st.session_state["orch_collection"] = selected_collection
    with c2:
        selected_collection = st.selectbox(
            "Collection",
            collection_options or [selected_collection],
            index=(collection_options or [selected_collection]).index(selected_collection),
            key="orch_collection",
        )
    product_options = PRODUCT_TYPES.get(selected_collection, [])
    selected_product = st.session_state.get("orch_product_type", defaults["product_type"])
    if selected_product not in product_options and product_options:
        selected_product = product_options[0]
        st.session_state["orch_product_type"] = selected_product
    with c3:
        selected_product = st.selectbox(
            "Product",
            product_options or [selected_product],
            index=(product_options or [selected_product]).index(selected_product),
            key="orch_product_type",
        )
    with c4:
        job_id = st.text_input(
            "Job id",
            value=st.session_state.get("orch_job_id") or f"manual-{uuid.uuid4().hex[:8]}",
            key="orch_job_id",
        )

    provider = PROVIDER_CLI_MAP.get(selected_provider_label, selected_provider_label.lower())
    is_landsat_flow = is_landsat_selection(
        provider=provider,
        collection=selected_collection,
        product_type=selected_product,
    )

    controls_left, controls_right = st.columns([1.2, 1])
    with controls_left:
        raw_uri = st.text_input(
            "Raw product path",
            value=st.session_state.get("orch_raw_uri", ""),
            placeholder="/data/downloads/raw/SCENE.SAFE.zip or /data/downloads/raw/LC08_SCENE.tar",
            key="orch_raw_uri",
        )
        source_zarr_uri = st.text_input(
            "Existing Zarr",
            value=st.session_state.get("orch_source_zarr_uri", ""),
            placeholder="/data/downloads/zarr/SCENE.zarr",
            key="orch_source_zarr_uri",
        )
        zarr_service_url = st.text_input(
            "Zarr service",
            value=st.session_state.get(
                "orch_zarr_service_url",
                os.getenv("NIMBUS_ZARR_SERVICE_URL", "http://nimbus-zarr:8010"),
            ),
            key="orch_zarr_service_url",
        )
        mask_service_url = st.text_input(
            "Mask service",
            value=st.session_state.get(
                "orch_mask_service_url",
                os.getenv("NIMBUS_MASK_SERVICE_URL", "http://nimbus-mask:8020"),
            ),
            key="orch_mask_service_url",
        )
        sen2like_service_url = ""
        sen2like_working_dir = ""
        if is_landsat_flow:
            sen2like_service_url = st.text_input(
                "Sen2Like service",
                value=st.session_state.get(
                    "orch_sen2like_service_url",
                    os.getenv("NIMBUS_SEN2LIKE_SERVICE_URL", "http://nimbus-sen2like:8030"),
                ),
                key="orch_sen2like_service_url",
            )
            sen2like_working_dir = st.text_input(
                "Sen2Like work dir",
                value=st.session_state.get(
                    "orch_sen2like_working_dir",
                    os.getenv("NIMBUS_SEN2LIKE_WORK_DIR", "/data/downloads/sen2like"),
                ),
                key="orch_sen2like_working_dir",
            )
    with controls_right:
        default_mask_mode = st.session_state.get("orch_mask_mode") or _mask_mode_from_types(
            st.session_state.get("orch_mask_types", [])
        )
        mask_mode = st.radio(
            "Masking",
            options=["none", "water", "cloud", "water_cloud"],
            index=["none", "water", "cloud", "water_cloud"].index(default_mask_mode),
            horizontal=True,
            key="orch_mask_mode",
            format_func=lambda value: {
                "none": "No mask",
                "water": "Water",
                "cloud": "Cloud",
                "water_cloud": "Water + cloud",
            }[value],
        )
        mask_types = _mask_types_from_mode(mask_mode)
        cube_mode = st.radio(
            "Cube",
            options=["none", "before_mask", "after_mask"],
            horizontal=True,
            key="orch_cube_mode",
            format_func=lambda value: {
                "none": "None",
                "before_mask": "Before mask",
                "after_mask": "After mask",
            }[value],
        )
        target_stage_options = _target_stage_options(
            is_landsat_flow=is_landsat_flow,
            has_mask=bool(mask_types),
            has_cube=cube_mode != "none",
        )
        if st.session_state.get("orch_target_stage") not in target_stage_options:
            st.session_state["orch_target_stage"] = "full"
        target_stage = st.selectbox(
            "Target stage",
            options=target_stage_options,
            key="orch_target_stage",
        )
        default_sen2like_workers = int(os.getenv("NIMBUS_SEN2LIKE_WORKERS", "1") or 1)
        sen2like_workers = int(
            st.session_state.get("orch_sen2like_workers", default_sen2like_workers)
        )
        if is_landsat_flow:
            sen2like_workers = int(
                st.number_input(
                    "Sen2Like workers",
                    min_value=1,
                    max_value=128,
                    value=sen2like_workers,
                    step=1,
                    key="orch_sen2like_workers",
                )
            )

    sen2like_kwargs = _sen2like_command_kwargs(
        enabled=is_landsat_flow,
        raw_uri=raw_uri,
        service_url=sen2like_service_url,
        working_dir=sen2like_working_dir,
        workers=sen2like_workers,
    )
    plan_command = build_stage_cli_command(
        action="plan",
        provider=provider,
        collection=selected_collection,
        product_type=selected_product,
        job_id=job_id,
        mask_types=mask_types,
        cube_mode=cube_mode,
        target_stage=None if target_stage == "full" else target_stage,
        **sen2like_kwargs,
    )
    run_command = build_stage_cli_command(
        action="run-stage",
        provider=provider,
        collection=selected_collection,
        product_type=selected_product,
        job_id=job_id,
        mask_types=mask_types,
        cube_mode=cube_mode,
        run_stage=_default_run_stage(target_stage, cube_mode, mask_types),
        raw_uri=raw_uri.strip() or None,
        source_zarr_uri=source_zarr_uri.strip() or None,
        zarr_service_url=zarr_service_url.strip() or None,
        mask_service_url=mask_service_url.strip() or None if mask_types else None,
        zarr_output_dir=os.getenv("NIMBUS_ZARR_OUTPUT_DIR", "/data/downloads/zarr/manual"),
        cube_output_dir=os.getenv("NIMBUS_CUBE_OUTPUT_DIR", "/data/downloads/zarr/cubes/manual"),
        execute=True,
        **sen2like_kwargs,
    )

    action_cols = st.columns([1, 1, 2])
    with action_cols[0]:
        plan_clicked = st.button("Plan", width="stretch", type="primary", key="orch_plan_btn")
    with action_cols[1]:
        run_clicked = st.button("Run stage", width="stretch", key="orch_run_btn")
    with action_cols[2]:
        timeout_seconds = st.slider(
            "CLI timeout",
            min_value=30,
            max_value=3600,
            value=int(st.session_state.get("orch_timeout_seconds", 300)),
            step=30,
            key="orch_timeout_seconds",
        )

    with st.expander("CLI command", expanded=False):
        st.code(
            " ".join(_shell_quote(item) for item in (run_command if run_clicked else plan_command)),
            language="bash",
        )

    current_signature = _command_signature(plan_command, run_command)
    if st.session_state.get("orch_last_signature") != current_signature:
        st.session_state.pop("orch_last_invocation", None)

    if plan_clicked:
        st.session_state["orch_last_invocation"] = run_stage_cli(
            plan_command,
            cwd=PROJECT_ROOT,
            timeout_seconds=int(timeout_seconds),
        )
        st.session_state["orch_last_signature"] = current_signature
    if run_clicked:
        st.session_state["orch_last_invocation"] = run_stage_cli(
            run_command,
            cwd=PROJECT_ROOT,
            timeout_seconds=int(timeout_seconds),
        )
        st.session_state["orch_last_signature"] = current_signature

    invocation = st.session_state.get("orch_last_invocation")
    if invocation is None:
        preview = _dry_plan_payload(
            selected_provider=provider,
            selected_collection=selected_collection,
            selected_product=selected_product,
            mask_types=mask_types,
            cube_mode=cube_mode,
            target_stage=target_stage,
            sen2like_service_url=sen2like_kwargs.get("sen2like_service_url"),
        )
        _render_payload(preview, return_code=None)
        return

    if invocation.error:
        st.error(invocation.error)
    if invocation.return_code not in (0, None):
        st.warning(f"CLI exited with code {invocation.return_code}.")
    _render_payload(invocation.payload, return_code=invocation.return_code)
    with st.expander("CLI output", expanded=False):
        if invocation.stdout:
            st.code(invocation.stdout, language="json")
        if invocation.stderr:
            st.code(invocation.stderr, language="text")


def render_pipeline_plan_summary(
    *,
    provider_label: str,
    collection: str,
    product_type: str,
    mask_types: list[str] | tuple[str, ...],
    cube_mode: str,
) -> None:
    provider = PROVIDER_CLI_MAP.get(provider_label, provider_label.lower())
    payload = _dry_plan_payload(
        selected_provider=provider,
        selected_collection=collection,
        selected_product=product_type,
        mask_types=list(mask_types),
        cube_mode=cube_mode,
        target_stage="full",
        sen2like_service_url=os.getenv("NIMBUS_SEN2LIKE_SERVICE_URL") or None,
    )
    _render_payload(
        payload,
        return_code=None,
        eyebrow="Selected job path",
        title="Pipeline plan",
        show_json=False,
    )


def _render_payload(
    payload: dict[str, Any],
    *,
    return_code: int | None,
    eyebrow: str = "CLI runtime",
    title: str = "Modular pipeline plan",
    show_json: bool = True,
) -> None:
    stages = _stages_from_payload(payload)
    results = {
        str(item.get("name")): item
        for item in list(payload.get("results") or [])
        if isinstance(item, dict)
    }
    completed = sum(1 for item in results.values() if str(item.get("status")) == "succeeded")
    skipped = sum(1 for item in results.values() if str(item.get("status")) == "skipped")
    failed = sum(1 for item in results.values() if str(item.get("status")) == "failed")
    total_duration = sum(float(item.get("duration_seconds") or 0.0) for item in results.values())
    status = str(payload.get("status") or "preview")

    metrics = [
        ("Status", status),
        ("Stages", str(len(stages))),
        ("Succeeded", str(completed)),
        ("Skipped", str(skipped)),
        ("Failed", str(failed)),
        ("Duration", f"{total_duration:.2f}s"),
    ]
    metric_html = "".join(
        f"<div class='nimbus-orch-metric'><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong></div>"
        for label, value in metrics
    )
    st.markdown(
        "<div class='nimbus-orch-shell'>"
        "<div class='nimbus-orch-head'>"
        f"<div><span class='nimbus-orch-eyebrow'>{html.escape(eyebrow)}</span>"
        f"<h3>{html.escape(title)}</h3></div>"
        f"<div class='nimbus-orch-return'>{_return_badge(return_code)}</div>"
        "</div>"
        f"<div class='nimbus-orch-metrics'>{metric_html}</div>"
        f"{_stage_cards_html(stages, results)}"
        "</div>",
        unsafe_allow_html=True,
    )

    if show_json and payload:
        with st.expander("JSON payload", expanded=False):
            st.json(payload)


def _return_badge(return_code: int | None) -> str:
    return f"exit {return_code}" if return_code is not None else "preview"


def _stage_cards_html(stages: list[dict[str, Any]], results: dict[str, dict[str, Any]]) -> str:
    cards = []
    for index, stage in enumerate(stages, start=1):
        name = str(stage.get("name") or "")
        label, description = STAGE_LABELS.get(name, (name.title(), "Pipeline stage"))
        result = results.get(name, {})
        status = str(result.get("status") or ("pending" if results else "planned"))
        status_class = STATUS_CLASS.get(status, "is-pending")
        duration = float(result.get("duration_seconds") or 0.0)
        depends = ", ".join(str(item) for item in list(stage.get("depends_on") or [])) or "root"
        reason = str(dict(result.get("metadata") or {}).get("reason") or "")
        outputs = list(result.get("outputs") or [])
        output_text = str(outputs[0]) if outputs else (reason or depends)
        cards.append(
            "<div class='nimbus-orch-stage {status_class}'>"
            "<div class='nimbus-orch-stage-top'>"
            "<span>{index:02d}</span><strong>{label}</strong>"
            "</div>"
            "<p>{description}</p>"
            "<div class='nimbus-orch-stage-meta'>"
            "<span>{status}</span><span>{duration:.2f}s</span>"
            "</div>"
            "<code>{output}</code>"
            "</div>".format(
                status_class=status_class,
                index=index,
                label=html.escape(label),
                description=html.escape(description),
                status=html.escape(status),
                duration=duration,
                output=html.escape(output_text[:140]),
            )
        )
    return "<div class='nimbus-orch-grid'>" + "".join(cards) + "</div>"


def _stages_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("stages"), list):
        return [dict(item) for item in payload["stages"] if isinstance(item, dict)]
    results = [dict(item) for item in list(payload.get("results") or []) if isinstance(item, dict)]
    return [{"name": item.get("name"), "depends_on": []} for item in results]


def _dry_plan_payload(
    *,
    selected_provider: str,
    selected_collection: str,
    selected_product: str,
    mask_types: list[str],
    cube_mode: str,
    target_stage: str,
    sen2like_service_url: str | None = None,
) -> dict[str, Any]:
    options = PipelineOptions(
        provider=selected_provider,
        collection=selected_collection,
        product_type=selected_product,
        mask_types=tuple(mask_types),
        cube_mode=cube_mode,
        sen2like_service_url=sen2like_service_url,
    )
    orchestrator = PipelineOrchestrator(build_default_pipeline_stages(options))
    stages = orchestrator.describe_plan(
        target_stage=None if target_stage == "full" else target_stage
    )
    return {
        "status": "preview",
        "provider": selected_provider,
        "collection": selected_collection,
        "product_type": selected_product,
        "mask_types": mask_types,
        "cube_mode": cube_mode,
        "stages": stages,
    }


def _target_stage_options(*, is_landsat_flow: bool, has_mask: bool, has_cube: bool) -> list[str]:
    stages = ["full", "fetch"]
    if is_landsat_flow:
        stages.append("sen2like")
    stages.append("zarr")
    if has_mask:
        stages.append("mask")
    if has_cube:
        stages.append("cube")
    return stages


def _mask_mode_from_types(mask_types: Any) -> str:
    normalized = {str(item).strip().lower() for item in list(mask_types or []) if item}
    if normalized == {"water", "cloud"}:
        return "water_cloud"
    if normalized == {"water"}:
        return "water"
    if normalized == {"cloud"}:
        return "cloud"
    return "none"


def _mask_types_from_mode(mask_mode: str) -> list[str]:
    value = str(mask_mode or "none")
    if value == "water":
        return ["water"]
    if value == "cloud":
        return ["cloud"]
    if value == "water_cloud":
        return ["water", "cloud"]
    return []


def _sen2like_command_kwargs(
    *,
    enabled: bool,
    raw_uri: str,
    service_url: str,
    working_dir: str,
    workers: int,
) -> dict[str, Any]:
    if not enabled:
        return {}
    return {
        "sen2like_service_url": service_url.strip() or None,
        "sen2like_working_dir": working_dir.strip() or None,
        "sen2like_workers": int(workers),
    }


def _command_signature(
    plan_command: list[str],
    run_command: list[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return (tuple(plan_command), tuple(run_command))


def _defaults(provider_label: str, collection: str, product_type: str) -> dict[str, str]:
    return {
        "provider_label": provider_label if provider_label in PROVIDERS else "Copernicus",
        "collection": collection,
        "product_type": product_type,
    }


def _default_run_stage(target_stage: str, cube_mode: str, mask_types: list[str]) -> str:
    if target_stage != "full":
        return target_stage
    if cube_mode == "after_mask":
        return "cube"
    if cube_mode == "before_mask" and mask_types:
        return "mask"
    if cube_mode == "before_mask":
        return "cube"
    if mask_types:
        return "mask"
    return "zarr"


def _shell_quote(value: str) -> str:
    if not value:
        return "''"
    safe = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_+-=/:.,")
    if all(char in safe for char in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"
