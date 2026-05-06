from __future__ import annotations

import html
import os
import uuid
from typing import Any

import streamlit as st

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
    selected_provider_label = st.session_state.get("orch_provider_label", defaults["provider_label"])
    if selected_provider_label not in provider_options:
        selected_provider_label = provider_options[0]

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

    controls_left, controls_right = st.columns([1.2, 1])
    with controls_left:
        raw_uri = st.text_input(
            "Raw Landsat path",
            value=st.session_state.get("orch_raw_uri", ""),
            placeholder="/data/downloads/raw/LC08_SCENE",
            key="orch_raw_uri",
        )
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
        mask_types = st.multiselect(
            "Masks",
            options=["water", "cloud"],
            default=st.session_state.get("orch_mask_types", []),
            key="orch_mask_types",
        )
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
        target_stage = st.selectbox(
            "Target stage",
            options=["full", "fetch", "sen2like", "zarr", "mask", "cube"],
            key="orch_target_stage",
        )
        sen2like_workers = st.number_input(
            "Sen2Like workers",
            min_value=1,
            max_value=128,
            value=int(st.session_state.get("orch_sen2like_workers", 4)),
            step=1,
            key="orch_sen2like_workers",
        )

    provider = PROVIDER_CLI_MAP.get(selected_provider_label, selected_provider_label.lower())
    plan_command = build_stage_cli_command(
        action="plan",
        provider=provider,
        collection=selected_collection,
        product_type=selected_product,
        job_id=job_id,
        mask_types=mask_types,
        cube_mode=cube_mode,
        target_stage=None if target_stage == "full" else target_stage,
        raw_uri=raw_uri.strip() or None,
        sen2like_service_url=sen2like_service_url.strip() or None,
        sen2like_working_dir=sen2like_working_dir.strip() or None,
        sen2like_workers=int(sen2like_workers),
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
        sen2like_service_url=sen2like_service_url.strip() or None,
        sen2like_working_dir=sen2like_working_dir.strip() or None,
        sen2like_workers=int(sen2like_workers),
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

    st.code(" ".join(_shell_quote(item) for item in (run_command if run_clicked else plan_command)), language="bash")

    if plan_clicked:
        st.session_state["orch_last_invocation"] = run_stage_cli(
            plan_command,
            cwd=PROJECT_ROOT,
            timeout_seconds=int(timeout_seconds),
        )
    if run_clicked:
        st.session_state["orch_last_invocation"] = run_stage_cli(
            run_command,
            cwd=PROJECT_ROOT,
            timeout_seconds=int(timeout_seconds),
        )

    invocation = st.session_state.get("orch_last_invocation")
    if invocation is None:
        preview = _dry_plan_payload(
            selected_provider=provider,
            selected_collection=selected_collection,
            selected_product=selected_product,
            mask_types=mask_types,
            cube_mode=cube_mode,
            target_stage=target_stage,
        )
        _render_payload(preview, command=plan_command, return_code=None)
        return

    if invocation.error:
        st.error(invocation.error)
    if invocation.return_code not in (0, None):
        st.warning(f"CLI exited with code {invocation.return_code}.")
    _render_payload(invocation.payload, command=invocation.command, return_code=invocation.return_code)
    with st.expander("CLI output", expanded=False):
        if invocation.stdout:
            st.code(invocation.stdout, language="json")
        if invocation.stderr:
            st.code(invocation.stderr, language="text")


def _render_payload(payload: dict[str, Any], *, command: list[str], return_code: int | None) -> None:
    stages = _stages_from_payload(payload)
    results = {str(item.get("name")): item for item in list(payload.get("results") or []) if isinstance(item, dict)}
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
        "<div><span class='nimbus-orch-eyebrow'>CLI runtime</span>"
        "<h3>Modular pipeline plan</h3></div>"
        f"<div class='nimbus-orch-return'>exit {return_code if return_code is not None else '-'}</div>"
        "</div>"
        f"<div class='nimbus-orch-metrics'>{metric_html}</div>"
        f"{_stage_cards_html(stages, results)}"
        "</div>",
        unsafe_allow_html=True,
    )

    if payload:
        with st.expander("JSON payload", expanded=False):
            st.json(payload)


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
) -> dict[str, Any]:
    stages = [
        {"name": "fetch", "depends_on": []},
        {"name": "sen2like", "depends_on": ["fetch"]},
        {"name": "zarr", "depends_on": ["fetch"]},
        {"name": "mask", "depends_on": ["zarr"]},
        {"name": "cube", "depends_on": ["zarr"]},
    ]
    if target_stage != "full":
        depends_by_name = {stage["name"]: list(stage["depends_on"]) for stage in stages}
        wanted = set()

        def visit(name: str) -> None:
            if name in wanted:
                return
            wanted.add(name)
            for dependency in depends_by_name.get(name, []):
                visit(dependency)

        visit(target_stage)
        stages = [stage for stage in stages if stage["name"] in wanted]
    return {
        "status": "preview",
        "provider": selected_provider,
        "collection": selected_collection,
        "product_type": selected_product,
        "mask_types": mask_types,
        "cube_mode": cube_mode,
        "stages": stages,
    }


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
