from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import streamlit as st

try:
    from streamlit_file_browser import st_file_browser
except ImportError:
    st_file_browser = None

from nimbuschain_fetch.models import JobState
from nimbuschain_fetch_ui.constants import (
    DOWNLOADS_DIR,
    RECENT_JOBS_FETCH_LIMIT,
    RECENT_JOBS_WINDOW_HOURS,
)
from nimbuschain_fetch_ui.downloads import count_downloaded_products
from nimbuschain_fetch_ui.jobs_helpers import (
    _api_request,
    _filter_recent_job_rows,
    _list_jobs,
    _recent_jobs_cutoff,
)
from nimbuschain_fetch_ui.zarr_utils import list_artifacts as _list_artifacts


def _fmt_mb(value: Any) -> float:
    try:
        return round(float(value or 0) / (1024 * 1024), 2)
    except Exception:
        return 0.0


def _fmt_progress(value: Any) -> str:
    try:
        return f"{float(value or 0):.1f}%"
    except Exception:
        return "0.0%"


def _fmt_timestamp(value: Any) -> str:
    if not value:
        return "-"
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def _render_job_summary_cards(rows: list[dict[str, Any]]) -> None:
    summary_jobs = len(rows)
    summary_success = sum(1 for row in rows if str(row.get("state")) == JobState.succeeded.value)
    summary_failed = sum(1 for row in rows if str(row.get("state")) == JobState.failed.value)
    summary_running = sum(
        1
        for row in rows
        if str(row.get("state")) in {
            JobState.queued.value,
            JobState.running.value,
            JobState.cancel_requested.value,
        }
    )
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Visible jobs", summary_jobs)
    with col2:
        st.metric("Running / queued", summary_running)
    with col3:
        st.metric("Succeeded / failed", f"{summary_success} / {summary_failed}")


def _render_jobs_table(rows: list[dict[str, Any]]) -> None:
    compact_rows = []
    for row in rows:
        compact_rows.append(
            {
                "job_id": str(row.get("job_id", ""))[:12],
                "provider": row.get("provider", "-"),
                "mission": row.get("collection", "-"),
                "product": row.get("product_type", "-"),
                "state": row.get("state", "-"),
                "progress": _fmt_progress(row.get("progress")),
                "downloaded_mb": _fmt_mb(row.get("bytes_downloaded")),
                "total_mb": _fmt_mb(row.get("bytes_total")),
                "updated": _fmt_timestamp(row.get("updated_at")),
            }
        )
    st.dataframe(compact_rows, width="stretch", hide_index=True)


def _render_job_details(rows: list[dict[str, Any]]) -> None:
    selected_job_id = st.selectbox(
        "Inspect job",
        options=[str(row.get("job_id", "")) for row in rows],
        format_func=lambda job_id: f"{job_id[:12]}...",
        index=0,
    )
    selected_job = next(
        (row for row in rows if str(row.get("job_id", "")) == selected_job_id),
        None,
    )
    if selected_job is None:
        return
    with st.expander("Job details", expanded=False):
        st.code(
            json.dumps(
                {
                    "job_id": selected_job.get("job_id"),
                    "provider": selected_job.get("provider"),
                    "collection": selected_job.get("collection"),
                    "product_type": selected_job.get("product_type"),
                    "state": selected_job.get("state"),
                    "pipeline_state": selected_job.get("pipeline_state"),
                    "pipeline_step": selected_job.get("pipeline_step"),
                    "progress": selected_job.get("progress"),
                    "bytes_downloaded": selected_job.get("bytes_downloaded"),
                    "bytes_total": selected_job.get("bytes_total"),
                    "created_at": selected_job.get("created_at"),
                    "started_at": selected_job.get("started_at"),
                    "updated_at": selected_job.get("updated_at"),
                    "scene_id": selected_job.get("scene_id"),
                    "errors": selected_job.get("errors"),
                },
                indent=2,
            ),
            language="json",
        )


def _render_outputs_block(title: str, outputs: list[str], empty_message: str) -> None:
    with st.container(border=True):
        st.markdown(f"**{title}**")
        if not outputs:
            st.caption(empty_message)
            return
        st.caption(f"{len(outputs)} item(s)")
        with st.expander(f"Show {title.lower()}", expanded=False):
            for item in outputs:
                st.code(str(item), language="text")


def _merge_output_values(*collections: Any) -> list[str]:
    merged: list[str] = []
    for collection in collections:
        values = collection
        if isinstance(values, (str, Path)):
            values = [values]
        for value in list(values or []):
            text = str(value or "").strip()
            if text and text not in merged:
                merged.append(text)
    return merged


def _artifact_runtime_exists(item: dict[str, Any]) -> bool:
    metadata = dict(item.get("metadata") or {})
    return bool(metadata.get("runtime_exists", True))


def _row_is_mask_job(row: dict[str, Any]) -> bool:
    return str(row.get("job_kind") or row.get("job_type") or "").strip().lower() in {
        "mask",
        "mask_existing_zarr",
    }


def _row_has_final_mask_success(row: dict[str, Any]) -> bool:
    if not _row_is_mask_job(row):
        return True
    state = str(row.get("state") or "").strip().lower()
    pipeline_state = str(row.get("pipeline_state") or "").strip().lower()
    return state == JobState.succeeded.value and pipeline_state == "masked_zarr_written"


def _render_masked_zarr_relations(masked_artifacts: list[dict[str, Any]]) -> None:
    relation_rows: list[dict[str, str]] = []
    for item in masked_artifacts:
        masked_uri = str(item.get("artifact_uri") or "").strip()
        source_uri = str(item.get("source_uri") or "").strip()
        metadata = dict(item.get("metadata") or {})
        if not source_uri:
            source_uri = str(metadata.get("source_zarr_uri") or "").strip()
        if not masked_uri:
            continue
        relation_rows.append(
            {
                "source_zarr": source_uri or "-",
                "masked_zarr": masked_uri,
            }
        )
    if not relation_rows:
        st.caption("No source to masked Zarr lineage is recorded for this job.")
        return
    with st.container(border=True):
        st.markdown("**Masked Zarr lineage**")
        st.caption("Source Zarr -> masked Zarr store")
        st.dataframe(relation_rows, width="stretch", hide_index=True)


def _mask_quality_snapshot(
    *,
    pipeline_metadata: dict[str, Any],
    conversion_metadata: dict[str, Any],
) -> dict[str, Any]:
    water = dict(conversion_metadata.get("water_mask") or pipeline_metadata.get("water_mask") or {})
    cloud = dict(conversion_metadata.get("cloud_mask") or pipeline_metadata.get("cloud_mask") or {})
    return {
        "water_status": str(water.get("status") or "").strip() or str(pipeline_metadata.get("water_mask", {}).get("status") or "").strip(),
        "water_runtime_mode": str(water.get("runtime_mode") or "").strip(),
        "water_fraction": float(
            water.get("water_fraction")
            or conversion_metadata.get("water_fraction")
            or pipeline_metadata.get("water_fraction")
            or 0.0
        ),
        "water_threshold_used": water.get("threshold_used"),
        "water_sensor_recipe": str(water.get("sensor_recipe") or "").strip(),
        "water_probability_path": str(water.get("probability_path") or "").strip(),
        "cloud_status": str(cloud.get("status") or "").strip(),
        "cloud_backend": str(cloud.get("backend") or "").strip(),
        "cloud_includes_shadows": bool(
            cloud.get("include_shadows")
            if cloud.get("include_shadows") is not None
            else cloud.get("includes_shadows")
            if cloud.get("includes_shadows") is not None
            else pipeline_metadata.get("include_shadows", False)
        ),
        "cloud_fraction": float(
            cloud.get("cloud_fraction")
            or conversion_metadata.get("cloud_fraction")
            or pipeline_metadata.get("cloud_fraction")
            or 0.0
        ),
        "cloud_only_fraction": float(
            cloud.get("cloud_only_fraction")
            or conversion_metadata.get("cloud_only_fraction")
            or pipeline_metadata.get("cloud_only_fraction")
            or 0.0
        ),
        "shadow_fraction": float(
            cloud.get("shadow_fraction")
            or conversion_metadata.get("shadow_fraction")
            or pipeline_metadata.get("shadow_fraction")
            or 0.0
        ),
        "cloud_mask_source": str(cloud.get("mask_source") or "").strip(),
        "cloud_probability_source": str(cloud.get("probability_source") or "").strip(),
        "cloud_sensor_recipe": str(cloud.get("sensor_recipe") or "").strip(),
        "cloud_probability_path": str(cloud.get("probability_path") or "").strip(),
    }


def _render_mask_quality_block(
    *,
    pipeline_metadata: dict[str, Any],
    conversion_metadata: dict[str, Any],
) -> None:
    summary = _mask_quality_snapshot(
        pipeline_metadata=pipeline_metadata,
        conversion_metadata=conversion_metadata,
    )
    if not any(
        [
            summary["water_status"],
            summary["cloud_status"],
            summary["water_runtime_mode"],
            summary["cloud_backend"],
            summary["water_fraction"] > 0.0,
            summary["cloud_fraction"] > 0.0,
        ]
    ):
        return

    with st.container(border=True):
        st.markdown("**Mask quality summary**")
        cols = st.columns(4)
        with cols[0]:
            st.metric("Cloud fraction", f"{summary['cloud_fraction'] * 100:.1f}%")
        with cols[1]:
            st.metric("Shadow fraction", f"{summary['shadow_fraction'] * 100:.1f}%")
        with cols[2]:
            st.metric("Water fraction", f"{summary['water_fraction'] * 100:.1f}%")
        with cols[3]:
            runtime_label = summary["water_runtime_mode"] or summary["cloud_backend"] or "-"
            st.metric("Runtime", runtime_label)

        lines = [
            f"Cloud status: {summary['cloud_status'] or '-'}",
            f"Cloud backend used: {summary['cloud_backend'] or '-'}",
            f"Cloud includes shadows: {'yes' if summary['cloud_includes_shadows'] else 'no'}",
            f"Cloud-only fraction: {summary['cloud_only_fraction'] * 100:.1f}%",
            f"Cloud mask source: {summary['cloud_mask_source'] or '-'}",
            f"Cloud confidence/debug layer: {summary['cloud_probability_path'] or '-'}",
            f"Cloud probability source: {summary['cloud_probability_source'] or '-'}",
            f"Cloud sensor recipe: {summary['cloud_sensor_recipe'] or '-'}",
            f"Water status: {summary['water_status'] or '-'}",
            f"Water runtime mode: {summary['water_runtime_mode'] or '-'}",
            f"Water threshold used: {summary['water_threshold_used'] if summary['water_threshold_used'] is not None else '-'}",
            f"Water sensor recipe: {summary['water_sensor_recipe'] or '-'}",
            f"Water probability/debug layer: {summary['water_probability_path'] or '-'}",
        ]
        st.code("\n".join(lines), language="text")


def _render_pipeline_result_section(
    api_url: str,
    api_key: str,
    rows: list[dict[str, Any]],
    *,
    artifact_type_filter: str | None = None,
) -> None:
    st.markdown("---")
    st.markdown("**Pipeline outputs**")
    selected_job_id = st.selectbox(
        "Inspect pipeline job",
        options=[str(row.get("job_id", "")) for row in rows],
        format_func=lambda job_id: f"{job_id[:12]}...",
        index=0,
        key="results_pipeline_job",
    )
    selected_row = next((row for row in rows if str(row.get("job_id", "")) == selected_job_id), None)
    if selected_row is None:
        return
    allow_mask_outputs = _row_has_final_mask_success(selected_row)

    result_payload: dict[str, Any] = {}
    try:
        response = _api_request("GET", api_url, f"/v1/jobs/{selected_job_id}/result", api_key=api_key, timeout=60)
        if response.ok:
            result_payload = dict(response.json() or {})
    except Exception:
        result_payload = {}

    raw_outputs = list(result_payload.get("raw_outputs") or selected_row.get("raw_outputs") or [])
    zarr_outputs = list(result_payload.get("zarr_outputs") or selected_row.get("zarr_outputs") or [])
    watermask_outputs = list(result_payload.get("watermask_outputs") or selected_row.get("watermask_outputs") or [])
    pipeline_metadata = dict(result_payload.get("pipeline_metadata") or selected_row.get("pipeline_metadata") or {})
    conversion_metadata = dict(result_payload.get("conversion_metadata") or selected_row.get("conversion_metadata") or {})
    result_metadata = dict(result_payload.get("metadata") or {})
    masked_artifacts, _masked_total = _list_artifacts(
        api_url,
        api_key,
        artifact_type="zarr_masked",
        job_id=selected_job_id,
        page=1,
        page_size=100,
    )
    masked_artifacts = [item for item in masked_artifacts if _artifact_runtime_exists(item)]
    masked_zarr_result_outputs = _merge_output_values(
        result_payload.get("masked_zarr_outputs"),
        result_payload.get("masked_zarr_uri"),
        result_metadata.get("masked_zarr_uri"),
        pipeline_metadata.get("masked_zarr_uri"),
        conversion_metadata.get("masked_zarr_uri"),
    )
    artifact_masked_outputs = [
        str(item.get("artifact_uri") or "").strip()
        for item in masked_artifacts
        if str(item.get("artifact_uri") or "").strip()
    ]
    masked_zarr_outputs = _merge_output_values(
        artifact_masked_outputs,
        masked_zarr_result_outputs if allow_mask_outputs else [],
    )
    if allow_mask_outputs and masked_zarr_result_outputs:
        source_uri = str(
            result_metadata.get("source_zarr_uri")
            or pipeline_metadata.get("source_zarr_uri")
            or conversion_metadata.get("source_zarr_uri")
            or ""
        ).strip()
        known_artifact_uris = {str(item.get("artifact_uri") or "").strip() for item in masked_artifacts}
        for artifact_uri in masked_zarr_result_outputs:
            if artifact_uri in known_artifact_uris:
                continue
            masked_artifacts.append(
                {
                    "artifact_uri": artifact_uri,
                    "source_uri": source_uri,
                    "metadata": {
                        "source_zarr_uri": source_uri,
                    },
                }
            )
    watermask_artifacts, _watermask_total = _list_artifacts(
        api_url,
        api_key,
        artifact_type="watermask",
        job_id=selected_job_id,
        page=1,
        page_size=100,
    )
    watermask_artifacts = [item for item in watermask_artifacts if _artifact_runtime_exists(item)]
    cloudmask_artifacts, _cloudmask_total = _list_artifacts(
        api_url,
        api_key,
        artifact_type="cloudmask",
        job_id=selected_job_id,
        page=1,
        page_size=100,
    )
    cloudmask_artifacts = [item for item in cloudmask_artifacts if _artifact_runtime_exists(item)]
    watermask_outputs_combined = _merge_output_values(
        watermask_outputs if allow_mask_outputs else [],
        [
            str(dict(conversion_metadata.get("water_mask") or {}).get("artifact_uri") or "").strip(),
            str(dict(pipeline_metadata.get("water_mask") or {}).get("artifact_uri") or "").strip(),
        ] if allow_mask_outputs else [],
        [
            str(item.get("artifact_uri") or "").strip()
            for item in watermask_artifacts
            if str(item.get("artifact_uri") or "").strip()
        ],
    )
    cloudmask_outputs = _merge_output_values(
        result_payload.get("cloudmask_outputs") if allow_mask_outputs else [],
        [
            str(dict(conversion_metadata.get("cloud_mask") or {}).get("artifact_uri") or "").strip(),
            str(dict(pipeline_metadata.get("cloud_mask") or {}).get("artifact_uri") or "").strip(),
        ] if allow_mask_outputs else [],
        [
        str(item.get("artifact_uri") or "").strip()
        for item in cloudmask_artifacts
        if str(item.get("artifact_uri") or "").strip()
        ],
    )

    visible_raw_outputs = raw_outputs if artifact_type_filter in {None, "", "raw"} else []
    visible_zarr_outputs = zarr_outputs if artifact_type_filter in {None, "", "zarr"} else []
    visible_masked_zarr_outputs = masked_zarr_outputs if artifact_type_filter in {None, "", "zarr_masked"} else []
    visible_watermask_outputs = watermask_outputs_combined if artifact_type_filter in {None, "", "watermask"} else []
    visible_cloudmask_outputs = cloudmask_outputs if artifact_type_filter in {None, "", "cloudmask"} else []

    top1, top2, top3, top4, top5 = st.columns(5)
    with top1:
        st.metric("State", str(selected_row.get("state") or "-"))
    with top2:
        st.metric("Pipeline", str(selected_row.get("pipeline_state") or "-"))
    with top3:
        st.metric("Raw outputs", len(raw_outputs))
    with top4:
        st.metric("Zarr outputs", len(zarr_outputs))
    with top5:
        st.metric("Masked Zarr", len(masked_zarr_outputs))

    meta1, meta2, meta3 = st.columns(3)
    with meta1:
        st.caption(
            f"Provider: `{selected_row.get('provider', '-')}`  \n"
            f"Mission: `{selected_row.get('collection', '-')}`"
        )
    with meta2:
        st.caption(
            f"Product: `{selected_row.get('product_type', '-')}`  \n"
            f"Updated: `{_fmt_timestamp(selected_row.get('updated_at'))}`"
        )
    with meta3:
        scene_hint = (
            pipeline_metadata.get("scene_id")
            or selected_row.get("scene_id")
            or conversion_metadata.get("scene_id")
            or "-"
        )
        st.caption(f"Scene: `{scene_hint}`")

    if _row_is_mask_job(selected_row) and not allow_mask_outputs:
        st.caption("Mask outputs stay hidden here until the job reaches `masked_zarr_written`. Legacy or lagging artifact rows are ignored.")

    out1, out2, out3 = st.columns(3)
    with out1:
        _render_outputs_block("Raw outputs", visible_raw_outputs, "No raw outputs match the selected artifact filter for this job.")
    with out2:
        _render_outputs_block("Zarr outputs", visible_zarr_outputs, "No Zarr outputs match the selected artifact filter for this job.")
    with out3:
        _render_outputs_block("Masked Zarr outputs", visible_masked_zarr_outputs, "No masked Zarr outputs match the selected artifact filter for this job.")

    if artifact_type_filter in {None, "", "zarr_masked"}:
        _render_masked_zarr_relations(masked_artifacts)

    _render_mask_quality_block(
        pipeline_metadata=pipeline_metadata,
        conversion_metadata=conversion_metadata,
    )

    if pipeline_metadata or conversion_metadata:
        with st.expander("Pipeline metadata", expanded=False):
            st.code(
                json.dumps(
                    {
                        "pipeline_metadata": pipeline_metadata,
                        "conversion_metadata": conversion_metadata,
                    },
                    indent=2,
                ),
                language="json",
            )


def _render_files_section(downloads_dir: Path) -> None:
    downloads_dir.mkdir(exist_ok=True, parents=True)
    n_files, total_mb = count_downloaded_products()
    st.markdown(
        f"""<div style='display:flex;gap:8px;margin:6px 0;'>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
            <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#e2e8f0;font-weight:700;'>{n_files}</div>
            <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;'>Files</div></div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
            <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#2dd4bf;font-weight:700;'>{total_mb:.1f} MB</div>
            <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;'>Total Size</div></div>
    </div>""",
        unsafe_allow_html=True,
    )

    with st.expander("Advanced file browser", expanded=False):
        use_comp = st.toggle(
            "Enable experimental browser",
            value=bool(st.session_state.get("use_file_browser_component", False)),
            help="Uses streamlit_file_browser. If you see missing *.map asset errors, disable this.",
        )
        st.session_state["use_file_browser_component"] = use_comp
        if use_comp and st_file_browser is not None:
            try:
                st_file_browser(
                    str(downloads_dir),
                    key="fb",
                    show_choose_file=True,
                    show_download_file=True,
                    show_delete_file=True,
                    show_new_folder=True,
                    show_upload_file=True,
                    show_rename_file=True,
                    show_rename_folder=True,
                    use_cache=True,
                )
            except Exception as exc:
                st.warning(f"File browser component failed: {exc}. Falling back.")
                use_comp = False
        elif use_comp and st_file_browser is None:
            st.info("streamlit_file_browser is not available in this runtime.")
        else:
            st.caption("Disabled by default to keep the Results tab light.")

    if use_comp and st_file_browser is not None:
        return

    files = [entry for entry in downloads_dir.rglob("*") if entry.is_file()]
    if not files:
        st.info("No files yet.")
        return

    rows = []
    for file_path in sorted(files, key=lambda item: item.stat().st_mtime, reverse=True):
        stat = file_path.stat()
        rows.append(
            {
                "path": str(file_path.relative_to(downloads_dir)),
                "size_MB": round(stat.st_size / (1024 * 1024), 3),
                "modified": dt.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    file_query = st.text_input("File search", value="", placeholder="path fragment")
    if file_query.strip():
        needle = file_query.strip().lower()
        rows = [row for row in rows if needle in row["path"].lower()]

    st.dataframe(rows[:100], width="stretch", hide_index=True)
    selected_path = st.selectbox("Select a file", options=[row["path"] for row in rows], index=0)
    local_path = downloads_dir / selected_path
    if local_path.exists() and local_path.is_file():
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        st.caption(f"Selected path: `{local_path}`")
        if file_size_mb <= 50:
            with local_path.open("rb") as handle:
                st.download_button(
                    "⬇️ Download selected",
                    data=handle.read(),
                    file_name=local_path.name,
                    mime="application/octet-stream",
                    width="stretch",
                )
        else:
            st.info(
                f"Browser download is disabled for large files ({file_size_mb:.1f} MB) to keep Streamlit responsive. "
                "Use the local path above."
            )


def render_results_tab(*, api_url: str, api_key: str) -> None:
    st.markdown(
        '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📂</span><span style="font-weight:600;font-size:.94rem;">Results</span></div>',
        unsafe_allow_html=True,
    )

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1, 1.2])
    with filter_col1:
        state_filter = st.selectbox(
            "State",
            ["", "queued", "running", "succeeded", "failed", "cancel_requested", "cancelled"],
            index=0,
        )
    with filter_col2:
        provider_filter = st.selectbox("Provider", ["", "copernicus", "usgs"], index=0)
    with filter_col3:
        artifact_type_filter = st.selectbox(
            "Artifact type",
            ["", "raw", "zarr", "zarr_masked"],
            index=0,
        )
    with filter_col4:
        result_job_query = st.text_input("Job search", value="", placeholder="job id fragment")

    jobs_rows, jobs_total = _list_jobs(
        api_url,
        api_key,
        state=state_filter or None,
        provider=provider_filter or None,
        date_from=_recent_jobs_cutoff(RECENT_JOBS_WINDOW_HOURS),
        page=1,
        page_size=RECENT_JOBS_FETCH_LIMIT,
    )
    jobs_rows = _filter_recent_job_rows(
        jobs_rows,
        hours=RECENT_JOBS_WINDOW_HOURS,
        limit=RECENT_JOBS_FETCH_LIMIT,
    )

    if result_job_query.strip():
        needle = result_job_query.strip().lower()
        jobs_rows = [
            row
            for row in jobs_rows
            if needle in str(row.get("job_id", "")).lower()
            or needle in str(row.get("scene_id", "")).lower()
            or needle in str(row.get("product_type", "")).lower()
        ]

    st.caption(
        f"Showing recent jobs only (last {RECENT_JOBS_WINDOW_HOURS}h + active). "
        f"{len(jobs_rows)} / {jobs_total} rows."
    )
    if jobs_rows:
        _render_job_summary_cards(jobs_rows)
        _render_jobs_table(jobs_rows)
        _render_pipeline_result_section(
            api_url,
            api_key,
            jobs_rows,
            artifact_type_filter=artifact_type_filter or None,
        )
        _render_job_details(jobs_rows)
    else:
        st.info("No jobs for selected filters.")

    st.markdown("---")
    _render_files_section(DOWNLOADS_DIR)
