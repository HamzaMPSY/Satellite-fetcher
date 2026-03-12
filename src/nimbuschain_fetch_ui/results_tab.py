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
    _filter_recent_job_rows,
    _list_jobs,
    _recent_jobs_cutoff,
)


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

    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 1.4])
    with filter_col1:
        state_filter = st.selectbox(
            "State",
            ["", "queued", "running", "succeeded", "failed", "cancel_requested", "cancelled"],
            index=0,
        )
    with filter_col2:
        provider_filter = st.selectbox("Provider", ["", "copernicus", "usgs"], index=0)
    with filter_col3:
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
        _render_job_details(jobs_rows)
    else:
        st.info("No jobs for selected filters.")

    st.markdown("---")
    _render_files_section(DOWNLOADS_DIR)
