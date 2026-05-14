from __future__ import annotations

import datetime as dt
from typing import Any

import streamlit as st

from nimbuschain_fetch_ui.jobs_helpers import _api_headers, _http_session, _parse_iso_datetime


COORDINATOR_TASK_STATUSES = (
    "queued",
    "preparing",
    "ready",
    "downloading",
    "done",
    "failed",
    "cancelled",
)


def fetch_status_json(
    *,
    base_url: str,
    path: str,
    api_key: str = "",
    timeout: int = 15,
) -> dict[str, Any]:
    try:
        response = _http_session().get(
            f"{base_url.rstrip('/')}{path}",
            headers=_api_headers(api_key),
            timeout=timeout,
        )
    except Exception as exc:
        return {
            "_ok": False,
            "_status_code": None,
            "_error": str(exc),
            "status": "unreachable",
            "ready": False,
        }

    try:
        payload = response.json()
    except Exception:
        payload = {"raw_body": response.text[:500]}
    payload["_ok"] = bool(response.ok)
    payload["_status_code"] = int(response.status_code)
    if not response.ok and "_error" not in payload:
        payload["_error"] = payload.get("detail") or response.text[:200]
    return payload


def refresh_api_runtime_statuses(*, api_url: str, api_key: str) -> dict[str, Any]:
    return {
        "api_health_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/health",
            api_key=api_key,
        ),
        "api_readiness_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/readiness",
            api_key=api_key,
        ),
        "worker_status_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/worker/status",
            api_key=api_key,
        ),
        "download_coordinator_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/worker/download-coordinator",
            api_key=api_key,
        ),
        "provider_status_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/providers/status",
            api_key=api_key,
        ),
        "service_status_checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_api_status_url": api_url,
    }


def refresh_zarr_runtime_statuses(*, api_url: str, api_key: str) -> dict[str, Any]:
    return {
        "zarr_health_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/converter/health",
            api_key=api_key,
        ),
        "zarr_readiness_snapshot": fetch_status_json(
            base_url=api_url,
            path="/v1/converter/readiness",
            api_key=api_key,
        ),
        "zarr_status_checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_zarr_status_url": api_url,
    }


def format_status_timestamp(value: str | None) -> str:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return "-"
    return parsed.astimezone().strftime("%H:%M:%S")


def _format_bytes(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if number < 0:
        return "-"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit_index = 0
    while number >= 1024.0 and unit_index < len(units) - 1:
        number /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(number)} {units[unit_index]}"
    return f"{number:.1f} {units[unit_index]}"


def _format_bps(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if number <= 0:
        return "-"
    return f"{_format_bytes(number)}/s"


def _coordinator_summary(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    summary = snapshot.get("summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _coordinator_status_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    providers = dict(summary.get("providers") or {})
    if not providers:
        return []

    provider_names: list[str] = []
    for provider_name in ("copernicus", "usgs"):
        if provider_name in providers:
            provider_names.append(provider_name)
    for provider_name in providers:
        if provider_name not in provider_names:
            provider_names.append(str(provider_name))

    rows: list[dict[str, Any]] = []
    totals = {status_name: 0 for status_name in COORDINATOR_TASK_STATUSES}
    for provider_name in provider_names:
        provider_payload = dict(providers.get(provider_name) or {})
        counts = dict(provider_payload.get("counts") or {})
        row: dict[str, Any] = {"provider": str(provider_name).upper()}
        total_for_provider = 0
        for status_name in COORDINATOR_TASK_STATUSES:
            count_value = int(counts.get(status_name, 0) or 0)
            row[status_name] = count_value
            totals[status_name] += count_value
            total_for_provider += count_value
        row["total"] = total_for_provider
        rows.append(row)

    if len(rows) > 1:
        total_row: dict[str, Any] = {"provider": "TOTAL"}
        total_row_total = 0
        for status_name in COORDINATOR_TASK_STATUSES:
            total_row[status_name] = totals[status_name]
            total_row_total += totals[status_name]
        total_row["total"] = total_row_total
        rows.append(total_row)

    return rows


def status_card_payload(snapshot: Any, *, kind: str) -> tuple[str, str, str]:
    if not isinstance(snapshot, dict) or not snapshot:
        return ("unknown", "No check yet", "#64748b")
    if snapshot.get("_error"):
        return ("error", str(snapshot.get("_error")), "#ef4444")

    if kind == "worker":
        alive = int(snapshot.get("workers_alive", 0) or 0)
        available = int(snapshot.get("capacity_available", 0) or 0)
        total = int(snapshot.get("capacity_total", 0) or 0)
        backlog = int(snapshot.get("queued_jobs", 0) or 0)
        if alive <= 0:
            return ("offline", f"0 alive · backlog {backlog}", "#ef4444")
        if available > 0:
            return ("ready", f"{alive} alive · {available}/{total} free", "#22c55e")
        return ("saturated", f"{alive} alive · 0/{total} free · backlog {backlog}", "#f59e0b")

    if kind == "coordinator":
        summary = _coordinator_summary(snapshot)
        if not summary:
            return ("unavailable", str(snapshot.get("_error") or "Coordinator snapshot unavailable"), "#64748b")
        machine = dict(summary.get("machine") or {})
        jobs = dict(summary.get("jobs") or {})
        active_downloads = int(machine.get("active_downloads", 0) or 0)
        pending_tasks = int(jobs.get("pending_tasks_total", 0) or 0)
        workers_reporting = int(snapshot.get("workers_reporting", 0) or 0)
        status_value = str(summary.get("status") or snapshot.get("status") or "unknown").strip().lower()
        if status_value == "unavailable" and workers_reporting <= 0:
            return ("unavailable", "No worker coordinator report yet", "#64748b")
        if active_downloads > 0 or pending_tasks > 0:
            return (
                "active",
                f"{active_downloads} active · {pending_tasks} pending · {workers_reporting} worker",
                "#22c55e" if active_downloads > 0 else "#f59e0b",
            )
        if status_value in {"not_initialized", "not_started"}:
            return ("idle", f"{workers_reporting} worker · coordinator not started yet", "#64748b")
        return ("idle", f"{workers_reporting} worker report · no active downloads", "#22c55e")

    ready = bool(snapshot.get("ready", False))
    status = str(snapshot.get("status", "unknown"))
    if bool(snapshot.get("_ok")) and status.lower() in {"ok", "healthy"} and not ("ready" in snapshot):
        return (status, "Service reachable", "#22c55e")
    failures = list(snapshot.get("critical_failures", []) or [])
    if ready:
        return (status, "No critical failures", "#22c55e")
    if failures:
        return (status, ", ".join(failures[:3]), "#ef4444")
    return (status, str(snapshot.get("_error") or "Not ready"), "#f59e0b")


def render_status_block(title: str, snapshot: Any, *, kind: str = "service") -> None:
    state, detail, color = status_card_payload(snapshot, kind=kind)
    status_code = snapshot.get("_status_code", "-") if isinstance(snapshot, dict) else "-"
    st.markdown(
        f"""
        <div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;margin-top:8px;">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
            <span style="font-size:.78rem;color:#94a3b8;font-weight:600;">{title}</span>
            <span style="font-size:.72rem;color:{color};font-weight:700;text-transform:uppercase;">{state}</span>
          </div>
          <div style="font-size:.72rem;color:#cbd5e1;margin-top:6px;">{detail}</div>
          <div style="font-size:.65rem;color:#64748b;margin-top:4px;">HTTP: {status_code}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_download_coordinator_dashboard(snapshot: Any) -> None:
    if not isinstance(snapshot, dict) or not snapshot:
        st.caption("Download coordinator snapshot unavailable.")
        return
    if snapshot.get("_error"):
        st.warning(str(snapshot.get("_error")))
        return

    summary = _coordinator_summary(snapshot)
    if not summary:
        st.caption("Download coordinator snapshot unavailable.")
        return

    machine = dict(summary.get("machine") or {})
    jobs = dict(summary.get("jobs") or {})
    providers = dict(summary.get("providers") or {})
    workers = list(snapshot.get("workers") or [])
    coordinator_source = str(snapshot.get("source") or "unknown").replace("_", " ")
    st.caption(
        f"Source: {coordinator_source} · worker reports: {len(workers)} · checked: {format_status_timestamp(snapshot.get('timestamp'))}"
    )
    pending_by_job = list(jobs.get("pending_by_job") or [])
    active_tasks = list(dict(summary.get("tasks") or {}).get("active") or [])
    recent_terminal = list(dict(summary.get("tasks") or {}).get("recent_terminal") or [])
    copernicus = dict(providers.get("copernicus") or {})
    usgs = dict(providers.get("usgs") or {})

    overview_tab, jobs_tab, copernicus_tab, usgs_tab, tasks_tab, workers_tab = st.tabs(
        ["Overview", "Jobs", "Copernicus", "USGS", "Tasks", "Workers"]
    )

    with overview_tab:
        metric_cols = st.columns(4)
        metric_cols[0].metric(
            "Active downloads",
            f"{int(machine.get('active_downloads', 0) or 0)}/{int(machine.get('active_download_limit', 0) or 0)}",
        )
        metric_cols[1].metric("Pending tasks", int(jobs.get("pending_tasks_total", 0) or 0))
        metric_cols[2].metric("Free disk", _format_bytes(machine.get("disk_free_bytes")))
        metric_cols[3].metric("Bandwidth cap", _format_bps(machine.get("bandwidth_limit_bps")))
        st.caption(
            f"Disk guard: min free {_format_bytes(machine.get('min_free_bytes'))} on {str(machine.get('disk_path') or '-')}"
        )

        status_rows = _coordinator_status_rows(summary)
        if status_rows:
            st.markdown("**Files by Status**")
            st.dataframe(status_rows, width="stretch", hide_index=True)
        else:
            st.caption("No coordinator task status rows yet.")

    with jobs_tab:
        if pending_by_job:
            st.dataframe(
                [
                    {
                        "provider": str(item.get("provider") or "").upper(),
                        "job_id": str(item.get("job_id") or ""),
                        "pending": int(item.get("pending_tasks", 0) or 0),
                        "queued": int(item.get("queued", 0) or 0),
                        "preparing": int(item.get("preparing", 0) or 0),
                        "ready": int(item.get("ready", 0) or 0),
                        "downloading": int(item.get("downloading", 0) or 0),
                        "updated": format_status_timestamp(item.get("updated_at")),
                    }
                    for item in pending_by_job
                ],
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("No jobs are waiting in the coordinator queue.")

    with copernicus_tab:
        st.caption(
            " · ".join(
                [
                    f"active {int(copernicus.get('active_downloads', 0) or 0)}",
                    f"pending {int(copernicus.get('pending_tasks', 0) or 0)}",
                    f"limits j/c/d {int(copernicus.get('job_limit', 0) or 0)}/{int(copernicus.get('control_plane_limit', 0) or 0)}/{int(copernicus.get('data_plane_limit', 0) or 0)}",
                ]
            )
        )
        account_rows = [
            {
                "account": str(item.get("account_label") or "").strip() or "primary",
                "active": int(item.get("active_downloads", 0) or 0),
                "cooldown_s": float(item.get("cooldown_seconds", 0.0) or 0.0),
                "max": int(item.get("max_concurrent_downloads", 0) or 0),
            }
            for item in list(copernicus.get("accounts") or [])
        ]
        if account_rows:
            st.dataframe(account_rows, width="stretch", hide_index=True)
        else:
            st.caption("No Copernicus account worker state yet.")

    with usgs_tab:
        st.caption(
            " · ".join(
                [
                    f"prepare {int(usgs.get('active_prepares', 0) or 0)}",
                    f"download {int(usgs.get('active_downloads', 0) or 0)}",
                    f"window {int(usgs.get('adaptive_window_current', 0) or 0)}/{int(usgs.get('adaptive_window_max', 0) or 0)}",
                    f"cooldown {float(usgs.get('cooldown_seconds', 0.0) or 0.0):.1f}s",
                ]
            )
        )
        st.dataframe(
            [
                {
                    "pending": int(usgs.get("pending_tasks", 0) or 0),
                    "ready": int(dict(usgs.get("counts") or {}).get("ready", 0) or 0),
                    "preparing": int(dict(usgs.get("counts") or {}).get("preparing", 0) or 0),
                    "peak_window": int(usgs.get("adaptive_window_peak", 0) or 0),
                    "success_streak": int(usgs.get("success_streak", 0) or 0),
                }
            ],
            width="stretch",
            hide_index=True,
        )

    with tasks_tab:
        if active_tasks:
            st.markdown("**Active / Pending Tasks**")
            st.dataframe(
                [
                    {
                        "provider": str(item.get("provider") or "").upper(),
                        "job_id": str(item.get("job_id") or ""),
                        "product_id": str(item.get("product_id") or ""),
                        "status": str(item.get("status") or "").lower(),
                        "account": str(item.get("account_label") or "-") or "-",
                        "attempts": int(item.get("attempts", 0) or 0),
                        "progress": (
                            f"{(100.0 * float(item.get('bytes_downloaded', 0) or 0) / float(item.get('bytes_total', 0) or 1)):.0f}%"
                            if int(item.get("bytes_total", 0) or 0) > 0
                            else "-"
                        ),
                        "retry_at": format_status_timestamp(item.get("retry_after")),
                        "updated": format_status_timestamp(item.get("updated_at")),
                    }
                    for item in active_tasks
                ],
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("No active coordinator tasks right now.")

        if recent_terminal:
            st.markdown("**Recent Terminal Tasks**")
            st.dataframe(
                [
                    {
                        "provider": str(item.get("provider") or "").upper(),
                        "job_id": str(item.get("job_id") or ""),
                        "product_id": str(item.get("product_id") or ""),
                        "status": str(item.get("status") or "").lower(),
                        "account": str(item.get("account_label") or "-") or "-",
                        "error": str(item.get("error_text") or ""),
                        "finished": format_status_timestamp(item.get("finished_at")),
                    }
                    for item in recent_terminal
                ],
                width="stretch",
                hide_index=True,
            )

    with workers_tab:
        if workers:
            st.dataframe(
                [
                    {
                        "worker_id": str(item.get("worker_id") or ""),
                        "hostname": str(item.get("hostname") or ""),
                        "runtime_role": str(item.get("runtime_role") or ""),
                        "last_seen": format_status_timestamp(item.get("last_seen_at")),
                        "status": str(dict(item.get("snapshot") or {}).get("status") or ""),
                        "active_downloads": int(
                            dict(dict(item.get("snapshot") or {}).get("machine") or {}).get("active_downloads", 0) or 0
                        ),
                        "pending_tasks": int(
                            dict(dict(item.get("snapshot") or {}).get("jobs") or {}).get("pending_tasks_total", 0) or 0
                        ),
                    }
                    for item in workers
                ],
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("No worker reports yet.")
