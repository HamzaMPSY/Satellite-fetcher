from __future__ import annotations

import datetime as dt
from typing import Any

import streamlit as st

from nimbuschain_fetch_ui.jobs_helpers import _api_headers, _http_session, _parse_iso_datetime


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
        "service_status_checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_api_status_url": api_url,
    }


def refresh_zarr_runtime_statuses(*, zarr_url: str) -> dict[str, Any]:
    return {
        "zarr_health_snapshot": fetch_status_json(
            base_url=zarr_url,
            path="/health",
        ),
        "zarr_readiness_snapshot": fetch_status_json(
            base_url=zarr_url,
            path="/readiness",
        ),
        "zarr_status_checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_zarr_status_url": zarr_url,
    }


def format_status_timestamp(value: str | None) -> str:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return "-"
    return parsed.astimezone().strftime("%H:%M:%S")


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
