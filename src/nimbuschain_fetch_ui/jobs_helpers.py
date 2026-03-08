import datetime as dt
import time
from typing import Any, Dict, List, Tuple

import requests
import streamlit as st
from requests.adapters import HTTPAdapter

from nimbuschain_fetch_ui.job_api_runtime import parse_sse_lines


def _api_headers(api_key: str) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key.strip():
        headers["X-API-Key"] = api_key.strip()
    return headers


@st.cache_resource(show_spinner=False)
def _http_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _api_request(
    method: str,
    api_url: str,
    path: str,
    *,
    api_key: str,
    payload: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
    timeout: int = 60,
) -> requests.Response:
    return _http_session().request(
        method=method,
        url=f"{api_url.rstrip('/')}{path}",
        headers=_api_headers(api_key),
        json=payload,
        params=params,
        timeout=timeout,
    )


def _parse_event_stream(lines: List[str]) -> Tuple[List[Dict[str, Any]], int]:
    return parse_sse_lines(lines)


def _drain_sse_events(
    api_url: str,
    api_key: str,
    since_id: int,
    *,
    read_timeout_seconds: float = 0.35,
    max_events: int = 150,
) -> Tuple[List[Dict[str, Any]], int, str]:
    params: Dict[str, Any] = {}
    if since_id > 0:
        params["since"] = since_id
    captured: List[str] = []
    try:
        with _http_session().get(
            f"{api_url.rstrip('/')}/v1/events",
            headers=_api_headers(api_key),
            params=params,
            timeout=(3, 3),
            stream=True,
        ) as response:
            if not response.ok:
                return [], since_id, f"SSE {response.status_code}: {response.text[:120]}"
            deadline = time.time() + read_timeout_seconds
            for line in response.iter_lines(decode_unicode=True):
                if line is None:
                    continue
                captured.append(line)
                if len(captured) >= max_events * 3:
                    break
                if time.time() > deadline:
                    break
    except Exception as exc:
        return [], since_id, str(exc)

    events, max_id = _parse_event_stream(captured)
    next_since = max(since_id, max_id)
    return events[:max_events], next_since, ""


def _refresh_job_statuses(api_url: str, api_key: str, job_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for job_id in job_ids:
        try:
            response = _api_request("GET", api_url, f"/v1/jobs/{job_id}", api_key=api_key, timeout=30)
            if response.ok:
                out[job_id] = response.json()
            else:
                out[job_id] = {"job_id": job_id, "state": "unknown", "errors": [response.text]}
        except Exception as exc:
            out[job_id] = {"job_id": job_id, "state": "unknown", "errors": [str(exc)]}
    return out


def _refresh_job_results(
    api_url: str,
    api_key: str,
    job_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for job_id in job_ids:
        try:
            response = _api_request("GET", api_url, f"/v1/jobs/{job_id}/result", api_key=api_key, timeout=30)
            if response.ok:
                out[job_id] = response.json()
        except Exception:
            continue
    return out


def _list_jobs(
    api_url: str,
    api_key: str,
    *,
    state: str | None = None,
    state_in: str | None = None,
    provider: str | None = None,
    collection: str | None = None,
    product_type: str | None = None,
    job_id_query: str | None = None,
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
    updated_from: dt.datetime | None = None,
    updated_to: dt.datetime | None = None,
    sort_by: str = "updated_at",
    sort_desc: bool = True,
    page: int = 1,
    page_size: int = 50,
) -> Tuple[List[Dict[str, Any]], int]:
    params: Dict[str, Any] = {"page": page, "page_size": page_size}
    if state:
        params["state"] = state
    if state_in:
        params["state_in"] = state_in
    if provider:
        params["provider"] = provider
    if collection:
        params["collection"] = collection
    if product_type:
        params["product_type"] = product_type
    if job_id_query:
        params["job_id_query"] = job_id_query
    if date_from:
        params["date_from"] = date_from.astimezone(dt.timezone.utc).isoformat()
    if date_to:
        params["date_to"] = date_to.astimezone(dt.timezone.utc).isoformat()
    if updated_from:
        params["updated_from"] = updated_from.astimezone(dt.timezone.utc).isoformat()
    if updated_to:
        params["updated_to"] = updated_to.astimezone(dt.timezone.utc).isoformat()
    params["sort_by"] = sort_by
    params["sort_desc"] = sort_desc
    try:
        response = _api_request("GET", api_url, "/v1/jobs", api_key=api_key, params=params, timeout=30)
        if not response.ok:
            return [], 0
        body = response.json()
        return list(body.get("items", [])), int(body.get("total", 0) or 0)
    except Exception:
        return [], 0


def _recent_jobs_cutoff(hours: int) -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max(1, hours))


def _fetch_recent_provider_jobs(api_url: str, api_key: str, *, provider: str | None, hours: int, limit: int) -> List[Dict[str, Any]]:
    rows, _total = _list_jobs(
        api_url,
        api_key,
        provider=provider,
        date_from=_recent_jobs_cutoff(hours),
        page=1,
        page_size=limit,
    )
    return rows


def _parse_iso_datetime(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _status_reference_time(row: Dict[str, Any]) -> dt.datetime | None:
    for key in ("finished_at", "started_at", "updated_at", "created_at", "accepted_at"):
        parsed = _parse_iso_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _is_recent_status(row: Dict[str, Any], now_utc: dt.datetime, hours: int) -> bool:
    state = str(row.get("state", "")).lower()
    if state in {"queued", "running", "cancel_requested"}:
        return True
    ref = _status_reference_time(row)
    if ref is None:
        return False
    return (now_utc - ref) <= dt.timedelta(hours=max(1, hours))


def _filter_recent_job_rows(rows: List[Dict[str, Any]], *, hours: int, limit: int) -> List[Dict[str, Any]]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    filtered = [row for row in rows if _is_recent_status(row, now_utc, hours)]

    def _sort_key(row: Dict[str, Any]) -> Tuple[int, float]:
        state = str(row.get("state", "")).lower()
        ref = _status_reference_time(row) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
        active_rank = 0 if state in {"queued", "running", "cancel_requested"} else 1
        return (active_rank, -ref.timestamp())

    filtered.sort(key=_sort_key)
    return filtered[: max(1, limit)]


def _job_matches_view(row: Dict[str, Any], view: str, *, now_utc: dt.datetime, recent_minutes: int, active_states: set[str]) -> bool:
    state = str(row.get("state", "")).lower()
    if view == "all":
        return True
    if view == "active":
        return state in active_states
    if view == "succeeded":
        return state == "succeeded"
    if view == "failed":
        return state == "failed"
    if view == "cancelled":
        return state == "cancelled"
    if view == "recent":
        ref = _status_reference_time(row)
        return ref is not None and (now_utc - ref) <= dt.timedelta(minutes=max(1, recent_minutes))
    return True


def _job_view_counts(rows: List[Dict[str, Any]], *, recent_minutes: int, active_states: set[str]) -> Dict[str, int]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    return {
        "all": len(rows),
        "active": len([row for row in rows if _job_matches_view(row, "active", now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]),
        "succeeded": len([row for row in rows if _job_matches_view(row, "succeeded", now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]),
        "failed": len([row for row in rows if _job_matches_view(row, "failed", now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]),
        "cancelled": len([row for row in rows if _job_matches_view(row, "cancelled", now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]),
        "recent": len([row for row in rows if _job_matches_view(row, "recent", now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]),
    }


def _filter_jobs_by_view(rows: List[Dict[str, Any]], view: str, *, recent_minutes: int, active_states: set[str]) -> List[Dict[str, Any]]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    return [row for row in rows if _job_matches_view(row, view, now_utc=now_utc, recent_minutes=recent_minutes, active_states=active_states)]


def _provider_scope_value(scope: str, current_provider: str | None) -> str | None:
    if scope == "current":
        return current_provider
    if scope == "all":
        return None
    return scope or None


def _job_matches_scope_filters(
    row: Dict[str, Any],
    *,
    provider: str | None,
    collection_query: str,
    product_query: str,
    job_query: str,
) -> bool:
    row_provider = str(row.get("provider", "")).strip().lower()
    row_collection = str(row.get("collection", "")).strip().lower()
    row_product = str(row.get("product_type", "")).strip().lower()
    row_job_id = str(row.get("job_id", "")).strip().lower()

    if provider and row_provider != provider.lower():
        return False
    if collection_query and collection_query.lower() not in row_collection:
        return False
    if product_query and product_query.lower() not in row_product:
        return False
    if job_query and job_query.lower() not in row_job_id:
        return False
    return True


def _filter_jobs_by_scope(
    rows: List[Dict[str, Any]],
    *,
    provider: str | None,
    collection_query: str,
    product_query: str,
    job_query: str,
) -> List[Dict[str, Any]]:
    return [
        row
        for row in rows
        if _job_matches_scope_filters(
            row,
            provider=provider,
            collection_query=collection_query,
            product_query=product_query,
            job_query=job_query,
        )
    ]


def _merge_job_rows(*groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for rows in groups:
        for row in rows:
            job_id = str(row.get("job_id", "")).strip()
            if not job_id:
                continue
            current = merged.get(job_id)
            if current is None:
                merged[job_id] = row
                continue
            current_ref = _status_reference_time(current) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
            row_ref = _status_reference_time(row) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
            if row_ref >= current_ref:
                merged[job_id] = row
    return sorted(
        merged.values(),
        key=lambda row: (_status_reference_time(row) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)).timestamp(),
        reverse=True,
    )
