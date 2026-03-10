
import os
import re
import sys
import math
import json
import time
import subprocess
import signal
import datetime as dt
import uuid
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

# Ensure repo src/ is on sys.path before importing package modules (helps when PYTHONPATH is missing)
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import geopandas as gpd
import shapely
import streamlit as st
from dataclasses import dataclass
from shapely.geometry import Polygon, box

try:
    from streamlit_file_browser import st_file_browser
except ImportError:
    st_file_browser = None

try:
    from utilities import ConfigLoader  # type: ignore
    _original_get_var = ConfigLoader.get_var

    def _patched_get_var(self, key, default=None):
        try:
            return _original_get_var(self, key, default=default)
        except Exception:
            return default

    ConfigLoader.get_var = _patched_get_var
except Exception:
    ConfigLoader = None

from nimbuschain_fetch_ui.aoi_utils import parse_aoi_text
from nimbuschain_fetch_ui.constants import (
    SRC_ROOT,
    PROJECT_ROOT,
    DOWNLOADS_DIR,
    ZARR_STORES_DIR,
    NOHUP_PATH,
    PID_PATH,
    DEFAULT_API_URL,
    DEFAULT_API_KEY,
    DEFAULT_ZARR_URL,
    RECENT_JOBS_WINDOW_HOURS,
    RECENT_JOB_CATEGORY_MINUTES,
    RECENT_JOBS_LIMIT,
    RECENT_JOBS_FETCH_LIMIT,
    JOB_MONITOR_REFRESH_EVERY,
    FINAL_JOB_STATES,
    ACTIVE_JOB_STATES,
    PATHS,
    MCFG,
    PROVIDERS,
    PRODUCT_TYPES,
    PROVIDER_CLI_MAP,
)
from nimbuschain_fetch_ui.component_leaflet import leaflet_map
from nimbuschain_fetch_ui.geo_utils import (
    ensure_4326,
    get_name_col,
    safe_union,
    parse_geometry,
    make_square_wkt,
    zoom_for_bounds,
    compute_intersections,
    find_tiles,
    _md5,
    selected_tiles_to_wkt,
    selected_tiles_to_geometry,
    mapping,
)
from nimbuschain_fetch_ui.job_api_runtime import (
    build_job_payload as build_job_payload_runtime,
    filter_active_job_ids,
    merge_status_rows as merge_status_rows_runtime,
    should_poll_fallback,
    summarize_statuses,
)
from nimbuschain_fetch_ui.jobs_helpers import (
    _api_headers,
    _http_session,
    _api_request,
    _parse_event_stream,
    _drain_sse_events,
    _refresh_job_statuses,
    _refresh_job_results,
    _list_jobs,
    _recent_jobs_cutoff,
    _fetch_recent_provider_jobs,
    _parse_iso_datetime,
    _status_reference_time,
    _is_recent_status,
    _filter_recent_job_rows,
    _job_matches_view,
    _job_view_counts,
    _filter_jobs_by_view,
    _provider_scope_value,
    _job_matches_scope_filters,
    _filter_jobs_by_scope,
    _merge_job_rows,
)
from nimbuschain_fetch_ui.preview_local import preview_products_local
from nimbuschain_fetch_ui.styling import CUSTOM_CSS
from nimbuschain_fetch_ui.logging_setup import configure_logging, logger
from nimbuschain_fetch_ui.data_loaders import (
    load_tiles,
    prepare_compact_grid,
    prepare_nocov_geojson,
)
from nimbuschain_fetch_ui.downloads import (
    reset_downloads,
    count_downloaded_products,
    parse_download_logs,
    _auto_parallel_strategy,
    _build_download_command,
    _bootstrap_download_runtime,
    _unlock_download_runtime,
    _read_pid,
    _pid_is_running,
)
from nimbuschain_fetch_ui.zarr_utils import (
    recent_source_candidates as _recent_source_candidates,
    available_zarr_stores as _available_zarr_stores,
    register_zarr_artifact as _register_zarr_artifact,
    zarr_service_schema as _zarr_service_schema,
    guess_scene_id as _guess_scene_id,
    guess_zarr_provider as _guess_zarr_provider,
    guess_zarr_collection as _guess_zarr_collection,
    guess_zarr_product_type as _guess_zarr_product_type,
    guess_raw_source_format as _guess_raw_source_format,
    zarr_supported_collections as _zarr_supported_collections,
    zarr_supported_product_types as _zarr_supported_product_types,
    default_zarr_output as _default_zarr_output,
    container_to_host_path_hint as _container_to_host_path_hint,
    human_size as _human_size,
    list_artifacts as _list_artifacts,
    artifact_visibility_status as _artifact_visibility_status,
    filter_visible_artifacts as _filter_visible_artifacts,
)

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


configure_logging(PROJECT_ROOT / "app_debug.log")
logger.info("=" * 60)
logger.info("Satellite Downloader v2 — app starting")
logger.info(f"PROJECT_ROOT : {PROJECT_ROOT}")
logger.info(f"DOWNLOADS_DIR: {DOWNLOADS_DIR}")
logger.info(f"NOHUP_PATH   : {NOHUP_PATH}")
logger.info(f"PID_PATH     : {PID_PATH}")
logger.info(f"Python       : {sys.executable}")
logger.info("=" * 60)







@st.cache_data(show_spinner="Previewing products for this AOI…", ttl=180)
def preview_products_cached(
    provider: str,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    max_items: int = 50,
    tile_ids: List[str] | None = None,
) -> Dict[str, Any]:
    return preview_products_local(
        provider=provider,
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi_wkt=aoi_wkt,
        max_items=max_items,
        tile_ids=tile_ids,
    )


@st.cache_data(show_spinner=False)
def prepare_tile_helpers(_gdf_id: str, ncol: str, system: str) -> Tuple[List[str], Dict[str, Tuple[float, float]]]:
    """Precompute tile names + representative points for O(1) lookup in UI events."""
    gdf = st.session_state.get(f"_raw_gdf_{system}")
    if gdf is None or gdf.empty or not ncol:
        return [], {}

    names = gdf[ncol].astype(str).tolist()
    all_names = sorted(set(names))
    centroids: Dict[str, Tuple[float, float]] = {}
    for name, geom in zip(names, gdf.geometry):
        if name in centroids or geom is None or getattr(geom, "is_empty", True):
            continue
        try:
            rp = geom.representative_point()
            centroids[name] = (float(rp.y), float(rp.x))
        except Exception:
            continue
    return all_names, centroids




def render_download_progress():
    logs = parse_download_logs()
    n_files, total_mb = count_downloaded_products()
    phase = logs.get("phase", "starting")

    pid = st.session_state.get("dl_pid") or _read_pid()
    active_runtime = bool(st.session_state.get("dl_running")) and _pid_is_running(pid)
    if not active_runtime and phase in {"starting", "initializing", "ready", "searching", "found", "downloading"}:
        phase = "idle"
        logs["batch"] = None
        logs["files"] = {}

    if logs.get("products_found", 0) > 0:
        st.session_state["dl_total_products"] = logs["products_found"]
    total_products = st.session_state.get("dl_total_products", 0)

    phase_info = {
        "idle":         ("ℹ️", "No active download.",               "#94a3b8"),
        "starting":     ("🔄", "Starting download process…",        "#94a3b8"),
        "initializing": ("⚙️", "Loading configuration & AOI…",      "#38bdf8"),
        "ready":        ("🔗", "Connecting to provider…",            "#38bdf8"),
        "searching":    ("🔍", "Searching for products (please wait)…", "#fbbf24"),
        "found":        ("📦", f"Found {total_products} products — starting download…", "#2dd4bf"),
        "downloading":  ("⬇️", f"Downloading {total_products} products…", "#06d6a0"),
        "done":         ("✅", "Download completed!",                "#06d6a0"),
    }
    icon, msg, color = phase_info.get(phase, ("🔄", "Processing…", "#94a3b8"))
    st.markdown(f"""<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);
        border-radius:10px;padding:10px 14px;margin-bottom:10px;display:flex;align-items:center;gap:10px;'>
        <span style='font-size:1.2rem;'>{icon}</span>
        <span style='font-family:JetBrains Mono;font-size:.82rem;color:{color};font-weight:600;'>{msg}</span>
    </div>""", unsafe_allow_html=True)

    batch = logs.get("batch")
    if batch:
        done, pct = batch.get("done", 0), batch.get("pct", 0)
        batch_total = batch.get("total", total_products)
        st.session_state["dl_completed"] = done
        if batch_total > 0:
            st.session_state["dl_total_products"] = batch_total
            total_products = batch_total
        start_ts = st.session_state.get("dl_start_time")
        eta_str = "calculating…"
        if start_ts and done > 0:
            elapsed = time.time() - start_ts
            remaining = (elapsed / done) * (max(total_products, done) - done)
            eta_str = _format_eta(remaining)
        st.markdown(f"""<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;margin-bottom:8px;'>
            <div style='display:flex;justify-content:space-between;margin-bottom:4px;'>
                <span style='font-family:JetBrains Mono;font-size:.78rem;color:#e2e8f0;font-weight:600;'>Batch Progress</span>
                <span style='font-family:JetBrains Mono;font-size:.7rem;color:#fbbf24;'>ETA: {eta_str}</span>
            </div>
            <div style='height:6px;background:#1a2236;border-radius:3px;overflow:hidden;margin-bottom:4px;'>
                <div style='height:100%;width:{pct}%;background:linear-gradient(90deg,#38bdf8,#2dd4bf);border-radius:3px;'></div>
            </div>
            <div style='display:flex;justify-content:space-between;font-family:JetBrains Mono;font-size:.65rem;color:#64748b;'>
                <span>{done}/{total_products or "—"}</span><span>{pct}%</span>
            </div></div>""", unsafe_allow_html=True)

    for fname, info in logs.get("files", {}).items():
        short = fname if len(fname) < 40 else fname[:18] + "…" + fname[-18:]
        pct = info.get("pct", 0)
        st.markdown(f"""<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;margin-bottom:6px;'>
            <div style='display:flex;justify-content:space-between;margin-bottom:4px;'>
                <span style='font-family:JetBrains Mono;font-size:.75rem;color:#e2e8f0;font-weight:600;'>{short}</span>
                <span style='font-family:JetBrains Mono;font-size:.68rem;color:#fbbf24;'>ETA: {info.get("eta")}</span>
            </div>
            <div style='height:6px;background:#1a2236;border-radius:3px;overflow:hidden;margin-bottom:4px;'>
                <div style='height:100%;width:{pct}%;background:linear-gradient(90deg,#a78bfa,#fb7185);border-radius:3px;'></div>
            </div>
            <div style='display:flex;justify-content:space-between;font-family:JetBrains Mono;font-size:.65rem;color:#64748b;'>
                <span>{info.get("done")}/{info.get("total")}</span><span>{pct}%</span>
            </div></div>""", unsafe_allow_html=True)

    completed = st.session_state.get("dl_completed", 0)
    st.markdown(f"""<div style='display:flex;gap:8px;margin-top:6px;'>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono;color:#2dd4bf;font-weight:700;'>{total_products or "—"}</div>
            <div style='font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Found</div></div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono;color:#e2e8f0;font-weight:700;'>{completed}</div>
            <div style='font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Downloaded</div></div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono;color:#a78bfa;font-weight:700;'>{n_files}</div>
            <div style='font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Files</div></div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono;color:#fbbf24;font-weight:700;'>{total_mb:.1f} MB</div>
            <div style='font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Size</div></div>
    </div>""", unsafe_allow_html=True)

    log_size = 0
    log_exists = NOHUP_PATH.exists()
    if log_exists:
        try:
            log_size = NOHUP_PATH.stat().st_size
        except Exception:
            pass

    pid = st.session_state.get("dl_pid") or _read_pid()
    pid_running = _pid_is_running(pid)

    st.markdown(f"""<div style='background:#0d1117;border:1px solid rgba(56,120,200,0.08);
        border-radius:8px;padding:8px 12px;margin-top:8px;font-family:JetBrains Mono;font-size:.7rem;color:#64748b;'>
        <b>Debug</b> · log_exists={log_exists} · log_size={log_size}B · phase={phase} ·
        pid={pid} · pid_alive={pid_running} ·
        dl_running={_ss("dl_running")} · products_found={logs.get("products_found", 0)}
    </div>""", unsafe_allow_html=True)

    if logs.get("errors"):
        with st.expander(f"⚠️ Errors ({len(logs['errors'])})", expanded=True):
            for err in logs["errors"][-10:]:
                st.text(err)

    if logs.get("logs"):
        with st.expander("📜 Recent Logs", expanded=not batch):
            for line in logs["logs"][-15:]:
                st.text(line)

    if log_exists and log_size > 0:
        with st.expander("🔬 Raw Log Tail (last 2KB)", expanded=False):
            try:
                raw = NOHUP_PATH.read_text(errors="replace")
                tail = raw[-2000:] if len(raw) > 2000 else raw
                st.code(tail, language="text")
            except Exception as e:
                st.warning(f"Cannot read raw log: {e}")



def init_state():
    defaults = {
        "tile_system": "sentinel-2",
        "geometry_text": "",
        "intersecting_tiles": [],
        "selected_tiles": [],
        "start_date": dt.date.today() - dt.timedelta(days=7),
        "end_date": dt.date.today(),
        "map_center": list(MCFG.DEFAULT_CENTER),
        "map_zoom": MCFG.DEFAULT_ZOOM,
        "show_grid": True,
        "show_nocov": False,
        "show_inter": True,
        "show_sel": True,
        "colorize": True,
        "opacity": 0.04,
        "click_sel": True,
        "api_url": DEFAULT_API_URL,
        "api_key": DEFAULT_API_KEY,
        "zarr_service_url": DEFAULT_ZARR_URL,
        "provider": "Copernicus",
        "satellite": "SENTINEL-2",
        "product": "S2MSI2A",
        "usgs_satellite": "Any",
        "dl_auto_refresh": True,
        "dl_last_event_id": 0,
        "dl_last_sse_ok": 0.0,
        "dl_event_errors": 0,
        "active_job_ids": [],
        "known_job_ids": [],
        "job_status_cache": {},
        "job_result_cache": {},
        "job_event_log": [],
        "dl_job_view": "all",
        "dl_job_provider_filter": "current",
        "dl_job_collection_filter": "",
        "dl_job_product_filter": "",
        "dl_job_id_query": "",
        "zarr_history": [],
        "zarr_selected_store": "",
        "zarr_artifact_query": "",
        "zarr_artifact_provider": "",
        "zarr_artifact_collection": "",
        "zarr_chunk_time": 1,
        "zarr_chunk_y": 512,
        "zarr_chunk_x": 512,
        "zarr_clear_encodings": True,
        "zarr_append_mode": False,
        "zarr_output_base": "/data/downloads/zarr",
        "zarr_prefetch": True,
        "zarr_band_config_path": str(PROJECT_ROOT / "src" / "nimbuschain_zarr_service" / "converter" / "config" / "bands.yml"),
        "zarr_log_level": "info",
        "zarr_cache_remote": True,
        "zarr_cleanup_remote": True,
        "dl_auto_cfg": {},
        "preview_key": "",
        "preview_items": [],
        "preview_total": 0,
        "preview_error": "",
        "preview_fetched": False,
        "fly_to": None,
        "use_file_browser_component": False,
        "api_health_snapshot": None,
        "api_readiness_snapshot": None,
        "worker_status_snapshot": None,
        "service_status_checked_at": "",
        "last_api_status_url": "",
        "zarr_health_snapshot": None,
        "zarr_readiness_snapshot": None,
        "zarr_status_checked_at": "",
        "last_zarr_status_url": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    if st.session_state.get("dl_job_provider_filter") not in {"current", "all", "copernicus", "usgs"}:
        st.session_state["dl_job_provider_filter"] = "current"


def _ss(key, default=None):
    return st.session_state.get(key, default)


def _resolve_usgs_product_type(selected_product_type: str, selected_satellite: str) -> str:
    product = str(selected_product_type or "").strip().upper()
    if not product:
        return ""
    if product[:1] in {"8", "9"} and product[1:].startswith("L"):
        return product

    sat = str(selected_satellite or "").strip()
    if sat in {"08", "8"}:
        return f"8{product}"
    if sat in {"09", "9"}:
        return f"9{product}"
    return product


def _fetch_status_json(
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


def _refresh_api_runtime_statuses() -> None:
    api_url = str(_ss("api_url", DEFAULT_API_URL)).strip()
    api_key = str(_ss("api_key", DEFAULT_API_KEY)).strip()
    st.session_state["api_health_snapshot"] = _fetch_status_json(
        base_url=api_url,
        path="/v1/health",
        api_key=api_key,
    )
    st.session_state["api_readiness_snapshot"] = _fetch_status_json(
        base_url=api_url,
        path="/v1/readiness",
        api_key=api_key,
    )
    st.session_state["worker_status_snapshot"] = _fetch_status_json(
        base_url=api_url,
        path="/v1/worker/status",
        api_key=api_key,
    )
    st.session_state["service_status_checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    st.session_state["last_api_status_url"] = api_url


def _refresh_zarr_runtime_statuses() -> None:
    zarr_url = str(_ss("zarr_service_url", DEFAULT_ZARR_URL)).strip()
    st.session_state["zarr_health_snapshot"] = _fetch_status_json(
        base_url=zarr_url,
        path="/health",
    )
    st.session_state["zarr_readiness_snapshot"] = _fetch_status_json(
        base_url=zarr_url,
        path="/readiness",
    )
    st.session_state["zarr_status_checked_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    st.session_state["last_zarr_status_url"] = zarr_url


def _format_status_timestamp(value: str | None) -> str:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return "-"
    return parsed.astimezone().strftime("%H:%M:%S")


def _status_card_payload(snapshot: Any, *, kind: str) -> tuple[str, str, str]:
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
    failures = list(snapshot.get("critical_failures", []) or [])
    if ready:
        return (status, "No critical failures", "#22c55e")
    if failures:
        return (status, ", ".join(failures[:3]), "#ef4444")
    return (status, str(snapshot.get("_error") or "Not ready"), "#f59e0b")


def _render_status_block(title: str, snapshot: Any, *, kind: str = "service") -> None:
    state, detail, color = _status_card_payload(snapshot, kind=kind)
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





def _render_download_jobs_panel_body(download_provider_api: str | None) -> None:
    active_statuses, _ = _list_jobs(
        _ss("api_url"),
        _ss("api_key"),
        state_in=",".join(sorted(ACTIVE_JOB_STATES)),
        sort_by="updated_at",
        sort_desc=True,
        page=1,
        page_size=200,
    )
    recent_statuses, _ = _list_jobs(
        _ss("api_url"),
        _ss("api_key"),
        updated_from=_recent_jobs_cutoff(RECENT_JOBS_WINDOW_HOURS),
        sort_by="updated_at",
        sort_desc=True,
        page=1,
        page_size=RECENT_JOBS_FETCH_LIMIT,
    )
    statuses = _filter_recent_job_rows(
        _merge_job_rows(active_statuses, recent_statuses),
        hours=RECENT_JOBS_WINDOW_HOURS,
        limit=RECENT_JOBS_FETCH_LIMIT,
    )
    st.session_state["active_job_ids"] = [
        str(item.get("job_id", "")).strip()
        for item in active_statuses
        if str(item.get("job_id", "")).strip()
    ]
    provider_scope_value = str(_ss("dl_job_provider_filter", "current"))
    collection_filter_value = str(_ss("dl_job_collection_filter", "")).strip()
    product_filter_value = str(_ss("dl_job_product_filter", "")).strip()
    job_query_value = str(_ss("dl_job_id_query", "")).strip()
    scoped_statuses = _filter_jobs_by_scope(
        statuses,
        provider=_provider_scope_value(provider_scope_value, download_provider_api),
        collection_query=collection_filter_value,
        product_query=product_filter_value,
        job_query=job_query_value,
    )
    scoped_stats = summarize_statuses(scoped_statuses)
    total_jobs = int(scoped_stats["total_jobs"])
    running_jobs = int(scoped_stats["active_jobs"])
    succeeded_jobs = int(scoped_stats["succeeded_jobs"])
    failed_jobs = int(scoped_stats["failed_jobs"])
    cancelled_jobs = int(scoped_stats["cancelled_jobs"])
    bytes_done = int(scoped_stats["bytes_downloaded"])
    bytes_total = int(scoped_stats["bytes_total"])
    progress_pct = float(scoped_stats["progress"])

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Jobs", total_jobs)
    with k2:
        st.metric("Active", running_jobs)
    with k3:
        st.metric("Succeeded", succeeded_jobs)
    with k4:
        st.metric("Failed", failed_jobs)
    with k5:
        st.metric("Cancelled", cancelled_jobs)
    st.progress(max(0.0, min(1.0, progress_pct / 100.0)))
    st.caption(f"Global progress: {progress_pct:.2f}% ({bytes_done}/{bytes_total} bytes)")
    st.caption(
        f"Summary is computed from the current filtered scope inside recent activity (last {RECENT_JOBS_WINDOW_HOURS}h) plus active jobs."
    )
    st.caption("Download panel shows recent activity plus active jobs. Full historical browsing stays in Results.")

    filter_col1, filter_col2 = st.columns([2, 4])
    with filter_col1:
        job_view_counts = _job_view_counts(
            scoped_statuses,
            recent_minutes=RECENT_JOB_CATEGORY_MINUTES,
            active_states=ACTIVE_JOB_STATES,
        )
        job_view = st.radio(
            "Visible jobs",
            options=["all", "active", "succeeded", "failed", "cancelled", "recent"],
            horizontal=True,
            key="dl_job_view",
            format_func=lambda value: {
                "all": f"All ({job_view_counts['all']})",
                "active": f"Active ({job_view_counts['active']})",
                "succeeded": f"Succeeded ({job_view_counts['succeeded']})",
                "failed": f"Failed ({job_view_counts['failed']})",
                "cancelled": f"Cancelled ({job_view_counts['cancelled']})",
                "recent": f"Recent {RECENT_JOB_CATEGORY_MINUTES}m ({job_view_counts['recent']})",
            }[value],
        )
    with filter_col2:
        adv1, adv2, adv3, adv4, adv5 = st.columns([1, 1, 1, 1, 0.8])
        with adv1:
            provider_scope = st.selectbox(
                "Provider filter",
                options=["current", "all", "copernicus", "usgs"],
                key="dl_job_provider_filter",
                format_func=lambda value: {
                    "current": f"Current ({download_provider_api or '-'})",
                    "all": "All providers",
                    "copernicus": "Copernicus",
                    "usgs": "USGS",
                }[value],
            )
        with adv2:
            collection_filter = st.text_input(
                "Mission filter",
                key="dl_job_collection_filter",
                placeholder="e.g. SENTINEL-2",
            ).strip()
        with adv3:
            product_filter = st.text_input(
                "Product filter",
                key="dl_job_product_filter",
                placeholder="e.g. S2MSI2A / 8L1TP",
            ).strip()
        with adv4:
            job_query = st.text_input(
                "Job ID contains",
                key="dl_job_id_query",
                placeholder="job id fragment",
            ).strip()
        with adv5:
            st.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)
            refresh_jobs_clicked = st.button("Refresh", width="stretch", key="refresh_jobs_button")

    base_visible_statuses = _filter_jobs_by_scope(
        statuses,
        provider=_provider_scope_value(provider_scope, download_provider_api),
        collection_query=collection_filter,
        product_query=product_filter,
        job_query=job_query,
    )
    visible_statuses = _filter_jobs_by_view(
        base_visible_statuses,
        job_view,
        recent_minutes=RECENT_JOB_CATEGORY_MINUTES,
        active_states=ACTIVE_JOB_STATES,
    )
    visible_total = len(base_visible_statuses)
    if job_view == "recent":
        st.caption(f"Showing jobs updated in the last {RECENT_JOB_CATEGORY_MINUTES} minutes inside the recent activity window.")
    st.caption(f"Showing {len(visible_statuses)} / {visible_total} matching jobs in the recent activity panel.")
    if refresh_jobs_clicked:
        st.caption("Jobs list refreshed.")

    result_cache = dict(_ss("job_result_cache", {}))
    visible_succeeded_job_ids = [
        str(item.get("job_id"))
        for item in visible_statuses
        if str(item.get("state", "")).lower() == "succeeded"
    ]
    missing_visible_result_ids = [
        job_id for job_id in visible_succeeded_job_ids if job_id not in result_cache
    ]
    if missing_visible_result_ids:
        result_cache.update(
            _refresh_job_results(_ss("api_url"), _ss("api_key"), missing_visible_result_ids)
        )
        st.session_state["job_result_cache"] = result_cache

    if not visible_statuses:
        st.info("No jobs match the selected filter.")

    for item in visible_statuses[:100]:
        job_id = str(item.get("job_id", "unknown"))
        state = str(item.get("state", "unknown"))
        progress = float(item.get("progress", 0.0) or 0.0)
        duration = item.get("duration_seconds")
        errors = item.get("errors", []) or []
        with st.container(border=True):
            h1, h2, h3, h4 = st.columns([3, 1, 1, 1])
            with h1:
                st.markdown(f"**{job_id}**")
                st.caption(
                    f"{item.get('provider', '-')}/{item.get('collection', '-')} · "
                    f"{item.get('product_type', '-')}"
                )
            with h2:
                st.metric("State", state)
            with h3:
                st.metric("Progress", f"{progress:.2f}%")
            with h4:
                st.metric("Duration", f"{float(duration):.1f}s" if duration is not None else "-")
            st.progress(max(0.0, min(1.0, progress / 100.0)))
            st.caption(
                f"{int(item.get('bytes_downloaded', 0) or 0)} / "
                f"{int(item.get('bytes_total', 0) or 0)} bytes · "
                f"Updated: {item.get('updated_at', '-')}"
            )
            if errors:
                st.error("\n".join(str(err) for err in errors[:5]))
            if state == "succeeded":
                result = result_cache.get(job_id, {})
                paths = list(result.get("paths", [])) if isinstance(result, dict) else []
                if paths:
                    st.caption(f"Result files: {len(paths)}")
                    with st.expander("Result paths", expanded=False):
                        for out_path in paths[:25]:
                            st.code(str(out_path), language="text")
                        if len(paths) > 25:
                            st.caption(f"Showing first 25 / {len(paths)} paths.")


@st.fragment(run_every=JOB_MONITOR_REFRESH_EVERY)
def _render_download_jobs_panel_live(download_provider_api: str | None) -> None:
    _render_download_jobs_panel_body(download_provider_api)


def _render_download_jobs_panel_static(download_provider_api: str | None) -> None:
    _render_download_jobs_panel_body(download_provider_api)



def render_sidebar(sat_tiles, gdf, nocov, ncol, skey, all_tile_names=None, tile_centroids=None):
    st.sidebar.markdown("""
    <div style="text-align:center;padding:.3rem 0 .6rem">
        <div style="font-size:1.6rem">🛰️</div>
        <div style="font-size:1rem;font-weight:700;color:#e2e8f0">Sat Downloader</div>
        <div style="font-size:.65rem;color:#64748b;letter-spacing:.06em">v2 — NATIVE LEAFLET</div>
    </div>""", unsafe_allow_html=True)

    with st.sidebar.expander("Connection", expanded=False):
        st.session_state["api_url"] = st.text_input("Service URL", value=_ss("api_url", DEFAULT_API_URL))
        st.session_state["api_key"] = st.text_input("API Key", value=_ss("api_key", DEFAULT_API_KEY), type="password")
        st.session_state["dl_auto_refresh"] = st.checkbox(
            "Auto refresh jobs",
            value=bool(_ss("dl_auto_refresh", True)),
            key="job_auto_refresh",
        )
        current_api_url = str(_ss("api_url", DEFAULT_API_URL)).strip()
        if (
            _ss("last_api_status_url", "") != current_api_url
            or _ss("api_health_snapshot") is None
            or _ss("api_readiness_snapshot") is None
            or _ss("worker_status_snapshot") is None
        ):
            _refresh_api_runtime_statuses()
        if st.button("Refresh service status", width="stretch", key="refresh_service_status_btn"):
            _refresh_api_runtime_statuses()
        st.caption(f"Last checked: {_format_status_timestamp(_ss('service_status_checked_at'))}")
        _render_status_block("API health", _ss("api_health_snapshot"), kind="service")
        _render_status_block("API readiness", _ss("api_readiness_snapshot"), kind="service")
        _render_status_block("Worker execution", _ss("worker_status_snapshot"), kind="worker")

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>📡</span><span style="font-weight:600;font-size:.88rem;">Data Source</span></div>', unsafe_allow_html=True)
    provider = st.sidebar.selectbox("Provider", list(PROVIDERS.keys()), index=list(PROVIDERS.keys()).index(_ss("provider", "Copernicus")), key="sb_prov")
    st.session_state["provider"] = provider
    missions = PROVIDERS.get(provider, [])
    if missions:
        ds = _ss("satellite", missions[0])
        satellite = st.sidebar.selectbox("Mission", missions, index=missions.index(ds) if ds in missions else 0, key="sb_sat")
    else:
        satellite = st.sidebar.text_input("Mission", value="", key="sb_sat_t")
    st.session_state["satellite"] = satellite
    prods = PRODUCT_TYPES.get(satellite, [])
    if prods:
        dp = _ss("product", prods[0])
        product = st.sidebar.selectbox("Product", prods, index=prods.index(dp) if dp in prods else 0, key="sb_prod")
    else:
        product = st.sidebar.text_input("Product", value="", key="sb_prod_t")
    st.session_state["product"] = product
    if provider == "USGS":
        usgs_sat = st.sidebar.selectbox(
            "Satellite",
            options=["Any", "08", "09"],
            index=["Any", "08", "09"].index(str(_ss("usgs_satellite", "Any")) if str(_ss("usgs_satellite", "Any")) in {"Any", "08", "09"} else "Any"),
            key="sb_usgs_satellite",
            help="Any = Landsat 8 et 9. 08/09 force le satellite.",
        )
        st.session_state["usgs_satellite"] = usgs_sat
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>🛰️</span><span style="font-weight:600;font-size:.88rem;">Tile System</span></div>', unsafe_allow_html=True)
    opts_list = []
    labs = {}
    if sat_tiles.get("sentinel-2", {}).get("tiles") is not None:
        opts_list.append("sentinel-2"); labs["sentinel-2"] = "Sentinel-2 (MGRS)"
    if sat_tiles.get("landsat", {}).get("tiles") is not None:
        opts_list.append("landsat"); labs["landsat"] = "Landsat (WRS-2)"
    if opts_list:
        ns = st.sidebar.radio("Grid", opts_list, format_func=lambda x: labs.get(x, x), index=opts_list.index(skey) if skey in opts_list else 0, horizontal=True, label_visibility="collapsed")
        if ns != skey:
            st.session_state["tile_system"] = ns
            st.session_state["selected_tiles"] = []
            st.session_state["intersecting_tiles"] = []
            st.rerun()

    st.sidebar.markdown("""<div style="margin:.3rem 0">
        <div style="display:flex;align-items:center;gap:10px;font-size:.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#0077BB"></div>Sentinel-2</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#EE7733"></div>Landsat</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#CCBB44"></div>AOI</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#AA3377"></div>Intersecting</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#EE3377"></div>Selected</div>
    </div>""", unsafe_allow_html=True)
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>📐</span><span style="font-weight:600;font-size:.88rem;">Area of Interest</span></div>', unsafe_allow_html=True)
    aoi_choices = ["Draw on map", "Preset square", "Paste WKT / GeoJSON"]
    aoi_mode = st.sidebar.radio("AOI", aoi_choices, horizontal=False, label_visibility="collapsed", index=aoi_choices.index(_ss("aoi_mode", "Draw on map")))
    st.session_state["aoi_mode"] = aoi_mode

    if aoi_mode == "Preset square":
        c1, c2 = st.sidebar.columns(2)
        with c1:
            sq_lat = st.number_input("Lat", value=float(st.session_state["map_center"][0]), format="%.4f", key="sq_lat")
        with c2:
            sq_lng = st.number_input("Lng", value=float(st.session_state["map_center"][1]), format="%.4f", key="sq_lng")
        sq_km = st.sidebar.number_input("Side (km)", min_value=0.1, value=25.0, step=5.0, key="sq_km")
        if st.sidebar.button("✅ Apply", width="stretch"):
            st.session_state["geometry_text"] = make_square_wkt(sq_lat, sq_lng, sq_km)
            st.session_state["map_center"] = [sq_lat, sq_lng]
            st.session_state["fly_to"] = json.dumps([sq_lat, sq_lng, 10])
            st.rerun()
    elif aoi_mode == "Paste WKT / GeoJSON":
        st.session_state["geometry_text"] = st.sidebar.text_area(
            "WKT/GeoJSON",
            value=_ss("geometry_text", ""),
            height=100,
            label_visibility="collapsed",
            placeholder="Paste WKT or GeoJSON…",
        )
        raw_txt = st.session_state["geometry_text"].strip()
        prev_txt = _ss("_last_paste_text", "")
        if raw_txt != prev_txt:
            st.session_state["_last_paste_text"] = raw_txt
            g = parse_geometry(raw_txt) if raw_txt else None
            if g is not None and not getattr(g, "is_empty", True):
                ct = g.centroid
                z = zoom_for_bounds(g.bounds)
                st.session_state["map_center"] = [float(ct.y), float(ct.x)]
                st.session_state["fly_to"] = json.dumps([float(ct.y), float(ct.x), int(z)])
            elif raw_txt:
                st.sidebar.caption("AOI invalide: impossible de zoomer (format WKT/GeoJSON non reconnu).")
    else:
        st.sidebar.caption("Draw rectangle/polygon on the map. Click tiles to select/deselect.")

    atxt = _ss("geometry_text", "")
    if atxt:
        with st.sidebar.expander("📋 AOI Preview", expanded=False):
            st.code(atxt[:400] + ("…" if len(atxt) > 400 else ""), language="text")
            if st.button("🗑️ Clear", width="stretch", key="clr_aoi"):
                st.session_state["geometry_text"] = ""
                st.session_state["intersecting_tiles"] = []
                st.rerun()
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>📅</span><span style="font-weight:600;font-size:.88rem;">Time Range</span></div>', unsafe_allow_html=True)
    today = dt.date.today()
    d1, d2 = st.sidebar.columns(2)
    with d1:
        sd = st.date_input("Start", value=st.session_state["start_date"], max_value=today, key="sd")
    with d2:
        ed = st.date_input("End", value=st.session_state["end_date"], min_value=sd, max_value=today, key="ed")
    if ed < sd:
        ed = sd
    st.session_state["start_date"] = sd
    st.session_state["end_date"] = ed
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>🔲</span><span style="font-weight:600;font-size:.88rem;">Grid & Display</span></div>', unsafe_allow_html=True)
    if gdf is not None and ncol:
        g1, g2 = st.sidebar.columns(2)
        with g1:
            st.session_state["show_grid"] = st.checkbox("Grid", value=st.session_state["show_grid"], key="cg")
            st.session_state["show_inter"] = st.checkbox("Intersects", value=st.session_state["show_inter"], key="ci")
            st.session_state["click_sel"] = st.checkbox("Click-select", value=st.session_state["click_sel"], key="cc")
        with g2:
            st.session_state["colorize"] = st.checkbox("Colorize", value=st.session_state["colorize"], key="cz")
            st.session_state["show_sel"] = st.checkbox("Selected", value=st.session_state["show_sel"], key="cs")
            if skey == "sentinel-2":
                st.session_state["show_nocov"] = st.checkbox("No-cov", value=st.session_state["show_nocov"], disabled=(nocov is None), key="cn")
        with st.sidebar.expander("⚙️ Advanced", expanded=False):
            st.session_state["opacity"] = float(st.slider("Fill opacity", 0.0, 0.2, float(st.session_state["opacity"]), step=0.01, key="op"))
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>🔍</span><span style="font-weight:600;font-size:.88rem;">Tile Search</span></div>', unsafe_allow_html=True)
    if gdf is not None and ncol:
        all_names = all_tile_names or []
        all_names_set = set(all_names)
        centroids = tile_centroids or {}
        ms_widget_key = f"ms_widget_{skey}"
        ms_sync_key = f"_ms_sync_sig_{skey}"
        pick_mode_key = f"pick_mode_{skey}"
        pick_index_key = f"pick_idx_{skey}"

        q = st.sidebar.text_input("Search", placeholder="e.g. 34UED or 233062", label_visibility="collapsed", key=f"ts_{skey}")
        if q:
            matches = find_tiles(gdf, ncol, q, 50)
            mids = matches[ncol].astype(str).tolist() if not matches.empty else []
            if not mids:
                st.sidebar.caption("No matches.")
            else:
                pk = st.sidebar.selectbox("Results", mids, index=0, key=f"tm_{skey}", label_visibility="collapsed")
                focus_sig = f"{q}|{pk}"
                prev_focus = _ss(f"_search_focus_{skey}", "")
                if pk and focus_sig != prev_focus:
                    cyx = centroids.get(str(pk))
                    if cyx:
                        st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                        st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                    st.session_state[f"_search_focus_{skey}"] = focus_sig
                    st.rerun()
                b1, b2, b3 = st.sidebar.columns(3)
                with b1:
                    if st.button("➕", width="stretch", key=f"ta_{skey}", help="Add"):
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        sel.add(pk)
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                with b2:
                    if st.button("🔄", width="stretch", key=f"tr_{skey}", help="Replace"):
                        st.session_state["selected_tiles"] = [pk]
                        st.rerun()
                with b3:
                    if st.button("🎯", width="stretch", key=f"tz_{skey}", help="Zoom"):
                        cyx = centroids.get(str(pk))
                        if cyx:
                            st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                            st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                            st.rerun()
        valid_sel = [t for t in st.session_state["selected_tiles"] if t in all_names_set]
        if len(valid_sel) != len(st.session_state["selected_tiles"]):
            st.session_state["selected_tiles"] = valid_sel

        sel_sig = "|".join(valid_sel)
        if _ss(ms_sync_key, "") != sel_sig:
            st.session_state.pop(ms_widget_key, None)
            st.session_state[ms_sync_key] = sel_sig

        widget_sel = st.sidebar.multiselect(
            "Selected",
            all_names,
            default=valid_sel,
            key=ms_widget_key,
            label_visibility="collapsed",
        )
        cur_sel = [str(t) for t in widget_sel]
        if sorted(cur_sel) != sorted(valid_sel):
            st.session_state["selected_tiles"] = cur_sel
            st.rerun()

        pick_label = (
            "🧭 Tile-by-tile mode: ON"
            if bool(_ss(pick_mode_key, False))
            else "🧭 Tile-by-tile mode: OFF"
        )
        if st.sidebar.button(
            pick_label,
            width="stretch",
            key=f"pick_mode_btn_{skey}",
            help="Sélectionne les tuiles une par une avec des boutons.",
        ):
            st.session_state[pick_mode_key] = not bool(_ss(pick_mode_key, False))
            st.rerun()

        if bool(_ss(pick_mode_key, False)):
            inter_candidates = [str(t) for t in _ss("intersecting_tiles", []) if str(t) in all_names_set]
            candidates = inter_candidates if inter_candidates else all_names
            if not candidates:
                st.sidebar.caption("No tile available for manual picker.")
            else:
                idx = int(_ss(pick_index_key, 0))
                if idx < 0 or idx >= len(candidates):
                    idx = 0
                    st.session_state[pick_index_key] = 0
                current_tile = str(candidates[idx])
                is_selected = current_tile in set(map(str, st.session_state["selected_tiles"]))
                st.sidebar.caption(f"Tile {idx + 1}/{len(candidates)}: {current_tile}")

                nav1, nav2, nav3 = st.sidebar.columns(3)
                with nav1:
                    if st.button("⬅️", width="stretch", key=f"pick_prev_{skey}", help="Previous tile"):
                        idx = (idx - 1) % len(candidates)
                        st.session_state[pick_index_key] = idx
                        nxt = str(candidates[idx])
                        cyx = centroids.get(nxt)
                        if cyx:
                            st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                            st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                        st.rerun()
                with nav2:
                    pick_btn = "➖ Unselect" if is_selected else "➕ Select"
                    if st.button(pick_btn, width="stretch", key=f"pick_toggle_{skey}", help="Toggle current tile"):
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        if current_tile in sel:
                            sel.remove(current_tile)
                        else:
                            sel.add(current_tile)
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                with nav3:
                    if st.button("➡️", width="stretch", key=f"pick_next_{skey}", help="Next tile"):
                        idx = (idx + 1) % len(candidates)
                        st.session_state[pick_index_key] = idx
                        nxt = str(candidates[idx])
                        cyx = centroids.get(nxt)
                        if cyx:
                            st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                            st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                        st.rerun()

                if st.sidebar.button("🎯 Zoom current tile", width="stretch", key=f"pick_zoom_{skey}"):
                    cyx = centroids.get(current_tile)
                    if cyx:
                        st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                        st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                        st.rerun()

        if cur_sel:
            if st.sidebar.button("✕ Clear", width="stretch", key=f"tc_{skey}"):
                st.session_state["selected_tiles"] = []
                st.session_state.pop(ms_widget_key, None)
                st.session_state[ms_sync_key] = ""
                st.rerun()
    return provider, satellite, product, aoi_mode



def main():
    st.set_page_config(
        page_title="Satellite Imagery Downloader",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    init_state()

    sat_tiles = load_tiles()
    skey = _ss("tile_system", "sentinel-2")
    td = sat_tiles.get(skey, {})
    gdf = td.get("tiles")
    nocov = td.get("nocov")
    ncol = get_name_col(gdf, skey) if gdf is not None else None

    if gdf is not None:
        st.session_state[f"_raw_gdf_{skey}"] = gdf
    if nocov is not None:
        st.session_state["_raw_nocov"] = nocov

    all_tile_names: List[str] = []
    tile_centroids: Dict[str, Tuple[float, float]] = {}
    if gdf is not None and ncol:
        gdf_id = f"{skey}_{ncol}_{len(gdf)}"
        all_tile_names, tile_centroids = prepare_tile_helpers(gdf_id, ncol, skey)

    provider, satellite, product, _aoi_mode = render_sidebar(
        sat_tiles, gdf, nocov, ncol, skey, all_tile_names, tile_centroids
    )

    st.markdown(
        """<div style='display:flex;align-items:center;gap:14px;margin-bottom:4px;'>
        <div style='font-size:1.6rem;background:linear-gradient(135deg,#38bdf8,#2dd4bf);width:44px;height:44px;border-radius:12px;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 12px rgba(56,189,248,0.35);'>🛰️</div>
        <div><div style='font-size:1.25rem;font-weight:700;color:#e2e8f0;'>Satellite Imagery Downloader</div><div style='font-size:0.72rem;color:#64748b;letter-spacing:.04em;'>Legacy UI + API Jobs Runtime</div></div>
    </div>""",
        unsafe_allow_html=True,
    )

    tab_map, tab_dl, tab_zarr, tab_res, tab_set = st.tabs(
        ["🗺️ Map", "⬇️ Download", "🧱 Zarr Conversion", "📂 Results", "🔧 Settings"]
    )

    with tab_map:
        aoi_geom = parse_geometry(_ss("geometry_text", ""))
        aoi_polys = []
        if aoi_geom and not aoi_geom.is_empty:
            if aoi_geom.geom_type == "Polygon":
                aoi_polys = [aoi_geom]
            elif aoi_geom.geom_type == "MultiPolygon":
                aoi_polys = list(aoi_geom.geoms)

        tnames, _ = compute_intersections(aoi_polys, gdf, ncol)
        st.session_state["intersecting_tiles"] = tnames or []

        grid_compact, grid_hash = "[]", ""
        if gdf is not None and ncol:
            gdf_id = f"{skey}_{ncol}_{len(gdf)}"
            grid_compact, grid_hash = prepare_compact_grid(gdf_id, ncol, skey)

        nocov_json, nocov_hash = "null", ""
        if skey == "sentinel-2" and nocov is not None:
            nocov_id = f"nocov_{len(nocov)}"
            nocov_json, nocov_hash = prepare_nocov_geojson(nocov_id)

        aoi_json = "null"
        aoi_hash = ""
        if aoi_geom and not aoi_geom.is_empty:
            aoi_json = json.dumps(mapping(aoi_geom))
            aoi_hash = _md5(aoi_json)

        options = json.dumps(
            {
                "show_grid": st.session_state["show_grid"],
                "colorize": st.session_state["colorize"],
                "opacity": st.session_state["opacity"],
                "show_nocov": st.session_state.get("show_nocov", False),
                "show_inter": st.session_state["show_inter"],
                "show_sel": st.session_state["show_sel"],
                "click_select": st.session_state["click_sel"],
            }
        )
        fly_to = st.session_state.pop("fly_to", None)

        st.markdown(
            '<div style="border-radius:14px;overflow:hidden;border:1px solid rgba(56,120,200,0.10);box-shadow:0 4px 20px rgba(0,0,0,0.3);">',
            unsafe_allow_html=True,
        )
        comp_result = leaflet_map(
            grid_compact=grid_compact,
            grid_hash=grid_hash,
            aoi_geojson=aoi_json,
            aoi_hash=aoi_hash,
            nocov_geojson=nocov_json if _ss("show_nocov") else "null",
            nocov_hash=nocov_hash if _ss("show_nocov") else "",
            inter_names=json.dumps(st.session_state["intersecting_tiles"]),
            sel_names=json.dumps(st.session_state["selected_tiles"]),
            options=options,
            tile_system=skey,
            center=json.dumps(st.session_state["map_center"]),
            zoom=int(st.session_state["map_zoom"]),
            fly_to=fly_to,
            key="leaflet_map",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        if comp_result and isinstance(comp_result, dict):
            if comp_result.get("type") == "aoi":
                wkt = comp_result.get("wkt", "")
                if wkt and wkt != _ss("geometry_text", ""):
                    st.session_state["geometry_text"] = wkt
                    st.rerun()
                elif not wkt and _ss("geometry_text", ""):
                    st.session_state["geometry_text"] = ""
                    st.session_state["intersecting_tiles"] = []
                    st.rerun()
            elif comp_result.get("type") == "tile_click":
                tid = str(comp_result.get("name", "")).strip()
                if tid:
                    sel = set(map(str, st.session_state["selected_tiles"]))
                    sel.symmetric_difference_update({tid})
                    st.session_state["selected_tiles"] = sorted(sel)
                    st.rerun()

        ni = len(st.session_state["intersecting_tiles"])
        ns = len(st.session_state["selected_tiles"])
        grid_label = skey.split("-")[0].upper()
        st.markdown(
            f"""<div style='display:flex;gap:8px;margin-top:6px;'>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#2dd4bf;font-weight:700;'>{ni}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Intersecting</div></div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#e2e8f0;font-weight:700;'>{ns}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Selected</div></div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#38bdf8;font-weight:700;'>{grid_label}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Grid</div></div>
        </div>""",
            unsafe_allow_html=True,
        )

    with tab_dl:
        effective_product_type = str(product)
        if provider == "USGS":
            effective_product_type = _resolve_usgs_product_type(
                selected_product_type=str(product),
                selected_satellite=str(_ss("usgs_satellite", "Any")),
            )

        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>⬇️</span><span style="font-weight:600;font-size:.94rem;">Download Manager</span></div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Provider", provider)
        with c2:
            st.metric("Mission", satellite)
        with c3:
            st.metric("Product", effective_product_type)
        st.markdown("---")

        selected_tiles_for_cmd = _ss("selected_tiles", [])
        selected_tiles_wkt = selected_tiles_to_wkt(gdf, ncol, selected_tiles_for_cmd)
        selected_tiles_geom = selected_tiles_to_geometry(gdf, ncol, selected_tiles_for_cmd)
        drawn_aoi_text = _ss("geometry_text", "").strip()
        drawn_aoi_geom = parse_geometry(drawn_aoi_text) if drawn_aoi_text else None
        use_selected_tiles_mode = bool(selected_tiles_wkt)
        aoi_text_for_download = (
            selected_tiles_geom.wkt
            if use_selected_tiles_mode and selected_tiles_geom is not None and not getattr(selected_tiles_geom, "is_empty", True)
            else drawn_aoi_text
        )
        preview_geom = selected_tiles_geom if use_selected_tiles_mode else drawn_aoi_geom
        preview_wkt = (
            preview_geom.wkt if (preview_geom is not None and not getattr(preview_geom, "is_empty", True)) else ""
        )
        collection = str(satellite).split(" ")[0]

        np_ = len(selected_tiles_for_cmd)
        ni_ = len(_ss("intersecting_tiles", []))
        if np_ > 0:
            st.markdown(
                f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">📦 {np_} tile(s) selected</div>',
                unsafe_allow_html=True,
            )
        elif ni_ > 0:
            st.markdown(
                f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#38bdf8;">ℹ️ {ni_} intersecting — select tiles to run batch by tile</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#fbbf24;">⚠️ No tiles — draw AOI or select tiles</div>',
                unsafe_allow_html=True,
            )

        preview_key = _md5(
            "|".join(
                [
                    provider,
                    collection,
                    str(effective_product_type),
                    str(st.session_state["start_date"]),
                    str(st.session_state["end_date"]),
                    "tiles" if use_selected_tiles_mode else "aoi",
                    preview_wkt,
                ]
            )
        )
        if _ss("preview_key", "") != preview_key:
            st.session_state["preview_key"] = preview_key
            st.session_state["preview_items"] = []
            st.session_state["preview_total"] = 0
            st.session_state["preview_error"] = ""
            st.session_state["preview_fetched"] = False

        pr1, pr2 = st.columns([2, 1])
        with pr1:
            st.markdown(
                '<div style="font-weight:600;font-size:.84rem;color:#e2e8f0;">Products Preview</div>',
                unsafe_allow_html=True,
            )
        with pr2:
            refresh_preview = st.button("🔎 Refresh Preview", width="stretch", key="refresh_preview")

        auto_preview = bool(preview_wkt) and not _ss("preview_fetched", False)
        if refresh_preview or auto_preview:
            prev = preview_products_cached(
                provider=provider,
                collection=collection,
                product_type=str(effective_product_type),
                start_date=str(st.session_state["start_date"]),
                end_date=str(st.session_state["end_date"]),
                aoi_wkt=preview_wkt,
                max_items=50,
                tile_ids=selected_tiles_for_cmd,
            )
            st.session_state["preview_items"] = prev.get("items", [])
            st.session_state["preview_total"] = int(prev.get("total", 0) or 0)
            st.session_state["preview_error"] = prev.get("error", "")
            st.session_state["preview_fetched"] = True

        if _ss("preview_error"):
            st.warning(f"Preview: {_ss('preview_error')}")
        else:
            p_total = int(_ss("preview_total", 0))
            p_items = _ss("preview_items", [])
            if p_total > 0:
                st.markdown(
                    f"<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#2dd4bf;'>🔎 {p_total} produit(s) trouvé(s)</div>",
                    unsafe_allow_html=True,
                )
                for it in p_items:
                    name = str(it.get("name", it.get("id", "product")))
                    tile = str(it.get("tile_id", "-"))
                    sensing = str(it.get("sensing_time", "-"))
                    size_mb = it.get("size_mb")
                    size_txt = f"{size_mb} MB" if size_mb not in (None, "") else "-"
                    st.markdown(
                        f"<div style='background:#0f172a;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;margin-top:6px;'>"
                        f"<div style='font-family:JetBrains Mono;font-size:.73rem;color:#e2e8f0;font-weight:600;'>{name}</div>"
                        f"<div style='font-family:JetBrains Mono;font-size:.66rem;color:#94a3b8;margin-top:3px;'>"
                        f"Tile: {tile} · Date: {sensing} · Size: {size_txt}</div></div>",
                        unsafe_allow_html=True,
                    )
                if p_total > len(p_items):
                    st.caption(f"Showing first {len(p_items)} results.")
            elif _ss("preview_fetched", False):
                st.info("Aucun produit trouvé pour cette AOI et cette période.")

        mode_text = "single job"
        if provider == "Copernicus" and len(selected_tiles_for_cmd) > 1:
            mode_text = f"batch by tile ({len(selected_tiles_for_cmd)} jobs)"
        elif provider == "Copernicus" and len(selected_tiles_for_cmd) == 1:
            mode_text = "single job with tile filter"
        st.caption(f"Submit mode: {mode_text}")

        d1, d2, d3, d4 = st.columns([2, 1, 1, 1])
        with d1:
            start_clicked = st.button("🚀 Start Download", width="stretch", type="primary")
        with d2:
            stop_clicked = st.button("⏹️ Stop", width="stretch")
        with d3:
            reset_clicked = st.button("🗑️ Reset", width="stretch")
        with d4:
            unlock_clicked = st.button("🔓 Unlock", width="stretch")

        if start_clicked:
            if not aoi_text_for_download:
                st.error("Define AOI or select tiles first.")
            else:
                try:
                    if provider == "Copernicus" and len(selected_tiles_for_cmd) > 1:
                        jobs = [
                            _build_job_payload(
                                provider_label=provider,
                                collection=collection,
                                product_type=str(effective_product_type),
                                start_date=st.session_state["start_date"],
                                end_date=st.session_state["end_date"],
                                aoi_wkt=aoi_text_for_download,
                                tile_id=tile_id,
                            )
                            for tile_id in selected_tiles_for_cmd
                        ]
                        response = _api_request(
                            "POST",
                            _ss("api_url"),
                            "/v1/jobs/batch",
                            api_key=_ss("api_key"),
                            payload={"jobs": jobs},
                        )
                        if response.ok:
                            created = [str(job_id) for job_id in response.json().get("job_ids", [])]
                            _upsert_known_jobs(created, active_job_ids=created)
                            st.success(f"Created {len(created)} jobs.")
                        else:
                            st.error(f"{response.status_code}: {response.text}")
                    else:
                        tile_id = selected_tiles_for_cmd[0] if (provider == "Copernicus" and len(selected_tiles_for_cmd) == 1) else None
                        payload = _build_job_payload(
                            provider_label=provider,
                            collection=collection,
                            product_type=str(effective_product_type),
                            start_date=st.session_state["start_date"],
                            end_date=st.session_state["end_date"],
                            aoi_wkt=aoi_text_for_download,
                            tile_id=tile_id,
                        )
                        response = _api_request(
                            "POST",
                            _ss("api_url"),
                            "/v1/jobs",
                            api_key=_ss("api_key"),
                            payload=payload,
                        )
                        if response.ok:
                            job_id = str(response.json().get("job_id", ""))
                            _upsert_known_jobs([job_id], active_job_ids=[job_id])
                            st.success(f"Created job: {job_id}")
                        else:
                            st.error(f"{response.status_code}: {response.text}")
                except Exception as exc:
                    st.error(str(exc))

        download_provider_api = PROVIDER_CLI_MAP.get(provider)
        active_job_rows_for_stop, _ = _list_jobs(
            _ss("api_url"),
            _ss("api_key"),
            state_in=",".join(sorted(ACTIVE_JOB_STATES)),
            provider=download_provider_api,
            sort_by="updated_at",
            sort_desc=True,
            page=1,
            page_size=200,
        )
        active_ids = [str(item.get("job_id", "")).strip() for item in active_job_rows_for_stop if str(item.get("job_id", "")).strip()]
        if stop_clicked:
            cancelled = 0
            for job_id in active_ids:
                try:
                    response = _api_request("DELETE", _ss("api_url"), f"/v1/jobs/{job_id}", api_key=_ss("api_key"), timeout=30)
                    if response.ok and bool(response.json().get("cancel_requested")):
                        cancelled += 1
                except Exception:
                    continue
            st.info(f"Cancel requested for {cancelled}/{len(active_ids)} active jobs for {download_provider_api or provider}.")

        if reset_clicked or unlock_clicked:
            st.session_state["active_job_ids"] = []
            st.session_state["job_status_cache"] = {}
            st.session_state["job_result_cache"] = {}
            st.session_state["job_event_log"] = []
            st.session_state["dl_last_event_id"] = 0
            st.session_state["dl_last_sse_ok"] = 0.0
            st.session_state["dl_event_errors"] = 0
            if reset_clicked:
                st.success("UI runtime reset (files preserved).")
            if unlock_clicked:
                st.success("Tracker unlocked.")

        if bool(_ss("dl_auto_refresh", True)):
            _render_download_jobs_panel_live(download_provider_api)
        else:
            _render_download_jobs_panel_static(download_provider_api)

    with tab_zarr:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🧱</span><span style="font-weight:600;font-size:.94rem;">Zarr Conversion</span></div>',
            unsafe_allow_html=True,
        )
        zc1, zc2 = st.columns([3, 1])
        with zc1:
            st.session_state["zarr_service_url"] = st.text_input(
                "Zarr service URL",
                value=_ss("zarr_service_url", DEFAULT_ZARR_URL),
                help="Example: http://nimbus-zarr:8010 (compose) or http://127.0.0.1:8010",
            )
        with zc2:
            current_zarr_url = str(_ss("zarr_service_url", DEFAULT_ZARR_URL)).strip()
            if (
                _ss("last_zarr_status_url", "") != current_zarr_url
                or _ss("zarr_health_snapshot") is None
                or _ss("zarr_readiness_snapshot") is None
            ):
                _refresh_zarr_runtime_statuses()
            if st.button("Refresh Zarr status", width="stretch"):
                _refresh_zarr_runtime_statuses()
        st.caption(f"Last checked: {_format_status_timestamp(_ss('zarr_status_checked_at'))}")
        zstatus1, zstatus2 = st.columns(2)
        with zstatus1:
            _render_status_block("Zarr health", _ss("zarr_health_snapshot"), kind="service")
        with zstatus2:
            _render_status_block("Zarr readiness", _ss("zarr_readiness_snapshot"), kind="service")

        zarr_schema = _zarr_service_schema(_ss("zarr_service_url", DEFAULT_ZARR_URL))
        resolution_policy_info = (zarr_schema.get("converter_config", {}) or {}).get("resolution_policy", {})
        if resolution_policy_info:
            collections_policy = dict(resolution_policy_info.get("collections") or {})
            sentinel2_target = (collections_policy.get("SENTINEL-2") or {}).get("target_pixel_size_meters")
            landsat_l1_target = (collections_policy.get("landsat_ot_c2_l1") or {}).get("target_pixel_size_meters")
            landsat_l2_target = (collections_policy.get("landsat_ot_c2_l2") or {}).get("target_pixel_size_meters")
            sentinel1_target = (collections_policy.get("SENTINEL-1") or {}).get("target_pixel_size_meters")
            st.caption(
                "Resolution policy: "
                f"Sentinel-2 -> {sentinel2_target if sentinel2_target is not None else 'native'} m, "
                f"Landsat L1 -> {landsat_l1_target if landsat_l1_target is not None else 'native'} m, "
                f"Landsat L2 -> {landsat_l2_target if landsat_l2_target is not None else 'native'} m, "
                f"Sentinel-1 -> {sentinel1_target if sentinel1_target is not None else 'native reference'}"
            )

        candidates = _recent_source_candidates()
        if not candidates:
            st.info("No recent raw files or source folders found in downloads.")
        raw_uri = st.selectbox(
            "Raw source",
            options=candidates if candidates else [""],
            index=0,
            key="zarr_raw_uri",
            help="Recent downloads and extracted source folders.",
            format_func=lambda value: (
                "-"
                if not value
                else f"{Path(str(value)).name}  |  {value}"
            ),
        )

        guessed_scene = _guess_scene_id(raw_uri) if raw_uri else ""
        default_provider = _guess_zarr_provider(raw_uri)
        default_collection = _guess_zarr_collection(default_provider, guessed_scene)
        default_product_type = _guess_zarr_product_type(default_provider, default_collection, guessed_scene)
        default_output = _default_zarr_output(guessed_scene)

        last_raw = str(_ss("zarr_last_raw_uri", ""))
        if raw_uri and raw_uri != last_raw:
            st.session_state["zarr_last_raw_uri"] = raw_uri
            st.session_state["zarr_provider_api"] = default_provider
            st.session_state["zarr_collection_api"] = default_collection
            st.session_state["zarr_product_type"] = default_product_type
            st.session_state["zarr_scene_id"] = guessed_scene
            st.session_state["zarr_output_uri"] = default_output
            st.session_state["zarr_last_auto_output"] = default_output

        provider_options = list((zarr_schema.get("supported_collections", {}) or {}).keys()) or ["copernicus", "usgs"]
        provider_default = _ss("zarr_provider_api", default_provider)
        provider_index = provider_options.index(provider_default) if provider_default in provider_options else 0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            provider_api = st.selectbox(
                "Provider",
                options=provider_options,
                index=provider_index,
                key="zarr_provider_api",
            )
        with col2:
            collection_options = _zarr_supported_collections(zarr_schema, provider_api) or [default_collection]
            current_collection = _ss("zarr_collection_api", default_collection)
            collection_index = collection_options.index(current_collection) if current_collection in collection_options else 0
            collection_api = st.selectbox(
                "Collection",
                options=collection_options,
                index=collection_index,
                key="zarr_collection_api",
            )
        with col3:
            product_options = _zarr_supported_product_types(zarr_schema, collection_api) or [default_product_type]
            current_product_type = _ss("zarr_product_type", default_product_type)
            product_index = product_options.index(current_product_type) if current_product_type in product_options else 0
            product_type = st.selectbox(
                "Product type",
                options=product_options,
                index=product_index,
                key="zarr_product_type",
            )
        with col4:
            scene_id = st.text_input("Scene ID", value=guessed_scene, key="zarr_scene_id")

        auto_output = _default_zarr_output(scene_id)
        current_output = str(_ss("zarr_output_uri", "")).strip()
        last_auto_output = str(_ss("zarr_last_auto_output", "")).strip()
        if (not current_output) or (current_output == last_auto_output):
            st.session_state["zarr_output_uri"] = auto_output
            st.session_state["zarr_last_auto_output"] = auto_output

        output_uri = st.text_input(
            "Output Zarr path",
            value=_default_zarr_output(scene_id),
            key="zarr_output_uri",
            help="Use /data/... so the generated Zarr is visible on the host via ./data/...",
        )

        run_convert = st.button("Run Zarr conversion", type="primary", width="stretch", disabled=not raw_uri)
        if run_convert:
            scene_id_value = scene_id.strip() or guessed_scene or "scene"
            output_uri_value = output_uri.strip()
            if output_uri_value.startswith("/tmp/"):
                output_uri_value = _default_zarr_output(scene_id_value)
                st.info(f"Output path adjusted to mounted volume: {output_uri_value}")

            payload = {
                "job_id": f"zarr-{uuid.uuid4().hex[:12]}",
                "pipeline_id": f"ui-zarr-{uuid.uuid4().hex[:8]}",
                "trace_id": f"ui-zarr-{uuid.uuid4().hex[:8]}",
                "provider": provider_api,
                "collection": collection_api.strip(),
                "product_type": product_type.strip() if product_type else None,
                "scene_id": scene_id_value,
                "raw_uri": raw_uri,
                "raw_format": _guess_raw_source_format(raw_uri),
                "output_uri": output_uri_value,
            }
            try:
                response = _http_session().post(
                    f"{_ss('zarr_service_url').rstrip('/')}/convert",
                    headers={"Content-Type": "application/json"},
                    json=payload,
                    timeout=300,
                )
                if response.ok:
                    body = response.json()
                    history = list(_ss("zarr_history", []))
                    history.insert(0, body)
                    st.session_state["zarr_history"] = history[:50]
                    _register_zarr_artifact(
                        _ss("api_url"),
                        _ss("api_key"),
                        convert_response=body,
                        raw_uri=raw_uri,
                        provider=provider_api,
                        collection=collection_api.strip(),
                        scene_id=scene_id_value,
                    )
                    _available_zarr_stores.clear()
                    _recent_source_candidates.clear()
                    zarr_uri = str(body.get("zarr_uri", "-"))
                    host_hint = _container_to_host_path_hint(zarr_uri)
                    if host_hint:
                        st.success(f"Zarr conversion completed: {zarr_uri} (host: {host_hint})")
                    else:
                        st.success(f"Zarr conversion completed: {zarr_uri}")
                else:
                    st.error(f"{response.status_code}: {response.text}")
            except Exception as exc:
                st.error(str(exc))

        st.markdown("---")
        st.caption("Registered and discovered Zarr stores")
        zaf1, zaf2, zaf3 = st.columns([2, 2, 3])
        with zaf1:
            zarr_provider_filter = st.selectbox(
                "Artifact provider",
                options=["", "copernicus", "usgs"],
                key="zarr_artifact_provider",
                format_func=lambda value: "All providers" if not value else value,
            )
        with zaf2:
            zarr_collection_filter = st.text_input(
                "Artifact mission",
                key="zarr_artifact_collection",
                placeholder="e.g. SENTINEL-2",
            ).strip()
        with zaf3:
            zarr_artifact_query = st.text_input(
                "Artifact path / scene search",
                key="zarr_artifact_query",
                placeholder="scene id or path fragment",
            ).strip()
        show_legacy_zarr = st.checkbox(
            "Show legacy/unregistered local Zarr stores",
            value=bool(_ss("show_legacy_zarr", False)),
            key="show_legacy_zarr",
            help="Legacy stores are old local Zarr directories discovered on disk but not registered by the current converter flow.",
        )

        artifacts, artifacts_total = _list_artifacts(
            _ss("api_url"),
            _ss("api_key"),
            artifact_type="zarr",
            provider=zarr_provider_filter or None,
            collection=zarr_collection_filter or None,
            uri_query=zarr_artifact_query or None,
            include_local=True,
            page=1,
            page_size=120,
        )
        visible_artifacts, hidden_legacy_count = _filter_visible_artifacts(
            artifacts,
            include_legacy=show_legacy_zarr,
        )
        if not visible_artifacts:
            st.info("No .zarr store found in the registry or on local disk yet.")
        else:
            caption = f"Showing {len(visible_artifacts)} / {artifacts_total} Zarr stores."
            if hidden_legacy_count:
                caption += f" Hidden legacy stores: {hidden_legacy_count}."
            st.caption(caption)
            store_options = [str(item["artifact_uri"]) for item in visible_artifacts]
            selected_store = st.selectbox(
                "Stored Zarr directory",
                options=store_options,
                index=0,
                key="zarr_selected_store",
                format_func=lambda value: next(
                    (
                        f"[{str(item.get('_visibility_status', _artifact_visibility_status(item))).upper()}] "
                        f"{Path(str(item['artifact_uri'])).name}  |  "
                        f"{_container_to_host_path_hint(str(item['artifact_uri'])) or str(item['artifact_uri'])}"
                        for item in visible_artifacts
                        if str(item["artifact_uri"]) == value
                    ),
                    value,
                ),
            )
            selected_store_meta = next(
                (item for item in visible_artifacts if str(item["artifact_uri"]) == selected_store),
                visible_artifacts[0],
            )
            selected_host_hint = _container_to_host_path_hint(str(selected_store_meta.get("artifact_uri", "")))
            meta_cols = st.columns(4)
            with meta_cols[0]:
                st.metric("Provider", str(selected_store_meta.get("provider", "-") or "-"))
            with meta_cols[1]:
                st.metric("Collection", str(selected_store_meta.get("collection", "-") or "-"))
            with meta_cols[2]:
                st.metric("Bands", len(selected_store_meta.get("band_names") or []))
            with meta_cols[3]:
                st.metric("Size", _human_size(selected_store_meta.get("size_bytes")))
            st.code(
                "\n".join(
                    [
                        f"Scene: {selected_store_meta.get('scene_id', '-')}",
                        f"Container path: {selected_store_meta.get('artifact_uri', '-')}",
                        f"Host path: {selected_host_hint or '-'}",
                        f"Visibility status: {selected_store_meta.get('_visibility_status', _artifact_visibility_status(selected_store_meta))}",
                        f"Data family: {selected_store_meta.get('data_family', '-')}",
                        f"Dimensions: {', '.join([str(v) for v in (selected_store_meta.get('dimensions') or [])]) or '-'}",
                        f"Shape: {selected_store_meta.get('shape', []) or '-'}",
                        f"Bands: {', '.join([str(v) for v in (selected_store_meta.get('band_names') or [])]) or '-'}",
                        f"Source: {selected_store_meta.get('source_uri', '-')}",
                        f"Updated: {selected_store_meta.get('updated_at', '-')}",
                    ]
                ),
                language="text",
            )

        st.markdown("---")
        st.caption("Recent Zarr conversions (session)")
        zarr_history = list(_ss("zarr_history", []))
        if not zarr_history:
            st.info("No conversion executed in this session yet.")
        else:
            for item in zarr_history[:20]:
                with st.container(border=True):
                    st.markdown(f"**{item.get('job_id', '-') }**")
                    st.caption(f"{item.get('data_family', '-') } · {item.get('zarr_uri', '-')}")
                    st.caption(f"Bands: {', '.join([str(b) for b in (item.get('band_names') or [])])}")
                    st.code(json.dumps(item.get("normalization_summary", {}), indent=2), language="json")

    with tab_res:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📂</span><span style="font-weight:600;font-size:.94rem;">Results</span></div>',
            unsafe_allow_html=True,
        )

        state_filter = st.selectbox("State", ["", "queued", "running", "succeeded", "failed", "cancel_requested", "cancelled"], index=0)
        provider_filter = st.selectbox("Provider", ["", "copernicus", "usgs"], index=0)
        jobs_rows, jobs_total = _list_jobs(
            _ss("api_url"),
            _ss("api_key"),
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
        st.caption(
            f"Showing recent jobs only (last {RECENT_JOBS_WINDOW_HOURS}h + active). "
            f"{len(jobs_rows)} / {jobs_total} rows."
        )
        if jobs_rows:
            st.dataframe(jobs_rows, width="stretch")
        else:
            st.info("No jobs for selected filters.")

        st.markdown("---")
        dl_dir = DOWNLOADS_DIR
        dl_dir.mkdir(exist_ok=True, parents=True)
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
        use_comp = st.toggle(
            "Use advanced file browser (experimental)",
            value=bool(_ss("use_file_browser_component", False)),
            help="Uses streamlit_file_browser. If you see missing *.map asset errors, disable this.",
        )
        st.session_state["use_file_browser_component"] = use_comp
        if use_comp and st_file_browser is not None:
            try:
                st_file_browser(
                    str(dl_dir),
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

        if (not use_comp) or (st_file_browser is None):
            files = [f for f in dl_dir.rglob("*") if f.is_file()]
            if not files:
                st.info("No files yet.")
            else:
                rows = []
                for f in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
                    rel = str(f.relative_to(dl_dir))
                    stt = f.stat()
                    rows.append(
                        {
                            "path": rel,
                            "size_MB": round(stt.st_size / (1024 * 1024), 3),
                            "modified": dt.datetime.fromtimestamp(stt.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                st.dataframe(rows[:500], width="stretch", hide_index=True)
                selected = st.selectbox("Select a file", options=[r["path"] for r in rows], index=0)
                sel_path = dl_dir / selected
                if sel_path.exists() and sel_path.is_file():
                    st.download_button(
                        "⬇️ Download selected",
                        data=sel_path.read_bytes(),
                        file_name=sel_path.name,
                        mime="application/octet-stream",
                        width="stretch",
                    )
    st.markdown(
        '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🔧</span><span style="font-weight:600;font-size:.94rem;">Settings</span></div>',
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Center", f"{st.session_state['map_center'][0]:.4f}, {st.session_state['map_center'][1]:.4f}")
    with c2:
        st.metric("Zoom", st.session_state["map_zoom"])
    with c3:
        st.metric("System", skey)
    st.markdown("---")
    st.code(f"API URL: {_ss('api_url')}\nDownloads dir: {DOWNLOADS_DIR}", language="text")
    st.markdown("---")

    st.markdown("**Converter / Zarr settings**")
    colz1, colz2, colz3 = st.columns(3)
    with colz1:
        st.session_state["zarr_chunk_time"] = st.number_input("Chunk time", min_value=1, value=int(_ss("zarr_chunk_time", 1)), step=1)
        st.session_state["zarr_clear_encodings"] = st.checkbox("Clear encodings", value=bool(_ss("zarr_clear_encodings", True)))
        st.session_state["zarr_prefetch"] = st.checkbox("Prefetch remote", value=bool(_ss("zarr_prefetch", True)))
    with colz2:
        st.session_state["zarr_chunk_y"] = st.number_input("Chunk y", min_value=1, value=int(_ss("zarr_chunk_y", 512)), step=64)
        st.session_state["zarr_append_mode"] = st.checkbox("Append mode (time)", value=bool(_ss("zarr_append_mode", False)))
        st.session_state["zarr_cache_remote"] = st.checkbox("Cache remote", value=bool(_ss("zarr_cache_remote", True)))
    with colz3:
        st.session_state["zarr_chunk_x"] = st.number_input("Chunk x", min_value=1, value=int(_ss("zarr_chunk_x", 512)), step=64)
        st.session_state["zarr_output_base"] = st.text_input("Output base", value=_ss("zarr_output_base", "/data/downloads/zarr"))
        st.session_state["zarr_cleanup_remote"] = st.checkbox("Cleanup temp", value=bool(_ss("zarr_cleanup_remote", True)))

    st.session_state["zarr_band_config_path"] = st.text_input(
        "Band config YAML",
        value=_ss("zarr_band_config_path"),
        help="Path to converter/config/bands.yml",
    )
    st.session_state["zarr_log_level"] = st.selectbox(
        "Converter log level",
        options=["info", "debug"],
        index=["info", "debug"].index(str(_ss("zarr_log_level", "info"))),
    )

    st.markdown("**Runtime notes**")
    st.markdown(
        """- Legacy map/tile UX preserved.
- Downloads go through FastAPI jobs (`/v1/jobs`) and worker service.
- Zarr conversions use the new reader/cube/writer pipeline (bands variable, fsspec remote, consolidated metadata).
- Reset/Unlock only clear UI runtime state, not downloaded files."""
    )


if __name__ == "__main__":
    main()
