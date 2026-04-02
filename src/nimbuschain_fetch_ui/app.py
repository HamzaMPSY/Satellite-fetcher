
import os
import re
import sys
import math
import json
import html
import time
import tempfile
import subprocess
import signal
import datetime as dt
import uuid
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import quote

# Ensure repo src/ is on sys.path before importing package modules (helps when PYTHONPATH is missing)
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import geopandas as gpd
import shapely
import streamlit as st
import streamlit.components.v1 as components
from dataclasses import dataclass
from shapely.geometry import Polygon, box

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
    RECENT_JOBS_WINDOW_HOURS,
    RECENT_JOB_CATEGORY_MINUTES,
    PROVIDER_ISSUE_WINDOW_MINUTES,
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
    _http_session,
    _api_request,
    _parse_event_stream,
    _drain_sse_events,
    _refresh_job_statuses,
    _refresh_job_results,
    _list_jobs,
    _recent_jobs_cutoff,
    _fetch_recent_provider_jobs,
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
    _parse_iso_datetime,
)
from nimbuschain_fetch_ui.preview_local import preview_products_local
from nimbuschain_fetch_ui.provider_auth import (
    provider_action_guidance,
    provider_actions_disabled,
    provider_auth_state_label,
    provider_preview_error_payload,
    select_provider_status,
)
from nimbuschain_fetch_ui.results_tab import render_results_tab
from nimbuschain_fetch_ui.runtime_status import (
    format_status_timestamp as _format_status_timestamp,
    refresh_api_runtime_statuses as _collect_api_runtime_statuses,
    refresh_zarr_runtime_statuses as _collect_zarr_runtime_statuses,
    render_status_block as _render_status_block,
)
from nimbuschain_fetch_ui.settings_tab import render_settings_tab
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


def _response_error_message(response: Any) -> str:
    try:
        payload = response.json()
    except Exception:
        return str(getattr(response, "text", "")).strip() or f"HTTP {getattr(response, 'status_code', '-')}"

    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("message") or payload.get("error")
        if isinstance(detail, list):
            return "; ".join(str(item) for item in detail)
        if detail:
            return str(detail)
    return str(getattr(response, "text", "")).strip() or f"HTTP {getattr(response, 'status_code', '-')}"


UI_LOG_PATH = Path(
    os.getenv(
        "NIMBUS_UI_LOG_PATH",
        str(Path(tempfile.gettempdir()) / "nimbuschain_fetch_ui" / "app_debug.log"),
    )
)
configure_logging(UI_LOG_PATH)
logger.info("=" * 60)
logger.info("Satellite Downloader v2 — app starting")
logger.info(f"PROJECT_ROOT : {PROJECT_ROOT}")
logger.info(f"UI_LOG_PATH   : {UI_LOG_PATH}")
logger.info(f"DOWNLOADS_DIR: {DOWNLOADS_DIR}")
logger.info(f"NOHUP_PATH   : {NOHUP_PATH}")
logger.info(f"PID_PATH     : {PID_PATH}")
logger.info(f"Python       : {sys.executable}")
logger.info("=" * 60)







@st.cache_data(show_spinner="Previewing products for this AOI…", ttl=180)
def preview_products_cached(
    api_url: str,
    api_key: str,
    provider: str,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    max_items: int = 50,
    tile_ids: List[str] | None = None,
) -> Dict[str, Any]:
    payload = {
        "provider": provider,
        "collection": collection,
        "product_type": product_type,
        "start_date": start_date,
        "end_date": end_date,
        "aoi_wkt": aoi_wkt,
        "max_items": max_items,
        "tile_ids": tile_ids or [],
    }
    try:
        response = _api_request(
            "POST",
            api_url,
            "/v1/preview",
            api_key=api_key,
            payload=payload,
            timeout=90,
        )
        if response.ok:
            body = response.json()
            if isinstance(body, dict):
                return {
                    "items": list(body.get("items", [])),
                    "total": int(body.get("total", 0) or 0),
                    "error": str(body.get("error", "") or ""),
                    "error_kind": str(body.get("error_kind", "") or ""),
                    "error_detail": str(body.get("error_detail", "") or ""),
                }
        else:
            return {
                "items": [],
                "total": 0,
                "error": f"Preview service failed: {_response_error_message(response)}",
                "error_kind": "technical",
                "error_detail": f"Preview service failed: {_response_error_message(response)}",
            }
    except Exception:
        pass

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


def _build_job_payload(
    *,
    provider_label: str,
    collection: str,
    product_type: str,
    start_date: dt.date,
    end_date: dt.date,
    aoi_wkt: str,
    tile_id: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    provider_api = PROVIDER_CLI_MAP[provider_label]
    return build_job_payload_runtime(
        provider=provider_api,
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi_wkt=aoi_wkt,
        tile_id=tile_id,
        output_dir=output_dir,
    )




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
        "dl_job_provider_filter": "all",
        "dl_job_collection_filter": "",
        "dl_job_product_filter": "",
        "dl_job_id_query": "",
        "zarr_selected_store": "",
        "zarr_artifact_query": "",
        "zarr_artifact_provider": "",
        "zarr_artifact_collection": "",
        "watermask_selected_store": "",
        "watermask_artifact_query": "",
        "watermask_artifact_provider": "",
        "watermask_artifact_collection": "",
        "show_legacy_watermask_zarr": False,
        "mask_mode": "water",
        "preferred_mask_job_ids": {},
        "mask_submission_notice": {},
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
        "preview_error_kind": "",
        "preview_error_detail": "",
        "preview_fetched": False,
        "fly_to": None,
        "use_file_browser_component": False,
        "api_health_snapshot": None,
        "api_readiness_snapshot": None,
        "worker_status_snapshot": None,
        "provider_status_snapshot": None,
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
    if st.session_state.get("dl_job_provider_filter") == "current":
        st.session_state["dl_job_provider_filter"] = "all"
    if st.session_state.get("dl_job_provider_filter") not in {"all", "copernicus", "usgs"}:
        st.session_state["dl_job_provider_filter"] = "all"


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


def _upsert_known_jobs(
    job_ids: list[str],
    *,
    active_job_ids: list[str] | None = None,
) -> None:
    known = [
        str(item).strip()
        for item in _ss("known_job_ids", [])
        if str(item).strip()
    ]
    active = [
        str(item).strip()
        for item in _ss("active_job_ids", [])
        if str(item).strip()
    ]

    for job_id in job_ids:
        normalized = str(job_id).strip()
        if not normalized:
            continue
        if normalized not in known:
            known.insert(0, normalized)

    if active_job_ids:
        for job_id in active_job_ids:
            normalized = str(job_id).strip()
            if not normalized:
                continue
            if normalized not in active:
                active.append(normalized)
            if normalized not in known:
                known.insert(0, normalized)

    st.session_state["known_job_ids"] = known[:1000]
    st.session_state["active_job_ids"] = active[:1000]


def _refresh_api_runtime_statuses() -> None:
    api_url = str(_ss("api_url", DEFAULT_API_URL)).strip()
    api_key = str(_ss("api_key", DEFAULT_API_KEY)).strip()
    st.session_state.update(
        _collect_api_runtime_statuses(api_url=api_url, api_key=api_key)
    )


def _refresh_zarr_runtime_statuses() -> None:
    api_url = str(_ss("api_url", DEFAULT_API_URL)).strip()
    api_key = str(_ss("api_key", DEFAULT_API_KEY)).strip()
    st.session_state.update(_collect_zarr_runtime_statuses(api_url=api_url, api_key=api_key))


def _raw_uri_candidates(raw_uri: str) -> set[str]:
    normalized = str(raw_uri or "").strip()
    candidates = {normalized}
    if normalized.startswith("/data/"):
        host_hint = _container_to_host_path_hint(normalized)
        if host_hint:
            candidates.add(str(host_hint).strip())
    else:
        host_hint = _container_to_host_path_hint(normalized)
        if host_hint:
            candidates.add(host_hint.strip())
    basename = Path(normalized).name if normalized else ""
    if basename:
        candidates.add(basename)
    return {item for item in candidates if item}


def _find_pipeline_job_for_raw_uri(api_url: str, api_key: str, raw_uri: str) -> str | None:
    target_candidates = _raw_uri_candidates(raw_uri)
    if not target_candidates:
        return None

    candidate_rows: list[dict[str, Any]] = []
    for page in range(1, 6):
        rows, _total = _list_jobs(
            api_url,
            api_key,
            updated_from=_recent_jobs_cutoff(24 * 30),
            page=page,
            page_size=200,
        )
        if not rows:
            break
        candidate_rows.extend(rows)
        if len(rows) < 200:
            break

    for row in candidate_rows:
        row_candidates = set()
        for value in list(row.get("raw_outputs") or []):
            row_candidates.update(_raw_uri_candidates(str(value)))
        for value in list(row.get("paths") or []):
            row_candidates.update(_raw_uri_candidates(str(value)))
        if target_candidates & row_candidates:
            return str(row.get("job_id") or "").strip() or None

    for row in candidate_rows[:50]:
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            continue
        try:
            response = _api_request("GET", api_url, f"/v1/jobs/{job_id}/result", api_key=api_key, timeout=30)
            if not response.ok:
                continue
            result_payload = response.json()
        except Exception:
            continue
        result_candidates = set()
        for value in list(result_payload.get("raw_outputs") or []):
            result_candidates.update(_raw_uri_candidates(str(value)))
        for value in list(result_payload.get("paths") or []):
            result_candidates.update(_raw_uri_candidates(str(value)))
        if target_candidates & result_candidates:
            return job_id
    return None


def _zarr_uri_candidates(zarr_uri: str) -> set[str]:
    normalized = str(zarr_uri or "").strip()
    candidates = {normalized}
    host_hint = _container_to_host_path_hint(normalized)
    if host_hint:
        candidates.add(str(host_hint).strip())
    basename = Path(normalized).name if normalized else ""
    if basename:
        candidates.add(basename)
    return {item for item in candidates if item}


def _find_pipeline_job_for_zarr_uri(
    api_url: str,
    api_key: str,
    zarr_uri: str,
    *,
    artifacts: list[dict[str, Any]] | None = None,
) -> str | None:
    target_candidates = _zarr_uri_candidates(zarr_uri)
    if not target_candidates:
        return None

    for item in artifacts or []:
        artifact_uri = str(item.get("artifact_uri") or "").strip()
        if artifact_uri and target_candidates & _zarr_uri_candidates(artifact_uri):
            for field_name in ("created_by_job_id", "source_job_id"):
                job_id = str(item.get(field_name) or "").strip()
                if job_id:
                    return job_id

    candidate_rows: list[dict[str, Any]] = []
    for page in range(1, 6):
        rows, _total = _list_jobs(
            api_url,
            api_key,
            updated_from=_recent_jobs_cutoff(24 * 30),
            page=page,
            page_size=200,
        )
        if not rows:
            break
        candidate_rows.extend(rows)
        if len(rows) < 200:
            break

    for row in candidate_rows:
        row_candidates = set()
        for value in list(row.get("zarr_outputs") or []):
            row_candidates.update(_zarr_uri_candidates(str(value)))
        if target_candidates & row_candidates:
            return str(row.get("job_id") or "").strip() or None

    for row in candidate_rows[:50]:
        job_id = str(row.get("job_id") or "").strip()
        if not job_id:
            continue
        try:
            response = _api_request("GET", api_url, f"/v1/jobs/{job_id}/result", api_key=api_key, timeout=30)
            if not response.ok:
                continue
            result_payload = response.json()
        except Exception:
            continue
        result_candidates = set()
        for value in list(result_payload.get("zarr_outputs") or []):
            result_candidates.update(_zarr_uri_candidates(str(value)))
        if target_candidates & result_candidates:
            return job_id
    return None


def _read_local_zarr_attributes(store_path: Path) -> dict[str, Any]:
    zarr_json_path = store_path / "zarr.json"
    if zarr_json_path.exists():
        try:
            payload = json.loads(zarr_json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if isinstance(payload, dict):
            return dict(payload.get("attributes") or {})
        return {}
    zattrs_path = store_path / ".zattrs"
    if zattrs_path.exists():
        try:
            payload = json.loads(zattrs_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if isinstance(payload, dict):
            return dict(payload)
    return {}


def _resolve_local_store_path(zarr_uri: str) -> Path | None:
    raw_value = str(zarr_uri or "").strip()
    if not raw_value:
        return None
    direct_path = Path(raw_value)
    if direct_path.exists():
        return direct_path
    host_hint = _container_to_host_path_hint(raw_value)
    if host_hint:
        host_path = Path(host_hint)
        if host_path.exists():
            return host_path
    return None


def _mask_quality_snapshot_from_backend(
    *,
    pipeline_metadata: dict[str, Any],
    conversion_metadata: dict[str, Any],
) -> dict[str, Any]:
    water = dict(conversion_metadata.get("water_mask") or pipeline_metadata.get("water_mask") or {})
    cloud = dict(conversion_metadata.get("cloud_mask") or pipeline_metadata.get("cloud_mask") or {})
    return {
        "cloud_backend": str(cloud.get("backend") or pipeline_metadata.get("backend") or "").strip(),
        "cloud_mask_source": str(cloud.get("mask_source") or "").strip(),
        "cloud_probability_source": str(cloud.get("probability_source") or "").strip(),
        "cloud_sensor_recipe": str(cloud.get("sensor_recipe") or cloud.get("sensor") or "").strip(),
        "cloud_includes_shadows": bool(
            cloud.get("include_shadows")
            if cloud.get("include_shadows") is not None
            else cloud.get("includes_shadows")
            if cloud.get("includes_shadows") is not None
            else pipeline_metadata.get("include_shadows", False)
        ),
        "cloud_shadow_fraction": float(
            cloud.get("shadow_fraction")
            or conversion_metadata.get("shadow_fraction")
            or pipeline_metadata.get("shadow_fraction")
            or 0.0
        ),
        "cloud_only_fraction": float(
            cloud.get("cloud_only_fraction")
            or conversion_metadata.get("cloud_only_fraction")
            or pipeline_metadata.get("cloud_only_fraction")
            or 0.0
        ),
        "cloud_fraction": float(
            cloud.get("cloud_fraction")
            or conversion_metadata.get("cloud_fraction")
            or pipeline_metadata.get("cloud_fraction")
            or 0.0
        ),
        "cloud_mask_path": str(cloud.get("mask_path") or "").strip(),
        "cloud_probability_path": str(cloud.get("probability_path") or "").strip(),
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
    }


def _mask_store_status(zarr_uri: str, *, mask_name: str) -> dict[str, Any]:
    raw_value = str(zarr_uri or "").strip()
    if not raw_value:
        return {"status": "unknown", "reason": "No Zarr store selected."}

    candidate_paths: list[Path] = []
    resolved_path = _resolve_local_store_path(raw_value)
    if resolved_path is not None:
        candidate_paths.append(resolved_path)

    if not candidate_paths:
        return {"status": "remote_or_unresolved", "reason": "Store path is remote or not mounted on this UI host."}

    store_path = candidate_paths[0]
    mask_group = store_path / "masks" / mask_name
    attrs = _read_local_zarr_attributes(store_path)
    attr_prefix = f"{mask_name}_mask"

    if mask_group.exists():
        return {
            "status": "written",
            "reason": str(attrs.get(f"{attr_prefix}_reason") or ""),
            "store_path": str(store_path),
            "mask_path": str(mask_group),
            "status_path": str(attrs.get(f"{attr_prefix}_status_path") or ""),
            "artifact_uri": str(attrs.get(f"{attr_prefix}_artifact_uri") or ""),
        }

    status_value = str(attrs.get(f"{attr_prefix}_status") or "").strip()
    if status_value:
        return {
            "status": status_value,
            "reason": str(attrs.get(f"{attr_prefix}_reason") or ""),
            "store_path": str(store_path),
            "mask_path": "",
            "status_path": str(attrs.get(f"{attr_prefix}_status_path") or ""),
            "artifact_uri": str(attrs.get(f"{attr_prefix}_artifact_uri") or ""),
        }

    return {
        "status": "not_written",
        "reason": "",
        "store_path": str(store_path),
        "mask_path": "",
        "status_path": "",
        "artifact_uri": "",
    }


def _water_mask_store_status(zarr_uri: str) -> dict[str, Any]:
    return _mask_store_status(zarr_uri, mask_name="water")


def _cloud_mask_store_status(zarr_uri: str) -> dict[str, Any]:
    return _mask_store_status(zarr_uri, mask_name="cloud")


def _normalize_mask_types(values: Any) -> tuple[str, ...]:
    raw_values = values
    if isinstance(raw_values, str):
        raw_values = [raw_values]
    normalized = {
        str(item).strip().lower()
        for item in list(raw_values or [])
        if str(item).strip().lower() in {"water", "cloud"}
    }
    ordered: list[str] = []
    for label in ("water", "cloud"):
        if label in normalized:
            ordered.append(label)
    return tuple(ordered)


def _mask_types_from_payload(payload: dict[str, Any]) -> tuple[str, ...]:
    metadata = dict(payload.get("metadata") or {})
    pipeline_metadata = dict(payload.get("pipeline_metadata") or {})
    conversion_metadata = dict(payload.get("conversion_metadata") or {})
    mask_payload = dict(metadata.get("mask") or {})
    values = (
        payload.get("mask_types")
        or metadata.get("mask_types")
        or pipeline_metadata.get("mask_types")
        or conversion_metadata.get("mask_types")
        or mask_payload.get("mask_types")
        or []
    )
    normalized = _normalize_mask_types(values)
    if normalized:
        return normalized
    inferred: set[str] = set()
    artifact_type = str(payload.get("artifact_type") or "").strip().lower()
    if artifact_type == "watermask":
        inferred.add("water")
    elif artifact_type == "cloudmask":
        inferred.add("cloud")
    water_payload = dict(
        conversion_metadata.get("water_mask")
        or pipeline_metadata.get("water_mask")
        or metadata.get("water_mask")
        or mask_payload.get("water_mask")
        or {}
    )
    cloud_payload = dict(
        conversion_metadata.get("cloud_mask")
        or pipeline_metadata.get("cloud_mask")
        or metadata.get("cloud_mask")
        or mask_payload.get("cloud_mask")
        or {}
    )
    if water_payload:
        inferred.add("water")
    if cloud_payload:
        inferred.add("cloud")
    return _normalize_mask_types(list(inferred))


def _mask_payload_matches_requested(payload: dict[str, Any], requested_mask_types: list[str]) -> bool:
    requested = _normalize_mask_types(requested_mask_types)
    if not requested:
        return True
    candidate = _mask_types_from_payload(payload)
    if not candidate:
        return False
    return set(requested).issubset(set(candidate))


def _payload_sort_key(payload: dict[str, Any]) -> tuple[float, float]:
    updated_at = _parse_iso_datetime(
        payload.get("updated_at")
        or payload.get("finished_at")
        or payload.get("started_at")
        or payload.get("created_at")
    )
    created_at = _parse_iso_datetime(payload.get("created_at"))
    updated_ts = updated_at.timestamp() if updated_at is not None else 0.0
    created_ts = created_at.timestamp() if created_at is not None else 0.0
    return (updated_ts, created_ts)


def _mask_request_key(source_zarr_uri: str, selected_mask_types: list[str]) -> str:
    normalized_source = str(source_zarr_uri or "").strip()
    normalized_mask_types = ",".join(_normalize_mask_types(selected_mask_types))
    return f"{normalized_source}::{normalized_mask_types}"


def _effective_manual_mask_status(
    *,
    selected_mask_types: list[str],
    water_store_status: dict[str, Any],
    cloud_store_status: dict[str, Any],
    latest_mask_artifacts: dict[str, Any],
) -> tuple[str, str]:
    derived_masked_zarr = dict(latest_mask_artifacts.get("masked_zarr") or {})
    derived_water = dict(latest_mask_artifacts.get("water_mask_raster") or {})
    derived_cloud = dict(latest_mask_artifacts.get("cloud_mask_raster") or {})

    per_type_status: dict[str, str] = {
        "water": "written"
        if (water_store_status.get("status") == "written" or bool(derived_water))
        else str(water_store_status.get("status") or "not_written").strip().lower(),
        "cloud": "written"
        if (cloud_store_status.get("status") == "written" or bool(derived_cloud))
        else str(cloud_store_status.get("status") or "not_written").strip().lower(),
    }

    requested = [item for item in selected_mask_types if item in {"water", "cloud"}]
    if not requested:
        return "unknown", ""

    if all(per_type_status[item] == "written" for item in requested):
        return "written", ""

    reasons: list[str] = []
    for item in requested:
        if per_type_status[item] in {"failed", "error", "cancelled"}:
            reason = str((water_store_status if item == "water" else cloud_store_status).get("reason") or "").strip()
            reasons.append(reason or f"{item} mask failed.")
    if reasons:
        return "failed", " ".join(reasons)

    if any(per_type_status[item] == "remote_or_unresolved" for item in requested):
        return "remote_or_unresolved", "Store path is remote or not mounted on this UI host."
    return "not_written", ""


def _latest_manual_mask_artifacts_for_source(
    api_url: str | None,
    api_key: str | None,
    *,
    source_zarr_uri: str,
    selected_mask_types: list[str],
    preferred_mask_job_id: str | None = None,
) -> dict[str, Any]:
    if not api_url or not source_zarr_uri:
        return {}
    items, _total = _list_artifacts(api_url, api_key, page=1, page_size=200)
    normalized_source = str(source_zarr_uri).strip()
    requested_mask_types = _normalize_mask_types(selected_mask_types)
    preferred_job = str(preferred_mask_job_id or "").strip()
    masked_candidates: list[dict[str, Any]] = []
    water_candidates: list[dict[str, Any]] = []
    cloud_candidates: list[dict[str, Any]] = []
    for item in items:
        item_type = str(item.get("artifact_type") or "").strip().lower()
        metadata = dict(item.get("metadata") or {})
        if metadata.get("runtime_exists") is False:
            continue
        item_source = str(item.get("source_uri") or "").strip()
        item_meta = metadata
        meta_source = str(item_meta.get("source_zarr_uri") or "").strip()
        if normalized_source not in {item_source, meta_source}:
            continue
        created_by_job_id = str(item.get("created_by_job_id") or "").strip()
        if preferred_job and created_by_job_id != preferred_job:
            continue
        if item_type == "zarr_masked":
            if _mask_payload_matches_requested(item, list(requested_mask_types)):
                masked_candidates.append(item)
        elif item_type == "watermask" and "water" in requested_mask_types:
            water_candidates.append(item)
        elif item_type == "cloudmask" and "cloud" in requested_mask_types:
            cloud_candidates.append(item)
    masked = max(masked_candidates, key=_payload_sort_key) if masked_candidates else None
    water_mask_raster = max(water_candidates, key=_payload_sort_key) if water_candidates else None
    cloud_mask_raster = max(cloud_candidates, key=_payload_sort_key) if cloud_candidates else None
    payload: dict[str, Any] = {}
    if masked:
        payload["masked_zarr"] = masked
        payload["status"] = "written"
    if water_mask_raster:
        payload["water_mask_raster"] = water_mask_raster
        payload.setdefault("status", "mask_only")
    if cloud_mask_raster:
        payload["cloud_mask_raster"] = cloud_mask_raster
        payload.setdefault("status", "mask_only")
    return payload


def _latest_mask_backend_snapshot(
    api_url: str | None,
    api_key: str | None,
    *,
    source_zarr_uri: str,
    source_job_id: str | None,
    selected_mask_types: list[str],
    preferred_mask_job_id: str | None = None,
) -> dict[str, Any]:
    if not api_url or not source_zarr_uri:
        return {}

    rows, _ = _list_jobs(
        api_url,
        api_key,
        updated_from=_recent_jobs_cutoff(RECENT_JOBS_WINDOW_HOURS),
        page=1,
        page_size=120,
    )
    normalized_source = str(source_zarr_uri).strip()
    normalized_source_job_id = str(source_job_id or "").strip()
    requested_mask_types = _normalize_mask_types(selected_mask_types)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not _job_is_mask_job(row):
            continue
        if requested_mask_types and not _mask_payload_matches_requested(row, list(requested_mask_types)):
            continue
        pipeline_meta = dict(row.get("pipeline_metadata") or {})
        conversion_meta = dict(row.get("conversion_metadata") or {})
        row_source_uri = str(
            pipeline_meta.get("source_zarr_uri")
            or conversion_meta.get("source_zarr_uri")
            or ""
        ).strip()
        row_source_job_id = str(
            row.get("source_job_id")
            or pipeline_meta.get("source_job_id")
            or conversion_meta.get("source_job_id")
            or ""
        ).strip()
        if normalized_source and row_source_uri == normalized_source:
            candidates.append(row)
            continue
        if normalized_source_job_id and row_source_job_id == normalized_source_job_id:
            candidates.append(row)

    if not candidates:
        return {}

    preferred_job = str(preferred_mask_job_id or "").strip()
    if preferred_job:
        preferred_candidates = [
            item for item in candidates if str(item.get("job_id") or "").strip() == preferred_job
        ]
        if preferred_candidates:
            candidates = preferred_candidates

    job_row = dict(max(candidates, key=_payload_sort_key) or {})
    job_id = str(job_row.get("job_id") or "").strip()
    result_payload: dict[str, Any] = {}
    if job_id:
        try:
            response = _api_request("GET", api_url, f"/v1/jobs/{job_id}/result", api_key=api_key, timeout=30)
            if response.ok:
                result_payload = dict(response.json() or {})
        except Exception:
            result_payload = {}

    pipeline_metadata = dict(result_payload.get("pipeline_metadata") or job_row.get("pipeline_metadata") or {})
    conversion_metadata = dict(result_payload.get("conversion_metadata") or job_row.get("conversion_metadata") or {})
    job_state = str(job_row.get("state") or "").strip().lower()
    masked_zarr_uri = str(
        result_payload.get("masked_zarr_uri")
        or result_payload.get("metadata", {}).get("masked_zarr_uri")
        or pipeline_metadata.get("masked_zarr_uri")
        or conversion_metadata.get("masked_zarr_uri")
        or ""
    ).strip()
    if job_state != "succeeded":
        masked_zarr_uri = ""
    return {
        "job": job_row,
        "result": result_payload,
        "pipeline_metadata": pipeline_metadata,
        "conversion_metadata": conversion_metadata,
        "quality": _mask_quality_snapshot_from_backend(
            pipeline_metadata=pipeline_metadata,
            conversion_metadata=conversion_metadata,
        ),
        "masked_zarr_uri": masked_zarr_uri,
        "water_mask": dict(conversion_metadata.get("water_mask") or pipeline_metadata.get("water_mask") or {}),
        "cloud_mask": dict(conversion_metadata.get("cloud_mask") or pipeline_metadata.get("cloud_mask") or {}),
    }


def _effective_manual_mask_status_from_backend(
    *,
    selected_mask_types: list[str],
    backend_snapshot: dict[str, Any],
    latest_mask_artifacts: dict[str, Any],
) -> tuple[str, str]:
    job_row = dict(backend_snapshot.get("job") or {})
    job_state = str(job_row.get("state") or "").strip().lower()
    requested = [item for item in selected_mask_types if item in {"water", "cloud"}]
    if job_row:
        if job_state in {"queued", "running", "cancel_requested"}:
            return job_state, ""
        if job_state == "cancelled":
            return "cancelled", "The latest mask job was cancelled."

        water_mask = dict(backend_snapshot.get("water_mask") or {})
        cloud_mask = dict(backend_snapshot.get("cloud_mask") or {})
        per_type_status = {
            "water": str(water_mask.get("status") or "not_written").strip().lower(),
            "cloud": str(cloud_mask.get("status") or "not_written").strip().lower(),
        }
        if requested and all(per_type_status[item] == "written" for item in requested):
            derived_uri = str(backend_snapshot.get("masked_zarr_uri") or "").strip()
            if derived_uri:
                return "written", ""

        reasons: list[str] = []
        for label, payload in (("water", water_mask), ("cloud", cloud_mask)):
            if label not in requested:
                continue
            status = str(payload.get("status") or "").strip().lower()
            reason = str(payload.get("reason") or "").strip()
            if status in {"failed", "error", "cancelled"}:
                reasons.append(reason or f"{label} mask failed.")
        if reasons or job_state == "failed":
            return "failed", " ".join(reasons) or "The latest mask job failed."
        return "not_written", ""

    derived_masked_zarr = dict(latest_mask_artifacts.get("masked_zarr") or {})
    per_type_status = {
        "water": "written" if latest_mask_artifacts.get("water_mask_raster") else "not_written",
        "cloud": "written" if latest_mask_artifacts.get("cloud_mask_raster") else "not_written",
    }
    if requested and all(per_type_status[item] == "written" for item in requested):
        return "written", ""

    if requested and any(per_type_status[item] == "written" for item in requested):
        return "mask_only", "Legacy mask artifacts exist, but no matching v2 backend job snapshot was found."
    return "not_written", ""


def _render_local_path_actions(*, title: str, path_value: str, open_label: str = "Open folder", copy_label: str = "Copy path") -> None:
    normalized = str(path_value or "").strip()
    if not normalized:
        return
    host_hint = _container_to_host_path_hint(normalized) or normalized
    target_path = Path(host_hint)
    open_target = target_path if target_path.is_dir() else target_path.parent
    open_url = f"file://{quote(str(open_target))}"
    safe_title = html.escape(title)
    components.html(
        f"""
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:4px 0 8px 0;">
          <div style="font-size:.78rem;color:#94a3b8;min-width:160px;">{safe_title}</div>
          <button onclick="navigator.clipboard.writeText({json.dumps(host_hint)})"
                  style="background:#0f172a;color:#e2e8f0;border:1px solid rgba(56,120,200,0.25);border-radius:10px;padding:8px 12px;cursor:pointer;">
            {html.escape(copy_label)}
          </button>
          <button onclick="window.open({json.dumps(open_url)}, '_blank')"
                  style="background:#0b2035;color:#7dd3fc;border:1px solid rgba(56,120,200,0.35);border-radius:10px;padding:8px 12px;cursor:pointer;">
            {html.escape(open_label)}
          </button>
          <div style="font-size:.72rem;color:#64748b;overflow-wrap:anywhere;">{html.escape(host_hint)}</div>
        </div>
        """,
        height=70,
    )


def _tracked_job_rows() -> list[dict[str, Any]]:
    tracked_ids: list[str] = []
    for source in ("active_job_ids", "known_job_ids"):
        for item in _ss(source, []):
            normalized = str(item).strip()
            if normalized and normalized not in tracked_ids:
                tracked_ids.append(normalized)
    if not tracked_ids:
        return []

    statuses = _refresh_job_statuses(
        _ss("api_url"),
        _ss("api_key"),
        tracked_ids[:50],
    )
    cache = dict(_ss("job_status_cache", {}))
    cache.update(statuses)
    st.session_state["job_status_cache"] = cache

    rows = [cache[job_id] for job_id in tracked_ids if job_id in cache]
    rows.sort(
        key=lambda row: (
            _status_reference_time(row)
            or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
        ).timestamp(),
        reverse=True,
    )
    return rows


def _ensure_job_results_loaded(statuses: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result_cache = dict(_ss("job_result_cache", {}))
    visible_succeeded_job_ids = [
        str(item.get("job_id"))
        for item in statuses
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
    return result_cache


def _render_job_cards(
    statuses: list[dict[str, Any]],
    *,
    result_cache: dict[str, dict[str, Any]],
    empty_message: str | None = None,
) -> None:
    if not statuses:
        if empty_message:
            st.info(empty_message)
        return

    for item in statuses[:100]:
        job_id = str(item.get("job_id", "unknown"))
        state = str(item.get("state", "unknown"))
        download_progress = float(item.get("progress", 0.0) or 0.0)
        pipeline_state = _job_pipeline_state(item)
        pipeline_progress = _job_pipeline_progress(item)
        duration = item.get("duration_seconds")
        errors = item.get("errors", []) or []
        error_summary = None
        if errors:
            first_error = str(errors[0]).strip()
            error_text = first_error.lower()
            if "504" in error_text:
                error_summary = "Copernicus download endpoint timed out (HTTP 504) before the first byte. Retry later."
            elif "catalogue.dataspace.copernicus.eu" in first_error and "503" in first_error:
                error_summary = "Copernicus catalogue is temporarily unavailable (HTTP 503). Retry later."
            elif len(first_error) > 240:
                error_summary = f"{first_error[:240]}..."
            else:
                error_summary = first_error
        with st.container(border=True):
            state_label, state_fg, state_bg, state_icon = _job_state_style(state)
            pipeline_label, pipeline_fg, pipeline_bg, pipeline_icon = _job_pipeline_style(item)
            provider_issue = _job_provider_issue_badge(item)
            job_kind = "mask" if _job_is_mask_job(item) else "fetch"
            if job_kind == "mask":
                kind_fg, kind_bg, kind_icon = "#c084fc", "rgba(192,132,252,0.16)", "▣"
            else:
                kind_fg, kind_bg, kind_icon = "#38bdf8", "rgba(56,189,248,0.14)", "⬇"
            badge_chunks = [
                f"<span style='display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;"
                f"font-size:.72rem;font-weight:700;color:{kind_fg};background:{kind_bg};text-transform:uppercase;'>{kind_icon} {job_kind}</span>",
                f"<span style='display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;"
                f"font-size:.72rem;font-weight:700;color:{state_fg};background:{state_bg};text-transform:uppercase;'>{state_icon} {state_label}</span>",
                f"<span style='display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;"
                f"font-size:.72rem;font-weight:700;color:{pipeline_fg};background:{pipeline_bg};text-transform:uppercase;'>{pipeline_icon} {pipeline_label}</span>"
            ]
            if provider_issue is not None:
                issue_label, issue_fg, issue_bg, issue_icon = provider_issue
                badge_chunks.append(
                    f"<span style='display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;"
                    f"font-size:.72rem;font-weight:700;color:{issue_fg};background:{issue_bg};text-transform:uppercase;'>{issue_icon} {issue_label}</span>"
                )
            st.markdown(
                "<div style='display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;'>"
                + "".join(badge_chunks)
                + "</div>",
                unsafe_allow_html=True,
            )
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
                st.metric("Pipeline", pipeline_label)
            with h4:
                st.metric("Duration", f"{float(duration):.1f}s" if duration is not None else "-")
            _render_pipeline_timeline(item)
            _render_job_progress_bar(pipeline_progress, _job_progress_visual_state(item))
            st.caption(
                f"Pipeline progress: {pipeline_progress:.2f}%"
                f" · Download progress: {download_progress:.2f}%"
                f" · Updated: {item.get('updated_at', '-')}"
            )
            pipeline_summary = _job_pipeline_summary(item)
            if pipeline_summary:
                st.caption(pipeline_summary)
            if job_kind == "mask":
                st.caption("Mask job: derived outputs are linked back to the source fetch job lineage.")
            pipeline_substate = _job_pipeline_substate(item)
            if pipeline_substate:
                st.caption(pipeline_substate)
            _render_job_pipeline_paths(item)
            if job_kind == "mask":
                mask_types = _normalize_mask_types(_mask_types_from_payload(item))
                masked_outputs = len(item.get("masked_zarr_outputs") or item.get("zarr_outputs") or [])
                water_outputs = len(item.get("watermask_outputs") or [])
                cloud_outputs = len(item.get("cloudmask_outputs") or [])
                st.caption(
                    f"Mask types: {', '.join(mask_types) or '-'}"
                    f" · Masked target outputs: {masked_outputs}"
                    f" · Water artifacts: {water_outputs}"
                    f" · Cloud artifacts: {cloud_outputs}"
                )
            else:
                st.caption(
                    f"Downloaded bytes: {int(item.get('bytes_downloaded', 0) or 0)} / "
                    f"{int(item.get('bytes_total', 0) or 0)}"
                )
            queued_reason = _job_queued_reason(item)
            if queued_reason:
                st.caption(f"Why queued: {queued_reason}")
            retry_details = _job_retry_details(item, provider_issue)
            if retry_details:
                st.caption(retry_details)
            if error_summary:
                st.error(error_summary)
                with st.expander("Error details", expanded=False):
                    for err in errors[:5]:
                        st.code(str(err), language="text")
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


def _job_error_texts(
    statuses: list[dict[str, Any]],
    provider_api: str | None,
    *,
    window_minutes: int | None = None,
) -> list[str]:
    if not provider_api:
        return []
    texts: list[str] = []
    now_utc = dt.datetime.now(dt.timezone.utc)
    for item in statuses:
        if str(item.get("provider", "")).strip().lower() != provider_api:
            continue
        if window_minutes is not None:
            ref = _status_reference_time(item)
            if ref is None:
                continue
            if (now_utc - ref) > dt.timedelta(minutes=max(1, window_minutes)):
                continue
        for err in item.get("errors", []) or []:
            text = str(err).strip()
            if text:
                texts.append(text.lower())
    return texts


def _job_state_style(state: str) -> tuple[str, str, str, str]:
    normalized = str(state or "unknown").strip().lower()
    if normalized == "running":
        return ("running", "#67e8f9", "rgba(34,211,238,0.14)", "▶")
    if normalized == "queued":
        return ("queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if normalized == "succeeded":
        return ("succeeded", "#4ade80", "rgba(74,222,128,0.14)", "✓")
    if normalized in {"failed", "cancelled"}:
        fg = "#f87171" if normalized == "failed" else "#c084fc"
        bg = "rgba(248,113,113,0.14)" if normalized == "failed" else "rgba(192,132,252,0.14)"
        icon = "✕" if normalized == "failed" else "■"
        return (normalized, fg, bg, icon)
    return (normalized or "unknown", "#94a3b8", "rgba(148,163,184,0.14)", "•")


def _job_pipeline_state(item: dict[str, Any]) -> str:
    pipeline_state = str(item.get("pipeline_state", "") or "").strip().lower()
    if pipeline_state:
        return pipeline_state
    state = str(item.get("state", "") or "").strip().lower()
    if _job_is_mask_job(item):
        if state == "succeeded" and (
            list(item.get("masked_zarr_outputs") or [])
            or list(item.get("zarr_outputs") or [])
        ):
            return "masked_zarr_written"
        return state or "queued"
    if state == "succeeded" and list(item.get("zarr_outputs") or []):
        return "zarr_written"
    return state or "queued"


def _job_pipeline_progress(item: dict[str, Any]) -> float:
    if item.get("pipeline_progress") is not None:
        return float(item.get("pipeline_progress", 0.0) or 0.0)
    return float(item.get("progress", 0.0) or 0.0)


def _job_pipeline_style(item: dict[str, Any]) -> tuple[str, str, str, str]:
    pipeline_state = _job_pipeline_state(item)
    if _job_is_mask_job(item):
        if pipeline_state == "resolving_source_zarr":
            return ("resolving source", "#93c5fd", "rgba(147,197,253,0.14)", "⌕")
        if pipeline_state == "running_cloud_inference":
            return ("cloud inference", "#38bdf8", "rgba(56,189,248,0.16)", "☁")
        if pipeline_state == "running_water_inference":
            return ("water inference", "#22d3ee", "rgba(34,211,238,0.16)", "≈")
        if pipeline_state == "writing_mask_artifacts":
            return ("writing masks", "#fbbf24", "rgba(251,191,36,0.16)", "✎")
        if pipeline_state == "writing_masked_zarr":
            return ("finalizing masks", "#38bdf8", "rgba(56,189,248,0.16)", "⬢")
        if pipeline_state == "registering_artifacts":
            return ("registering", "#fbbf24", "rgba(251,191,36,0.16)", "⚙")
        if pipeline_state == "masked_zarr_written":
            return ("mask ready", "#4ade80", "rgba(74,222,128,0.14)", "✓")
        if pipeline_state == "cancelled":
            return ("cancelled", "#c084fc", "rgba(192,132,252,0.14)", "■")
        if pipeline_state == "failed":
            return ("failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
        return ("queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if pipeline_state == "searching":
        return ("searching", "#93c5fd", "rgba(147,197,253,0.14)", "⌕")
    if pipeline_state == "downloading":
        return ("downloading", "#67e8f9", "rgba(34,211,238,0.14)", "↓")
    if pipeline_state == "downloaded":
        return ("download complete", "#86efac", "rgba(134,239,172,0.14)", "✓")
    if pipeline_state == "zarr_queued":
        return ("zarr queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if pipeline_state == "zarr_converting":
        return ("zarr converting", "#38bdf8", "rgba(56,189,248,0.16)", "⚙")
    if pipeline_state == "zarr_written":
        return ("zarr ready", "#4ade80", "rgba(74,222,128,0.14)", "⬢")
    if pipeline_state == "zarr_failed":
        return ("zarr failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    if pipeline_state == "cancelled":
        return ("cancelled", "#c084fc", "rgba(192,132,252,0.14)", "■")
    if pipeline_state == "failed":
        return ("failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    return ("queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")


def _job_progress_visual_state(item: dict[str, Any]) -> str:
    pipeline_state = _job_pipeline_state(item)
    if pipeline_state in {"failed", "zarr_failed"}:
        return "failed"
    if pipeline_state == "cancelled":
        return "cancelled"
    if pipeline_state in {"queued", "zarr_queued"}:
        return "queued"
    if pipeline_state in {"zarr_written", "masked_zarr_written"}:
        return "succeeded"
    return "running"


def _job_pipeline_summary(item: dict[str, Any]) -> str | None:
    pipeline_state = _job_pipeline_state(item)
    pipeline_step = str(item.get("pipeline_step", "") or "").strip().lower()
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    conversion_meta = dict(item.get("conversion_metadata") or {})
    if _job_is_mask_job(item):
        mask_types = _normalize_mask_types(
            item.get("mask_types")
            or pipeline_meta.get("mask_types")
            or conversion_meta.get("mask_types")
            or []
        )
        mask_label = " + ".join(mask_types) if mask_types else "mask"
        if pipeline_state == "queued":
            return f"Mask job queued for {mask_label} on an existing Zarr store."
        if pipeline_state == "resolving_source_zarr":
            return "Resolving the source Zarr and validating that it is eligible for masking."
        if pipeline_state == "running_cloud_inference":
            return "Running cloud and shadow inference directly on the selected Zarr store."
        if pipeline_state == "running_water_inference":
            return "Running water inference directly on the selected Zarr store."
        if pipeline_state == "writing_mask_artifacts":
            return "Finalizing cloud/water mask arrays and metadata inside the selected Zarr store."
        if pipeline_state == "writing_masked_zarr":
            return "Finalizing in-place mask metadata on the selected Zarr store."
        if pipeline_state == "registering_artifacts":
            return "Registering the completed mask job and updated Zarr in the backend."
        if pipeline_state == "masked_zarr_written":
            return "Masks are written and the selected Zarr is ready."
        if pipeline_state == "failed":
            return "Mask job failed before finalizing valid in-place masks."
        if pipeline_state == "cancelled":
            return "Mask job was cancelled."
        return None
    bytes_downloaded = int(item.get("bytes_downloaded", 0) or 0)
    raw_count = len(item.get("raw_outputs") or []) or int(pipeline_meta.get("raw_output_count", 0) or 0)
    zarr_count = len(item.get("zarr_outputs") or []) or int(pipeline_meta.get("zarr_output_count", 0) or 0)
    products_found = int(pipeline_meta.get("products_found", 0) or 0)
    suffix_parts: list[str] = []
    if raw_count:
        suffix_parts.append(f"raw outputs: {raw_count}")
    if zarr_count:
        suffix_parts.append(f"zarr outputs: {zarr_count}")
    suffix = f" ({' · '.join(suffix_parts)})" if suffix_parts else ""
    if pipeline_state == "searching":
        return "Searching the provider catalogue for matching products."
    if pipeline_state == "downloading":
        if products_found > 0 and bytes_downloaded <= 0:
            return f"Products were found. Waiting for the first download byte from the provider ({products_found} selected)."
        return f"Raw product download is in progress{suffix}."
    if pipeline_state == "downloaded":
        return f"Download complete. Preparing Zarr conversion{suffix}."
    if pipeline_state == "zarr_queued":
        return f"Download complete. Zarr conversion is queued{suffix}."
    if pipeline_state == "zarr_converting":
        if pipeline_step == "registering_artifact":
            return f"Download complete. Zarr data is written. Registering the store{suffix}."
        return f"Download complete. Zarr conversion is running{suffix}."
    if pipeline_state == "zarr_written":
        return f"Download complete. Zarr output is ready{suffix}."
    if pipeline_state == "zarr_failed":
        return f"Download completed, but Zarr conversion failed{suffix}."
    if pipeline_state == "failed":
        if products_found > 0 and bytes_downloaded <= 0:
            return f"Search succeeded ({products_found} products found), but the provider download failed before the first byte."
        return "Pipeline failed before reaching a usable Zarr output."
    if pipeline_state == "cancelled":
        return "Pipeline was cancelled."
    return None


def _job_pipeline_substate(item: dict[str, Any]) -> str | None:
    pipeline_state = _job_pipeline_state(item)
    pipeline_step = str(item.get("pipeline_step", "") or "").strip().lower()
    conversion_meta = dict(item.get("conversion_metadata") or {})
    if _job_is_mask_job(item):
        mask_types = _normalize_mask_types(
            item.get("mask_types")
            or dict(item.get("pipeline_metadata") or {}).get("mask_types")
            or conversion_meta.get("mask_types")
            or []
        )
        if pipeline_state == "queued":
            return f"Requested mask types: {', '.join(mask_types) or '-'}."
        if pipeline_state == "running_cloud_inference":
            return "Cloud inference is active. The selected Zarr will receive masks/cloud and masks/cloud_probability."
        if pipeline_state == "running_water_inference":
            return "Water inference is active. The selected Zarr will receive masks/water and masks/water_probability."
        if pipeline_state == "writing_masked_zarr":
            return "Consolidating metadata and finalizing the selected Zarr after in-place mask writes."
        return None
    current_index = int(conversion_meta.get("current_index", 0) or 0)
    total = int(conversion_meta.get("total", 0) or 0)
    item_suffix = f" ({current_index}/{total})" if current_index > 0 and total > 0 else ""
    if pipeline_state == "zarr_queued":
        return f"Waiting for the worker to start Zarr conversion{item_suffix}."
    if pipeline_state == "zarr_converting":
        if pipeline_step == "writing_chunks":
            return f"Writing chunks into the Zarr store{item_suffix}."
        if pipeline_step == "registering_artifact":
            return f"Registering the Zarr artifact in the backend{item_suffix}."
        return f"Zarr conversion is active{item_suffix}."
    if pipeline_state == "downloaded":
        return "Raw product is available locally. The backend is about to start the Zarr stage."
    return None


def _job_pipeline_paths(item: dict[str, Any]) -> tuple[str | None, str | None]:
    if _job_is_mask_job(item):
        pipeline_meta = dict(item.get("pipeline_metadata") or {})
        conversion_meta = dict(item.get("conversion_metadata") or {})
        source_uri = str(
            pipeline_meta.get("source_zarr_uri")
            or conversion_meta.get("source_zarr_uri")
            or ""
        ).strip()
        masked_uri = str(
            pipeline_meta.get("masked_zarr_uri")
            or conversion_meta.get("masked_zarr_uri")
            or ""
        ).strip()
        if not masked_uri:
            masked_outputs = list(item.get("masked_zarr_outputs") or item.get("zarr_outputs") or [])
            if masked_outputs:
                masked_uri = str(masked_outputs[-1]).strip()
        return (source_uri or None, masked_uri or None)
    conversion_meta = dict(item.get("conversion_metadata") or {})
    raw_uri = str(conversion_meta.get("current_raw_uri") or "").strip()
    if not raw_uri:
        raw_outputs = list(item.get("raw_outputs") or [])
        if raw_outputs:
            raw_uri = str(raw_outputs[0]).strip()
    zarr_uri = str(conversion_meta.get("current_output_uri") or "").strip()
    if not zarr_uri:
        zarr_outputs = list(item.get("zarr_outputs") or [])
        if zarr_outputs:
            zarr_uri = str(zarr_outputs[-1]).strip()
    return (raw_uri or None, zarr_uri or None)


def _render_job_pipeline_paths(item: dict[str, Any]) -> None:
    raw_uri, zarr_uri = _job_pipeline_paths(item)
    if not raw_uri and not zarr_uri:
        return
    lines: list[str] = []
    if _job_is_mask_job(item):
        if raw_uri:
            lines.append(f"Source Zarr: {Path(raw_uri).name}")
        if zarr_uri:
            label = "Target Zarr"
            if raw_uri and Path(raw_uri).name == Path(zarr_uri).name:
                label = "Target Zarr (same store)"
            lines.append(f"{label}: {Path(zarr_uri).name}")
    else:
        if raw_uri:
            lines.append(f"Source raw: {Path(raw_uri).name}")
        if zarr_uri:
            lines.append(f"Zarr target: {Path(zarr_uri).name}")
    st.code("\n".join(lines), language="text")


def _render_pipeline_timeline(item: dict[str, Any]) -> None:
    pipeline_state = _job_pipeline_state(item)
    state = str(item.get("state", "") or "").strip().lower()
    if _job_is_mask_job(item):
        mask_types = _normalize_mask_types(_mask_types_from_payload(item))
        steps = [
            ("resolving_source_zarr", "Resolve"),
        ]
        if "cloud" in mask_types:
            steps.append(("running_cloud_inference", "Cloud"))
        if "water" in mask_types:
            steps.append(("running_water_inference", "Water"))
        steps.extend(
            [
                ("writing_mask_artifacts", "Artifacts"),
                ("registering_artifacts", "Register"),
                ("masked_zarr_written", "Ready"),
            ]
        )
    else:
        steps = [
            ("searching", "Search"),
            ("downloading", "Download"),
            ("downloaded", "Downloaded"),
            ("zarr_queued", "Zarr queued"),
            ("zarr_converting", "Convert"),
            ("zarr_written", "Ready"),
        ]
    order = {key: idx for idx, (key, _label) in enumerate(steps)}
    anchor_state = pipeline_state
    if pipeline_state == "queued":
        anchor_state = steps[0][0]
    elif pipeline_state == "failed":
        anchor_state = "running_water_inference" if _job_is_mask_job(item) and "running_water_inference" in order else steps[max(0, len(steps) - 2)][0]
    elif pipeline_state == "zarr_failed":
        anchor_state = "zarr_converting"
    elif pipeline_state == "cancelled":
        anchor_state = steps[0][0] if _job_is_mask_job(item) else "downloading"
    anchor_index = order.get(anchor_state, 0)
    chips: list[str] = []
    for idx, (key, label) in enumerate(steps):
        if state == "succeeded" and pipeline_state == "zarr_written":
            status_kind = "done" if idx <= anchor_index else "pending"
        elif pipeline_state in {"failed", "zarr_failed"} and idx == anchor_index:
            status_kind = "failed"
        elif idx < anchor_index:
            status_kind = "done"
        elif idx == anchor_index:
            status_kind = "current"
        else:
            status_kind = "pending"
        if status_kind == "done":
            fg, bg, border = "#4ade80", "rgba(74,222,128,0.12)", "rgba(74,222,128,0.28)"
        elif status_kind == "current":
            fg, bg, border = "#38bdf8", "rgba(56,189,248,0.12)", "rgba(56,189,248,0.32)"
        elif status_kind == "failed":
            fg, bg, border = "#f87171", "rgba(248,113,113,0.12)", "rgba(248,113,113,0.28)"
        else:
            fg, bg, border = "#94a3b8", "rgba(148,163,184,0.08)", "rgba(148,163,184,0.14)"
        chips.append(
            f"<span style='display:inline-flex;align-items:center;justify-content:center;"
            f"padding:4px 10px;border-radius:999px;border:1px solid {border};"
            f"background:{bg};color:{fg};font-size:.72rem;font-weight:600;'>{label}</span>"
        )
    st.markdown(
        "<div style='display:flex;gap:8px;flex-wrap:wrap;margin:6px 0 8px 0;'>"
        + "".join(chips)
        + "</div>",
        unsafe_allow_html=True,
    )


def _job_progress_color(state: str) -> str:
    normalized = str(state or "unknown").strip().lower()
    if normalized == "running":
        return "#22d3ee"
    if normalized == "queued":
        return "#fbbf24"
    if normalized == "succeeded":
        return "#4ade80"
    if normalized == "failed":
        return "#f87171"
    if normalized == "cancelled":
        return "#c084fc"
    return "#64748b"


def _render_job_progress_bar(progress: float, state: str) -> None:
    width_pct = max(0.0, min(100.0, float(progress or 0.0)))
    bar_color = _job_progress_color(state)
    st.markdown(
        f"""
        <div style="height:8px;background:#1f2937;border-radius:999px;overflow:hidden;margin:6px 0 4px 0;">
          <div style="height:100%;width:{width_pct:.2f}%;background:{bar_color};border-radius:999px;transition:width .2s ease;"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _job_queued_reason(item: dict[str, Any]) -> str | None:
    if str(item.get("state", "")).strip().lower() != "queued":
        return None

    provider_api = str(item.get("provider", "")).strip().lower()
    worker_snapshot = _ss("worker_status_snapshot")
    if not isinstance(worker_snapshot, dict) or worker_snapshot.get("_error"):
        return "worker status is unavailable."

    workers_alive = int(worker_snapshot.get("workers_alive", 0) or 0)
    capacity_available = int(worker_snapshot.get("capacity_available", 0) or 0)
    queued_jobs = int(worker_snapshot.get("queued_jobs", 0) or 0)
    running_jobs = int(worker_snapshot.get("running_jobs", 0) or 0)

    if workers_alive <= 0:
        return "no worker is alive."

    provider_state = (worker_snapshot.get("provider_capacity") or {}).get(provider_api) or {}
    if isinstance(provider_state, dict) and bool(provider_state.get("blocked_by_limit")):
        running = int(provider_state.get("running", 0) or 0)
        limit_total = int(provider_state.get("limit_total", 0) or 0)
        queued = int(provider_state.get("queued", 0) or 0)
        return f"{provider_api} provider throttle is reached ({running}/{limit_total} running, {queued} queued for this provider)."

    if capacity_available <= 0:
        return f"all worker slots are busy ({running_jobs} running, {queued_jobs} queued)."

    return "waiting in the queue to be picked by an available worker."


def _job_provider_issue_badge(item: dict[str, Any]) -> tuple[str, str, str, str] | None:
    provider_api = str(item.get("provider", "")).strip().lower()
    if not provider_api:
        return None

    error_texts = [str(err).strip().lower() for err in (item.get("errors", []) or []) if str(err).strip()]
    if any(
        "503" in text
        or "504" in text
        or "service unavailable" in text
        or "temporarily unavailable" in text
        or "gateway timeout" in text
        for text in error_texts
    ):
        return ("provider unavailable", "#f87171", "rgba(248,113,113,0.14)", "⚠")
    if any("429" in text or "retry later" in text or "rate limit" in text or "too many requests" in text for text in error_texts):
        return ("provider retrying", "#fbbf24", "rgba(251,191,36,0.16)", "↻")

    worker_snapshot = _ss("worker_status_snapshot")
    if isinstance(worker_snapshot, dict):
        provider_state = (worker_snapshot.get("provider_capacity") or {}).get(provider_api) or {}
        if bool(provider_state.get("blocked_by_limit")) and str(item.get("state", "")).strip().lower() == "queued":
            return ("provider throttled", "#38bdf8", "rgba(56,189,248,0.16)", "⏸")
    return None


def _job_retry_details(
    item: dict[str, Any],
    provider_issue: tuple[str, str, str, str] | None,
) -> str | None:
    if provider_issue is None or provider_issue[0] != "provider retrying":
        return None
    retry_count = int(item.get("retry_count", 0) or 0)
    last_retry_at = item.get("last_retry_at")
    if retry_count <= 0 and not last_retry_at:
        return None
    parts: list[str] = []
    if retry_count > 0:
        parts.append(f"Retry count: {retry_count}")
    if last_retry_at:
        parts.append(f"Last retry: {last_retry_at}")
    return " · ".join(parts)


def _job_is_mask_job(item: dict[str, Any]) -> bool:
    job_kind = str(item.get("job_kind") or "").strip().lower()
    if job_kind == "mask":
        return True
    job_type = str(item.get("job_type") or "").strip().lower()
    if job_type == "mask_existing_zarr":
        return True
    if len(list(item.get("watermask_outputs") or [])) > 0:
        return True
    if len(list(item.get("cloudmask_outputs") or [])) > 0:
        return True
    conversion_meta = dict(item.get("conversion_metadata") or {})
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    for meta in (conversion_meta, pipeline_meta):
        for key in ("masked_zarr_uri", "water_mask_output_zarr_uri", "water_mask_artifact_uri", "cloud_mask_output_zarr_uri", "cloud_mask_artifact_uri"):
            value = str(meta.get(key) or "").strip()
            if value:
                return True
    return False


def _provider_runtime_badge(
    provider_api: str | None,
    statuses: list[dict[str, Any]],
) -> tuple[str, str, str]:
    if not provider_api:
        return ("unknown", "Provider state unavailable.", "#64748b")

    provider_label = provider_api.capitalize()
    worker_snapshot = _ss("worker_status_snapshot")
    provider_state = {}
    if isinstance(worker_snapshot, dict):
        provider_state = (worker_snapshot.get("provider_capacity") or {}).get(provider_api) or {}

    error_texts = _job_error_texts(
        statuses,
        provider_api,
        window_minutes=PROVIDER_ISSUE_WINDOW_MINUTES,
    )
    if any(
        "503" in text
        or "504" in text
        or "service unavailable" in text
        or "temporarily unavailable" in text
        or "gateway timeout" in text
        for text in error_texts
    ):
        return ("unavailable", f"{provider_label} is temporarily unavailable.", "#ef4444")
    if any(
        "429" in text or "retry later" in text or "rate limit" in text or "too many requests" in text
        for text in error_texts
    ):
        return ("retrying", f"{provider_label} is retrying after transient provider errors.", "#f59e0b")
    if isinstance(provider_state, dict) and bool(provider_state.get("blocked_by_limit")):
        limit_total = int(provider_state.get("limit_total", 0) or 0)
        running = int(provider_state.get("running", 0) or 0)
        return ("throttled", f"{provider_label} is at provider limit ({running}/{limit_total}).", "#38bdf8")
    return ("healthy", f"{provider_label} is healthy.", "#22c55e")


def _render_provider_runtime_badge(provider_api: str | None, statuses: list[dict[str, Any]]) -> None:
    state, detail, color = _provider_runtime_badge(provider_api, statuses)
    st.markdown(
        f"""
        <div style="display:inline-flex;align-items:center;gap:8px;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:999px;padding:6px 12px;margin:8px 0 2px 0;">
          <span style="font-size:.72rem;font-weight:700;color:{color};text-transform:uppercase;">{state}</span>
          <span style="font-size:.72rem;color:#cbd5e1;">{detail}</span>
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
    provider_scope_value = str(_ss("dl_job_provider_filter", "all"))
    collection_filter_value = str(_ss("dl_job_collection_filter", "")).strip()
    product_filter_value = str(_ss("dl_job_product_filter", "")).strip()
    job_query_value = str(_ss("dl_job_id_query", "")).strip()
    scoped_statuses = _filter_jobs_by_scope(
        statuses,
        provider=_provider_scope_value(provider_scope_value, None),
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
    st.caption("This panel separates fetch jobs and mask jobs and keeps live cancel controls in one place.")
    _render_provider_runtime_badge(download_provider_api, scoped_statuses)

    active_ids = [
        str(item.get("job_id", "")).strip()
        for item in scoped_statuses
        if str(item.get("state", "")).strip().lower() in ACTIVE_JOB_STATES
        and str(item.get("job_id", "")).strip()
    ]
    cancel_clicked = False
    if active_ids:
        cancel_clicked = st.button(
            f"⏹ Cancel active jobs ({len(active_ids)})",
            width="stretch",
            key="jobs_cancel_active_btn",
        )
    else:
        st.caption("No active jobs in the current scope.")

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
        active_filter_count = sum(
            1
            for value in (
                str(_ss("dl_job_provider_filter", "all")) != "all",
                bool(str(_ss("dl_job_collection_filter", "")).strip()),
                bool(str(_ss("dl_job_product_filter", "")).strip()),
                bool(str(_ss("dl_job_id_query", "")).strip()),
            )
            if value
        )
        refresh_jobs_clicked = False
        expander_label = "Advanced job filters"
        if active_filter_count:
            expander_label = f"Advanced job filters ({active_filter_count})"
        with st.expander(expander_label, expanded=False):
            adv1, adv2, adv3, adv4, adv5 = st.columns([1, 1, 1, 1, 0.8])
            with adv1:
                provider_scope = st.selectbox(
                    "Provider filter",
                    options=["all", "copernicus", "usgs"],
                    key="dl_job_provider_filter",
                    format_func=lambda value: {
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
        provider=_provider_scope_value(provider_scope, None),
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
    fetch_visible_statuses = [row for row in visible_statuses if not _job_is_mask_job(row)]
    mask_visible_statuses = [row for row in visible_statuses if _job_is_mask_job(row)]
    st.caption("Changing provider, mission, or product in the form only affects new submissions.")
    visible_total = len(base_visible_statuses)
    if job_view == "recent":
        st.caption(f"Showing jobs updated in the last {RECENT_JOB_CATEGORY_MINUTES} minutes inside the recent activity window.")
    st.caption(
        f"Showing {len(visible_statuses)} / {visible_total} matching jobs "
        f"(fetch: {len(fetch_visible_statuses)} · mask: {len(mask_visible_statuses)})."
    )
    if refresh_jobs_clicked:
        st.caption("Jobs list refreshed.")

    if cancel_clicked:
        cancelled = 0
        for job_id in active_ids:
            try:
                response = _api_request(
                    "DELETE",
                    _ss("api_url"),
                    f"/v1/jobs/{job_id}",
                    api_key=_ss("api_key"),
                    timeout=30,
                )
                if response.ok and bool(response.json().get("cancel_requested")):
                    cancelled += 1
            except Exception:
                continue
        st.info(f"Cancel requested for {cancelled}/{len(active_ids)} active jobs in the current scope.")

    result_cache = _ensure_job_results_loaded(visible_statuses)
    mask_header = f"Mask jobs ({len(mask_visible_statuses)})"
    with st.expander(mask_header, expanded=bool(mask_visible_statuses)):
        _render_job_cards(
            mask_visible_statuses,
            result_cache=result_cache,
            empty_message="No mask jobs match the selected filter.",
        )
    fetch_header = f"Fetch jobs ({len(fetch_visible_statuses)})"
    with st.expander(fetch_header, expanded=bool(fetch_visible_statuses) and not bool(mask_visible_statuses)):
        _render_job_cards(
            fetch_visible_statuses,
            result_cache=result_cache,
            empty_message="No fetch jobs match the selected filter.",
        )


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
            or _ss("provider_status_snapshot") is None
        ):
            _refresh_api_runtime_statuses()
        if st.button("Refresh service status", width="stretch", key="refresh_service_status_btn"):
            _refresh_api_runtime_statuses()
        st.caption(f"Last checked: {_format_status_timestamp(_ss('service_status_checked_at'))}")
        _render_status_block("API health", _ss("api_health_snapshot"), kind="service")
        _render_status_block("API readiness", _ss("api_readiness_snapshot"), kind="service")
        _render_status_block("Worker execution", _ss("worker_status_snapshot"), kind="worker")
        selected_provider_api = PROVIDER_CLI_MAP.get(str(_ss("provider", "Copernicus")))
        selected_provider_status = select_provider_status(_ss("provider_status_snapshot"), selected_provider_api or "")
        if selected_provider_status is not None:
            status_label = provider_auth_state_label(selected_provider_status)
            detail = str(selected_provider_status.get("message") or "-")
            st.markdown(
                "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;margin-top:8px;'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;'><span style='font-size:.78rem;color:#94a3b8;font-weight:600;'>{str(selected_provider_status.get('provider') or '').upper()} auth</span>"
                f"<span style='font-size:.72rem;color:{'#22c55e' if status_label == 'valid' else '#ef4444' if status_label in {'missing', 'credentials invalid', 'credentials missing'} else '#f59e0b'};font-weight:700;text-transform:uppercase;'>{status_label}</span></div>"
                f"<div style='font-size:.72rem;color:#cbd5e1;margin-top:6px;'>{detail}</div>"
                f"<div style='font-size:.65rem;color:#64748b;margin-top:4px;'>Runtime env · username present: {'yes' if selected_provider_status.get('username_present') else 'no'} · token present: {'yes' if selected_provider_status.get('token_present') or selected_provider_status.get('password_present') else 'no'}</div>"
                "</div>",
                unsafe_allow_html=True,
            )

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

    tab_map, tab_dl, tab_mask, tab_jobs, tab_res, tab_set = st.tabs(
        ["🗺️ Map", "⬇️ Download", "🧪 Mask", "📋 Jobs", "📂 Results", "🔧 Settings"]
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
        download_provider_api = PROVIDER_CLI_MAP.get(provider)
        provider_status_snapshot = _ss("provider_status_snapshot")
        current_provider_status = select_provider_status(provider_status_snapshot, download_provider_api or "")
        provider_blocked = provider_actions_disabled(download_provider_api or "", provider_status_snapshot)
        provider_guidance = provider_action_guidance(download_provider_api or "", provider_status_snapshot)

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
            st.session_state["preview_error_kind"] = ""
            st.session_state["preview_error_detail"] = ""
            st.session_state["preview_fetched"] = False

        pr1, pr2 = st.columns([2, 1])
        with pr1:
            st.markdown(
                '<div style="font-weight:600;font-size:.84rem;color:#e2e8f0;">Products Preview</div>',
                unsafe_allow_html=True,
            )
        with pr2:
            refresh_preview = st.button(
                "🔎 Refresh Preview",
                width="stretch",
                key="refresh_preview",
                disabled=provider_blocked,
            )

        if provider == "USGS" and current_provider_status is not None:
            auth_message = str(current_provider_status.get("message") or "")
            if provider_blocked:
                st.error(auth_message or "USGS runtime authentication is blocking preview and download.")
                if provider_guidance:
                    st.caption(provider_guidance)
            elif auth_message:
                st.success(auth_message)

        if provider_blocked:
            blocked_preview = provider_preview_error_payload(download_provider_api or "", provider_status_snapshot)
            st.session_state["preview_items"] = []
            st.session_state["preview_total"] = 0
            st.session_state["preview_error"] = blocked_preview["error"]
            st.session_state["preview_error_kind"] = blocked_preview["error_kind"]
            st.session_state["preview_error_detail"] = blocked_preview["error_detail"]
            st.session_state["preview_fetched"] = True

        auto_preview = bool(preview_wkt) and not bool(_ss("preview_fetched", False)) and not provider_blocked
        if refresh_preview or auto_preview:
            prev = preview_products_cached(
                api_url=_ss("api_url"),
                api_key=_ss("api_key"),
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
            st.session_state["preview_error_kind"] = prev.get("error_kind", "")
            st.session_state["preview_error_detail"] = prev.get("error_detail", "")
            st.session_state["preview_fetched"] = True
        if preview_wkt:
            st.caption("Preview refreshes automatically when AOI, dates, provider, or product change.")

        if _ss("preview_error"):
            error_kind = str(_ss("preview_error_kind", "") or "")
            error_text = str(_ss("preview_error", "") or "")
            error_detail = str(_ss("preview_error_detail", "") or "")
            if error_kind in {"credentials_invalid", "credentials_missing"}:
                st.error(f"Preview: {error_text}")
            elif error_kind == "provider_unavailable":
                st.warning(f"Preview: {error_text}")
            else:
                st.info(f"Preview: {error_text}")
            if error_detail and error_detail != error_text:
                with st.expander("Preview error details", expanded=False):
                    st.code(error_detail, language="text")
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

        d1, d2 = st.columns([2, 1])
        with d1:
            start_clicked = st.button(
                "🚀 Start Download",
                width="stretch",
                type="primary",
                disabled=provider_blocked,
            )
        with d2:
            st.markdown(
                "<div style='background:#0f172a;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:12px;'>"
                "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Monitoring</div>"
                "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Use the Jobs tab</div>"
                "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>Live job progress, fetch/mask separation, and cancel controls are shown there.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        with st.expander("Advanced controls", expanded=False):
            st.caption("Reset clears UI state only. Unlock releases the local tracker if it gets stuck.")
            adv1, adv2 = st.columns(2)
            with adv1:
                reset_clicked = st.button("🗑️ Reset UI state", width="stretch")
            with adv2:
                unlock_clicked = st.button("🔓 Unlock tracker", width="stretch")

        if bool(_ss("dl_auto_refresh", True)):
            checked_at = _parse_iso_datetime(_ss("service_status_checked_at"))
            if (
                _ss("last_api_status_url", "") != str(_ss("api_url", DEFAULT_API_URL)).strip()
                or _ss("worker_status_snapshot") is None
                or checked_at is None
                or (dt.datetime.now(dt.timezone.utc) - checked_at).total_seconds() >= 8.0
            ):
                _refresh_api_runtime_statuses()

        worker_snapshot = _ss("worker_status_snapshot")
        if not isinstance(worker_snapshot, dict) or worker_snapshot.get("_error"):
            st.warning("Worker status unavailable. If jobs stay queued, refresh service status in Connection.")
        else:
            workers_alive = int(worker_snapshot.get("workers_alive", 0) or 0)
            queued_jobs = int(worker_snapshot.get("queued_jobs", 0) or 0)
            running_jobs = int(worker_snapshot.get("running_jobs", 0) or 0)
            capacity_available = int(worker_snapshot.get("capacity_available", 0) or 0)
            capacity_total = int(worker_snapshot.get("capacity_total", 0) or 0)
            if workers_alive <= 0:
                st.error("No worker alive. Jobs will stay queued until the worker service is running.")
            elif queued_jobs > 0 and capacity_available <= 0:
                st.info(
                    f"Worker saturated: {running_jobs} running, {queued_jobs} queued, {capacity_available}/{capacity_total} slots free."
                )
            else:
                st.caption(
                    f"Worker alive: {workers_alive} · running: {running_jobs} · queued: {queued_jobs} · free slots: {capacity_available}/{capacity_total}"
                )
            provider_capacity = worker_snapshot.get("provider_capacity") or {}
            provider_state = provider_capacity.get(download_provider_api or "")
            if isinstance(provider_state, dict) and download_provider_api:
                provider_limit_total = int(provider_state.get("limit_total", 0) or 0)
                provider_running = int(provider_state.get("running", 0) or 0)
                provider_queued = int(provider_state.get("queued", 0) or 0)
                provider_available = int(provider_state.get("available", 0) or 0)
                provider_label = download_provider_api.capitalize()
                if bool(provider_state.get("blocked_by_limit")):
                    st.info(
                        f"{provider_label} provider limit reached: {provider_running}/{provider_limit_total} running for this provider, {provider_queued} queued waiting on the provider throttle."
                    )
                else:
                    st.caption(
                        f"{provider_label} provider limit: {provider_running}/{provider_limit_total} used · queued for provider: {provider_queued} · free provider slots: {provider_available}"
                    )
            pruned_workers = int(worker_snapshot.get("workers_pruned", 0) or 0)
            if pruned_workers > 0:
                st.caption(f"Cleaned {pruned_workers} stale worker heartbeat(s) automatically.")
        if provider_blocked and provider_guidance:
            st.warning(f"Download blocked: {provider_guidance}")

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

        st.caption("Download tab is launch-only. Live activity is monitored in the Jobs tab.")

    with tab_jobs:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📋</span><span style="font-weight:600;font-size:.94rem;">Jobs</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("Fetch jobs cover search/download/Zarr conversion. Mask jobs cover derived masking actions on existing Zarr stores.")
        if bool(_ss("dl_auto_refresh", True)):
            _render_download_jobs_panel_live(download_provider_api)
        else:
            _render_download_jobs_panel_static(download_provider_api)

    with tab_mask:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🧪</span><span style="font-weight:600;font-size:.94rem;">Mask</span></div>',
            unsafe_allow_html=True,
        )
        st.caption(
            "The automatic pipeline stops at Zarr. Each launch here creates a separate mask job, trackable in `📋 Jobs`, and writes the requested masks directly into the selected Zarr store."
        )

        summary1, summary2, summary3 = st.columns(3)
        with summary1:
            st.markdown(
                "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:14px;'>"
                "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Step 1</div>"
                "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Choose a Zarr store</div>"
                "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>Pick an existing store registered by the pipeline or discovered on disk.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        with summary2:
            st.markdown(
                "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:14px;'>"
                "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Step 2</div>"
                "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Review context</div>"
                "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>Confirm the scene, provider, job lineage, and local mask status before running.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        with summary3:
            st.markdown(
                "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:14px;'>"
                "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Step 3</div>"
                "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Launch tracked mask job</div>"
                "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>The action submits a separate mask job that writes masks directly into the selected Zarr instead of restarting the fetch pipeline.</div>"
                "</div>",
                unsafe_allow_html=True,
            )

        mask_mode = st.radio(
            "Mask mode",
            options=["water", "cloud", "water + cloud"],
            horizontal=True,
            key="mask_mode",
            format_func=lambda value: {
                "water": "Water",
                "cloud": "Cloud",
                "water + cloud": "Water + cloud",
            }[value],
        )
        cloud_include_shadows = True
        st.info(
            "Mask runs now use the single model-first mask service. The UI no longer exposes backend or threshold choices. "
            "Choose only the mask mode: `water`, `cloud`, or `water + cloud`."
        )
        if mask_mode in {"water", "water + cloud"}:
            st.caption(
                "Water writes `masks/water` and `masks/water_probability` directly into the selected Zarr. "
                "In combined runs, cloud/shadow obstruction is applied before water."
            )
        if mask_mode in {"cloud", "water + cloud"}:
            st.caption(
                "Cloud writes `masks/cloud` as the final obstruction mask and `masks/cloud_probability` as a debug/confidence layer. "
                "Cloud shadows are included automatically in the final cloud obstruction mask."
            )

        st.markdown("---")
        w1, w2, w3 = st.columns([2, 2, 3])
        with w1:
            artifact_provider = st.selectbox(
                "Zarr provider",
                options=["", "copernicus", "usgs"],
                key="watermask_artifact_provider",
                format_func=lambda value: "All providers" if not value else value,
            )
        with w2:
            artifact_collection = st.text_input(
                "Zarr mission",
                key="watermask_artifact_collection",
                placeholder="e.g. SENTINEL-2",
            ).strip()
        with w3:
            artifact_query = st.text_input(
                "Zarr path / scene search",
                key="watermask_artifact_query",
                placeholder="scene id or store path fragment",
            ).strip()
        show_legacy_stores = st.checkbox(
            "Show legacy or unregistered local Zarr stores",
            value=bool(_ss("show_legacy_watermask_zarr", False)),
            key="show_legacy_watermask_zarr",
            help="Legacy stores are visible for inspection, but manual masking works best on stores linked to a backend job.",
        )

        artifacts, artifacts_total = _list_artifacts(
            _ss("api_url"),
            _ss("api_key"),
            artifact_type="zarr",
            provider=artifact_provider or None,
            collection=artifact_collection or None,
            uri_query=artifact_query or None,
            include_local=True,
            page=1,
            page_size=120,
        )
        visible_artifacts, hidden_legacy_count = _filter_visible_artifacts(
            artifacts,
            include_legacy=show_legacy_stores,
        )

        if not visible_artifacts:
            st.info("No existing Zarr store was found in the registry or on local disk.")
        else:
            caption = f"Showing {len(visible_artifacts)} / {artifacts_total} Zarr stores."
            if hidden_legacy_count:
                caption += f" Hidden legacy stores: {hidden_legacy_count}."
            st.caption(caption)

            store_options = [str(item["artifact_uri"]) for item in visible_artifacts]
            selected_store = st.selectbox(
                "Existing Zarr store",
                options=store_options,
                index=0,
                key="watermask_selected_store",
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
            selected_store_uri = str(selected_store_meta.get("artifact_uri") or "").strip()
            selected_host_hint = _container_to_host_path_hint(selected_store_uri)
            selected_job_id = _find_pipeline_job_for_zarr_uri(
                _ss("api_url"),
                _ss("api_key"),
                selected_store_uri,
                artifacts=visible_artifacts,
            )
            selected_mask_types = ["water", "cloud"] if mask_mode == "water + cloud" else [mask_mode]
            mask_request_key = _mask_request_key(selected_store_uri, selected_mask_types)
            preferred_mask_job_id = str(
                dict(_ss("preferred_mask_job_ids", {})).get(mask_request_key) or ""
            ).strip()
            backend_mask_snapshot = _latest_mask_backend_snapshot(
                _ss("api_url"),
                _ss("api_key"),
                source_zarr_uri=selected_store_uri,
                source_job_id=selected_job_id,
                selected_mask_types=selected_mask_types,
                preferred_mask_job_id=preferred_mask_job_id,
            )
            latest_mask_artifacts = _latest_manual_mask_artifacts_for_source(
                _ss("api_url"),
                _ss("api_key"),
                source_zarr_uri=selected_store_uri,
                selected_mask_types=selected_mask_types,
                preferred_mask_job_id=str(dict(backend_mask_snapshot.get("job") or {}).get("job_id") or "").strip(),
            )
            scene_id = str(selected_store_meta.get("scene_id") or Path(selected_store_uri).stem).strip()
            provider_api = str(selected_store_meta.get("provider") or "").strip()
            collection_api = str(selected_store_meta.get("collection") or "").strip()
            product_type = str((selected_store_meta.get("metadata") or {}).get("product_type") or "").strip()
            masked_store_meta = dict(latest_mask_artifacts.get("masked_zarr") or {})
            water_mask_raster_meta = dict(latest_mask_artifacts.get("water_mask_raster") or {})
            cloud_mask_raster_meta = dict(latest_mask_artifacts.get("cloud_mask_raster") or {})
            backend_water_mask = dict(backend_mask_snapshot.get("water_mask") or {})
            backend_cloud_mask = dict(backend_mask_snapshot.get("cloud_mask") or {})
            derived_status_value, derived_status_reason = _effective_manual_mask_status_from_backend(
                selected_mask_types=selected_mask_types,
                backend_snapshot=backend_mask_snapshot,
                latest_mask_artifacts=latest_mask_artifacts,
            )
            derived_masked_zarr_uri = str(
                masked_store_meta.get("artifact_uri")
                or backend_mask_snapshot.get("masked_zarr_uri")
                or ""
            ).strip()
            masked_store_label = (
                f"{derived_masked_zarr_uri} (same source store)"
                if derived_masked_zarr_uri and derived_masked_zarr_uri == selected_store_uri
                else (derived_masked_zarr_uri or "-")
            )
            derived_water_mask_artifact_uri = str(water_mask_raster_meta.get("artifact_uri") or "").strip()
            derived_cloud_mask_artifact_uri = str(cloud_mask_raster_meta.get("artifact_uri") or "").strip()
            quality_snapshot = dict(backend_mask_snapshot.get("quality") or {})
            backend_job_state = str(dict(backend_mask_snapshot.get("job") or {}).get("state") or "").strip().lower()

            meta_cols = st.columns(4)
            with meta_cols[0]:
                st.metric("Provider", provider_api or "-")
            with meta_cols[1]:
                st.metric("Collection", collection_api or "-")
            with meta_cols[2]:
                st.metric("Bands", len(selected_store_meta.get("band_names") or []))
            with meta_cols[3]:
                st.metric("Mask status", derived_status_value or "unknown")

            details_lines = [
                f"Scene: {scene_id or '-'}",
                f"Container path: {selected_store_uri or '-'}",
                f"Host path: {selected_host_hint or '-'}",
                f"Associated job: {selected_job_id or '-'}",
                f"Visibility status: {selected_store_meta.get('_visibility_status', _artifact_visibility_status(selected_store_meta))}",
                f"Data family: {selected_store_meta.get('data_family', '-')}",
                f"Dimensions: {', '.join([str(v) for v in (selected_store_meta.get('dimensions') or [])]) or '-'}",
                f"Shape: {selected_store_meta.get('shape', []) or '-'}",
                f"Bands: {', '.join([str(v) for v in (selected_store_meta.get('band_names') or [])]) or '-'}",
                f"Water status: {backend_water_mask.get('status') or '-'}",
                f"Cloud status: {backend_cloud_mask.get('status') or '-'}",
                f"Target Zarr store: {masked_store_label}",
                f"Updated: {selected_store_meta.get('updated_at', '-')}",
                f"Size: {_human_size(selected_store_meta.get('size_bytes'))}",
            ]
            st.code("\n".join(details_lines), language="text")

            if derived_status_value == "written":
                st.success("The selected Zarr already has the requested masks recorded by the latest mask job.")
            elif derived_status_value in {"queued", "running", "cancel_requested"}:
                st.info(
                    f"The latest mask job is `{derived_status_value}`. "
                    "Track progress in `📋 Jobs`."
                )
            elif derived_status_value == "mask_only":
                st.warning(derived_status_reason or "Legacy mask artifacts exist, but no matching v2 backend job snapshot was found.")
            elif derived_status_value in {"failed", "error"}:
                st.error(derived_status_reason or "The last selected mask attempt failed for this store.")
            else:
                st.info("No in-place mask result is currently recorded for this source store.")

            submission_notice = dict(_ss("mask_submission_notice", {}))
            if (
                str(submission_notice.get("source_zarr_uri") or "").strip() == selected_store_uri
                and _normalize_mask_types(list(submission_notice.get("mask_types") or [])) == _normalize_mask_types(selected_mask_types)
            ):
                submitted_job_id = str(submission_notice.get("job_id") or "").strip()
                submitted_state = str(submission_notice.get("job_state") or "queued").strip().lower() or "queued"
                st.success(
                    f"Mask job `{submitted_job_id}` submitted for `{Path(selected_store_uri).name}`. "
                    "Track progress in `📋 Jobs`."
                )
                st.code(
                    "\n".join(
                        [
                            f"Source Zarr: {selected_store_uri}",
                            f"Mask types: {', '.join(selected_mask_types)}",
                            f"Job id: {submitted_job_id or '-'}",
                            f"Job state: {submitted_state or '-'}",
                        ]
                    ),
                    language="text",
                )
                st.session_state["mask_submission_notice"] = {}

            if quality_snapshot:
                diag_cols = st.columns(4)
                with diag_cols[0]:
                    st.metric("Cloud engine", quality_snapshot.get("cloud_backend") or "-")
                with diag_cols[1]:
                    st.metric("Cloud/shadow", f"{float(quality_snapshot.get('cloud_fraction', 0.0)) * 100:.1f}%")
                with diag_cols[2]:
                    st.metric("Shadow fraction", f"{float(quality_snapshot.get('cloud_shadow_fraction', 0.0)) * 100:.1f}%")
                with diag_cols[3]:
                    st.metric("Water fraction", f"{float(quality_snapshot.get('water_fraction', 0.0)) * 100:.1f}%")
                quality_lines = [
                    f"Cloud engine used: {quality_snapshot.get('cloud_backend') or '-'}",
                    f"Cloud includes shadows: {'yes' if quality_snapshot.get('cloud_includes_shadows') else 'no'}",
                    f"Cloud mask source: {quality_snapshot.get('cloud_mask_source') or '-'}",
                    f"Cloud probability source: {quality_snapshot.get('cloud_probability_source') or '-'}",
                    f"Cloud sensor recipe: {quality_snapshot.get('cloud_sensor_recipe') or '-'}",
                    f"Cloud mask layer: {quality_snapshot.get('cloud_mask_path') or '-'}",
                    f"Cloud confidence/debug layer: {quality_snapshot.get('cloud_probability_path') or '-'}",
                    f"Cloud-only fraction: {float(quality_snapshot.get('cloud_only_fraction', 0.0)) * 100:.1f}%",
                    f"Shadow fraction: {float(quality_snapshot.get('cloud_shadow_fraction', 0.0)) * 100:.1f}%",
                    f"Water runtime: {quality_snapshot.get('water_runtime_mode') or '-'}",
                    f"Water probability/debug layer: {quality_snapshot.get('water_probability_path') or '-'}",
                    f"Water threshold used: {quality_snapshot.get('water_threshold_used') if quality_snapshot.get('water_threshold_used') is not None else '-'}",
                    f"Water sensor recipe: {quality_snapshot.get('water_sensor_recipe') or '-'}",
                ]
                st.code("\n".join(quality_lines), language="text")

            if (
                backend_water_mask.get("mask_path")
                or backend_cloud_mask.get("mask_path")
                or derived_masked_zarr_uri
            ):
                extra_lines = [
                    f"Latest mask job: {str(dict(backend_mask_snapshot.get('job') or {}).get('job_id') or '-').strip() or '-'}",
                    f"Latest mask job state: {backend_job_state or '-'}",
                    f"Water mask layer: {backend_water_mask.get('mask_path') or '-'}",
                    f"Cloud mask layer: {backend_cloud_mask.get('mask_path') or '-'}",
                    f"Target Zarr store: {masked_store_label}",
                ]
                st.code("\n".join(extra_lines), language="text")
                if derived_masked_zarr_uri:
                    _render_local_path_actions(
                        title="Target Zarr store",
                        path_value=derived_masked_zarr_uri,
                        open_label="Open Zarr folder",
                        copy_label="Copy Zarr path",
                    )

            action_col1, action_col2 = st.columns([3, 2])
            with action_col1:
                action_label = "Create mask job"
                if derived_status_value == "written":
                    action_label = "Re-run mask job"
                run_mask = st.button(
                    action_label,
                    type="primary",
                    width="stretch",
                    disabled=not selected_job_id,
                )
            with action_col2:
                st.markdown(
                    "<div style='background:#0f172a;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:12px;'>"
                    "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Execution mode</div>"
                    "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Separate tracked mask job</div>"
                    "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>This action creates a separate mask job and writes the requested masks directly into the selected Zarr.</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )

            if not selected_job_id:
                st.warning(
                    "This Zarr store is not linked to a known backend job yet. "
                    "Use a registered pipeline output to run manual masking."
                )

            if run_mask:
                try:
                    mask_endpoint = f"/v1/jobs/{selected_job_id}/mask"
                    payload = {
                        "zarr_uri": selected_store_uri,
                        "scene_id": scene_id or None,
                        "product_type": product_type or None,
                        "mask_types": selected_mask_types,
                        "overwrite": True,
                    }
                    if "cloud" in selected_mask_types:
                        payload["include_shadows"] = bool(cloud_include_shadows)
                    response = _api_request(
                        "POST",
                        _ss("api_url"),
                        mask_endpoint,
                        api_key=_ss("api_key"),
                        payload=payload,
                        timeout=1800,
                    )
                    if response.ok:
                        body = response.json()
                        manual_job_id = str(body.get("job_id") or selected_job_id)
                        cache = dict(_ss("job_status_cache", {}))
                        cache[manual_job_id] = dict(body.get("job") or {})
                        st.session_state["job_status_cache"] = cache
                        _upsert_known_jobs([manual_job_id], active_job_ids=[manual_job_id])
                        body_mask_types = [
                            str(item).strip().lower()
                            for item in list(body.get("mask_types") or selected_mask_types)
                        ]
                        job_state = str((body.get("job") or {}).get("state") or "").strip().lower()
                        preferred_jobs = dict(_ss("preferred_mask_job_ids", {}))
                        preferred_jobs[mask_request_key] = manual_job_id
                        st.session_state["preferred_mask_job_ids"] = preferred_jobs
                        st.session_state["mask_submission_notice"] = {
                            "source_zarr_uri": selected_store_uri,
                            "mask_types": body_mask_types,
                            "job_id": manual_job_id,
                            "job_state": job_state or "queued",
                        }
                        st.rerun()
                    else:
                        st.error(f"{response.status_code}: {_response_error_message(response)}")
                except Exception as exc:
                    st.error(str(exc))

        st.markdown("---")
        with st.expander("Recent Zarr pipeline outputs", expanded=False):
            recent_rows, _ = _list_jobs(
                _ss("api_url"),
                _ss("api_key"),
                updated_from=_recent_jobs_cutoff(RECENT_JOBS_WINDOW_HOURS),
                page=1,
                page_size=40,
            )
            zarr_rows = [
                row for row in recent_rows
                if list(row.get("zarr_outputs") or [])
                or str(row.get("pipeline_state") or "") in {"zarr_written", "zarr_failed"}
            ]
            if not zarr_rows:
                st.info("No recent Zarr-producing pipeline job was found.")
            else:
                for item in zarr_rows[:20]:
                    with st.container(border=True):
                        st.markdown(f"**{item.get('job_id', '-') }**")
                        st.caption(
                            f"{item.get('provider', '-')}/{item.get('collection', '-')}"
                            f" · pipeline state: {item.get('pipeline_state', '-')}"
                        )
                        if item.get("zarr_outputs"):
                            for output_path in list(item.get("zarr_outputs") or [])[:5]:
                                st.code(str(output_path), language="text")

    with tab_res:
        render_results_tab(api_url=_ss("api_url"), api_key=_ss("api_key"))

    with tab_set:
        render_settings_tab(
            skey=skey,
            api_url=_ss("api_url"),
            downloads_dir=DOWNLOADS_DIR,
            map_center=st.session_state["map_center"],
            map_zoom=st.session_state["map_zoom"],
            provider_status_snapshot=_ss("provider_status_snapshot"),
        )


if __name__ == "__main__":
    main()
