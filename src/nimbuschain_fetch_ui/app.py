
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

from nimbuschain_fetch.pipeline_timeline import (
    advance_pipeline_timeline,
    refresh_pipeline_timeline,
)
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
    load_country_catalog,
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
from nimbuschain_fetch_ui.orchestrator_tab import render_pipeline_plan_summary
from nimbuschain_fetch_ui.results_tab import render_results_tab
from nimbuschain_fetch_ui.runtime_status import (
    format_status_timestamp as _format_status_timestamp,
    refresh_api_runtime_statuses as _collect_api_runtime_statuses,
    refresh_zarr_runtime_statuses as _collect_zarr_runtime_statuses,
    render_download_coordinator_dashboard as _render_download_coordinator_dashboard,
    render_status_block as _render_status_block,
)
from nimbuschain_fetch_ui.settings_tab import render_settings_tab
from nimbuschain_fetch_ui.styling import CUSTOM_CSS
from nimbuschain_fetch_ui.timeline_display import (
    display_pipeline_stages as _display_pipeline_stages,
    display_stage_key as _display_stage_key,
    _stage_error_summary,
)
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
    inspect_local_zarr_store as _inspect_local_zarr_store,
    requested_mask_state as _requested_mask_state,
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
    mask_types: list[str] | None = None,
    download_strategy: str | None = None,
    cube_mode: str | None = None,
    cube_start_date: dt.date | None = None,
    cube_end_date: dt.date | None = None,
    cube_layout: str | None = None,
    cube_target_crs: str | None = None,
    cube_target_resolution_m: int | None = None,
    cube_overlap_policy: str | None = None,
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
        mask_types=mask_types,
        download_strategy=download_strategy,
        cube_mode=cube_mode,
        cube_start_date=cube_start_date,
        cube_end_date=cube_end_date,
        cube_layout=cube_layout,
        cube_target_crs=cube_target_crs,
        cube_target_resolution_m=cube_target_resolution_m,
        cube_overlap_policy=cube_overlap_policy,
    )


def _preview_available_dates(items: list[dict[str, Any]]) -> list[dt.date]:
    available: set[dt.date] = set()
    for item in list(items or []):
        raw = str(item.get("sensing_time") or "").strip()
        if not raw:
            continue
        try:
            available.add(dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).date())
        except Exception:
            continue
    return sorted(available)




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
        "country_continent_filter": "All",
        "country_name": "",
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
        "download_coordinator_snapshot": None,
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


def _default_tile_system_for_provider(
    provider_label: str,
    sat_tiles: dict[str, Any],
) -> str | None:
    desired = "landsat" if str(provider_label or "").strip() == "USGS" else "sentinel-2"
    if sat_tiles.get(desired, {}).get("tiles") is None:
        return None
    return desired


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


def _ensure_api_runtime_statuses(*, force: bool = False, max_age_seconds: float | None = None) -> None:
    current_api_url = str(_ss("api_url", DEFAULT_API_URL)).strip()
    snapshot_keys = (
        "api_health_snapshot",
        "api_readiness_snapshot",
        "worker_status_snapshot",
        "download_coordinator_snapshot",
        "provider_status_snapshot",
    )
    snapshots_missing = any(_ss(key) is None for key in snapshot_keys)
    checked_at = _parse_iso_datetime(_ss("service_status_checked_at"))
    stale = False
    if max_age_seconds is not None:
        stale = checked_at is None or (
            dt.datetime.now(dt.timezone.utc) - checked_at
        ).total_seconds() >= max_age_seconds
    if (
        force
        or _ss("last_api_status_url", "") != current_api_url
        or snapshots_missing
        or stale
    ):
        _refresh_api_runtime_statuses()


def _provider_status_color(status_label: str) -> str:
    normalized = str(status_label or "").strip().lower()
    if normalized == "valid":
        return "#22c55e"
    if normalized in {"missing", "credentials invalid", "credentials missing"}:
        return "#ef4444"
    return "#f59e0b"


def _render_provider_auth_panel(selected_provider_api: str | None) -> dict[str, Any] | None:
    selected_provider_status = select_provider_status(
        _ss("provider_status_snapshot"),
        selected_provider_api or "",
    )
    if selected_provider_status is None:
        st.caption("Provider auth snapshot unavailable.")
        return None

    status_label = provider_auth_state_label(selected_provider_status)
    detail = str(selected_provider_status.get("message") or "-")
    pool_line = ""
    if str(selected_provider_status.get("provider") or "").strip().lower() == "copernicus":
        pool_line = (
            f" · account pool: "
            f"{int(selected_provider_status.get('account_pool_size', 0) or 0)} account(s)"
            f" · per-account concurrency: "
            f"{int(selected_provider_status.get('account_pool_concurrency', 0) or 0)}"
        )
    st.markdown(
        "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;margin-top:8px;'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;'><span style='font-size:.78rem;color:#94a3b8;font-weight:600;'>{str(selected_provider_status.get('provider') or '').upper()} auth</span>"
        f"<span style='font-size:.72rem;color:{_provider_status_color(status_label)};font-weight:700;text-transform:uppercase;'>{status_label}</span></div>"
        f"<div style='font-size:.72rem;color:#cbd5e1;margin-top:6px;'>{detail}</div>"
        f"<div style='font-size:.65rem;color:#64748b;margin-top:4px;'>Runtime env · username present: {'yes' if selected_provider_status.get('username_present') else 'no'} · token present: {'yes' if selected_provider_status.get('token_present') or selected_provider_status.get('password_present') else 'no'}{pool_line}</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    return selected_provider_status


def _render_download_runtime_panel(download_provider_api: str | None) -> None:
    refresh_clicked = st.button(
        "Refresh runtime status",
        key="downloads_runtime_refresh_btn",
        width="stretch",
    )
    _ensure_api_runtime_statuses(
        force=refresh_clicked,
        max_age_seconds=8.0 if bool(_ss("dl_auto_refresh", True)) else None,
    )
    st.caption(f"Last checked: {_format_status_timestamp(_ss('service_status_checked_at'))}")

    row1 = st.columns(2)
    with row1[0]:
        _render_status_block("API health", _ss("api_health_snapshot"), kind="service")
    with row1[1]:
        _render_status_block("API readiness", _ss("api_readiness_snapshot"), kind="service")
    row2 = st.columns(2)
    with row2[0]:
        _render_status_block("Worker execution", _ss("worker_status_snapshot"), kind="worker")
    with row2[1]:
        _render_status_block("Download coordinator", _ss("download_coordinator_snapshot"), kind="coordinator")

    selected_provider_status = _render_provider_auth_panel(download_provider_api)
    provider_status_snapshot = _ss("provider_status_snapshot")
    provider_guidance = provider_action_guidance(download_provider_api or "", provider_status_snapshot)
    provider_blocked = provider_actions_disabled(download_provider_api or "", provider_status_snapshot)

    worker_snapshot = _ss("worker_status_snapshot")
    if not isinstance(worker_snapshot, dict) or worker_snapshot.get("_error"):
        st.warning("Worker status unavailable. If jobs stay queued, refresh service status in Connection.")
        if provider_blocked and provider_guidance:
            st.warning(f"Download blocked: {provider_guidance}")
        return

    workers_alive = int(worker_snapshot.get("workers_alive", 0) or 0)
    queued_jobs = int(worker_snapshot.get("queued_jobs", 0) or 0)
    running_jobs = int(worker_snapshot.get("running_jobs", 0) or 0)
    capacity_available = int(worker_snapshot.get("capacity_available", 0) or 0)
    capacity_total = int(worker_snapshot.get("capacity_total", 0) or 0)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Workers alive", workers_alive)
    metric_cols[1].metric("Running jobs", running_jobs)
    metric_cols[2].metric("Queued jobs", queued_jobs)
    metric_cols[3].metric("Free slots", f"{capacity_available}/{capacity_total}")

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
    elif selected_provider_status is None:
        st.caption("Provider auth guidance unavailable for the selected provider.")


def _render_download_coordinator_panel() -> None:
    refresh_clicked = st.button(
        "Refresh coordinator",
        key="downloads_coordinator_refresh_btn",
        width="stretch",
    )
    _ensure_api_runtime_statuses(
        force=refresh_clicked,
        max_age_seconds=8.0 if bool(_ss("dl_auto_refresh", True)) else None,
    )
    coordinator_snapshot = _ss("download_coordinator_snapshot")
    if isinstance(coordinator_snapshot, dict) and not coordinator_snapshot.get("_error"):
        _render_download_coordinator_dashboard(coordinator_snapshot)
    else:
        st.warning("Download coordinator snapshot unavailable.")


def _render_downloads_overview(download_provider_api: str | None) -> None:
    worker_snapshot = _ss("worker_status_snapshot")
    coordinator_snapshot = _ss("download_coordinator_snapshot")
    coordinator_summary = {}
    if isinstance(coordinator_snapshot, dict):
        coordinator_summary = dict(coordinator_snapshot.get("summary") or {})

    jobs = dict(coordinator_summary.get("jobs") or {})
    machine = dict(coordinator_summary.get("machine") or {})
    providers = dict(coordinator_summary.get("providers") or {})

    workers_alive = int(worker_snapshot.get("workers_alive", 0) or 0) if isinstance(worker_snapshot, dict) else 0
    running_jobs = int(worker_snapshot.get("running_jobs", 0) or 0) if isinstance(worker_snapshot, dict) else 0
    active_downloads = int(machine.get("active_downloads", 0) or 0)
    pending_tasks = int(jobs.get("pending_tasks_total", 0) or 0)

    summary_cols = st.columns(4)
    summary_cols[0].metric("Workers", workers_alive)
    summary_cols[1].metric("Running jobs", running_jobs)
    summary_cols[2].metric("Active downloads", active_downloads)
    summary_cols[3].metric("Pending tasks", pending_tasks)

    if download_provider_api:
        provider_summary = dict(providers.get(download_provider_api) or {})
        provider_counts = dict(provider_summary.get("counts") or {})
        provider_label = download_provider_api.capitalize()
        st.caption(
            f"{provider_label} snapshot · pending {int(provider_summary.get('pending_tasks', 0) or 0)}"
            f" · downloading {int(provider_counts.get('downloading', 0) or 0)}"
            f" · ready {int(provider_counts.get('ready', 0) or 0)}"
            f" · failed {int(provider_counts.get('failed', 0) or 0)}"
        )


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


def _store_mask_layers_label(store_summary: dict[str, Any]) -> str:
    primary_masks = list(store_summary.get("primary_masks") or [])
    debug_layers = list(store_summary.get("debug_layers") or [])
    if not primary_masks and not debug_layers:
        return "none"
    labels = list(primary_masks)
    for layer_name in debug_layers:
        if layer_name not in labels:
            labels.append(layer_name)
    return ", ".join(labels)


def _mask_request_target_label(mask_types: list[str] | tuple[str, ...]) -> str:
    normalized = _normalize_mask_types(mask_types)
    if normalized == ("water", "cloud"):
        return "water + cloud"
    if normalized == ("water",):
        return "water"
    if normalized == ("cloud",):
        return "cloud"
    return "mask"


def _mask_mode_to_types(mask_mode: str) -> list[str]:
    normalized = str(mask_mode or "").strip().lower()
    if normalized == "water + cloud":
        return ["water", "cloud"]
    if normalized in {"water", "cloud"}:
        return [normalized]
    return []


def _mask_store_sort_key(entry: dict[str, Any]) -> tuple[int, int, float]:
    requested_status = str(dict(entry.get("requested_state") or {}).get("status") or "").strip().lower()
    visibility_status = str(entry.get("visibility_status") or "").strip().lower()
    updated_at = _parse_iso_datetime(entry.get("updated_at"))
    updated_ts = updated_at.timestamp() if updated_at is not None else 0.0
    requested_rank = {
        "not_written": 0,
        "partial": 1,
        "remote_or_unresolved": 2,
        "written": 3,
        "unknown": 4,
    }.get(requested_status, 5)
    visibility_rank = {"current": 0, "local": 1, "legacy": 2}.get(visibility_status, 3)
    return (requested_rank, visibility_rank, -updated_ts)


def _mask_store_option_label(entry: dict[str, Any]) -> str:
    scene_id = str(entry.get("scene_id") or entry.get("store_name") or "-").strip()
    requested_label = str(dict(entry.get("requested_state") or {}).get("label") or "Unknown").strip()
    visibility_status = str(entry.get("visibility_status") or "").strip().upper() or "CURRENT"
    tracking_label = "tracked" if entry.get("associated_job_id") else "untracked"
    band_count = int(entry.get("band_count") or 0)
    mask_layers = _store_mask_layers_label(entry)
    return (
        f"[{requested_label.upper()}] {scene_id}"
        f" | {entry.get('provider', '-')}/{entry.get('collection', '-')}"
        f" | {band_count} bands"
        f" | masks: {mask_layers}"
        f" | {visibility_status}"
        f" | {tracking_label}"
    )


def _mask_types_from_payload(payload: dict[str, Any]) -> tuple[str, ...]:
    metadata = dict(payload.get("metadata") or {})
    pipeline_metadata = dict(payload.get("pipeline_metadata") or {})
    conversion_metadata = dict(payload.get("conversion_metadata") or {})
    request_payload = dict(payload.get("request") or {})
    mask_payload = dict(metadata.get("mask") or {})
    pipeline_mask_payload = dict(pipeline_metadata.get("mask") or {})
    conversion_mask_payload = dict(conversion_metadata.get("mask") or {})
    request_mask_payload = dict(request_payload.get("mask") or {})
    values = (
        payload.get("mask_types")
        or request_payload.get("mask_types")
        or metadata.get("mask_types")
        or pipeline_metadata.get("mask_types")
        or conversion_metadata.get("mask_types")
        or request_mask_payload.get("mask_types")
        or pipeline_mask_payload.get("mask_types")
        or conversion_mask_payload.get("mask_types")
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
        or conversion_mask_payload.get("water_mask")
        or pipeline_mask_payload.get("water_mask")
        or metadata.get("water_mask")
        or mask_payload.get("water_mask")
        or {}
    )
    cloud_payload = dict(
        conversion_metadata.get("cloud_mask")
        or pipeline_metadata.get("cloud_mask")
        or conversion_mask_payload.get("cloud_mask")
        or pipeline_mask_payload.get("cloud_mask")
        or metadata.get("cloud_mask")
        or mask_payload.get("cloud_mask")
        or {}
    )
    if water_payload:
        inferred.add("water")
    if cloud_payload:
        inferred.add("cloud")
    return _normalize_mask_types(list(inferred))


def _cube_mode_from_payload(payload: dict[str, Any]) -> str:
    pipeline_metadata = dict(payload.get("pipeline_metadata") or {})
    request_payload = dict(payload.get("request") or {})
    timeline_payload = payload.get("pipeline_timeline")
    if not isinstance(timeline_payload, dict):
        timeline_payload = pipeline_metadata.get("timeline")
    if not isinstance(timeline_payload, dict):
        timeline_payload = {}
    candidate = str(
        payload.get("cube_mode")
        or request_payload.get("cube_mode")
        or pipeline_metadata.get("cube_mode")
        or timeline_payload.get("cube_mode")
        or "none"
    ).strip().lower() or "none"
    if candidate in {"before_mask", "after_mask"}:
        return candidate
    return "none"


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


def _format_bytes_compact(value: Any) -> str:
    return _human_size(max(0, int(value or 0)))


def _format_rate_compact(value: Any) -> str:
    try:
        numeric = float(value or 0.0)
    except Exception:
        numeric = 0.0
    if numeric <= 0:
        return "-"
    return f"{_human_size(int(numeric))}/s"


def _format_eta_compact(value: Any) -> str:
    try:
        seconds = int(float(value))
    except Exception:
        return "-"
    if seconds <= 0:
        return "<1m"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _format_eta(value: Any) -> str:
    try:
        seconds = int(float(value))
    except Exception:
        return "-"
    if seconds <= 0:
        return "done"
    return _format_eta_compact(seconds)


def _format_duration_compact(value: Any) -> str:
    try:
        seconds = max(0, int(round(float(value))))
    except Exception:
        return "-"
    if seconds <= 0:
        return "<1s"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _download_strategy_label(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "copernicus_account_pool":
        return "Account pool"
    if normalized == "adaptive_local":
        return "Adaptive local"
    if not normalized:
        return "Default"
    return normalized.replace("_", " ").title()


def _download_status_style(status: str) -> tuple[str, str]:
    normalized = str(status or "").strip().lower()
    if normalized == "completed":
        return ("#22c55e", "rgba(34,197,94,0.18)")
    if normalized == "rate_limited":
        return ("#f59e0b", "rgba(245,158,11,0.18)")
    if normalized == "retrying":
        return ("#fb923c", "rgba(251,146,60,0.18)")
    if normalized == "running":
        return ("#22d3ee", "rgba(34,211,238,0.16)")
    if normalized == "cancelled":
        return ("#c084fc", "rgba(192,132,252,0.18)")
    if normalized == "failed":
        return ("#f87171", "rgba(248,113,113,0.18)")
    return ("#94a3b8", "rgba(148,163,184,0.14)")


def _job_download_telemetry(item: dict[str, Any]) -> dict[str, Any] | None:
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    telemetry = dict(pipeline_meta.get("download_telemetry") or {})
    if not telemetry:
        progress_pct = float(item.get("progress", 0.0) or 0.0)
        bytes_downloaded = int(item.get("bytes_downloaded", 0) or 0)
        bytes_total = max(int(item.get("bytes_total", 0) or 0), bytes_downloaded)
        if bytes_downloaded <= 0 and bytes_total <= 0 and not pipeline_meta.get("account_pool_assignments"):
            return None
        telemetry = {
            "status": "completed" if progress_pct >= 100.0 else "running",
            "strategy": str(pipeline_meta.get("download_strategy") or "default").strip().lower() or "default",
            "selected_accounts": int(pipeline_meta.get("account_pool_selected_accounts", 0) or 0),
            "pool_size": int(pipeline_meta.get("account_pool_size", 0) or 0),
            "per_account_concurrency": int(pipeline_meta.get("account_pool_per_account_concurrency", 0) or 0),
            "products_found": int(pipeline_meta.get("products_found", 0) or pipeline_meta.get("products_requested", 0) or 0),
            "products_downloaded": int(pipeline_meta.get("products_downloaded", 0) or 0),
            "files_known": 0,
            "files_completed": 0,
            "bytes_downloaded": bytes_downloaded,
            "bytes_total": bytes_total,
            "progress_pct": progress_pct,
            "speed_bps": 0.0,
            "eta_seconds": None,
            "started_at": str(pipeline_meta.get("download_started_at") or "").strip() or None,
            "finished_at": str(pipeline_meta.get("download_finished_at") or "").strip() or None,
            "duration_seconds": pipeline_meta.get("download_window_seconds"),
            "last_file": None,
            "retry_count_total": int(item.get("retry_count", 0) or 0),
            "rate_limited_accounts": 0,
            "accounts": [],
        }
    accounts = list(telemetry.get("accounts") or [])
    if not accounts and list(pipeline_meta.get("account_pool_assignments") or []):
        accounts = []
        for assignment in list(pipeline_meta.get("account_pool_assignments") or []):
            label = str((assignment or {}).get("account_label") or "").strip()
            if not label:
                continue
            accounts.append(
                {
                    "account_label": label,
                    "status": "completed" if float(telemetry.get("progress_pct", 0.0) or 0.0) >= 100.0 else "queued",
                    "product_count_assigned": int((assignment or {}).get("product_count") or 0),
                    "files_completed": 0,
                    "active_file_count": 0,
                    "bytes_downloaded": 0,
                    "bytes_total": 0,
                    "progress_pct": 0.0,
                    "retry_count": 0,
                    "current_file": None,
                    "last_retry_reason": None,
                }
            )
        telemetry["accounts"] = accounts
    telemetry.setdefault("started_at", str(pipeline_meta.get("download_started_at") or "").strip() or None)
    telemetry.setdefault("finished_at", str(pipeline_meta.get("download_finished_at") or "").strip() or None)
    telemetry.setdefault("duration_seconds", pipeline_meta.get("download_window_seconds"))
    if int(telemetry.get("selected_accounts", 0) or 0) <= 0 and accounts:
        telemetry["selected_accounts"] = len(
            {
                str((account or {}).get("account_label") or "").strip()
                for account in accounts
                if str((account or {}).get("account_label") or "").strip()
            }
        )
    return telemetry


def _render_job_download_telemetry(item: dict[str, Any]) -> bool:
    telemetry = _job_download_telemetry(item)
    if telemetry is None:
        return False

    pipeline_state = _job_pipeline_state(item)
    status = str(telemetry.get("status") or "running").strip().lower() or "running"
    strategy = str(telemetry.get("strategy") or "default").strip().lower() or "default"
    overall_progress = max(0.0, min(100.0, float(telemetry.get("progress_pct", item.get("progress", 0.0)) or 0.0)))
    bytes_downloaded = int(telemetry.get("bytes_downloaded", item.get("bytes_downloaded", 0)) or 0)
    bytes_total = max(int(telemetry.get("bytes_total", item.get("bytes_total", 0)) or 0), bytes_downloaded)
    speed_bps = float(telemetry.get("speed_bps", 0.0) or 0.0)
    eta_seconds = telemetry.get("eta_seconds")
    selected_accounts = int(telemetry.get("selected_accounts", 0) or 0)
    pool_size = int(telemetry.get("pool_size", 0) or 0)
    per_account_concurrency = int(telemetry.get("per_account_concurrency", 0) or 0)
    products_found = int(telemetry.get("products_found", 0) or 0)
    products_downloaded = int(telemetry.get("products_downloaded", 0) or 0)
    download_window_seconds = telemetry.get("duration_seconds")
    retry_count_total = int(telemetry.get("retry_count_total", item.get("retry_count", 0)) or 0)
    accounts = list(telemetry.get("accounts") or [])
    account_rows_used = len(
        {
            str((account or {}).get("account_label") or "").strip()
            for account in accounts
            if str((account or {}).get("account_label") or "").strip()
        }
    )
    accounts_used = max(selected_accounts, account_rows_used)
    if accounts_used <= 0 and bytes_total > 0:
        accounts_used = 1
    color, tint = _download_status_style(status)

    summary_parts = [("Strategy", _download_strategy_label(strategy))]
    if pool_size > 0 or accounts_used > 0:
        if pool_size > 0:
            summary_parts.append(("Accounts used", f"{accounts_used}/{pool_size}"))
        else:
            summary_parts.append(("Accounts used", str(accounts_used or "-")))
    summary_parts.extend(
        [
            ("Products", f"{products_downloaded}/{products_found}" if products_found > 0 else str(products_downloaded or "-")),
            ("Volume", f"{_format_bytes_compact(bytes_downloaded)} / {_format_bytes_compact(bytes_total)}"),
            ("Throughput", _format_rate_compact(speed_bps)),
            ("ETA", _format_eta_compact(eta_seconds) if eta_seconds is not None and pipeline_state == "downloading" else ("done" if status == "completed" or pipeline_state not in {"searching", "downloading"} else "-")),
        ]
    )
    if download_window_seconds is not None:
        summary_parts.append(("Download window", _format_duration_compact(download_window_seconds)))
    if per_account_concurrency > 0:
        summary_parts.append(("Per account cap", str(per_account_concurrency)))
    if retry_count_total > 0:
        summary_parts.append(("Retries", str(retry_count_total)))

    summary_html = "".join(
        (
            "<div style='min-width:110px;display:flex;flex-direction:column;gap:2px;'>"
            f"<span style='font-size:.66rem;color:#64748b;text-transform:uppercase;'>{html.escape(label)}</span>"
            f"<span style='font-size:1rem;color:#e5e7eb;font-weight:700;'>{html.escape(value)}</span>"
            "</div>"
        )
        for label, value in summary_parts
    )
    st.markdown(
        (
            "<div style='margin-top:10px;'>"
            "<div style='display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;'>"
            f"<div style='display:flex;gap:12px;flex-wrap:wrap;align-items:flex-start;'>{summary_html}</div>"
            f"<span style='display:inline-flex;align-items:center;border-radius:6px;padding:4px 8px;font-size:.72rem;"
            f"font-weight:700;color:{color};background:{tint};text-transform:uppercase;'>{html.escape(status.replace('_', ' '))}</span>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            "<div style='margin-top:8px;margin-bottom:6px;'>"
            f"<div style='height:10px;background:#172033;border-radius:6px;overflow:hidden;'>"
            f"<div style='height:100%;width:{overall_progress:.2f}%;background:{color};border-radius:6px;transition:width .25s ease;'></div>"
            "</div>"
            f"<div style='display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-top:4px;font-size:.72rem;color:#94a3b8;'>"
            f"<span>Download {overall_progress:.2f}%</span>"
            f"<span>{html.escape(_format_bytes_compact(bytes_downloaded))} of {html.escape(_format_bytes_compact(bytes_total))}</span>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    if not accounts:
        return True

    rows_html = []
    for account in accounts:
        account_label = str(account.get("account_label") or "account").strip() or "account"
        account_status = str(account.get("status") or "queued").strip().lower() or "queued"
        lane_color, lane_tint = _download_status_style(account_status)
        files_completed = int(account.get("files_completed", 0) or 0)
        products_assigned = int(account.get("product_count_assigned", 0) or 0)
        active_file_count = int(account.get("active_file_count", 0) or 0)
        retry_count = int(account.get("retry_count", 0) or 0)
        account_bytes_done = int(account.get("bytes_downloaded", 0) or 0)
        account_bytes_total = max(int(account.get("bytes_total", 0) or 0), account_bytes_done)
        account_progress = max(0.0, min(100.0, float(account.get("progress_pct", 0.0) or 0.0)))
        current_file = str(account.get("current_file") or "").strip()
        meta_bits = [f"{files_completed}/{products_assigned} files" if products_assigned > 0 else f"{files_completed} files"]
        if active_file_count > 0:
            meta_bits.append(f"{active_file_count} active")
        if retry_count > 0:
            meta_bits.append(f"{retry_count} retries")
        retry_reason = str(account.get("last_retry_reason") or "").strip().lower()
        if retry_reason == "http_429":
            meta_bits.append("rate limited")

        rows_html.append(
            (
                "<div style='padding:8px 0;border-top:1px solid rgba(148,163,184,0.12);'>"
                "<div style='display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center;'>"
                "<div style='display:flex;gap:10px;flex-wrap:wrap;align-items:center;'>"
                f"<span style='font-size:.84rem;font-weight:700;color:#e5e7eb;'>{html.escape(account_label)}</span>"
                f"<span style='display:inline-flex;align-items:center;border-radius:6px;padding:3px 8px;font-size:.68rem;font-weight:700;color:{lane_color};background:{lane_tint};text-transform:uppercase;'>{html.escape(account_status.replace('_', ' '))}</span>"
                f"<span style='font-size:.70rem;color:#94a3b8;'>{html.escape(' · '.join(meta_bits))}</span>"
                "</div>"
                f"<span style='font-size:.72rem;color:#cbd5e1;'>{html.escape(_format_bytes_compact(account_bytes_done))} / {html.escape(_format_bytes_compact(account_bytes_total))}</span>"
                "</div>"
                f"<div style='height:6px;background:#172033;border-radius:6px;overflow:hidden;margin-top:6px;'>"
                f"<div style='height:100%;width:{account_progress:.2f}%;background:{lane_color};border-radius:6px;transition:width .25s ease;'></div>"
                "</div>"
                f"<div style='display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-top:4px;font-size:.68rem;color:#64748b;'>"
                f"<span>{account_progress:.2f}%</span>"
                f"<span>{html.escape(Path(current_file).name) if current_file else ''}</span>"
                "</div>"
                "</div>"
            )
        )
    st.markdown("".join(rows_html), unsafe_allow_html=True)
    return True


def _render_compact_job_metric(label: str, value: Any) -> None:
    st.markdown(
        "<div class='nimbus-job-metric'>"
        f"<span>{html.escape(str(label))}</span>"
        f"<strong>{html.escape(str(value or '-'))}</strong>"
        "</div>",
        unsafe_allow_html=True,
    )


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
        duration = _job_elapsed_seconds(item)
        current_stage = _current_timeline_stage(item)
        current_stage_label = str((current_stage or {}).get("label") or "-")
        current_stage_duration = _format_runtime_duration(_stage_elapsed_seconds(current_stage or {}))
        errors = item.get("errors", []) or []
        error_summary = None
        if errors:
            first_error = str(errors[0]).strip()
            error_text = first_error.lower()
            if "429" in error_text or "rate limit" in error_text or "too many requests" in error_text:
                error_summary = "Copernicus download endpoint rate-limited this job (HTTP 429). The provider asked for retries before the first byte."
            elif "504" in error_text:
                error_summary = "Copernicus download endpoint timed out (HTTP 504) before the first byte. Retry later."
            elif "catalogue.dataspace.copernicus.eu" in first_error and "503" in first_error:
                error_summary = "Copernicus catalogue is temporarily unavailable (HTTP 503). Retry later."
            elif "sen2like" in error_text or "spark" in error_text:
                error_summary = _stage_error_summary(first_error)
            elif len(first_error) > 240:
                error_summary = f"{first_error[:240]}..."
            else:
                error_summary = first_error
        with st.container(border=True):
            action_feedback = dict(st.session_state.get("job_action_feedback", {})).get(job_id)
            state_label, state_fg, state_bg, state_icon = _job_state_style(state)
            pipeline_label, pipeline_fg, pipeline_bg, pipeline_icon = _job_pipeline_style(item)
            provider_issue = _job_provider_issue_badge(item)
            badge_chunks = [
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
            h1, h2, h3, h4, h5 = st.columns([3, 1, 1, 1, 1])
            with h1:
                st.markdown(f"**{job_id}**")
                st.caption(
                    f"{item.get('provider', '-')}/{item.get('collection', '-')} · "
                    f"{item.get('product_type', '-')}"
                )
            with h2:
                _render_compact_job_metric("State", state_label)
            with h3:
                _render_compact_job_metric("Pipeline", pipeline_label)
            with h4:
                _render_compact_job_metric("Job elapsed", _format_runtime_duration(duration))
            with h5:
                _render_compact_job_metric("Stage elapsed", current_stage_duration)
            _render_pipeline_timeline(item)
            _render_job_progress_bar(pipeline_progress, _job_progress_visual_state(item))
            st.caption(
                f"Current stage: {current_stage_label}"
                f" · Pipeline state: {pipeline_state}"
                f" · Pipeline progress: {pipeline_progress:.2f}%"
                f" · Download progress: {download_progress:.2f}%"
                f" · Updated: {item.get('updated_at', '-')}"
            )
            rendered_download_panel = _render_job_download_telemetry(item)
            pipeline_summary = _job_pipeline_summary(item)
            if pipeline_summary:
                st.caption(pipeline_summary)
            pipeline_substate = _job_pipeline_substate(item)
            if pipeline_substate:
                st.caption(pipeline_substate)
            _render_job_pipeline_paths(item)
            pipeline_mask_types = _normalize_mask_types(_mask_types_from_payload(item))
            pipeline_mask_mode = str(dict(item.get("pipeline_metadata") or {}).get("mask_mode") or "").strip().lower()
            if pipeline_mask_types and pipeline_mask_mode == "integrated":
                st.caption(
                    f"Integrated masking: {', '.join(pipeline_mask_types)}"
                    " · Writes happen in-place in the same Zarr produced by the pipeline."
                )
            if not rendered_download_panel:
                st.caption(
                    f"Downloaded volume: {_format_bytes_compact(item.get('bytes_downloaded', 0))} / "
                    f"{_format_bytes_compact(item.get('bytes_total', 0))}"
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
            if isinstance(action_feedback, dict):
                feedback_kind = str(action_feedback.get("kind") or "").strip().lower()
                feedback_message = str(action_feedback.get("message") or "").strip()
                if feedback_message:
                    if feedback_kind == "error":
                        st.error(feedback_message)
                    else:
                        st.success(feedback_message)
            show_resume_action = str(state).strip().lower() == "failed"
            if show_resume_action:
                can_resume = bool(item.get("can_resume"))
                resume_label = str(
                    item.get("resume_label")
                    or ("Resume Pipeline" if can_resume else "Can't Resume")
                ).strip() or ("Resume Pipeline" if can_resume else "Can't Resume")
                resume_reason = str(item.get("resume_reason") or "").strip()
                if resume_reason:
                    prefix = "Resume available:" if can_resume else "Resume unavailable:"
                    st.caption(f"{prefix} {resume_reason}")
                if st.button(
                    resume_label,
                    key=f"resume_job_button_{job_id}",
                    width="stretch",
                    disabled=not can_resume,
                ):
                    try:
                        response = _api_request(
                            "POST",
                            _ss("api_url"),
                            f"/v1/jobs/{job_id}/resume",
                            api_key=_ss("api_key"),
                            timeout=60,
                        )
                        if response.ok:
                            payload = dict(response.json() or {})
                            resumed_job = dict(payload.get("job") or {})
                            resumed_job_id = str(
                                payload.get("resumed_job_id")
                                or resumed_job.get("job_id")
                                or job_id
                            ).strip() or job_id
                            feedback_message = str(payload.get("message") or f"{resume_label} requested.").strip()
                            status_cache = dict(st.session_state.get("job_status_cache", {}))
                            if resumed_job:
                                status_cache[resumed_job_id] = resumed_job
                            st.session_state["job_status_cache"] = status_cache
                            action_messages = dict(st.session_state.get("job_action_feedback", {}))
                            action_messages[job_id] = {"kind": "success", "message": feedback_message}
                            st.session_state["job_action_feedback"] = action_messages
                            active_job_ids = list(st.session_state.get("active_job_ids", []))
                            if resumed_job_id and resumed_job_id not in active_job_ids:
                                active_job_ids.append(resumed_job_id)
                            st.session_state["active_job_ids"] = active_job_ids
                        else:
                            action_messages = dict(st.session_state.get("job_action_feedback", {}))
                            action_messages[job_id] = {
                                "kind": "error",
                                "message": f"{response.status_code}: {_response_error_message(response)}",
                            }
                            st.session_state["job_action_feedback"] = action_messages
                    except Exception as exc:
                        action_messages = dict(st.session_state.get("job_action_feedback", {}))
                        action_messages[job_id] = {"kind": "error", "message": str(exc)}
                        st.session_state["job_action_feedback"] = action_messages
                    st.rerun()
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
    mask_types = _mask_types_from_payload(item)
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    cube_outputs = list(item.get("cube_outputs") or pipeline_meta.get("cube_outputs") or [])
    cube_status = str(pipeline_meta.get("cube_status") or "").strip().lower()
    if state == "succeeded" and (
        str(pipeline_meta.get("mask_status") or "").strip().lower() == "written"
        or (mask_types and str(pipeline_meta.get("mask_mode") or "").strip().lower() == "integrated")
    ):
        return "masked_zarr_written"
    if state == "succeeded" and (cube_outputs or cube_status == "written"):
        return "cube_written"
    if state == "succeeded" and list(item.get("zarr_outputs") or []):
        return "zarr_written"
    return state or "queued"


def _job_pipeline_progress(item: dict[str, Any]) -> float:
    if item.get("pipeline_progress") is not None:
        return float(item.get("pipeline_progress", 0.0) or 0.0)
    return float(item.get("progress", 0.0) or 0.0)


def _format_runtime_duration(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, str) and not value.strip():
        return "-"
    try:
        seconds = max(0.0, float(value))
    except Exception:
        return "-"
    if seconds < 60.0:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minute = divmod(minutes, 60)
    return f"{hours}h {minute:02d}m"


def _job_elapsed_seconds(item: dict[str, Any]) -> float | None:
    stage_duration = _display_stage_duration_seconds(item)
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    orchestrator = dict(pipeline_metadata.get("orchestrator") or {})
    orchestrator_duration = _elapsed_between(
        orchestrator.get("started_at"),
        orchestrator.get("finished_at") or item.get("updated_at"),
    )
    if orchestrator_duration is not None:
        if stage_duration is None:
            return orchestrator_duration
        if _stage_duration_looks_partial(
            item,
            stage_duration=stage_duration,
            orchestrator_duration=orchestrator_duration,
        ):
            return orchestrator_duration
        return stage_duration

    if stage_duration is not None:
        return stage_duration

    row_duration = item.get("duration_seconds")
    if row_duration is not None:
        return row_duration

    started_at = item.get("started_at") or item.get("created_at")
    finished_at = item.get("finished_at") or item.get("updated_at")
    row_window_duration = _elapsed_between(started_at, finished_at)
    if row_window_duration is not None:
        return row_window_duration

    return None


def _stage_duration_looks_partial(
    item: dict[str, Any],
    *,
    stage_duration: float,
    orchestrator_duration: float,
) -> bool:
    if orchestrator_duration <= max(stage_duration, 0.0) * 1.25:
        return False
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    stage_results = [
        dict(stage)
        for stage in list(pipeline_metadata.get("stage_results") or [])
        if isinstance(stage, dict)
    ]
    if not stage_results:
        return False
    terminal_state = str(item.get("state") or "").strip().lower() in FINAL_JOB_STATES
    if not terminal_state:
        return False
    completed_stages = [
        stage
        for stage in stage_results
        if str(stage.get("status") or "").strip().lower() in {"succeeded", "skipped"}
    ]
    zero_duration_count = sum(
        1
        for stage in completed_stages
        if float(stage.get("duration_seconds") or 0.0) <= 0.0
    )
    return zero_duration_count >= max(1, len(completed_stages) // 2)


def _display_stages_are_terminal_success(
    item: dict[str, Any],
    stages: list[dict[str, Any]],
) -> bool:
    if str(item.get("state") or "").strip().lower() != "succeeded":
        return False
    statuses = {str(stage.get("status") or "").strip().lower() for stage in stages}
    return not statuses.intersection({"pending", "queued", "running", "failed", "cancelled"})


def _terminal_ready_stage() -> dict[str, Any]:
    return {
        "key": "ready",
        "label": "Ready",
        "badge": "RDY",
        "status": "done",
        "duration_seconds": None,
    }


def _stage_display_has_warning(stage: dict[str, Any]) -> bool:
    metadata = dict(stage.get("metadata") or {})
    stage_key = str(stage.get("key") or "").strip().lower()
    status = str(stage.get("status") or "").strip().lower()
    if bool(metadata.get("fallback_to_raw")):
        return True
    if str(metadata.get("sen2like_status") or "").strip().lower() == "raw_fallback":
        return True
    if stage_key == "cube" and status == "skipped":
        return bool(
            str(metadata.get("reason") or metadata.get("cube_reason") or "").strip()
            or list(metadata.get("cube_tiles_skipped") or [])
        )
    return False


def _stage_status_label_for_display(stage: dict[str, Any], status_kind: str) -> str:
    metadata = dict(stage.get("metadata") or {})
    explicit_label = str(
        metadata.get("status_label")
        or metadata.get("display_status")
        or ""
    ).strip().lower()
    if explicit_label:
        return explicit_label.replace("_", " ")
    return {
        "pending": "waiting",
        "queued": "queued",
        "running": "live",
        "done": "done",
        "skipped": "skipped",
        "failed": "failed",
        "cancelled": "stopped",
    }.get(status_kind, status_kind.replace("_", " "))


def _terminal_pipeline_label(item: dict[str, Any]) -> str:
    state = str(item.get("state") or "").strip().lower()
    if state == "succeeded":
        return "Ready"
    if state == "failed":
        return "Failed"
    if state == "cancelled":
        return "Stopped"
    return "Waiting"


def _display_stage_duration_seconds(item: dict[str, Any]) -> float | None:
    timeline = _pipeline_timeline_snapshot(item)
    raw_stages = [
        dict(stage)
        for stage in list(timeline.get("stages") or [])
        if isinstance(stage, dict)
    ]
    stages = _display_pipeline_stages(item, timeline, raw_stages)
    durations = [
        duration
        for duration in (_stage_elapsed_seconds(stage) for stage in stages)
        if duration is not None
    ]
    if not durations:
        return None
    return sum(durations)


def _stage_elapsed_seconds(stage: dict[str, Any]) -> float | None:
    if not stage:
        return None
    value = stage.get("duration_seconds")
    if value is not None:
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            pass

    status = str(stage.get("status") or "").strip().lower()
    finished_at = stage.get("finished_at")
    if finished_at is None and status in {"running", "queued"}:
        finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
    return _elapsed_between(stage.get("started_at"), finished_at)


def _elapsed_between(started_at: Any, finished_at: Any) -> float | None:
    started = _parse_iso_datetime(started_at)
    finished = _parse_iso_datetime(finished_at)
    if started is None or finished is None:
        return None
    return max(0.0, (finished - started).total_seconds())


def _promote_timeline_stage_for_display(
    stage: dict[str, Any],
    *,
    reference_time: str | None,
) -> bool:
    current_status = str(stage.get("status") or "pending").strip().lower() or "pending"
    if current_status in {"failed", "cancelled"}:
        return False

    changed = current_status != "done"
    stage["status"] = "done"
    if not stage.get("detail_label") or current_status in {"pending", "queued", "running"}:
        stage["detail_label"] = str(stage.get("label") or stage.get("key") or "Stage")

    if reference_time is None:
        if stage.get("duration_seconds") is None:
            stage["duration_seconds"] = 0.0
            changed = True
        return changed

    if stage.get("started_at") is None:
        stage["started_at"] = reference_time
        changed = True
    if current_status in {"pending", "queued", "running"} or stage.get("finished_at") is None:
        stage["finished_at"] = reference_time
        changed = True
    if current_status in {"pending", "queued", "running"} or stage.get("duration_seconds") is None:
        stage["duration_seconds"] = 0.0
        changed = True
    return changed


def _timeline_display_completion_anchor(
    timeline: dict[str, Any],
    *,
    job_state: str,
    pipeline_state: str,
) -> str | None:
    stages = [dict(stage) for stage in list(timeline.get("stages") or []) if isinstance(stage, dict)]
    if not stages:
        return None

    stage_keys = [str(stage.get("key") or "").strip().lower() for stage in stages]
    ready_stage = next(
        (
            stage
            for stage in stages
            if str(stage.get("key") or "").strip().lower() == "ready"
        ),
        None,
    )
    ready_status = str((ready_stage or {}).get("status") or "").strip().lower()
    if ready_status in {"done", "failed", "cancelled"}:
        return "ready"

    normalized_job_state = str(job_state or "").strip().lower()
    normalized_pipeline_state = str(pipeline_state or "").strip().lower()
    current_stage = str(timeline.get("current_stage") or "").strip().lower()

    if normalized_job_state != "succeeded":
        if current_stage == "ready" and "ready" in stage_keys:
            return "ready"
        return None

    if normalized_pipeline_state == "masked_zarr_written" and "ready" in stage_keys:
        return "ready"
    if normalized_pipeline_state == "cube_written" and "cube" in stage_keys:
        cube_index = stage_keys.index("cube")
        trailing = [key for key in stage_keys[cube_index + 1:] if key != "ready"]
        return "ready" if not trailing and "ready" in stage_keys else "cube"
    if normalized_pipeline_state == "zarr_written" and "convert" in stage_keys:
        trailing = [key for key in stage_keys[stage_keys.index("convert") + 1:] if key != "ready"]
        return "ready" if not trailing and "ready" in stage_keys else "convert"
    if current_stage == "ready" and "ready" in stage_keys:
        return "ready"
    return None


def _normalize_timeline_for_display(
    timeline: dict[str, Any],
    *,
    item: dict[str, Any],
) -> dict[str, Any]:
    stages = [dict(stage) for stage in list(timeline.get("stages") or []) if isinstance(stage, dict)]
    if not stages:
        return timeline

    snapshot = dict(timeline)
    snapshot["stages"] = stages
    job_state = str(snapshot.get("job_state") or item.get("state") or "").strip().lower()
    pipeline_state = str(snapshot.get("pipeline_state") or _job_pipeline_state(item) or "").strip().lower()
    anchor_key = _timeline_display_completion_anchor(
        snapshot,
        job_state=job_state,
        pipeline_state=pipeline_state,
    )
    if anchor_key is None:
        return snapshot

    stage_keys = [str(stage.get("key") or "").strip().lower() for stage in stages]
    if anchor_key not in stage_keys:
        return snapshot

    anchor_stage = stages[stage_keys.index(anchor_key)]
    reference_time = str(
        anchor_stage.get("finished_at")
        or anchor_stage.get("started_at")
        or snapshot.get("updated_at")
        or item.get("finished_at")
        or item.get("updated_at")
        or dt.datetime.now(dt.timezone.utc).isoformat()
    ).strip() or None

    changed = False
    for stage in stages[: stage_keys.index(anchor_key) + 1]:
        changed = _promote_timeline_stage_for_display(
            stage,
            reference_time=reference_time,
        ) or changed

    if not changed:
        return snapshot

    snapshot["visual_normalized"] = True
    if anchor_key == "ready":
        snapshot["current_stage"] = "ready"
        snapshot["current_stage_label"] = str(anchor_stage.get("label") or "Ready")
        snapshot["terminal"] = True
    elif not snapshot.get("current_stage"):
        snapshot["current_stage"] = anchor_key
        snapshot["current_stage_label"] = str(anchor_stage.get("label") or anchor_key.title())
    return snapshot


def _pipeline_timeline_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    pipeline_metadata = dict(item.get("pipeline_metadata") or {})
    existing_timeline = item.get("pipeline_timeline")
    if not isinstance(existing_timeline, dict):
        existing_timeline = pipeline_metadata.get("timeline")
    if not isinstance(existing_timeline, dict):
        existing_timeline = {}

    state_value = str(item.get("state") or "").strip().lower()
    if state_value in ACTIVE_JOB_STATES:
        timeline_timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    else:
        timeline_timestamp = (
            item.get("finished_at")
            or item.get("updated_at")
            or item.get("started_at")
            or dt.datetime.now(dt.timezone.utc).isoformat()
        )

    pipeline_state = str(item.get("pipeline_state") or _job_pipeline_state(item) or "").strip().lower()
    pipeline_step = str(item.get("pipeline_step") or "").strip().lower() or None
    job_kind = "mask" if _job_is_mask_job(item) else "fetch"
    normalized_mask_types = list(_normalize_mask_types(_mask_types_from_payload(item)))
    cube_mode = _cube_mode_from_payload(item)

    if existing_timeline:
        snapshot = refresh_pipeline_timeline(
            dict(existing_timeline),
            timestamp=timeline_timestamp,
            job_state=state_value,
            pipeline_state=pipeline_state,
            pipeline_step=pipeline_step,
            job_kind=job_kind,
            mask_types=normalized_mask_types,
            cube_mode=cube_mode,
        )
    else:
        snapshot = advance_pipeline_timeline(
            dict(existing_timeline),
            job_state=state_value,
            pipeline_state=pipeline_state,
            pipeline_step=pipeline_step,
            pipeline_progress=_job_pipeline_progress(item),
            timestamp=timeline_timestamp,
            job_kind=job_kind,
            mask_types=normalized_mask_types,
            cube_mode=cube_mode,
        )

    return _normalize_timeline_for_display(
        snapshot,
        item=item,
    )


def _current_timeline_stage(item: dict[str, Any]) -> dict[str, Any] | None:
    timeline = _pipeline_timeline_snapshot(item)
    current_stage_key = _display_stage_key(str(timeline.get("current_stage") or "").strip().lower())
    raw_stages = [dict(stage) for stage in list(timeline.get("stages") or []) if isinstance(stage, dict)]
    stages = _display_pipeline_stages(item, timeline, raw_stages)
    if _display_stages_are_terminal_success(item, stages):
        return _terminal_ready_stage()
    for stage in stages:
        if str(stage.get("key") or "").strip().lower() == current_stage_key:
            return stage
    for stage in stages:
        if str(stage.get("status") or "").strip().lower() in {"running", "queued"}:
            return stage
    for stage in reversed(stages):
        if str(stage.get("status") or "").strip().lower() in {"done", "skipped", "failed", "cancelled"}:
            return stage
    return None


def _job_pipeline_style(item: dict[str, Any]) -> tuple[str, str, str, str]:
    pipeline_state = _job_pipeline_state(item)
    if _job_is_mask_job(item):
        if pipeline_state == "resolving_source_zarr":
            return ("resolving source", "#93c5fd", "rgba(147,197,253,0.14)", "⌕")
        if pipeline_state == "running_cloud_inference":
            return ("cloud inference", "#38bdf8", "rgba(56,189,248,0.16)", "☁")
        if pipeline_state == "running_water_inference":
            return ("water inference", "#22d3ee", "rgba(34,211,238,0.16)", "≈")
        if pipeline_state in {"writing_mask_artifacts", "writing_masked_zarr", "registering_artifacts"}:
            return ("finalizing masks", "#38bdf8", "rgba(56,189,248,0.16)", "⬢")
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
    if pipeline_state == "sen2like_queued":
        return ("sen2like queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if pipeline_state == "sen2like_running":
        return ("sen2like running", "#38bdf8", "rgba(56,189,248,0.16)", "⚙")
    if pipeline_state == "sen2like_written":
        return ("sen2like ready", "#4ade80", "rgba(74,222,128,0.14)", "S2")
    if pipeline_state == "zarr_queued":
        return ("zarr queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if pipeline_state == "zarr_converting":
        return ("zarr converting", "#38bdf8", "rgba(56,189,248,0.16)", "⚙")
    if pipeline_state == "zarr_written":
        return ("zarr ready", "#4ade80", "rgba(74,222,128,0.14)", "⬢")
    if pipeline_state == "cube_queued":
        return ("cube queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")
    if pipeline_state == "cube_building":
        return ("cube building", "#14b8a6", "rgba(20,184,166,0.16)", "▣")
    if pipeline_state == "cube_written":
        return ("cube ready", "#4ade80", "rgba(74,222,128,0.14)", "▣")
    if pipeline_state == "running_cloud_inference":
        return ("cloud inference", "#38bdf8", "rgba(56,189,248,0.16)", "☁")
    if pipeline_state == "running_water_inference":
        return ("water inference", "#22d3ee", "rgba(34,211,238,0.16)", "≈")
    if pipeline_state == "masked_zarr_written":
        return ("pipeline ready", "#4ade80", "rgba(74,222,128,0.14)", "✓")
    if pipeline_state == "cube_failed":
        return ("cube failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    if pipeline_state == "sen2like_failed":
        return ("sen2like failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    if pipeline_state == "zarr_failed":
        return ("zarr failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    if pipeline_state == "cancelled":
        return ("cancelled", "#c084fc", "rgba(192,132,252,0.14)", "■")
    if pipeline_state == "failed":
        return ("failed", "#f87171", "rgba(248,113,113,0.14)", "✕")
    return ("queued", "#fbbf24", "rgba(251,191,36,0.16)", "⏳")


def _job_progress_visual_state(item: dict[str, Any]) -> str:
    pipeline_state = _job_pipeline_state(item)
    if pipeline_state in {"failed", "sen2like_failed", "zarr_failed", "cube_failed"}:
        return "failed"
    if pipeline_state == "cancelled":
        return "cancelled"
    if pipeline_state in {"queued", "zarr_queued", "cube_queued"}:
        return "queued"
    if pipeline_state in {"zarr_written", "masked_zarr_written", "cube_written"}:
        return "succeeded"
    return "running"


def _job_pipeline_summary(item: dict[str, Any]) -> str | None:
    pipeline_state = _job_pipeline_state(item)
    pipeline_step = str(item.get("pipeline_step", "") or "").strip().lower()
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    conversion_meta = dict(item.get("conversion_metadata") or {})
    error_texts = [str(err).strip().lower() for err in (item.get("errors", []) or []) if str(err).strip()]
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
        if pipeline_state in {"writing_mask_artifacts", "writing_masked_zarr", "registering_artifacts"}:
            return "Finalizing in-place mask metadata on the selected Zarr store."
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
    cube_count = len(item.get("cube_outputs") or []) or int(pipeline_meta.get("cube_output_count", 0) or 0)
    products_found = int(pipeline_meta.get("products_found", 0) or 0)
    mask_types = _mask_types_from_payload(item)
    integrated_mask = bool(mask_types) and str(pipeline_meta.get("mask_mode") or "").strip().lower() == "integrated"
    cube_mode = str(pipeline_meta.get("cube_mode") or "none").strip().lower() or "none"
    suffix_parts: list[str] = []
    if raw_count:
        suffix_parts.append(f"raw outputs: {raw_count}")
    if zarr_count:
        suffix_parts.append(f"zarr outputs: {zarr_count}")
    if cube_count:
        suffix_parts.append(f"cubes: {cube_count}")
    suffix = f" ({' · '.join(suffix_parts)})" if suffix_parts else ""
    if pipeline_state == "searching":
        return "Searching the provider catalogue for matching products."
    if pipeline_state == "downloading":
        if products_found > 0 and bytes_downloaded <= 0:
            return f"Products were found. Waiting for the first download byte from the provider ({products_found} selected)."
        return f"Raw product download is in progress{suffix}."
    if pipeline_state == "downloaded":
        return f"Download complete. Preparing Zarr conversion{suffix}."
    if pipeline_state == "sen2like_queued":
        return f"Landsat download complete. Sen2Like normalization is queued before Zarr{suffix}."
    if pipeline_state == "sen2like_running":
        return f"Landsat download complete. Sen2Like is normalizing products before Zarr{suffix}."
    if pipeline_state == "sen2like_written":
        return f"Sen2Like outputs are ready. Zarr conversion will use the normalized Sentinel-like products{suffix}."
    if pipeline_state == "zarr_queued":
        return f"Download complete. Zarr conversion is queued{suffix}."
    if pipeline_state == "zarr_converting":
        if pipeline_step == "registering_artifact":
            return f"Download complete. Zarr data is written. Registering the store{suffix}."
        return f"Download complete. Zarr conversion is running{suffix}."
    if pipeline_state == "zarr_written":
        if integrated_mask:
            return f"Download complete. Zarr output is ready and the same job will continue with {' + '.join(mask_types)} masking{suffix}."
        return f"Download complete. Zarr output is ready{suffix}."
    if pipeline_state == "cube_queued":
        placement = "before masking" if cube_mode == "before_mask" else "after masking" if cube_mode == "after_mask" else "after conversion"
        return f"Scene Zarr outputs are ready. Cube building is queued {placement}{suffix}."
    if pipeline_state == "cube_building":
        placement = "before masking" if cube_mode == "before_mask" else "after masking" if cube_mode == "after_mask" else "after conversion"
        return f"Stacking scene Zarr outputs into grouped time cubes {placement}{suffix}."
    if pipeline_state == "running_cloud_inference":
        return f"Download and conversion completed. Running cloud masking in-place on the resulting Zarr{suffix}."
    if pipeline_state == "running_water_inference":
        return f"Download and conversion completed. Running water masking in-place on the resulting Zarr{suffix}."
    if pipeline_state == "masked_zarr_written":
        return f"Download complete. Zarr output and requested masks are ready{suffix}."
    if pipeline_state == "cube_written":
        return f"Download complete. Cube outputs are ready{suffix}."
    if pipeline_state == "cube_failed":
        return f"Download and conversion succeeded, but cube building failed{suffix}."
    if pipeline_state == "sen2like_failed":
        return f"Download succeeded, but Sen2Like normalization failed before Zarr conversion{suffix}."
    if pipeline_state == "zarr_failed":
        return f"Download completed, but Zarr conversion failed{suffix}."
    if pipeline_state == "failed":
        if integrated_mask and zarr_count > 0:
            return f"Fetch and Zarr conversion succeeded, but the in-place masking phase failed{suffix}."
        if products_found > 0 and bytes_downloaded <= 0 and any(
            "429" in text or "rate limit" in text or "too many requests" in text
            for text in error_texts
        ):
            return (
                f"Search succeeded ({products_found} products found), but the provider rate-limited "
                "the download endpoint before the first byte."
            )
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
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    if _job_is_mask_job(item):
        mask_types = _normalize_mask_types(
            item.get("mask_types")
            or dict(item.get("pipeline_metadata") or {}).get("mask_types")
            or conversion_meta.get("mask_types")
            or []
        )
        mask_workers = int(pipeline_meta.get("mask_parallel_workers", 0) or 0)
        mask_total_scenes = int(pipeline_meta.get("mask_total_scenes", 0) or 0)
        mask_completed_scenes = int(pipeline_meta.get("mask_completed_scenes", 0) or 0)
        mask_suffix = (
            f" ({mask_completed_scenes}/{mask_total_scenes})"
            if mask_total_scenes > 0 and mask_completed_scenes > 0
            else ""
        )
        worker_suffix = f" with {mask_workers} parallel worker(s)" if mask_workers > 1 else ""
        if pipeline_state == "queued":
            return f"Requested mask types: {', '.join(mask_types) or '-'}."
        if pipeline_state == "running_cloud_inference":
            return (
                "Cloud inference is active"
                f"{mask_suffix}{worker_suffix}. The selected Zarr will receive masks/cloud and masks/cloud_probability."
            )
        if pipeline_state == "running_water_inference":
            return (
                "Water inference is active"
                f"{mask_suffix}{worker_suffix}. The selected Zarr will receive masks/water and masks/water_probability."
            )
        if pipeline_state == "writing_masked_zarr":
            return "Consolidating metadata and finalizing the selected Zarr after in-place mask writes."
        return None
    mask_types = _mask_types_from_payload(item)
    integrated_mask = bool(mask_types) and str(pipeline_meta.get("mask_mode") or "").strip().lower() == "integrated"
    cube_mode = str(pipeline_meta.get("cube_mode") or "none").strip().lower() or "none"
    download_strategy = str(pipeline_meta.get("download_strategy") or "default").strip().lower()
    if download_strategy == "copernicus_account_pool" and pipeline_state in {
        "searching",
        "downloading",
        "downloaded",
    }:
        selected_accounts = int(pipeline_meta.get("account_pool_selected_accounts", 0) or 0)
        pool_size = int(pipeline_meta.get("account_pool_size", 0) or 0)
        per_account = int(pipeline_meta.get("account_pool_per_account_concurrency", 0) or 0)
        fallback_reason = str(pipeline_meta.get("account_pool_fallback_reason") or "").strip().lower()
        if selected_accounts > 0 and pool_size > 0 and per_account > 0:
            return (
                f"Copernicus account pool active: {selected_accounts}/{pool_size} accounts selected"
                f" · target {per_account} downloads per account."
            )
        if fallback_reason == "insufficient_accounts":
            return "Copernicus account pool was requested, but the backend fell back to the primary account because no extra accounts were available."
    current_index = int(conversion_meta.get("current_index", 0) or 0)
    total = int(conversion_meta.get("total", 0) or 0)
    items_completed = int(conversion_meta.get("items_completed", 0) or 0)
    items_total = int(conversion_meta.get("items_total", 0) or total or 0)
    parallel_workers = int(
        conversion_meta.get("parallel_workers", 0)
        or pipeline_meta.get("zarr_parallel_workers", 0)
        or 0
    )
    if items_total > 0 and items_completed > 0:
        item_suffix = f" ({items_completed}/{items_total})"
    else:
        item_suffix = f" ({current_index}/{total})" if current_index > 0 and total > 0 else ""
    worker_suffix = f" with {parallel_workers} parallel worker(s)" if parallel_workers > 1 else ""
    if pipeline_state == "zarr_queued":
        return f"Waiting for the worker to start Zarr conversion{item_suffix}."
    if pipeline_state in {"sen2like_queued", "sen2like_running", "sen2like_written", "sen2like_failed"}:
        inputs = int(pipeline_meta.get("sen2like_input_count", 0) or 0)
        outputs = int(pipeline_meta.get("sen2like_output_count", 0) or 0)
        service_url = str(pipeline_meta.get("sen2like_service_url") or "").strip()
        parts = ["Landsat -> Sentinel-like normalization"]
        if inputs:
            parts.append(f"inputs: {inputs}")
        if outputs:
            parts.append(f"outputs: {outputs}")
        if service_url:
            parts.append("service configured")
        return " · ".join(parts) + "."
    if pipeline_state == "zarr_converting":
        if pipeline_step == "writing_chunks":
            return f"Writing chunks into the Zarr store{item_suffix}{worker_suffix}."
        if pipeline_step == "registering_artifact":
            return f"Registering the Zarr artifact in the backend{item_suffix}{worker_suffix}."
        return f"Zarr conversion is active{item_suffix}{worker_suffix}."
    if pipeline_state in {"cube_queued", "cube_building", "cube_written", "cube_failed"}:
        tiles_built = int(len(pipeline_meta.get("cube_tiles_built") or []))
        cube_count = int(pipeline_meta.get("cube_output_count", 0) or 0)
        start_date = str(dict(pipeline_meta.get("cube_date_range") or {}).get("start_date") or "").strip()
        end_date = str(dict(pipeline_meta.get("cube_date_range") or {}).get("end_date") or "").strip()
        parts = [
            f"Cube stage: {cube_mode or '-'}",
            f"outputs: {cube_count}",
            f"groups built: {tiles_built}",
        ]
        if start_date or end_date:
            parts.append(f"date range: {start_date or '?'} -> {end_date or '?'}")
        return " · ".join(parts) + "."
    if pipeline_state == "downloaded":
        return "Raw product is available locally. The backend is about to start the Zarr stage."
    if integrated_mask and pipeline_state == "zarr_written":
        mask_workers = int(pipeline_meta.get("mask_parallel_workers", 0) or 0)
        worker_suffix = f" Planned scene workers: {mask_workers}." if mask_workers > 1 else ""
        return f"Mask orchestration requested: {', '.join(mask_types)}.{worker_suffix}"
    if integrated_mask and pipeline_state == "running_cloud_inference":
        mask_workers = int(pipeline_meta.get("mask_parallel_workers", 0) or 0)
        mask_total_scenes = int(pipeline_meta.get("mask_total_scenes", 0) or 0)
        mask_completed_scenes = int(pipeline_meta.get("mask_completed_scenes", 0) or 0)
        suffix = (
            f" ({mask_completed_scenes}/{mask_total_scenes})"
            if mask_total_scenes > 0 and mask_completed_scenes > 0
            else ""
        )
        worker_suffix = f" with {mask_workers} parallel worker(s)" if mask_workers > 1 else ""
        return (
            "Cloud layers will be written into `masks/cloud` and `masks/cloud_probability`"
            f" in the same Zarr store{suffix}{worker_suffix}."
        )
    if integrated_mask and pipeline_state == "running_water_inference":
        mask_workers = int(pipeline_meta.get("mask_parallel_workers", 0) or 0)
        mask_total_scenes = int(pipeline_meta.get("mask_total_scenes", 0) or 0)
        mask_completed_scenes = int(pipeline_meta.get("mask_completed_scenes", 0) or 0)
        suffix = (
            f" ({mask_completed_scenes}/{mask_total_scenes})"
            if mask_total_scenes > 0 and mask_completed_scenes > 0
            else ""
        )
        worker_suffix = f" with {mask_workers} parallel worker(s)" if mask_workers > 1 else ""
        return (
            "Water layers will be written into `masks/water` and `masks/water_probability`"
            f" in the same Zarr store{suffix}{worker_suffix}."
        )
    return None


def _job_pipeline_paths(item: dict[str, Any]) -> tuple[str | None, str | None]:
    pipeline_state = _job_pipeline_state(item)
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
    if pipeline_state in {"cube_queued", "cube_building", "cube_written", "cube_failed"}:
        cube_outputs = list(item.get("cube_outputs") or dict(item.get("pipeline_metadata") or {}).get("cube_outputs") or [])
        if cube_outputs:
            zarr_uri = str(cube_outputs[-1]).strip() or zarr_uri
    return (raw_uri or None, zarr_uri or None)


def _basename_label(value: Any) -> str:
    text = str(value or "").strip()
    return Path(text).name if text else "-"


def _job_pipeline_path_lines(item: dict[str, Any]) -> list[str]:
    raw_uri, zarr_uri = _job_pipeline_paths(item)
    lines: list[str] = []
    if _job_is_mask_job(item):
        if raw_uri:
            lines.append(f"Source Zarr: {Path(raw_uri).name}")
        if zarr_uri:
            label = "Target Zarr"
            if raw_uri and Path(raw_uri).name == Path(zarr_uri).name:
                label = "Target Zarr (same store)"
            lines.append(f"{label}: {Path(zarr_uri).name}")
        return lines

    raw_outputs = [str(path) for path in list(item.get("raw_outputs") or []) if str(path).strip()]
    zarr_outputs = [str(path) for path in list(item.get("zarr_outputs") or []) if str(path).strip()]
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    cube_outputs = [
        str(path)
        for path in list(item.get("cube_outputs") or pipeline_meta.get("cube_outputs") or [])
        if str(path).strip()
    ]
    pipeline_state = _job_pipeline_state(item)
    if len(raw_outputs) > 1 or len(zarr_outputs) > 1:
        if raw_outputs:
            lines.append(f"Sources: {len(raw_outputs)} raw file{'s' if len(raw_outputs) != 1 else ''}")
        if zarr_outputs:
            lines.append(f"Zarr stores: {len(zarr_outputs)}")
        preview_count = min(3, max(len(raw_outputs), len(zarr_outputs)))
        for index in range(preview_count):
            source = _basename_label(raw_outputs[index] if index < len(raw_outputs) else "")
            target = _basename_label(zarr_outputs[index] if index < len(zarr_outputs) else "")
            lines.append(f"{index + 1}. {source} -> {target}")
        remaining = max(len(raw_outputs), len(zarr_outputs)) - preview_count
        if remaining > 0:
            lines.append(f"+ {remaining} more scene{'s' if remaining != 1 else ''}")
    else:
        if raw_uri:
            lines.append(f"Source raw: {Path(raw_uri).name}")
        if zarr_uri:
            label = "Cube target" if pipeline_state in {"cube_queued", "cube_building", "cube_written", "cube_failed"} else "Zarr target"
            lines.append(f"{label}: {Path(zarr_uri).name}")

    if cube_outputs:
        lines.append(f"Cube stores: {len(cube_outputs)}")
        for index, cube_uri in enumerate(cube_outputs[:3], start=1):
            lines.append(f"cube {index}. {_basename_label(cube_uri)}")
        if len(cube_outputs) > 3:
            lines.append(f"+ {len(cube_outputs) - 3} more cube stores")
    return lines


def _render_job_pipeline_paths(item: dict[str, Any]) -> None:
    lines = _job_pipeline_path_lines(item)
    if not lines:
        return
    st.code("\n".join(lines), language="text")


_CONVERT_TIMELINE_STEP_KEYS = {"writing_chunks", "registering_artifact"}


def _conversion_progress_snapshot(item: dict[str, Any]) -> dict[str, int]:
    conversion_meta = dict(item.get("conversion_metadata") or {})
    pipeline_meta = dict(item.get("pipeline_metadata") or {})
    raw_outputs = list(item.get("raw_outputs") or [])
    zarr_outputs = list(item.get("zarr_outputs") or [])

    total = max(
        int(conversion_meta.get("items_total", 0) or 0),
        int(conversion_meta.get("total", 0) or 0),
        int(pipeline_meta.get("raw_output_count", 0) or 0),
        len(raw_outputs),
    )
    completed = max(
        int(conversion_meta.get("items_completed", 0) or 0),
        int(pipeline_meta.get("zarr_output_count", 0) or 0),
        len(zarr_outputs),
    )
    active = max(int(conversion_meta.get("items_active", 0) or 0), 0)
    workers = max(
        int(conversion_meta.get("parallel_workers", 0) or 0),
        int(pipeline_meta.get("zarr_parallel_workers", 0) or 0),
    )
    current_index = max(int(conversion_meta.get("current_index", 0) or 0), 0)

    if total > 0:
        completed = min(completed, total)
        active = min(active, max(total - completed, 0))
        current_index = min(
            max(current_index, completed + (1 if active > 0 else 0)),
            total,
        )

    return {
        "total": total,
        "completed": completed,
        "active": active,
        "workers": workers,
        "current_index": current_index,
    }


def _conversion_progress_parts(item: dict[str, Any]) -> list[str]:
    snapshot = _conversion_progress_snapshot(item)
    total = snapshot["total"]
    completed = snapshot["completed"]
    active = snapshot["active"]
    workers = snapshot["workers"]
    current_index = snapshot["current_index"]

    parts: list[str] = []
    if total > 0:
        if completed > 0 or active > 0:
            parts.append(f"{completed}/{total} completed")
        elif current_index > 0:
            parts.append(f"scene {current_index}/{total}")
    if active > 0:
        parts.append(f"{active} active")
    if workers > 1:
        parts.append(f"{workers} workers")
    return parts


def _timeline_duration_seconds(
    started_at: Any,
    finished_at: Any,
) -> float | None:
    started = _parse_iso_datetime(started_at)
    finished = _parse_iso_datetime(finished_at)
    if started is None or finished is None:
        return None
    return max(0.0, (finished - started).total_seconds())


def _timeline_breakdown_rows(
    item: dict[str, Any],
    timeline: dict[str, Any],
) -> tuple[list[dict[str, str]], bool]:
    raw_steps = [dict(step) for step in list(timeline.get("steps") or []) if isinstance(step, dict)]
    if not raw_steps:
        return ([], False)

    rows: list[dict[str, str]] = []
    collapsed_convert_rows = False
    now_iso = timeline.get("updated_at")

    index = 0
    while index < len(raw_steps):
        step = raw_steps[index]
        step_key = str(step.get("key") or "").strip().lower()
        if step_key not in _CONVERT_TIMELINE_STEP_KEYS:
            rows.append(
                {
                    "step": str(step.get("label") or step.get("key") or "-"),
                    "detail": "-",
                    "status": str(step.get("status") or "-"),
                    "started": _format_status_timestamp(step.get("started_at")),
                    "finished": _format_status_timestamp(step.get("finished_at")),
                    "duration": _format_runtime_duration(step.get("duration_seconds")),
                    "stage": str(step.get("group_label") or step.get("group") or "-"),
                }
            )
            index += 1
            continue

        grouped_steps: list[dict[str, Any]] = []
        while index < len(raw_steps):
            candidate = dict(raw_steps[index])
            candidate_key = str(candidate.get("key") or "").strip().lower()
            if candidate_key not in _CONVERT_TIMELINE_STEP_KEYS:
                break
            grouped_steps.append(candidate)
            index += 1

        if not grouped_steps:
            continue

        collapsed_convert_rows = collapsed_convert_rows or len(grouped_steps) > 1
        first_step = grouped_steps[0]
        last_step = grouped_steps[-1]
        status = str(last_step.get("status") or "-").strip().lower() or "-"
        finished_at = last_step.get("finished_at")
        if status in {"running", "queued"}:
            finished_at = None
        duration_seconds = _timeline_duration_seconds(
            first_step.get("started_at"),
            finished_at or now_iso,
        )
        progress_parts = _conversion_progress_parts(item)
        action_label = str(last_step.get("label") or last_step.get("key") or "").strip()
        detail_parts: list[str] = []
        if action_label:
            action_prefix = "Current action" if status in {"running", "queued"} else "Last action"
            detail_parts.append(f"{action_prefix}: {action_label}")
        detail_parts.extend(progress_parts)
        rows.append(
            {
                "step": "Zarr Conversion",
                "detail": " · ".join(detail_parts) if detail_parts else "-",
                "status": str(last_step.get("status") or "-"),
                "started": _format_status_timestamp(first_step.get("started_at")),
                "finished": _format_status_timestamp(finished_at),
                "duration": _format_runtime_duration(duration_seconds),
                "stage": "Zarr",
            }
        )

    return (rows, collapsed_convert_rows)


def _render_pipeline_timeline(item: dict[str, Any]) -> None:
    timeline = _pipeline_timeline_snapshot(item)
    raw_stages = [dict(stage) for stage in list(timeline.get("stages") or []) if isinstance(stage, dict)]
    stages = _display_pipeline_stages(item, timeline, raw_stages)
    if not stages:
        return

    current_stage_key = _display_stage_key(str(timeline.get("current_stage") or "").strip().lower())
    done_count = sum(
        1
        for stage in stages
        if str(stage.get("status") or "").strip().lower() == "done"
    )
    skipped_count = sum(
        1
        for stage in stages
        if str(stage.get("status") or "").strip().lower() == "skipped"
    )
    active_count = sum(
        1
        for stage in stages
        if str(stage.get("status") or "").strip().lower() in {"running", "queued"}
    )
    waiting_count = sum(
        1
        for stage in stages
        if str(stage.get("status") or "").strip().lower() == "pending"
    )
    progress_count = done_count + skipped_count
    stage_progress = (100.0 * progress_count / len(stages)) if stages else 0.0
    current_stage = next(
        (
            stage
            for stage in stages
            if str(stage.get("key") or "").strip().lower() == current_stage_key
        ),
        None,
    )
    if current_stage is None:
        current_stage = next(
            (
                stage
                for stage in stages
                if str(stage.get("status") or "").strip().lower() in {"running", "queued"}
            ),
            None,
        )
    if current_stage is None:
        current_stage = next(
            (
                stage
                for stage in reversed(stages)
                if str(stage.get("status") or "").strip().lower() in {"done", "skipped", "failed", "cancelled"}
            ),
            None,
        )
    terminal_success = _display_stages_are_terminal_success(item, stages)
    current_stage_label = (
        _terminal_pipeline_label(item)
        if terminal_success
        else str((current_stage or {}).get("label") or timeline.get("current_stage_label") or "").strip()
        or "Waiting"
    )
    mask_types = _normalize_mask_types(_mask_types_from_payload(item))
    cube_mode = str(timeline.get("cube_mode") or _cube_mode_from_payload(item) or "none").strip().lower()
    flow_bits = [str(stage.get("label") or stage.get("key") or "Stage") for stage in stages]
    detail_bits: list[str] = []
    if cube_mode == "before_mask":
        detail_bits.append("cube before masks")
    elif cube_mode == "after_mask":
        detail_bits.append("cube after masks")
    else:
        detail_bits.append("direct Zarr pipeline")
    if mask_types:
        detail_bits.append("masks: " + " + ".join(mask.title() for mask in mask_types))
    else:
        detail_bits.append("no integrated masks")

    overview_cards = [
        ("Done", f"{done_count}/{len(stages)}"),
    ]
    if skipped_count > 0:
        overview_cards.append(("Skipped", str(skipped_count)))
    overview_cards.extend(
        [
            ("Current", current_stage_label),
            ("Waiting", str(waiting_count)),
        ]
    )
    if active_count > 0:
        overview_cards.append(("Live", str(active_count)))

    cards: list[str] = []
    for index, stage in enumerate(stages, start=1):
        status_kind = str(stage.get("status") or "pending").strip().lower() or "pending"
        stage_key = str(stage.get("key") or "").strip().lower()
        status_label = _stage_status_label_for_display(stage, status_kind)
        raw_label = str(stage.get("label") or stage_key or "Stage")
        label = html.escape(raw_label)
        badge = html.escape(str(stage.get("badge") or "STEP"))
        detail = str(stage.get("detail_label") or raw_label)
        duration_label = _format_runtime_duration(_stage_elapsed_seconds(stage))
        started_label = _format_status_timestamp(stage.get("started_at"))
        finished_label = _format_status_timestamp(stage.get("finished_at"))
        progress_bits: list[str] = []
        if stage_key == "zarr":
            progress_bits = _conversion_progress_parts(item)

        detail_line = detail
        meta_pills: list[str] = []
        if status_kind == "queued":
            detail_line = "Queued to start"
        elif status_kind == "running":
            if duration_label != "-":
                meta_pills.append(duration_label)
            meta_pills.extend(progress_bits)
        elif status_kind == "pending":
            detail_line = "Waiting for previous stage"
        elif status_kind in {"done", "skipped", "failed", "cancelled"}:
            when_label = finished_label if finished_label != "-" else started_label
            if duration_label != "-":
                meta_pills.append(duration_label)
            if when_label != "-":
                meta_pills.append(when_label)

        if not meta_pills and status_kind == "pending":
            meta_pills.append("blocked")
        elif not meta_pills and status_kind == "queued":
            meta_pills.append("scheduled")
        elif not meta_pills and status_kind == "skipped":
            meta_pills.append("not required")

        card_classes = ["nimbus-stage-card", f"nimbus-stage-{status_kind}"]
        if stage_key == current_stage_key and status_kind in {"running", "queued"}:
            card_classes.append("is-current")
        if _stage_display_has_warning(stage):
            card_classes.append("has-warning")

        pill_markup = "".join(
            "<span class='nimbus-stage-pill'>{text}</span>".format(text=html.escape(str(text)))
            for text in meta_pills
        )

        cards.append(
            "<div class='{classes}'>"
            "<div class='nimbus-stage-head'>"
            "<div class='nimbus-stage-chip-row'>"
            "<span class='nimbus-stage-index'>{index:02d}</span>"
            "<span class='nimbus-stage-badge'>{badge}</span>"
            "</div>"
            "<span class='nimbus-stage-status'>{status}</span>"
            "</div>"
            "<div class='nimbus-stage-title'>{label}</div>"
            "<div class='nimbus-stage-detail'>{detail}</div>"
            "<div class='nimbus-stage-pills'>{pills}</div>"
            "</div>".format(
                classes=" ".join(card_classes),
                index=index,
                badge=badge,
                status=html.escape(status_label),
                label=label,
                detail=html.escape(detail_line),
                pills=pill_markup,
            )
        )

    st.markdown(
        "<div class='nimbus-pipeline-shell'>"
        "<div class='nimbus-pipeline-overview'>"
        "<div class='nimbus-pipeline-title-block'>"
        "<div class='nimbus-pipeline-eyebrow'>Pipeline map</div>"
        "<div class='nimbus-pipeline-headline'>{headline}</div>"
        "<div class='nimbus-pipeline-subtitle'>{subtitle}</div>"
        "</div>"
        "<div class='nimbus-pipeline-metrics'>{metrics}</div>"
        "</div>"
        "<div class='nimbus-pipeline-progress'><span style='width:{progress:.2f}%'></span></div>"
        "<div class='nimbus-stage-grid'>{cards}</div>"
        "</div>".format(
            headline=html.escape(" -> ".join(flow_bits)),
            subtitle=html.escape(
                f"{' · '.join(detail_bits)} · {done_count} done · {skipped_count} skipped · "
                f"{active_count} live · {waiting_count} waiting"
            ),
            metrics="".join(
                (
                    "<div class='nimbus-pipeline-metric'>"
                    f"<span>{html.escape(label_text)}</span>"
                    f"<strong>{html.escape(value_text)}</strong>"
                    "</div>"
                )
                for label_text, value_text in overview_cards
            ),
            progress=stage_progress,
            cards="".join(cards),
        ),
        unsafe_allow_html=True,
    )

    display_rows, convert_rows_grouped = _timeline_breakdown_rows(item, timeline)
    if not display_rows:
        return
    with st.expander("Step breakdown", expanded=False):
        if convert_rows_grouped:
            st.caption(
                "Zarr rows are grouped here. Repeated write/register events usually mean "
                "different scenes are finishing, not a backend loop."
            )
        st.dataframe(
            display_rows,
            width="stretch",
            hide_index=True,
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
        return ("provider rate-limited", "#fbbf24", "rgba(251,191,36,0.16)", "↻")

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
    if provider_issue is None or provider_issue[0] != "provider rate-limited":
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
    return job_type == "mask_existing_zarr"


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
        return ("rate-limited", f"{provider_label} is rate-limiting downloads and the backend is backing off.", "#f59e0b")
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
    all_statuses = _filter_recent_job_rows(
        _merge_job_rows(active_statuses, recent_statuses),
        hours=RECENT_JOBS_WINDOW_HOURS,
        limit=RECENT_JOBS_FETCH_LIMIT,
    )
    statuses = [row for row in all_statuses if not _job_is_mask_job(row)]
    st.session_state["active_job_ids"] = [
        str(item.get("job_id", "")).strip()
        for item in active_statuses
        if not _job_is_mask_job(item)
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
        st.metric("Pipelines", total_jobs)
    with k2:
        st.metric("Active", running_jobs)
    with k3:
        st.metric("Succeeded", succeeded_jobs)
    with k4:
        st.metric("Failed", failed_jobs)
    with k5:
        st.metric("Cancelled", cancelled_jobs)
    st.progress(max(0.0, min(1.0, progress_pct / 100.0)))
    st.caption(
        f"Global progress: {progress_pct:.2f}%"
        f" · {_format_bytes_compact(bytes_done)} / {_format_bytes_compact(bytes_total)}"
    )
    st.caption(
        f"Summary is computed from the current filtered scope inside recent activity (last {RECENT_JOBS_WINDOW_HOURS}h) plus active jobs."
    )
    st.caption("This panel shows the unified pipeline only: search, download, Zarr conversion, and optional in-place masking.")
    _render_provider_runtime_badge(download_provider_api, scoped_statuses)

    active_ids = [
        str(item.get("job_id", "")).strip()
        for item in scoped_statuses
        if str(item.get("state", "")).strip().lower() in ACTIVE_JOB_STATES
        and str(item.get("job_id", "")).strip()
    ]
    cancel_clicked = False
    reset_runtime_clicked = False
    if active_ids:
        action_col1, action_col2 = st.columns(2)
        with action_col1:
            cancel_clicked = st.button(
                f"⏹ Cancel active pipelines ({len(active_ids)})",
                width="stretch",
                key="jobs_cancel_active_btn",
            )
        with action_col2:
            reset_runtime_clicked = st.button(
                "♻️ Force reset runtime",
                width="stretch",
                key="jobs_force_reset_runtime_btn",
            )
    else:
        st.caption("No active pipeline is running in the current scope.")
        reset_runtime_clicked = st.button(
            "♻️ Force reset runtime",
            width="stretch",
            key="jobs_force_reset_runtime_btn_idle",
        )
    st.caption(
        "Force reset preserves history and files, but immediately closes non-terminal jobs, cancels queued/download coordinator tasks, and clears worker heartbeats."
    )

    filter_col1, filter_col2 = st.columns([2, 4])
    with filter_col1:
        job_view_counts = _job_view_counts(
            scoped_statuses,
            recent_minutes=RECENT_JOB_CATEGORY_MINUTES,
            active_states=ACTIVE_JOB_STATES,
        )
        job_view = st.radio(
            "Visible pipeline runs",
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
    st.caption("Changing provider, mission, or product in the form only affects new submissions.")
    visible_total = len(base_visible_statuses)
    if job_view == "recent":
        st.caption(f"Showing pipeline runs updated in the last {RECENT_JOB_CATEGORY_MINUTES} minutes inside the recent activity window.")
    st.caption(f"Showing {len(visible_statuses)} / {visible_total} matching pipeline runs.")
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
        st.info(f"Cancel requested for {cancelled}/{len(active_ids)} active pipelines in the current scope.")

    if reset_runtime_clicked:
        try:
            response = _api_request(
                "POST",
                _ss("api_url"),
                "/v1/jobs/reset-active",
                api_key=_ss("api_key"),
                timeout=60,
            )
            if response.ok:
                payload = dict(response.json() or {})
                st.session_state["active_job_ids"] = []
                st.session_state["job_status_cache"] = {}
                st.session_state["job_result_cache"] = {}
                st.session_state["job_event_log"] = []
                st.session_state["dl_last_event_id"] = 0
                st.session_state["dl_last_sse_ok"] = 0.0
                st.session_state["dl_event_errors"] = 0
                st.success(
                    "Runtime reset applied: "
                    f"{int(payload.get('jobs_cancelled', 0) or 0)} jobs cancelled, "
                    f"{int(payload.get('coordinator_tasks_cancelled', 0) or 0)} coordinator tasks cancelled, "
                    f"{int(payload.get('worker_heartbeats_cleared', 0) or 0)} worker heartbeats cleared."
                )
            else:
                st.error(f"{response.status_code}: {response.text}")
        except Exception as exc:
            st.error(str(exc))

    result_cache = _ensure_job_results_loaded(visible_statuses)
    _render_job_cards(
        visible_statuses,
        result_cache=result_cache,
        empty_message="No pipeline runs match the selected filter.",
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
        refresh_clicked = st.button("Refresh service status", width="stretch", key="refresh_service_status_btn")
        _ensure_api_runtime_statuses(force=refresh_clicked)
        st.caption(f"Last checked: {_format_status_timestamp(_ss('service_status_checked_at'))}")
        _render_status_block("API health", _ss("api_health_snapshot"), kind="service")
        _render_status_block("API readiness", _ss("api_readiness_snapshot"), kind="service")
        _render_status_block("Worker execution", _ss("worker_status_snapshot"), kind="worker")
        _render_status_block("Download coordinator", _ss("download_coordinator_snapshot"), kind="coordinator")
        selected_provider_api = PROVIDER_CLI_MAP.get(str(_ss("provider", "Copernicus")))
        _render_provider_auth_panel(selected_provider_api)

    st.sidebar.markdown('<div style="display:flex;align-items:center;gap:6px;padding-top:.3rem"><span>📡</span><span style="font-weight:600;font-size:.88rem;">Data Source</span></div>', unsafe_allow_html=True)
    provider = st.sidebar.selectbox("Provider", list(PROVIDERS.keys()), index=list(PROVIDERS.keys()).index(_ss("provider", "Copernicus")), key="sb_prov")
    st.session_state["provider"] = provider
    synced_provider = str(st.session_state.get("_tile_system_synced_provider") or "")
    desired_tile_system = _default_tile_system_for_provider(provider, sat_tiles)
    if desired_tile_system and synced_provider != provider:
        st.session_state["_tile_system_synced_provider"] = provider
        if st.session_state.get("tile_system") != desired_tile_system:
            st.session_state["tile_system"] = desired_tile_system
            st.session_state["selected_tiles"] = []
            st.session_state["intersecting_tiles"] = []
            st.rerun()
    else:
        st.session_state["_tile_system_synced_provider"] = provider
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
    aoi_choices = ["Draw on map", "Preset square", "Country", "Paste WKT / GeoJSON"]
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
    elif aoi_mode == "Country":
        country_catalog = load_country_catalog()
        if not country_catalog:
            st.sidebar.warning("Country catalog unavailable.")
        else:
            continents = ["All"] + sorted(
                {str(item.get("continent") or "Other") for item in country_catalog}
            )
            selected_continent = str(_ss("country_continent_filter", "All") or "All")
            if selected_continent not in continents:
                selected_continent = "All"
            selected_continent = st.sidebar.selectbox(
                "Continent",
                continents,
                index=continents.index(selected_continent),
                key="country_continent_filter",
            )
            filtered_countries = [
                item
                for item in country_catalog
                if selected_continent == "All" or item.get("continent") == selected_continent
            ]
            country_names = [str(item.get("name") or "") for item in filtered_countries]
            if not country_names:
                st.sidebar.caption("No country available for this filter.")
                country_names = [""]
            selected_country = str(_ss("country_name", "") or "")
            if selected_country not in country_names:
                selected_country = country_names[0] if country_names else ""
                st.session_state["country_name"] = selected_country
            selected_country = st.sidebar.selectbox(
                "Country",
                country_names,
                index=country_names.index(selected_country) if selected_country in country_names else 0,
                key="country_name",
            )
            selected_record = next(
                (item for item in filtered_countries if item.get("name") == selected_country),
                None,
            )
            if selected_record is not None:
                subtitle = str(selected_record.get("continent") or "").strip()
                iso_a3 = str(selected_record.get("iso_a3") or "").strip()
                if iso_a3:
                    subtitle = f"{subtitle} · {iso_a3}" if subtitle else iso_a3
                if subtitle:
                    st.sidebar.caption(subtitle)

                center = list(selected_record.get("center") or st.session_state["map_center"])
                bounds = tuple(selected_record.get("bounds") or ())
                zoom = zoom_for_bounds(bounds) if len(bounds) == 4 else int(st.session_state["map_zoom"])

                action_cols = st.sidebar.columns(2)
                with action_cols[0]:
                    if st.button("✅ Apply", width="stretch", key="apply_country_aoi"):
                        st.session_state["geometry_text"] = str(selected_record.get("wkt") or "")
                        st.session_state["map_center"] = [float(center[0]), float(center[1])]
                        st.session_state["map_zoom"] = int(zoom)
                        st.session_state["fly_to"] = json.dumps(
                            [float(center[0]), float(center[1]), int(zoom)]
                        )
                        st.rerun()
                with action_cols[1]:
                    if st.button("🎯 Focus", width="stretch", key="focus_country_aoi"):
                        st.session_state["map_center"] = [float(center[0]), float(center[1])]
                        st.session_state["map_zoom"] = int(zoom)
                        st.session_state["fly_to"] = json.dumps(
                            [float(center[0]), float(center[1]), int(zoom)]
                        )
                        st.rerun()
            st.sidebar.caption("Select a country to use its boundary as AOI.")
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
        <div style='font-size:1.05rem;font-weight:800;background:linear-gradient(135deg,#38bdf8,#2dd4bf);color:#020617;width:44px;height:44px;border-radius:12px;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 12px rgba(56,189,248,0.35);'>NC</div>
        <div><div style='font-size:1.25rem;font-weight:700;color:#e2e8f0;'>NimbusChain Pipeline</div><div style='font-size:0.72rem;color:#64748b;letter-spacing:.04em;'>AOI · Pipeline jobs · Runtime monitoring</div></div>
    </div>""",
        unsafe_allow_html=True,
    )

    download_provider_api = PROVIDER_CLI_MAP.get(provider)

    tab_map, tab_launch, tab_downloads, tab_res, tab_set = st.tabs(
        ["AOI & Tiles", "Pipeline Job", "Downloads & Queue", "Results", "Settings"]
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

    with tab_launch:
        effective_product_type = str(product)
        if provider == "USGS":
            effective_product_type = _resolve_usgs_product_type(
                selected_product_type=str(product),
                selected_satellite=str(_ss("usgs_satellite", "Any")),
            )

        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🚀</span><span style="font-weight:600;font-size:.94rem;">Preview & Pipeline Launch</span></div>',
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
        _ensure_api_runtime_statuses(
            max_age_seconds=8.0 if bool(_ss("dl_auto_refresh", True)) else None,
        )
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

        mode_text = "single pipeline run"
        if provider == "Copernicus" and len(selected_tiles_for_cmd) > 1:
            mode_text = f"pipeline batch by tile ({len(selected_tiles_for_cmd)} runs)"
        elif provider == "Copernicus" and len(selected_tiles_for_cmd) == 1:
            mode_text = "single pipeline run with tile filter"
        st.caption(f"Submit mode: {mode_text}")

        download_strategy = "default"
        if provider == "Copernicus":
            pool_size = int((current_provider_status or {}).get("account_pool_size", 0) or 0)
            pool_concurrency = int((current_provider_status or {}).get("account_pool_concurrency", 0) or 0)
            pool_available = bool((current_provider_status or {}).get("account_pool_configured"))
            download_strategy = st.radio(
                "Download execution",
                options=["default", "copernicus_account_pool"],
                horizontal=True,
                key="download_execution_mode",
                format_func=lambda value: {
                    "default": "Stable single account",
                    "copernicus_account_pool": "Account pool test",
                }[value],
            )
            if download_strategy == "copernicus_account_pool":
                if pool_available:
                    st.caption(
                        f"Account pool test mode is enabled. Available accounts: {pool_size} · per-account concurrency target: {pool_concurrency}."
                    )
                else:
                    st.caption(
                        "Account pool test mode was selected, but no extra Copernicus accounts are configured yet. The job will fall back to the primary account."
                    )

        download_mask_mode = st.radio(
            "Pipeline final stage",
            options=["none", "water", "cloud", "water + cloud"],
            horizontal=True,
            key="download_mask_mode",
            format_func=lambda value: {
                "none": "Zarr only",
                "water": "Zarr + water",
                "cloud": "Zarr + cloud",
                "water + cloud": "Zarr + cloud + water",
            }[value],
        )
        requested_download_mask_types = _mask_mode_to_types(download_mask_mode)
        if requested_download_mask_types:
            st.caption(
                "This pipeline will continue after Zarr conversion and write the requested masks directly into the same Zarr store."
            )
        else:
            st.caption("This pipeline stops at the Zarr stage.")

        preview_available_dates = _preview_available_dates(list(_ss("preview_items", []) or []))
        cube_mode_options = ["none", "before_mask", "after_mask"]
        cube_mode_help = "Cube building uses only dates that are actually available in the current preview results."
        selected_cube_mode = st.radio(
            "Cube building",
            options=cube_mode_options,
            horizontal=True,
            key="download_cube_mode",
            format_func=lambda value: {
                "none": "No cube",
                "before_mask": "Cube before masking",
                "after_mask": "Cube after masking",
            }[value],
            help=cube_mode_help,
        )
        if selected_cube_mode == "after_mask" and not requested_download_mask_types:
            st.caption("Cube after masking requires at least one mask type in the Zarr + mask mode above.")

        cube_start_date = st.session_state["start_date"]
        cube_end_date = st.session_state["end_date"]
        selected_cube_layout = "grouped_time"
        cube_target_crs: str | None = None
        cube_target_resolution_m = 10
        cube_overlap_policy = "least_cloud"
        cube_ready = len(preview_available_dates) >= 2
        if selected_cube_mode != "none":
            layout_col, overlap_col = st.columns([1, 1])
            with layout_col:
                selected_cube_layout = st.selectbox(
                    "Cube layout",
                    options=["grouped_time", "daily_mosaic"],
                    key="download_cube_layout",
                    format_func=lambda value: {
                        "grouped_time": "Grouped time cube",
                        "daily_mosaic": "Daily multi-tile mosaic",
                    }[value],
                )
            with overlap_col:
                cube_overlap_policy = st.selectbox(
                    "Overlap policy",
                    options=["least_cloud", "latest", "earliest", "first_valid"],
                    key="download_cube_overlap_policy",
                    format_func=lambda value: {
                        "least_cloud": "Least cloud",
                        "latest": "Latest",
                        "earliest": "Earliest",
                        "first_valid": "First valid",
                    }[value],
                    disabled=selected_cube_layout != "daily_mosaic",
                )
            if selected_cube_layout == "daily_mosaic":
                res_col, crs_col = st.columns([1, 1])
                with res_col:
                    cube_target_resolution_m = int(
                        st.number_input(
                            "Mosaic resolution",
                            min_value=1,
                            max_value=1000,
                            value=int(st.session_state.get("download_cube_target_resolution_m", 10)),
                            step=1,
                            key="download_cube_target_resolution_m",
                        )
                    )
                with crs_col:
                    cube_target_crs = st.text_input(
                        "Target CRS",
                        value=st.session_state.get("download_cube_target_crs", ""),
                        placeholder="auto or EPSG:32631",
                        key="download_cube_target_crs",
                    ).strip() or None
                if provider == "Copernicus" and len(selected_tiles_for_cmd) > 1:
                    st.caption("Daily mosaic keeps selected tiles in one pipeline run instead of creating one job per tile.")
            if cube_ready:
                default_cube_start = min(
                    max(st.session_state["start_date"], preview_available_dates[0]),
                    preview_available_dates[-1],
                )
                default_cube_end = min(
                    max(st.session_state["end_date"], default_cube_start),
                    preview_available_dates[-1],
                )
                c1, c2 = st.columns(2)
                with c1:
                    cube_start_date = st.date_input(
                        "Cube start",
                        value=default_cube_start,
                        min_value=preview_available_dates[0],
                        max_value=preview_available_dates[-1],
                        key="cube_start_date",
                    )
                with c2:
                    cube_end_date = st.date_input(
                        "Cube end",
                        value=max(default_cube_end, cube_start_date),
                        min_value=cube_start_date,
                        max_value=preview_available_dates[-1],
                        key="cube_end_date",
                    )
                preview_date_text = ", ".join(day.isoformat() for day in preview_available_dates[:8])
                if len(preview_available_dates) > 8:
                    preview_date_text = f"{preview_date_text}, ..."
                st.caption(f"Available dates from preview: {preview_date_text}")
            else:
                st.warning(
                    "Cube building needs at least two available acquisition dates in the current preview. Refresh preview or widen the search dates first."
                )

        render_pipeline_plan_summary(
            provider_label=provider,
            collection=collection,
            product_type=str(effective_product_type),
            mask_types=requested_download_mask_types,
            cube_mode=selected_cube_mode,
        )

        d1, d2 = st.columns([2, 1])
        with d1:
            start_clicked = st.button(
                "🚀 Start Pipeline",
                width="stretch",
                type="primary",
                disabled=provider_blocked,
            )
        with d2:
            st.markdown(
                "<div style='background:#0f172a;border:1px solid rgba(56,120,200,0.10);border-radius:12px;padding:12px;'>"
                "<div style='font-size:.72rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;'>Monitoring</div>"
                "<div style='font-size:1rem;font-weight:700;color:#e2e8f0;margin-top:2px;'>Use Downloads & Queue</div>"
                "<div style='font-size:.74rem;color:#94a3b8;margin-top:6px;'>Live pipelines, provider throttling, coordinator state and queue fairness are grouped there.</div>"
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

        if provider_blocked and provider_guidance:
            st.warning(f"Download blocked: {provider_guidance}")

        if start_clicked:
            if not aoi_text_for_download:
                st.error("Define AOI or select tiles first.")
            elif selected_cube_mode == "after_mask" and not requested_download_mask_types:
                st.error("Cube after masking requires enabling cloud, water, or cloud + water masking.")
            elif selected_cube_mode != "none" and not cube_ready:
                st.error("Cube building needs at least two available acquisition dates in the current preview.")
            else:
                try:
                    create_single_mosaic_job = (
                        selected_cube_mode != "none"
                        and selected_cube_layout == "daily_mosaic"
                    )
                    if provider == "Copernicus" and len(selected_tiles_for_cmd) > 1 and not create_single_mosaic_job:
                        jobs = [
                            _build_job_payload(
                                provider_label=provider,
                                collection=collection,
                                product_type=str(effective_product_type),
                                start_date=st.session_state["start_date"],
                                end_date=st.session_state["end_date"],
                                aoi_wkt=aoi_text_for_download,
                                tile_id=tile_id,
                                mask_types=requested_download_mask_types,
                                download_strategy=download_strategy,
                                cube_mode=selected_cube_mode,
                                cube_start_date=cube_start_date,
                                cube_end_date=cube_end_date,
                                cube_layout=selected_cube_layout,
                                cube_target_crs=cube_target_crs,
                                cube_target_resolution_m=cube_target_resolution_m,
                                cube_overlap_policy=cube_overlap_policy,
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
                            st.success(f"Created {len(created)} pipeline runs.")
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
                            mask_types=requested_download_mask_types,
                            download_strategy=download_strategy,
                            cube_mode=selected_cube_mode,
                            cube_start_date=cube_start_date,
                            cube_end_date=cube_end_date,
                            cube_layout=selected_cube_layout,
                            cube_target_crs=cube_target_crs,
                            cube_target_resolution_m=cube_target_resolution_m,
                            cube_overlap_policy=cube_overlap_policy,
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
                            st.success(f"Created pipeline run: {job_id}")
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

        st.caption("This tab is for preview and submission only. Live downloads and queue state are monitored in Downloads & Queue.")

    with tab_downloads:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>⬇️</span><span style="font-weight:600;font-size:.94rem;">Downloads & Queue</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("All live pipelines, downloads, provider throttling, runtime health and coordinator state are grouped here.")
        refresh_downloads = st.button(
            "Refresh downloads view",
            key="downloads_overview_refresh_btn",
            width="stretch",
        )
        _ensure_api_runtime_statuses(
            force=refresh_downloads,
            max_age_seconds=8.0 if bool(_ss("dl_auto_refresh", True)) else None,
        )
        _render_downloads_overview(download_provider_api)

        dl_live_tab, dl_coord_tab, dl_runtime_tab = st.tabs(
            ["🧭 Pipeline Monitor", "📦 Download Coordinator", "⚙️ Runtime Health"]
        )

        with dl_live_tab:
            st.caption("Unified pipeline monitor: each card now shows the real step timeline, elapsed time per stage, and the raw breakdown used by the backend.")
            if bool(_ss("dl_auto_refresh", True)):
                _render_download_jobs_panel_live(download_provider_api)
            else:
                _render_download_jobs_panel_static(download_provider_api)

        with dl_coord_tab:
            st.caption("Persistent local coordinator view: files by status, pending jobs, Copernicus workers, USGS adaptive window, and recent tasks.")
            _render_download_coordinator_panel()

        with dl_runtime_tab:
            st.caption("Service health, worker capacity, provider auth and throttling state for the selected provider.")
            _render_download_runtime_panel(download_provider_api)


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
