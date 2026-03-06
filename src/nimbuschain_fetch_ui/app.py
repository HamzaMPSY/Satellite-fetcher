"""
Satellite Imagery Downloader — Professional Edition v2

Complete rewrite replacing Folium/streamlit-folium with a native Leaflet
component using Canvas renderer for maximum performance.  The map is rendered
inside a custom Streamlit component (declare_component) with bidirectional
communication: Python sends grid/AOI/selection data → JS renders with Canvas;
JS sends drawn AOI / clicked tiles → Python processes them.

Key performance improvements over v1:
 • Canvas renderer (single <canvas> element vs thousands of SVG paths)
 • Client-side viewport filtering (no Python rerun on pan/zoom)
 • Compact grid format (name + bbox only, ~2 MB vs 20 MB GeoJSON)
 • No st.rerun() loop — only explicit user actions trigger reruns
"""

import os
import re
import sys
import math
import json
import time
import hashlib
import subprocess
import signal
import datetime as dt
import uuid
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass

import geopandas as gpd
import requests
import shapely
import streamlit as st
import streamlit.components.v1 as components
from loguru import logger
from requests.adapters import HTTPAdapter
from shapely.geometry import Polygon, mapping, box
from shapely import wkt as shapely_wkt
from shapely.ops import unary_union

try:
    from streamlit_file_browser import st_file_browser
except ImportError:
    st_file_browser = None

try:
    from utilities import ConfigLoader  # type: ignore
    # FIX: The old monkey-patch replaced get_var with a version that always
    # used default=None, which broke callers that relied on the _MISSING
    # sentinel (e.g. download_manager passing explicit defaults).
    # The new ConfigLoader already handles defaults properly via _MISSING,
    # so the patch is no longer needed.  We only wrap to guard against
    # exceptions that would crash the Streamlit UI during import.
    _original_get_var = ConfigLoader.get_var
    def _patched_get_var(self, key, default=None):
        try:
            return _original_get_var(self, key, default=default)
        except Exception:
            return default
    ConfigLoader.get_var = _patched_get_var
except Exception:
    ConfigLoader = None

# Prefer the mounted source tree inside the container over the installed wheel.
SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from nimbuschain_fetch_ui.aoi_utils import parse_aoi_text
from nimbuschain_fetch_ui.job_api_runtime import (
    build_job_payload as build_job_payload_runtime,
    filter_active_job_ids,
    merge_status_rows as merge_status_rows_runtime,
    parse_sse_lines,
    should_poll_fallback,
    summarize_statuses,
)
from nimbuschain_fetch_ui.preview_local import preview_products_local


# ═══════════════════════════════════════════════════════════════════════════════
# PROJECT PATHS
# ═══════════════════════════════════════════════════════════════════════════════
# Streamlit can be launched from any working directory (VSCode, terminal, etc.).
# The CLI and relative file paths must therefore be resolved against the project
# root (the folder containing this script) to behave consistently.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOWNLOADS_DIR = Path(os.getenv("NIMBUS_UI_DATA_DIR", "/data/downloads"))
ZARR_STORES_DIR = DOWNLOADS_DIR / "zarr"
NOHUP_PATH = PROJECT_ROOT / "nohup.out"
PID_PATH = PROJECT_ROOT / "job_pid"
DEFAULT_API_URL = os.getenv("NIMBUS_SERVICE_URL", "http://nimbus-api:8000")
DEFAULT_API_KEY = os.getenv("NIMBUS_API_KEY", "")
DEFAULT_ZARR_URL = os.getenv("NIMBUS_ZARR_SERVICE_URL", "http://nimbus-zarr:8010")
RECENT_JOBS_WINDOW_HOURS = int(os.getenv("NIMBUS_UI_RECENT_JOBS_HOURS", "72"))
RECENT_JOB_CATEGORY_MINUTES = int(os.getenv("NIMBUS_UI_RECENT_JOB_MINUTES", "60"))
RECENT_JOBS_LIMIT = int(os.getenv("NIMBUS_UI_RECENT_JOBS_LIMIT", "80"))
RECENT_JOBS_FETCH_LIMIT = max(100, RECENT_JOBS_LIMIT * 3)
JOB_MONITOR_REFRESH_EVERY = os.getenv("NIMBUS_UI_JOB_REFRESH_EVERY", "3s")
FINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}
ACTIVE_JOB_STATES = {"queued", "running", "cancel_requested"}

# ═══════════════════════════════════════════════════════════════════════════════
# LOGURU SETUP — File + console logging for diagnostics
# ═══════════════════════════════════════════════════════════════════════════════
_APP_LOG = PROJECT_ROOT / "app_debug.log"
# Remove default stderr handler and add one with a cleaner format
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
)
logger.add(
    str(_APP_LOG),
    level="DEBUG",
    rotation="5 MB",
    retention="3 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
)
logger.info("=" * 60)
logger.info("Satellite Downloader v2 — app starting")
logger.info(f"PROJECT_ROOT : {PROJECT_ROOT}")
logger.info(f"DOWNLOADS_DIR: {DOWNLOADS_DIR}")
logger.info(f"NOHUP_PATH   : {NOHUP_PATH}")
logger.info(f"PID_PATH     : {PID_PATH}")
logger.info(f"Python       : {sys.executable}")
logger.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class TilePaths:
    # Prefer mounted container paths, then workspace fallback.
    S2_GEOJSON: str = "/data/Sentinel-2-tiles/sentinel-2_grids.geojson"
    S2_NOCOV: str = "/data/Sentinel-2-tiles/sentinel-2_no_coverage.geojson"
    S2_GEOJSON_FALLBACK: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_grids.geojson")
    S2_NOCOV_FALLBACK: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_no_coverage.geojson")
    LANDSAT_GEOJSON: str = "/data/Landsat-tiles/wrs2_descending.geojson"
    LANDSAT_GEOJSON_FALLBACK: str = str(PROJECT_ROOT / "data" / "Landsat-tiles" / "wrs2_descending.geojson")


@dataclass(frozen=True)
class MapConfig:
    MIN_GRID_ZOOM: int = 5
    DEFAULT_CENTER: Tuple[float, float] = (48.8566, 2.3522)
    DEFAULT_ZOOM: int = 8
    MAP_HEIGHT: int = 700


PATHS = TilePaths()
MCFG = MapConfig()

PROVIDERS: Dict[str, List[str]] = {
    "Copernicus": ["SENTINEL-1", "SENTINEL-2", "SENTINEL-3", "SENTINEL-5P"],
    "USGS": ["landsat_ot_c2_l1", "landsat_ot_c2_l2"],
}

PRODUCT_TYPES: Dict[str, List[str]] = {
    "SENTINEL-1": ["RAW", "GRD", "SLC", "IW_SLC__1S"],
    "SENTINEL-2": ["S2MSI1C", "S2MSI2A"],
    "SENTINEL-3": [
        "S3OL1EFR", "S3OL1ERR", "S3SL1RBT", "S3OL2WFR", "S3OL2WRR",
        "S3OL2LFR", "S3OL2LRR", "S3SL2LST", "S3SL2FRP", "S3SR2LAN",
        "S3SY2SYN", "S3SY2VGP", "S3SY2VG1", "S3SY2V10", "S3SY2AOD",
    ],
    "SENTINEL-5P": [
        "L2__NO2___", "L2__CH4___", "L2__CO____",
        "L2__O3____", "L2__SO2___", "L2__HCHO__",
    ],
    "landsat_ot_c2_l1": ["L1TP", "L1GT", "L1GS"],
    "landsat_ot_c2_l2": ["L2SP", "L2SR"],
}

# ── FIX: Explicit mapping from UI provider name to CLI --provider value ──
PROVIDER_CLI_MAP: Dict[str, str] = {
    "Copernicus": "copernicus",
    "USGS": "usgs",
}


# ═══════════════════════════════════════════════════════════════════════════════
# STYLING
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
:root {
    --bg-0: #040814;
    --bg-1: #08101f;
    --bg-2: rgba(15, 23, 42, 0.78);
    --panel: rgba(12, 18, 34, 0.82);
    --panel-strong: rgba(17, 24, 39, 0.92);
    --line: rgba(56, 120, 200, 0.14);
    --line-strong: rgba(56, 189, 248, 0.28);
    --text: #e2e8f0;
    --muted: #8aa0bc;
    --accent: #38bdf8;
    --accent-2: #2dd4bf;
    --shadow: 0 18px 48px rgba(2, 8, 23, 0.42);
}
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.12), transparent 28%),
        radial-gradient(circle at top right, rgba(45, 212, 191, 0.08), transparent 24%),
        linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 56%, #02050d 100%) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', system-ui, sans-serif !important;
}
[data-testid="stHeader"] {
    background: rgba(4, 8, 20, 0.72) !important;
    backdrop-filter: blur(14px);
}
[data-testid="stMainBlockContainer"],
.main .block-container {
    padding-top: 4.5rem !important;
    padding-bottom: 2rem !important;
}
[data-testid="stSidebar"] {
    background:
        linear-gradient(180deg, rgba(8, 16, 31, 0.97), rgba(5, 10, 21, 0.98)) !important;
    border-right: 1px solid var(--line) !important;
    box-shadow: inset -1px 0 0 rgba(255,255,255,0.02) !important;
}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label, [data-testid="stSidebar"] span {
    color: var(--text) !important;
}
[data-testid="stSidebar"] .stTextInput > div > div,
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stDateInput > div > div,
[data-testid="stSidebar"] .stTextArea textarea {
    background: rgba(10, 16, 30, 0.9) !important;
    border: 1px solid rgba(56, 120, 200, 0.12) !important;
    border-radius: 12px !important;
}
.stButton > button {
    background: rgba(56,189,248,0.08) !important;
    color: var(--accent) !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    border-radius: 12px !important;
    font-weight: 600 !important;
    transition: all 0.2s ease !important;
    box-shadow: 0 10px 30px rgba(2, 8, 23, 0.22) !important;
}
.stButton > button:hover {
    background: rgba(56,189,248,0.16) !important;
    border-color: var(--accent) !important;
    box-shadow: 0 14px 36px rgba(56,189,248,0.12) !important;
    transform: translateY(-1px);
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, var(--accent), var(--accent-2)) !important;
    border: none !important;
    color: #060a14 !important;
    font-weight: 700 !important;
}
.stTextInput > div > div > input,
.stTextArea textarea,
.stSelectbox > div > div,
.stDateInput > div > div {
    border-radius: 12px !important;
}
.stTabs [data-baseweb="tab-list"] {
    background: linear-gradient(180deg, rgba(8,16,31,0.92), rgba(7,12,24,0.95)) !important;
    border-radius: 16px !important;
    padding: 6px !important;
    border: 1px solid var(--line) !important;
    gap: 4px !important;
    width: 100% !important;
    box-shadow: var(--shadow) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 12px !important;
    color: var(--muted) !important;
    font-weight: 600 !important;
    flex: 1 1 0% !important;
    justify-content: center !important;
    padding: 10px 14px !important;
    font-size: 0.9rem !important;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(180deg, rgba(56,189,248,0.18), rgba(45,212,191,0.10)) !important;
    color: var(--accent) !important;
    border: 1px solid rgba(56,189,248,0.14) !important;
}
[data-testid="stExpander"] {
    background: var(--panel-strong) !important;
    border: 1px solid var(--line) !important;
    border-radius: 12px !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stMetric"] {
    background: linear-gradient(180deg, rgba(12,18,34,0.92), rgba(9,14,28,0.92)) !important;
    border: 1px solid var(--line) !important;
    border-radius: 14px !important;
    padding: 0.7rem 0.9rem !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stMetricLabel"] {
    color: var(--muted) !important;
}
[data-testid="stMetricValue"] {
    color: var(--text) !important;
}
pre, code {
    background: #0b1120 !important;
    color: var(--text) !important;
    border-radius: 8px !important;
}
[data-testid="stDataFrame"],
[data-testid="stMarkdownContainer"] table {
    border: 1px solid var(--line) !important;
    border-radius: 14px !important;
    overflow: hidden !important;
    box-shadow: var(--shadow) !important;
}
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: #060a14; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.18); border-radius: 3px; }
/* Hide the leaflet component iframe border */
iframe[title="leaflet_map"] {
    border: none !important;
    border-radius: 14px !important;
    box-shadow: var(--shadow) !important;
}
</style>
"""


# ═══════════════════════════════════════════════════════════════════════════════
# LEAFLET COMPONENT HTML
# ═══════════════════════════════════════════════════════════════════════════════

LEAFLET_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.css"/>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:#060a14;overflow:hidden}
#map{width:100%;height:100%}
.tile-tooltip{
    background:rgba(6,10,20,.95)!important;
    color:#48cae4!important;
    border:1px solid rgba(56,120,200,.35)!important;
    border-radius:6px!important;
    padding:5px 10px!important;
    font-size:12px!important;
    font-family:'JetBrains Mono',monospace!important;
    box-shadow:0 2px 10px rgba(0,0,0,.5)!important;
    pointer-events:none!important;
    white-space:nowrap!important;
}
.tile-tooltip::before{
    border-top-color:rgba(56,120,200,.35)!important;
}
.leaflet-draw-toolbar a{background-color:#111827!important;border-color:rgba(56,120,200,.2)!important;color:#38bdf8!important}
.leaflet-draw-toolbar a:hover{background-color:#1e293b!important}
.draw-toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:#111827;border:1px solid rgba(56,189,248,.3);border-radius:12px;padding:12px 20px;color:#38bdf8;font:600 13px/1.4 'DM Sans',sans-serif;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.5);display:none;max-width:90%;text-align:center}
.draw-toast.show{display:block;animation:toastIn .3s ease}
@keyframes toastIn{from{opacity:0;transform:translateX(-50%) translateY(10px)}to{opacity:1;transform:translateX(-50%) translateY(0)}}
.zoom-hint{position:absolute;top:10px;left:50%;transform:translateX(-50%);background:rgba(17,24,39,.92);border:1px solid rgba(251,191,36,.3);border-radius:8px;padding:6px 14px;color:#fbbf24;font:500 12px 'DM Sans',sans-serif;z-index:800;pointer-events:none;display:none}
/* FIX: Ensure Leaflet tooltip pane is above canvas */
.leaflet-tooltip-pane{z-index:650!important}
.leaflet-popup-pane{z-index:700!important}
</style>
</head>
<body>
<div id="map"></div>
<div class="draw-toast" id="drawToast"></div>
<div class="zoom-hint" id="zoomHint"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.js"></script>
<script>
(function(){
"use strict";

// ── Streamlit Component Protocol (minimal) ──────────────────────────
const Streamlit = {
    setComponentValue: function(v){
        window.parent.postMessage({isStreamlitMessage:true,type:"streamlit:setComponentValue",value:v},"*");
    },
    setFrameHeight: function(h){
        window.parent.postMessage({isStreamlitMessage:true,type:"streamlit:setFrameHeight",height:h},"*");
    }
};
window.parent.postMessage({isStreamlitMessage:true,type:"streamlit:componentReady",apiVersion:1},"*");

// ── State ────────────────────────────────────────────────────────────
let map = null;
let allTiles = [];            // compact: [{n:"01CCV", b:[minx,miny,maxx,maxy]}, ...]
let gridLayer = null;
let aoiLayer = null;
let interLayer = null;
let selLayer = null;
let nocovLayer = null;
let drawControl = null;
let drawnItems = null;
let interNames = new Set();
let selNames = new Set();
let tileSystem = "sentinel-2";
let showGrid = true;
let colorize = true;
let gridOpacity = 0.04;
let showNocov = false;
let showInter = true;
let showSel = true;
let clickSelect = true;   // FIX: default to true so clicks always register
let lastSentJSON = "";
let clickSeq = 0;

// FIX: Use SVG renderer for grid tiles to ensure tooltips and clicks work
const gridRenderer = L.svg({padding:0.5});

const MIN_GRID_ZOOM = 5;

// ── Color functions ──────────────────────────────────────────────────
const colColors = [];
for(let i=0;i<60;i++){
    const hue=(i*137.508)%360;
    const sat=70+(i%3)*10;
    const lit=45+(i%2)*15;
    colColors.push("hsl("+hue+","+sat+"%,"+lit+"%)");
}

function s2Color(name){
    if(!name||name.length<2) return "#0077BB";
    const z=parseInt(name.substring(0,2),10);
    if(isNaN(z)||z<1||z>60) return "#0077BB";
    return colColors[z-1];
}

function lsColor(name){
    if(!name||name.length<3) return "#EE7733";
    const p=parseInt(name.substring(0,3),10);
    if(isNaN(p)) return "#EE7733";
    const hue=18+((Math.min(233,Math.max(1,p))-1)*28/233);
    return "hsl("+hue+",85%,52%)";
}

function tileColor(name){
    return tileSystem==="landsat"?lsColor(name):s2Color(name);
}

function tileStyle(name){
    const isInter = showInter && interNames.has(name);
    const isSel   = showSel && selNames.has(name);
    if(isSel) return {color:"#EE3377",weight:3,fillOpacity:0.12,dashArray:"6,4"};
    if(isInter) return {color:"#AA3377",weight:2.2,fillOpacity:0.09};
    const c = colorize ? tileColor(name) : (tileSystem==="landsat"?"#EE7733":"#0077BB");
    return {color:c,weight:1.2,fillOpacity:gridOpacity};
}

// ── Map Init ─────────────────────────────────────────────────────────
function initMap(center, zoom){
    map = L.map("map",{
        center: center,
        zoom: zoom,
        zoomControl: true,
        preferCanvas: false,
        maxZoom: 19,
        minZoom: 2
    });

    // Base layers
    const satellite = L.tileLayer(
        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        {attribution:"Esri",maxNativeZoom:19,maxZoom:22}
    );
    const dark = L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        {attribution:"CartoDB",maxZoom:20}
    );
    const streets = L.tileLayer(
        "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        {attribution:"OSM",maxZoom:19}
    );
    satellite.addTo(map);
    L.control.layers({"Satellite":satellite,"Dark":dark,"Streets":streets},{},
        {position:"topright",collapsed:true}).addTo(map);

    // Draw control
    drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);
    drawControl = new L.Control.Draw({
        position:"topleft",
        draw:{
            polyline:false, circle:false, marker:false, circlemarker:false,
            rectangle:{shapeOptions:{color:"#CCBB44",weight:2,fillOpacity:0.08}},
            polygon:{shapeOptions:{color:"#CCBB44",weight:2,fillOpacity:0.08}}
        },
        edit:{featureGroup:drawnItems,edit:true,remove:true}
    });
    map.addControl(drawControl);

    // Draw events
    map.on(L.Draw.Event.CREATED, function(e){
        drawnItems.clearLayers();
        drawnItems.addLayer(e.layer);
        sendDrawnAOI();
    });
    map.on(L.Draw.Event.EDITED, sendDrawnAOI);
    map.on(L.Draw.Event.DELETED, function(){
        drawnItems.clearLayers();
        sendDrawnAOI();
    });

    // Viewport-driven grid update
    let moveTimer = null;
    map.on("moveend zoomend", function(){
        clearTimeout(moveTimer);
        moveTimer = setTimeout(updateGridDisplay, 80);
    });
    map.on("zoomend", updateZoomHint);

    updateZoomHint();
    Streamlit.setFrameHeight(document.body.scrollHeight || 700);
}

// ── Send drawn AOI back to Python ────────────────────────────────────
function sendDrawnAOI(){
    let wkt = "";
    drawnItems.eachLayer(function(layer){
        const latlngs = layer.getLatLngs();
        if(!latlngs || !latlngs.length) return;
        const ring = latlngs[0] || latlngs;
        if(ring.length < 3) return;
        const coords = ring.map(function(ll){return ll.lng.toFixed(6)+" "+ll.lat.toFixed(6)});
        coords.push(coords[0]);
        wkt = "POLYGON(("+coords.join(",")+"))";
    });
    maybeSend({type:"aoi", wkt:wkt});
    if(wkt){
        showToast("AOI drawn — processing...");
    }
}

// ── Tile click handler ───────────────────────────────────────────────
function onTileClick(name){
    if(!name) return;
    if(!clickSelect) return;
    clickSeq += 1;
    maybeSend({type:"tile_click", name:name, seq:clickSeq});
    showToast("Tile: " + name + " (toggled)");
}

// ── Deduplicated send ────────────────────────────────────────────────
function maybeSend(data){
    const j = JSON.stringify(data);
    if(j !== lastSentJSON){
        lastSentJSON = j;
        Streamlit.setComponentValue(data);
    }
}

// ── Zoom hint ────────────────────────────────────────────────────────
function updateZoomHint(){
    const hint = document.getElementById("zoomHint");
    if(!map || !hint) return;
    const z = map.getZoom();
    if(showGrid && z < MIN_GRID_ZOOM){
        hint.textContent = "Zoom to "+MIN_GRID_ZOOM+"+ for grid (current: "+z+")";
        hint.style.display = "block";
    } else {
        hint.style.display = "none";
    }
}

// ── Toast ────────────────────────────────────────────────────────────
function showToast(msg){
    const el = document.getElementById("drawToast");
    if(!el) return;
    el.textContent = msg;
    el.classList.add("show");
    setTimeout(function(){el.classList.remove("show")}, 2500);
}

function closeRing(coords){
    if(!Array.isArray(coords) || coords.length < 3) return [];
    const ring = coords.slice();
    const first = ring[0];
    const last = ring[ring.length - 1];
    if(!last || first[0] !== last[0] || first[1] !== last[1]){
        ring.push([first[0], first[1]]);
    }
    return ring;
}

function compactGeomToGeoJSON(t){
    if(t && Array.isArray(t.g) && t.g.length){
        if(t.g.length === 1){
            return {type:"Polygon", coordinates:[closeRing(t.g[0])]};
        }
        return {
            type:"MultiPolygon",
            coordinates: t.g.map(function(r){ return [closeRing(r)]; })
        };
    }
    const b = t.b;
    return {
        type:"Polygon",
        coordinates:[[
            [b[0],b[1]], [b[2],b[1]],
            [b[2],b[3]], [b[0],b[3]],
            [b[0],b[1]]
        ]]
    };
}

// ── Grid Display (client-side viewport filtering) ────────────────────
function updateGridDisplay(){
    if(!map) return;
    const zoom = map.getZoom();

    if(!showGrid || zoom < MIN_GRID_ZOOM || !allTiles.length){
        if(gridLayer){map.removeLayer(gridLayer); gridLayer=null;}
        return;
    }

    const bounds = map.getBounds();
    const w = bounds.getWest(), e = bounds.getEast();
    const s = bounds.getSouth(), n = bounds.getNorth();

    const visible = [];
    for(let i=0; i<allTiles.length; i++){
        const t = allTiles[i];
        const b = t.b;
        if(b[2]>=w && b[0]<=e && b[3]>=s && b[1]<=n){
            visible.push(t);
        }
    }

    const features = [];
    for(let i=0; i<visible.length; i++){
        const t = visible[i];
        features.push({
            type:"Feature",
            properties:{name:t.n},
            geometry: compactGeomToGeoJSON(t)
        });
    }

    const geojson = {type:"FeatureCollection", features:features};

    if(gridLayer){
        map.removeLayer(gridLayer);
        gridLayer = null;
    }

    gridLayer = L.geoJSON(geojson, {
        renderer: gridRenderer,
        interactive: true,
        bubblingMouseEvents: false,
        style: function(f){ return tileStyle(f.properties.name); },
        onEachFeature: function(f, layer){
            var name = f.properties.name;
            layer.bindTooltip(name, {
                className:"tile-tooltip",
                sticky:true,
                direction:"top",
                offset:[0,-8],
                opacity:1
            });
            layer.on("click", function(e){
                if(e){ L.DomEvent.stop(e); }
                onTileClick(name);
            });
            layer.on("mouseover", function(){
                if(!selNames.has(name) && !interNames.has(name)){
                    layer.setStyle({weight:2.5, fillOpacity:0.12});
                }
            });
            layer.on("mouseout", function(){
                layer.setStyle(tileStyle(name));
            });
        }
    }).addTo(map);
}

// ── Refresh grid styles ──────────────────────────────────────────────
function refreshGridStyles(){
    if(!gridLayer) return;
    gridLayer.eachLayer(function(layer){
        if(layer.feature && layer.feature.properties){
            var name = layer.feature.properties.name;
            layer.setStyle(tileStyle(name));
        }
    });
}

// ── AOI layer ────────────────────────────────────────────────────────
function updateAOI(geojsonStr){
    if(aoiLayer){map.removeLayer(aoiLayer); aoiLayer=null;}
    if(!geojsonStr || geojsonStr==="null") return;
    try{
        var data = JSON.parse(geojsonStr);
        aoiLayer = L.geoJSON(data, {
            style:{color:"#CCBB44",weight:2.5,fillOpacity:0.10,dashArray:"5,5"}
        }).addTo(map);
    }catch(e){
        console.error("AOI parse error:", e);
    }
}

// ── No-coverage layer ────────────────────────────────────────────────
function updateNocov(geojsonStr){
    if(nocovLayer){map.removeLayer(nocovLayer); nocovLayer=null;}
    if(!showNocov || !geojsonStr || geojsonStr==="null") return;
    try{
        var data = JSON.parse(geojsonStr);
        nocovLayer = L.geoJSON(data, {
            style:{color:"#CC3311",weight:1.5,fillOpacity:0.04}
        }).addTo(map);
    }catch(e){
        console.error("Nocov parse error:", e);
    }
}

// ── Handle render from Streamlit ─────────────────────────────────────
let prevGridHash = null;
let prevAoiHash = null;
let prevNocovHash = null;
let prevInterHash = null;
let prevSelHash = null;
let prevStyleKey = "";

function onStreamlitRender(args){
    var opts = args.options ? JSON.parse(args.options) : {};
    showGrid    = opts.show_grid !== false;
    colorize    = opts.colorize !== false;
    gridOpacity = opts.opacity != null ? opts.opacity : 0.04;
    showNocov   = !!opts.show_nocov;
    showInter   = opts.show_inter !== false;
    showSel     = opts.show_sel !== false;
    clickSelect = opts.click_select !== false;
    tileSystem  = args.tile_system || "sentinel-2";

    var center = args.center ? JSON.parse(args.center) : [48.8566, 2.3522];
    var zoom = args.zoom || 8;

    if(!map){
        initMap(center, zoom);
    }

    if(args.fly_to){
        var ft = JSON.parse(args.fly_to);
        map.flyTo([ft[0], ft[1]], ft[2] || map.getZoom(), {duration:0.5});
    }

    var gh = args.grid_hash || "";
    if(gh !== prevGridHash && args.grid_compact){
        prevGridHash = gh;
        try{
            allTiles = JSON.parse(args.grid_compact);
        }catch(e){allTiles=[];}
        if(gridLayer){map.removeLayer(gridLayer); gridLayer=null;}
        updateGridDisplay();
    }

    var ah = args.aoi_hash || "";
    if(ah !== prevAoiHash){
        prevAoiHash = ah;
        updateAOI(args.aoi_geojson);
    }

    var nh = args.nocov_hash || "";
    if(nh !== prevNocovHash){
        prevNocovHash = nh;
        updateNocov(args.nocov_geojson);
    }

    var newInter = args.inter_names || "[]";
    var newSel   = args.sel_names   || "[]";
    var interChanged = (newInter !== prevInterHash);
    var selChanged   = (newSel   !== prevSelHash);
    var styleKey = [
        tileSystem,
        showInter ? "1" : "0",
        showSel ? "1" : "0",
        colorize ? "1" : "0",
        String(gridOpacity)
    ].join("|");
    var styleChanged = (styleKey !== prevStyleKey);

    if(interChanged || selChanged){
        interNames = new Set(JSON.parse(newInter));
        selNames   = new Set(JSON.parse(newSel));
        prevInterHash = newInter;
        prevSelHash   = newSel;
    }
    if(interChanged || selChanged || styleChanged){
        refreshGridStyles();
        prevStyleKey = styleKey;
    }

    updateZoomHint();
}

// ── Listen for Streamlit render events ───────────────────────────────
window.addEventListener("message", function(event){
    if(!event.data) return;
    if(event.data.type === "streamlit:render"){
        onStreamlitRender(event.data.args || {});
    }
});

})();
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT SETUP
# ═══════════════════════════════════════════════════════════════════════════════

# Create a minimal Streamlit component directory containing a single index.html.
# Some environments may mount the script directory read-only; fall back to a
# temporary directory if needed.
try:
    _COMP_DIR = PROJECT_ROOT / "_leaflet_comp"
    _COMP_DIR.mkdir(exist_ok=True)
    (_COMP_DIR / "index.html").write_text(LEAFLET_HTML, encoding="utf-8")
except Exception as e:
    import tempfile
    _COMP_DIR = Path(tempfile.gettempdir()) / "sat_downloader_leaflet_comp"
    _COMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        (_COMP_DIR / "index.html").write_text(LEAFLET_HTML, encoding="utf-8")
    except Exception:
        raise RuntimeError(f"Unable to write Leaflet component HTML: {e}")

_leaflet_func = components.declare_component("leaflet_map", path=str(_COMP_DIR))


def leaflet_map(
    grid_compact: str = "[]",
    grid_hash: str = "",
    aoi_geojson: Optional[str] = None,
    aoi_hash: str = "",
    nocov_geojson: Optional[str] = None,
    nocov_hash: str = "",
    inter_names: str = "[]",
    sel_names: str = "[]",
    options: str = "{}",
    tile_system: str = "sentinel-2",
    center: str = "[48.8566, 2.3522]",
    zoom: int = 8,
    fly_to: Optional[str] = None,
    key: str = "leaflet_map",
) -> Optional[Dict[str, Any]]:
    """Render the Leaflet map component and return user interactions."""
    result = _leaflet_func(
        grid_compact=grid_compact,
        grid_hash=grid_hash,
        aoi_geojson=aoi_geojson or "null",
        aoi_hash=aoi_hash,
        nocov_geojson=nocov_geojson or "null",
        nocov_hash=nocov_hash,
        inter_names=inter_names,
        sel_names=sel_names,
        options=options,
        tile_system=tile_system,
        center=center,
        zoom=zoom,
        fly_to=fly_to,
        key=key,
        default=None,
        height=MCFG.MAP_HEIGHT,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# GEO UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_4326(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    return gdf.set_crs(epsg=4326) if gdf.crs is None else gdf.to_crs(epsg=4326)


def get_name_col(gdf: Optional[gpd.GeoDataFrame], system: str) -> Optional[str]:
    if gdf is None or gdf.empty:
        return None
    candidates = (
        ["PR", "PATH_ROW", "WRSPR", "PATH", "name", "Name"]
        if system == "landsat"
        else ["Name", "name", "TILE_ID", "tile_id", "MGRS_TILE", "mgrs"]
    )
    for c in candidates:
        if c in gdf.columns:
            return c
    for c in gdf.columns:
        if c != "geometry" and gdf[c].dtype == object:
            return c
    return None


def safe_union(geoms):
    """Robust union across Shapely versions and environments."""
    if not geoms:
        return None
    try:
        ua = getattr(shapely, "union_all", None)
        if callable(ua):
            return ua(geoms)
    except Exception:
        pass
    try:
        return unary_union(geoms)
    except Exception:
        pass
    u = geoms[0]
    for g in geoms[1:]:
        try:
            u = u.union(g)
        except Exception:
            continue
    return u


def parse_geometry(text: str):
    return parse_aoi_text(text)


def make_square_wkt(lat, lng, km):
    half = km / 2.0
    dlat = half / 111.0
    dlon = half / (111.0 * max(0.05, abs(math.cos(math.radians(lat)))))
    p = Polygon([
        (lng - dlon, lat - dlat), (lng + dlon, lat - dlat),
        (lng + dlon, lat + dlat), (lng - dlon, lat + dlat),
        (lng - dlon, lat - dlat),
    ])
    return shapely_wkt.dumps(p, rounding_precision=6)


def zoom_for_bounds(bounds: Tuple[float, float, float, float]) -> int:
    """Compute a practical Leaflet zoom level from lon/lat bounds span."""
    try:
        minx, miny, maxx, maxy = bounds
        span = max(abs(float(maxx) - float(minx)), abs(float(maxy) - float(miny)))
    except Exception:
        return 10

    if span > 120:
        return 2
    if span > 60:
        return 3
    if span > 30:
        return 4
    if span > 15:
        return 5
    if span > 8:
        return 6
    if span > 4:
        return 7
    if span > 2:
        return 8
    if span > 1:
        return 9
    if span > 0.5:
        return 10
    if span > 0.2:
        return 11
    if span > 0.1:
        return 12
    if span > 0.05:
        return 13
    return 14


def compute_intersections(polys, gdf, ncol):
    """Return intersecting tile names and intersecting subset GeoDataFrame."""
    if gdf is None or gdf.empty or not polys or not ncol:
        return [], None
    try:
        au = safe_union(polys)
        if au is None or getattr(au, "is_empty", True):
            return [], gdf.iloc[0:0]
        try:
            sindex = gdf.sindex
            possible_idx = list(sindex.intersection(au.bounds))
            candidates = gdf.iloc[possible_idx]
        except Exception:
            candidates = gdf
        c = candidates[candidates.intersects(au)].copy()
        if c.empty:
            return [], c
        c = c[[ncol, "geometry"]].copy()
        return sorted(c[ncol].astype(str).unique().tolist()), c
    except Exception as e:
        logger.error(f"Intersection: {e}")
        return [], None


def find_tiles(gdf, col, query, limit=50):
    q = (query or "").strip()
    if not q:
        return gdf.iloc[0:0]
    s = gdf[col].astype(str)
    exact = gdf[s.str.upper() == q.upper()]
    if not exact.empty:
        return exact[[col, "geometry"]].copy()
    return gdf[s.str.contains(q, case=False, na=False)][[col, "geometry"]].iloc[:limit].copy()


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:12]


def _compact_rings(geom, simplify_tol: float = 0.0, precision: int = 4) -> List[List[List[float]]]:
    """
    Encode geometry as compact exterior rings:
      - Polygon    -> [ring]
      - MultiPolygon -> [ring1, ring2, ...]

    Rings are not closed (first point not repeated at the end) to save bytes.
    """
    if geom is None or getattr(geom, "is_empty", True):
        return []

    g = geom
    if simplify_tol > 0:
        try:
            g = g.simplify(simplify_tol, preserve_topology=True)
        except Exception:
            g = geom

    polys = []
    gtype = getattr(g, "geom_type", "")
    if gtype == "Polygon":
        polys = [g]
    elif gtype == "MultiPolygon":
        # Keep all parts; some polar/dateline tiles are multipart.
        polys = [p for p in g.geoms if p is not None and not p.is_empty]
    else:
        return []

    rings: List[List[List[float]]] = []
    for p in polys:
        try:
            coords = [[round(float(x), precision), round(float(y), precision)] for x, y in p.exterior.coords]
        except Exception:
            continue
        if len(coords) < 4:
            continue
        if coords[0] == coords[-1]:
            coords = coords[:-1]
        if len(coords) >= 3:
            rings.append(coords)
    return rings


def selected_tiles_to_wkt(gdf, ncol, selected_tiles) -> str:
    """Build a multi-line WKT text (one polygon per line) from selected tile IDs."""
    if gdf is None or gdf.empty or not ncol or not selected_tiles:
        return ""

    sel = {str(t).strip() for t in selected_tiles if str(t).strip()}
    if not sel:
        return ""

    try:
        subset = gdf[gdf[ncol].astype(str).isin(sel)]
    except Exception:
        return ""

    wkts: List[str] = []
    for geom in subset.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        gtype = getattr(geom, "geom_type", "")
        if gtype == "Polygon":
            wkts.append(geom.wkt)
        elif gtype == "MultiPolygon":
            wkts.extend([p.wkt for p in geom.geoms if p is not None and not p.is_empty])
    return "\n".join(wkts)


def selected_tiles_to_geometry(gdf, ncol, selected_tiles):
    """Build a dissolved geometry from selected tile IDs."""
    if gdf is None or gdf.empty or not ncol or not selected_tiles:
        return None
    sel = {str(t).strip() for t in selected_tiles if str(t).strip()}
    if not sel:
        return None
    try:
        subset = gdf[gdf[ncol].astype(str).isin(sel)]
    except Exception:
        return None
    geoms = [g for g in subset.geometry if g is not None and not getattr(g, "is_empty", True)]
    return safe_union(geoms) if geoms else None


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


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading Sentinel-2 grid…")
def load_s2():
    tiles, nocov = None, None
    for p in [PATHS.S2_GEOJSON, PATHS.S2_GEOJSON_FALLBACK]:
        if tiles is not None:
            break
        try:
            if Path(p).exists():
                tiles = ensure_4326(gpd.read_file(p))
                logger.info(f"Loaded Sentinel-2 grid from {p} ({len(tiles)} tiles)")
        except Exception as e:
            logger.warning(f"Failed to read Sentinel-2 grid '{p}': {e}")
    try:
        for p in [PATHS.S2_NOCOV, PATHS.S2_NOCOV_FALLBACK]:
            if Path(p).exists():
                nocov = ensure_4326(gpd.read_file(p))
                break
    except Exception as e:
        logger.warning(f"Failed to read Sentinel-2 no-coverage '{PATHS.S2_NOCOV}': {e}")
        nocov = None
    if tiles is not None and not tiles.empty:
        try:
            _ = tiles.sindex
        except Exception:
            pass
    return tiles, nocov


@st.cache_data(show_spinner="Loading Landsat WRS-2 grid…")
def load_landsat():
    for p in [PATHS.LANDSAT_GEOJSON, PATHS.LANDSAT_GEOJSON_FALLBACK]:
        try:
            if Path(p).exists():
                gdf = ensure_4326(gpd.read_file(p))
                if gdf is not None and not gdf.empty:
                    logger.info(f"Loaded Landsat grid from {p} ({len(gdf)} tiles)")
                    try:
                        _ = gdf.sindex
                    except Exception:
                        pass
                return gdf
        except Exception as e:
            logger.warning(f"Failed to read Landsat grid '{p}': {e}")
    return None


def load_tiles():
    s2, s2n = load_s2()
    ls = load_landsat()
    return {"sentinel-2": {"tiles": s2, "nocov": s2n}, "landsat": {"tiles": ls, "nocov": None}}


@st.cache_data(show_spinner="Preparing grid for display…")
def prepare_compact_grid(_gdf_id: str, ncol: str, system: str) -> Tuple[str, str]:
    gdf = st.session_state.get(f"_raw_gdf_{system}")
    if gdf is None or gdf.empty or not ncol:
        return "[]", ""
    names = gdf[ncol].astype(str).tolist()
    geoms = gdf.geometry.tolist()
    bdf = gdf.geometry.bounds.round(4)
    include_geom = (system == "landsat")
    simplify_tol = 0.02 if include_geom else 0.0
    features = []
    for name, geom, minx, miny, maxx, maxy in zip(
        names, geoms, bdf["minx"], bdf["miny"], bdf["maxx"], bdf["maxy"]
    ):
        item = {"n": name, "b": [float(minx), float(miny), float(maxx), float(maxy)]}
        if include_geom:
            rings = _compact_rings(geom, simplify_tol=simplify_tol, precision=4)
            if rings:
                item["g"] = rings
        features.append(item)
    js = json.dumps(features, separators=(",", ":"))
    return js, _md5(js)


@st.cache_data(show_spinner=False)
def prepare_nocov_geojson(_nocov_id: str) -> Tuple[str, str]:
    gdf = st.session_state.get("_raw_nocov")
    if gdf is None or gdf.empty:
        return "null", ""
    simplified = gdf.copy()
    simplified["geometry"] = simplified.geometry.simplify(0.01, preserve_topology=True)
    js = simplified[["geometry"]].to_json()
    return js, _md5(js)


# ═══════════════════════════════════════════════════════════════════════════════
# DOWNLOAD MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _close_log_fh():
    """Safely close the subprocess log file handle if it's still open."""
    fh = st.session_state.pop("_dl_log_fh", None)
    if fh is not None:
        try:
            if not fh.closed:
                fh.close()
                logger.debug("[DL] Closed log file handle")
        except Exception as e:
            logger.warning(f"[DL] Error closing log fh: {e}")


def reset_downloads(dl_dir: Optional[str] = None, clear_files: bool = True):
    """Reset UI download state and optionally clear files in downloads directory."""
    logger.info(f"[DL] reset_downloads() called (clear_files={clear_files})")
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if clear_files:
        if dl_path.exists():
            import shutil
            shutil.rmtree(dl_path, ignore_errors=True)
        dl_path.mkdir(parents=True, exist_ok=True)
    else:
        dl_path.mkdir(parents=True, exist_ok=True)

    # Close any open log file handle
    _close_log_fh()

    # Clear logs and PID file
    try:
        NOHUP_PATH.write_text("")
    except Exception:
        pass
    try:
        PID_PATH.write_text("")
    except Exception:
        pass
    for key in list(st.session_state.keys()):
        if key.startswith("dl_") or key == "_dl_log_fh":
            del st.session_state[key]
    st.session_state.update({
        "dl_start_time": None, "dl_total_products": 0,
        "dl_completed": 0, "dl_running": False,
    })
    logger.info("[DL] Reset complete")


@st.cache_data(ttl=2, show_spinner=False)
def count_downloaded_products(dl_dir: Optional[str] = None):
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if not dl_path.exists():
        return 0, 0.0
    real_files = [f for f in dl_path.rglob("*") if f.is_file()]
    total_size = sum(f.stat().st_size for f in real_files) / (1024 * 1024)
    return len(real_files), total_size


def parse_download_logs(path: Optional[str] = None):
    """Parse nohup.out to extract download progress, status phase, and errors."""
    lp = Path(path) if path else NOHUP_PATH
    if not lp.exists():
        logger.debug(f"[DL] Log file does not exist: {lp}")
        return {"batch": None, "files": {}, "logs": [], "products_found": 0,
                "errors": [], "phase": "starting"}

    brx = re.compile(r"Concurrent Downloads:\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>\d+)/(?P<tot>\d+)")
    drx = re.compile(r"Downloading\s+(?P<fn>.+?):\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>[\d.]+\S*)/(?P<t>[\d.]+\S*)\s*\[(?:.+?)<(?P<eta>[0-9:?\-]+)\]")

    prx = re.compile(r"Found\s+(?P<n>\d+)\s+products?", re.IGNORECASE)
    search_rx = re.compile(r"Searching for products", re.IGNORECASE)
    config_rx = re.compile(r"Configuration loaded", re.IGNORECASE)
    geom_rx = re.compile(r"Geometry loaded", re.IGNORECASE)
    provider_rx = re.compile(r"Initialized provider", re.IGNORECASE)
    done_rx = re.compile(r"completed successfully", re.IGNORECASE)
    downloading_rx = re.compile(r"Downloading all products", re.IGNORECASE)

    erx = re.compile(r"(ERROR\s*\||Traceback \(most recent|raise \w+Error|Exception:)", re.IGNORECASE)

    result = {"batch": None, "files": {}, "logs": [], "products_found": 0,
              "errors": [], "phase": "starting"}

    try:
        text = lp.read_text(errors="replace")
        # tqdm often writes carriage returns (\r) when not attached to a TTY.
        text = text.replace("\r", "\n")
    except Exception as e:
        logger.warning(f"[DL] Failed to read log file: {e}")
        return result

    line_count = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        line_count += 1

        mb = brx.search(line)
        if mb:
            result["batch"] = {
                "done": int(mb.group("d")),
                "pct": int(mb.group("pct")),
                "total": int(mb.group("tot")),
            }
            result["phase"] = "downloading"
            continue

        md = drx.search(line)
        if md:
            result["files"][md.group("fn")] = {
                "pct": int(md.group("pct")),
                "done": md.group("d"),
                "total": md.group("t"),
                "eta": md.group("eta"),
            }
            result["phase"] = "downloading"
            continue

        mp = prx.search(line)
        if mp:
            result["products_found"] = max(result["products_found"], int(mp.group("n")))
            if result["phase"] != "downloading":
                result["phase"] = "found"
            continue

        if done_rx.search(line):
            result["phase"] = "done"
            continue
        if downloading_rx.search(line):
            result["phase"] = "downloading"
            continue
        if search_rx.search(line):
            if result["phase"] in ("starting", "ready"):
                result["phase"] = "searching"
            continue
        if provider_rx.search(line):
            if result["phase"] == "starting":
                result["phase"] = "ready"
            continue
        if config_rx.search(line) or geom_rx.search(line):
            if result["phase"] == "starting":
                result["phase"] = "initializing"
            continue

        if erx.search(line):
            result["errors"].append(line)
        else:
            result["logs"].append(line)
            if len(result["logs"]) > 30:
                result["logs"] = result["logs"][-30:]

    logger.debug(f"[DL] Parsed {line_count} lines from log — phase={result['phase']}, "
                 f"products_found={result['products_found']}, errors={len(result['errors'])}, "
                 f"batch={result['batch']}")
    return result


def _format_eta(seconds):
    if seconds > 3600:
        return f"{seconds/3600:.1f}h"
    elif seconds > 60:
        return f"{seconds/60:.0f}m {seconds%60:.0f}s"
    return f"{seconds:.0f}s"


def _read_pid() -> Optional[int]:
    try:
        txt = PID_PATH.read_text().strip()
        return int(txt) if txt else None
    except Exception:
        return None


def _write_pid(pid: int) -> None:
    try:
        PID_PATH.write_text(str(pid))
        logger.debug(f"[DL] Wrote PID {pid} to {PID_PATH}")
    except Exception as e:
        logger.warning(f"[DL] Failed to write PID: {e}")


def _pid_is_running(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _terminate_pid(pid: Optional[int], grace_seconds: float = 1.5) -> bool:
    """Terminate PID with SIGTERM then SIGKILL fallback. Returns True if not running."""
    if not pid:
        return True
    if not _pid_is_running(pid):
        return True
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as e:
        logger.warning(f"[DL] Failed to SIGTERM PID {pid}: {e}")
    deadline = time.time() + max(0.0, grace_seconds)
    while time.time() < deadline:
        if not _pid_is_running(pid):
            return True
        time.sleep(0.1)
    if _pid_is_running(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception as e:
            logger.warning(f"[DL] Failed to SIGKILL PID {pid}: {e}")
    return not _pid_is_running(pid)


def _find_cli_pids() -> List[int]:
    """Find background CLI downloader processes (best-effort)."""
    try:
        cp = subprocess.run(
            ["pgrep", "-f", "cli.py --provider"],
            capture_output=True,
            text=True,
            check=False,
        )
        pids: List[int] = []
        for line in (cp.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                pid = int(line)
            except Exception:
                continue
            if pid != os.getpid():
                pids.append(pid)
        return sorted(set(pids))
    except Exception as e:
        logger.debug(f"[DL] Unable to list CLI processes with pgrep: {e}")
        return []


def _unlock_download_runtime(kill_orphans: bool = False) -> bool:
    """
    Unlock download runtime state.
    - Stops PID from job_pid/session.
    - Optionally kills orphan cli.py processes.
    """
    ok = True
    pid = st.session_state.get("dl_pid") or _read_pid()
    if pid and _pid_is_running(pid):
        logger.info(f"[DL] Unlock: terminating active PID {pid}")
        ok = _terminate_pid(pid) and ok

    if kill_orphans:
        for opid in _find_cli_pids():
            if pid and opid == pid:
                continue
            if _pid_is_running(opid):
                logger.info(f"[DL] Unlock: terminating orphan PID {opid}")
                ok = _terminate_pid(opid) and ok

    _close_log_fh()
    st.session_state["dl_running"] = False
    st.session_state.pop("dl_pid", None)
    try:
        PID_PATH.write_text("")
    except Exception:
        pass
    return ok


def _bootstrap_download_runtime() -> None:
    """
    Initialize download runtime state on app startup.
    Prevent stale nohup logs from appearing as an active download when no PID is alive.
    """
    if st.session_state.get("_dl_bootstrapped", False):
        return

    pid = st.session_state.get("dl_pid") or _read_pid()
    alive = _pid_is_running(pid)

    if alive:
        st.session_state["dl_running"] = True
        st.session_state["dl_pid"] = pid
    else:
        st.session_state["dl_running"] = False
        st.session_state.pop("dl_pid", None)
        try:
            PID_PATH.write_text("")
        except Exception:
            pass
        # Clear stale log content from previous runs to avoid "ghost download" UI.
        try:
            if NOHUP_PATH.exists() and NOHUP_PATH.stat().st_size > 0:
                NOHUP_PATH.write_text("")
        except Exception:
            pass

    st.session_state["_dl_bootstrapped"] = True


def _check_cli_exists():
    """Check if cli.py exists and return its path, or None."""
    candidates = [
        PROJECT_ROOT / "cli.py",
        PROJECT_ROOT / "src" / "cli.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            logger.debug(f"[DL] Found CLI at {candidate}")
            return str(candidate)
    logger.warning(f"[DL] cli.py not found in {[str(c) for c in candidates]}")
    return None


def _recent_rate_limit_hits(path: Optional[Path] = None, tail_chars: int = 25000) -> int:
    """Count recent 429/rate-limit hits from the CLI log tail."""
    lp = path or NOHUP_PATH
    if not lp.exists():
        return 0
    try:
        raw = lp.read_text(errors="replace")
    except Exception:
        return 0
    tail = raw[-tail_chars:] if len(raw) > tail_chars else raw
    return len(re.findall(r"(?:\b429\b|rate limit)", tail, flags=re.IGNORECASE))


def _auto_parallel_strategy(
    provider: str,
    start_date: dt.date,
    end_date: dt.date,
    preview_total: int,
    selected_tile_count: int,
) -> Dict[str, int]:
    """
    Auto-tune concurrency for practical speed while reducing overload risk.
    Returns dict with:
      - max_concurrent
      - parallel_days
      - concurrent_per_day
    """
    try:
        n_days = max(1, (end_date - start_date).days + 1)
    except Exception:
        n_days = 1

    est_products = max(1, int(preview_total or 0), int(selected_tile_count or 0))
    recent_429 = _recent_rate_limit_hits()

    if provider == "Copernicus":
        # Target practical max without overloading CDSE.
        parallel_days = 1
        concurrent_per_day = 2
        if n_days >= 10 and est_products >= 30 and recent_429 == 0:
            parallel_days = 3
            concurrent_per_day = 2
        elif n_days >= 3 and est_products >= 10:
            parallel_days = 2
            concurrent_per_day = 2
        elif est_products >= 4:
            parallel_days = 1
            concurrent_per_day = 3

        # Adaptive throttle if previous run shows rate limiting.
        if recent_429 >= 10:
            parallel_days = 1
            concurrent_per_day = 1
        elif recent_429 >= 5:
            parallel_days = min(parallel_days, 2)
            concurrent_per_day = max(1, concurrent_per_day - 1)
        elif recent_429 >= 2 and parallel_days >= 3:
            parallel_days = 2

        concurrent_per_day = max(1, min(3, concurrent_per_day))
        total = max(1, parallel_days * concurrent_per_day)
        if total > 6:
            parallel_days = max(1, 6 // concurrent_per_day)
            total = max(1, parallel_days * concurrent_per_day)
        return {
            "max_concurrent": max(1, min(4, concurrent_per_day if parallel_days > 1 else total)),
            "parallel_days": max(1, parallel_days),
            "concurrent_per_day": max(1, concurrent_per_day),
        }

    if provider == "USGS":
        # USGS generally tolerates more parallelism than Copernicus.
        if est_products <= 2:
            mc = 3
        elif est_products <= 8:
            mc = 6
        elif est_products <= 20:
            mc = 8
        else:
            mc = 10
        if n_days >= 7:
            mc = min(12, mc + 2)
        if recent_429 >= 5:
            mc = max(2, mc - 2)
        return {"max_concurrent": mc, "parallel_days": 1, "concurrent_per_day": 1}

    return {"max_concurrent": 4, "parallel_days": 1, "concurrent_per_day": 1}


def _build_download_command(
    provider, satellite, product, start_date, end_date, aoi_file,
    selected_tiles=None,
    max_concurrent: int = 4,
    parallel_days: int = 1,
    concurrent_per_day: int = 2,
):
    """Build the CLI download command with proper arguments."""
    cli_path = _check_cli_exists()
    if not cli_path:
        return None, "cli.py not found — check your project structure"

    import shlex

    cli_provider = PROVIDER_CLI_MAP.get(provider)
    if not cli_provider:
        cli_provider = provider.lower().replace(" ", "_")
        logger.warning(
            f"[DL] Provider '{provider}' not in PROVIDER_CLI_MAP, falling back to '{cli_provider}'"
        )

    collection = str(satellite).split(" ")[0]

    aoi_path = Path(aoi_file)
    if not aoi_path.is_absolute():
        aoi_path = PROJECT_ROOT / aoi_path

    cmd_parts = [
        sys.executable,
        "-u",
        cli_path,
        "--provider",
        cli_provider,
        "--collection",
        collection,
    ]

    try:
        mc = max(1, int(max_concurrent))
        cmd_parts.extend(["--max-concurrent", str(mc)])
    except Exception:
        pass

    if cli_provider == "copernicus":
        try:
            pd = max(1, int(parallel_days))
            cmd_parts.extend(["--parallel-days", str(pd)])
        except Exception:
            pass
        try:
            cpd = max(1, int(concurrent_per_day))
            cmd_parts.extend(["--concurrent-per-day", str(cpd)])
        except Exception:
            pass

    if product and str(product).strip():
        cmd_parts.extend(["--product-type", str(product)])

    if selected_tiles and cli_provider == "copernicus":
        if len(selected_tiles) == 1:
            cmd_parts.extend(["--tile-id", selected_tiles[0]])

    cmd_parts.extend(
        [
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--aoi_file",
            str(aoi_path),
            "--log-type",
            "all",
        ]
    )
    cmd = " ".join(shlex.quote(str(part)) for part in cmd_parts)
    logger.info(f"[DL] Built command: {cmd}")
    return cmd, None


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

    # ── FIX: Show raw log file info for debugging ──
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

    # ── FIX: Always show raw log tail for debugging ──
    if log_exists and log_size > 0:
        with st.expander("🔬 Raw Log Tail (last 2KB)", expanded=False):
            try:
                raw = NOHUP_PATH.read_text(errors="replace")
                tail = raw[-2000:] if len(raw) > 2000 else raw
                st.code(tail, language="text")
            except Exception as e:
                st.warning(f"Cannot read raw log: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════

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
        "dl_auto_cfg": {},
        "preview_key": "",
        "preview_items": [],
        "preview_total": 0,
        "preview_error": "",
        "preview_fetched": False,
        "fly_to": None,
        "use_file_browser_component": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    if st.session_state.get("dl_job_provider_filter") not in {"current", "all", "copernicus", "usgs"}:
        st.session_state["dl_job_provider_filter"] = "current"


def _ss(key, default=None):
    return st.session_state.get(key, default)


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


def _merge_status_rows(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return merge_status_rows_runtime(rows)


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


def _upsert_known_jobs(job_ids: List[str], *, active_job_ids: List[str] | None = None) -> None:
    known = [str(item) for item in _ss("known_job_ids", [])]
    active = [str(item) for item in _ss("active_job_ids", [])]
    for job_id in job_ids:
        if not job_id:
            continue
        if job_id not in known:
            known.insert(0, job_id)
    for job_id in [str(item) for item in (active_job_ids or [])]:
        if not job_id:
            continue
        if job_id not in active:
            active.append(job_id)
    st.session_state["known_job_ids"] = known[:1000]
    st.session_state["active_job_ids"] = active[:1000]


def _recent_jobs_cutoff() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max(1, RECENT_JOBS_WINDOW_HOURS))


def _fetch_recent_provider_jobs(api_url: str, api_key: str, *, provider: str | None) -> List[Dict[str, Any]]:
    rows, _total = _list_jobs(
        api_url,
        api_key,
        provider=provider,
        date_from=_recent_jobs_cutoff(),
        page=1,
        page_size=RECENT_JOBS_FETCH_LIMIT,
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


def _is_recent_status(row: Dict[str, Any], now_utc: dt.datetime) -> bool:
    state = str(row.get("state", "")).lower()
    if state in ACTIVE_JOB_STATES:
        return True
    ref = _status_reference_time(row)
    if ref is None:
        return False
    return (now_utc - ref) <= dt.timedelta(hours=max(1, RECENT_JOBS_WINDOW_HOURS))


def _filter_recent_job_rows(rows: List[Dict[str, Any]], *, limit: int = RECENT_JOBS_LIMIT) -> List[Dict[str, Any]]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    filtered = [row for row in rows if _is_recent_status(row, now_utc)]

    def _sort_key(row: Dict[str, Any]) -> Tuple[int, float]:
        state = str(row.get("state", "")).lower()
        ref = _status_reference_time(row) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
        active_rank = 0 if state in ACTIVE_JOB_STATES else 1
        return (active_rank, -ref.timestamp())

    filtered.sort(key=_sort_key)
    return filtered[: max(1, limit)]


def _looks_like_scene_name(name: str) -> bool:
    upper_name = str(name or "").upper()
    if not upper_name:
        return False
    if upper_name.endswith(".SAFE") or upper_name.endswith(".SAFE.ZIP"):
        return True
    if upper_name.startswith(("S1", "S2")) and "_" in upper_name:
        return True
    if upper_name.startswith(("LC08_", "LC09_", "LE07_", "LT05_", "LT04_", "LM05_")):
        return True
    return False


@st.cache_data(ttl=5, show_spinner=False)
def _manifest_source_candidates(limit: int = 200) -> List[str]:
    if not DOWNLOADS_DIR.exists():
        return []

    candidates: Dict[str, float] = {}
    for manifest_path in DOWNLOADS_DIR.rglob("manifest.json"):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        for raw_path_value in list(payload.get("paths", [])):
            try:
                raw_path = Path(str(raw_path_value))
                if not raw_path.exists():
                    continue
                if raw_path.is_dir():
                    candidates[str(raw_path)] = raw_path.stat().st_mtime
                elif raw_path.is_file():
                    candidates[str(raw_path)] = raw_path.stat().st_mtime
            except OSError:
                continue

        parent_dir = manifest_path.parent
        try:
            for child in parent_dir.iterdir():
                if _looks_like_scene_name(child.name):
                    candidates[str(child)] = child.stat().st_mtime
        except OSError:
            continue

    return [
        item[0]
        for item in sorted(candidates.items(), key=lambda pair: pair[1], reverse=True)[: max(1, limit)]
    ]


def _prune_known_job_ids(cache: Dict[str, Dict[str, Any]], active_ids: List[str]) -> None:
    now_utc = dt.datetime.now(dt.timezone.utc)
    active_set = {str(job_id) for job_id in active_ids}
    kept: List[str] = []
    for job_id in [str(item) for item in _ss("known_job_ids", [])]:
        if job_id in active_set:
            kept.append(job_id)
            continue
        row = cache.get(job_id)
        if row and _is_recent_status(row, now_utc):
            kept.append(job_id)
    st.session_state["known_job_ids"] = kept[:200]


@st.cache_data(ttl=3, show_spinner=False)
def _recent_source_candidates(limit: int = 200) -> List[str]:
    if not DOWNLOADS_DIR.exists():
        return []

    def _is_supported_raw_file(path: Path) -> bool:
        lower_name = path.name.lower()
        if lower_name.endswith(".safe.zip"):
            return True
        return path.suffix.lower() in {".zip", ".tar", ".gz", ".tgz", ".nc", ".tif", ".tiff"}

    def _is_supported_raw_dir(path: Path) -> bool:
        lower_name = path.name.lower()
        if lower_name.endswith(".safe") or lower_name.endswith(".zarr"):
            return lower_name.endswith(".safe")
        if _looks_like_scene_name(path.name):
            return True
        try:
            children = list(path.iterdir())
        except OSError:
            return False
        if any(child.is_file() and child.name.lower() == "manifest.safe" for child in children):
            return True
        if any(child.is_file() and child.name.upper().endswith("_MTL.TXT") for child in children):
            return True
        if any(child.is_file() and child.suffix.lower() == ".nc" for child in children):
            return True
        tif_count = sum(
            1 for child in children if child.is_file() and child.suffix.lower() in {".tif", ".tiff", ".jp2"}
        )
        return tif_count >= 3

    candidates: Dict[str, float] = {}
    for path in DOWNLOADS_DIR.rglob("*"):
        try:
            if path.is_dir():
                if _is_supported_raw_dir(path):
                    candidates[str(path)] = path.stat().st_mtime
            elif path.is_file() and _is_supported_raw_file(path):
                candidates[str(path)] = path.stat().st_mtime
        except OSError:
            continue

    for manifest_candidate in _manifest_source_candidates(limit=max(limit, 200)):
        try:
            manifest_path = Path(manifest_candidate)
            if manifest_path.exists():
                candidates[str(manifest_path)] = manifest_path.stat().st_mtime
        except OSError:
            continue

    return [
        item[0]
        for item in sorted(candidates.items(), key=lambda pair: pair[1], reverse=True)[: max(1, limit)]
    ]


@st.cache_data(ttl=3, show_spinner=False)
def _available_zarr_stores(limit: int = 120) -> List[Dict[str, Any]]:
    if not ZARR_STORES_DIR.exists():
        return []
    stores: List[Dict[str, Any]] = []
    for path in ZARR_STORES_DIR.rglob("*.zarr"):
        if not path.is_dir():
            continue
        try:
            stat = path.stat()
            stores.append(
                {
                    "path": str(path),
                    "name": path.name,
                    "mtime": stat.st_mtime,
                    "host_hint": _container_to_host_path_hint(str(path)),
                    "entries": sorted(
                        [child.name for child in path.iterdir() if child.is_dir() or child.is_file()]
                    )[:12],
                }
            )
        except OSError:
            continue
    stores.sort(key=lambda item: item["mtime"], reverse=True)
    return stores[: max(1, limit)]


def _list_artifacts(
    api_url: str,
    api_key: str,
    *,
    artifact_type: str | None = None,
    provider: str | None = None,
    collection: str | None = None,
    scene_id: str | None = None,
    job_id: str | None = None,
    uri_query: str | None = None,
    include_local: bool = False,
    page: int = 1,
    page_size: int = 100,
) -> Tuple[List[Dict[str, Any]], int]:
    params: Dict[str, Any] = {
        "page": page,
        "page_size": page_size,
        "include_local": str(include_local).lower(),
    }
    if artifact_type:
        params["artifact_type"] = artifact_type
    if provider:
        params["provider"] = provider
    if collection:
        params["collection"] = collection
    if scene_id:
        params["scene_id"] = scene_id
    if job_id:
        params["job_id"] = job_id
    if uri_query:
        params["uri_query"] = uri_query
    try:
        response = _api_request("GET", api_url, "/v1/artifacts", api_key=api_key, params=params, timeout=30)
        if not response.ok:
            return [], 0
        body = response.json()
        return list(body.get("items", [])), int(body.get("total", 0) or 0)
    except Exception:
        return [], 0


def _path_size_bytes(path_value: str) -> int | None:
    if not path_value:
        return None
    path = Path(path_value)
    if not path.exists():
        return None
    try:
        if path.is_file():
            return int(path.stat().st_size)
        total = 0
        file_count = 0
        for child in path.rglob("*"):
            if not child.is_file():
                continue
            total += int(child.stat().st_size)
            file_count += 1
            if file_count > 5000:
                return None
        return total
    except OSError:
        return None


def _register_zarr_artifact(
    api_url: str,
    api_key: str,
    *,
    convert_response: Dict[str, Any],
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
) -> bool:
    zarr_uri = str(convert_response.get("zarr_uri", "")).strip()
    if not zarr_uri:
        return False
    summary = convert_response.get("normalization_summary", {}) or {}
    zarr_summary = summary.get("zarr_summary", {}) or {}
    metadata = {
        "normalization_summary": summary,
        "registered_via": "streamlit_ui",
    }
    payload = {
        "artifact_type": "zarr",
        "artifact_uri": zarr_uri,
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "source_uri": raw_uri,
        "created_by_job_id": str(convert_response.get("job_id", "")).strip() or None,
        "data_family": convert_response.get("data_family"),
        "band_names": list(convert_response.get("band_names") or zarr_summary.get("band_names") or []),
        "dimensions": list(convert_response.get("dimensions") or zarr_summary.get("dimensions") or []),
        "shape": list(zarr_summary.get("shape") or []),
        "size_bytes": _path_size_bytes(zarr_uri),
        "metadata": metadata,
    }
    try:
        response = _api_request("POST", api_url, "/v1/artifacts", api_key=api_key, payload=payload, timeout=30)
        return bool(response.ok)
    except Exception:
        return False


def _human_size(size_bytes: Any) -> str:
    try:
        value = int(size_bytes)
    except Exception:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def _job_matches_view(row: Dict[str, Any], view: str, *, now_utc: dt.datetime) -> bool:
    state = str(row.get("state", "")).lower()
    if view == "all":
        return True
    if view == "active":
        return state in ACTIVE_JOB_STATES
    if view == "succeeded":
        return state == "succeeded"
    if view == "failed":
        return state == "failed"
    if view == "cancelled":
        return state == "cancelled"
    if view == "recent":
        ref = _status_reference_time(row)
        return ref is not None and (now_utc - ref) <= dt.timedelta(minutes=max(1, RECENT_JOB_CATEGORY_MINUTES))
    return True


def _job_view_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    return {
        "all": len(rows),
        "active": len([row for row in rows if _job_matches_view(row, "active", now_utc=now_utc)]),
        "succeeded": len([row for row in rows if _job_matches_view(row, "succeeded", now_utc=now_utc)]),
        "failed": len([row for row in rows if _job_matches_view(row, "failed", now_utc=now_utc)]),
        "cancelled": len([row for row in rows if _job_matches_view(row, "cancelled", now_utc=now_utc)]),
        "recent": len([row for row in rows if _job_matches_view(row, "recent", now_utc=now_utc)]),
    }


def _filter_jobs_by_view(rows: List[Dict[str, Any]], view: str) -> List[Dict[str, Any]]:
    now_utc = dt.datetime.now(dt.timezone.utc)
    return [row for row in rows if _job_matches_view(row, view, now_utc=now_utc)]


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


def _guess_scene_id(raw_uri: str) -> str:
    name = Path(raw_uri).name
    for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(raw_uri).stem


def _guess_zarr_provider(raw_uri: str) -> str:
    lower_name = str(raw_uri or "").lower()
    if any(token in lower_name for token in ("landsat", "lc08", "lc09", "le07", "lt05")):
        return "usgs"
    return "copernicus"


def _guess_zarr_collection(provider_api: str, scene_id: str) -> str:
    scene_upper = str(scene_id or "").upper()
    if provider_api == "usgs":
        if "_L1" in scene_upper:
            return "landsat_ot_c2_l1"
        return "landsat_ot_c2_l2"
    if scene_upper.startswith("S1"):
        return "SENTINEL-1"
    return "SENTINEL-2"


def _default_zarr_output(scene_id: str) -> str:
    safe_scene = re.sub(r"[^A-Za-z0-9._-]+", "_", (scene_id or "scene")).strip("._-")
    if not safe_scene:
        safe_scene = "scene"
    return f"/data/downloads/zarr/{safe_scene}.zarr"


def _container_to_host_path_hint(path_value: str) -> str:
    if not path_value:
        return ""
    if path_value.startswith("/data/"):
        return "." + path_value
    if path_value == "/data":
        return "./data"
    return ""


def _resolve_usgs_product_type(selected_product_type: str, selected_satellite: str) -> str:
    product = str(selected_product_type or "").strip().upper()
    if not product:
        return ""
    if product[0] in {"8", "9"} and product[1:].startswith("L"):
        return product

    sat = str(selected_satellite or "").strip()
    if sat in {"08", "8"}:
        return f"8{product}"
    if sat in {"09", "9"}:
        return f"9{product}"
    return product


def _build_job_payload(
    *,
    provider_label: str,
    collection: str,
    product_type: str,
    start_date: dt.date,
    end_date: dt.date,
    aoi_wkt: str,
    tile_id: str | None = None,
) -> Dict[str, Any]:
    provider_api = PROVIDER_CLI_MAP[provider_label]
    return build_job_payload_runtime(
        provider=provider_api,
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi_wkt=aoi_wkt,
        tile_id=tile_id,
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
        updated_from=_recent_jobs_cutoff(),
        sort_by="updated_at",
        sort_desc=True,
        page=1,
        page_size=RECENT_JOBS_FETCH_LIMIT,
    )
    statuses = _filter_recent_job_rows(
        _merge_job_rows(active_statuses, recent_statuses),
        limit=RECENT_JOBS_FETCH_LIMIT,
    )
    stats = summarize_statuses(statuses)
    total_jobs = int(stats["total_jobs"])
    running_jobs = int(stats["active_jobs"])
    succeeded_jobs = int(stats["succeeded_jobs"])
    failed_jobs = int(stats["failed_jobs"])
    cancelled_jobs = int(stats["cancelled_jobs"])
    bytes_done = int(stats["bytes_downloaded"])
    bytes_total = int(stats["bytes_total"])
    progress_pct = float(stats["progress"])
    st.session_state["active_job_ids"] = [
        str(item.get("job_id", "")).strip()
        for item in active_statuses
        if str(item.get("job_id", "")).strip()
    ]

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
        f"Summary is computed from recent activity (last {RECENT_JOBS_WINDOW_HOURS}h) plus active jobs."
    )
    st.caption("Download panel shows recent activity plus active jobs. Full historical browsing stays in Results.")

    provider_scope_value = str(_ss("dl_job_provider_filter", "current"))
    collection_filter_value = str(_ss("dl_job_collection_filter", "")).strip()
    product_filter_value = str(_ss("dl_job_product_filter", "")).strip()
    job_query_value = str(_ss("dl_job_id_query", "")).strip()
    filtered_statuses = _filter_jobs_by_scope(
        statuses,
        provider=_provider_scope_value(provider_scope_value, download_provider_api),
        collection_query=collection_filter_value,
        product_query=product_filter_value,
        job_query=job_query_value,
    )

    filter_col1, filter_col2 = st.columns([2, 4])
    with filter_col1:
        job_view_counts = _job_view_counts(filtered_statuses)
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
            refresh_jobs_clicked = st.button("Refresh", use_container_width=True, key="refresh_jobs_button")

    base_visible_statuses = _filter_jobs_by_scope(
        statuses,
        provider=_provider_scope_value(provider_scope, download_provider_api),
        collection_query=collection_filter,
        product_query=product_filter,
        job_query=job_query,
    )
    visible_statuses = _filter_jobs_by_view(base_visible_statuses, job_view)
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


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

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
        if st.button("Check health", use_container_width=True, key="check_health_btn"):
            try:
                response = _api_request(
                    "GET",
                    st.session_state["api_url"],
                    "/v1/health",
                    api_key=st.session_state["api_key"],
                    timeout=30,
                )
                if response.ok:
                    st.success(response.json())
                else:
                    st.error(f"{response.status_code}: {response.text}")
            except Exception as exc:
                st.error(str(exc))

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
        if st.sidebar.button("✅ Apply", use_container_width=True):
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
            if st.button("🗑️ Clear", use_container_width=True, key="clr_aoi"):
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
                    if st.button("➕", use_container_width=True, key=f"ta_{skey}", help="Add"):
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        sel.add(pk)
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                with b2:
                    if st.button("🔄", use_container_width=True, key=f"tr_{skey}", help="Replace"):
                        st.session_state["selected_tiles"] = [pk]
                        st.rerun()
                with b3:
                    if st.button("🎯", use_container_width=True, key=f"tz_{skey}", help="Zoom"):
                        cyx = centroids.get(str(pk))
                        if cyx:
                            st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                            st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                            st.rerun()
        valid_sel = [t for t in st.session_state["selected_tiles"] if t in all_names_set]
        if len(valid_sel) != len(st.session_state["selected_tiles"]):
            st.session_state["selected_tiles"] = valid_sel

        # Force a safe widget reset when selection changes from map/search buttons.
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
            use_container_width=True,
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
                    if st.button("⬅️", use_container_width=True, key=f"pick_prev_{skey}", help="Previous tile"):
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
                    if st.button(pick_btn, use_container_width=True, key=f"pick_toggle_{skey}", help="Toggle current tile"):
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        if current_tile in sel:
                            sel.remove(current_tile)
                        else:
                            sel.add(current_tile)
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                with nav3:
                    if st.button("➡️", use_container_width=True, key=f"pick_next_{skey}", help="Next tile"):
                        idx = (idx + 1) % len(candidates)
                        st.session_state[pick_index_key] = idx
                        nxt = str(candidates[idx])
                        cyx = centroids.get(nxt)
                        if cyx:
                            st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                            st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                        st.rerun()

                if st.sidebar.button("🎯 Zoom current tile", use_container_width=True, key=f"pick_zoom_{skey}"):
                    cyx = centroids.get(current_tile)
                    if cyx:
                        st.session_state["map_center"] = [float(cyx[0]), float(cyx[1])]
                        st.session_state["fly_to"] = json.dumps([float(cyx[0]), float(cyx[1]), 10])
                        st.rerun()

        if cur_sel:
            if st.sidebar.button("✕ Clear", use_container_width=True, key=f"tc_{skey}"):
                st.session_state["selected_tiles"] = []
                st.session_state.pop(ms_widget_key, None)
                st.session_state[ms_sync_key] = ""
                st.rerun()
    return provider, satellite, product, aoi_mode


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

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
            refresh_preview = st.button("🔎 Refresh Preview", use_container_width=True, key="refresh_preview")

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
            start_clicked = st.button("🚀 Start Download", use_container_width=True, type="primary")
        with d2:
            stop_clicked = st.button("⏹️ Stop", use_container_width=True)
        with d3:
            reset_clicked = st.button("🗑️ Reset", use_container_width=True)
        with d4:
            unlock_clicked = st.button("🔓 Unlock", use_container_width=True)

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
            if st.button("Check Zarr health", use_container_width=True):
                try:
                    resp = _http_session().get(
                        f"{_ss('zarr_service_url').rstrip('/')}/health",
                        timeout=15,
                    )
                    if resp.ok:
                        st.success(resp.json())
                    else:
                        st.error(f"{resp.status_code}: {resp.text}")
                except Exception as exc:
                    st.error(str(exc))

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
        default_output = _default_zarr_output(guessed_scene)

        # Keep form fields synchronized when the selected raw source changes.
        last_raw = str(_ss("zarr_last_raw_uri", ""))
        if raw_uri and raw_uri != last_raw:
            st.session_state["zarr_last_raw_uri"] = raw_uri
            st.session_state["zarr_provider_api"] = default_provider
            st.session_state["zarr_collection_api"] = default_collection
            st.session_state["zarr_scene_id"] = guessed_scene
            st.session_state["zarr_output_uri"] = default_output
            st.session_state["zarr_last_auto_output"] = default_output

        col1, col2, col3 = st.columns(3)
        with col1:
            provider_api = st.selectbox(
                "Provider",
                options=["copernicus", "usgs"],
                index=0 if default_provider == "copernicus" else 1,
                key="zarr_provider_api",
            )
        with col2:
            collection_api = st.text_input(
                "Collection",
                value=_guess_zarr_collection(provider_api, guessed_scene),
                key="zarr_collection_api",
            )
        with col3:
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

        run_convert = st.button("Run Zarr conversion", type="primary", use_container_width=True, disabled=not raw_uri)
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
                "scene_id": scene_id_value,
                "raw_uri": raw_uri,
                "raw_format": "archive",
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
        if not artifacts:
            st.info("No .zarr store found in the registry or on local disk yet.")
        else:
            st.caption(f"Showing {len(artifacts)} / {artifacts_total} Zarr stores.")
            store_options = [str(item["artifact_uri"]) for item in artifacts]
            selected_store = st.selectbox(
                "Stored Zarr directory",
                options=store_options,
                index=0,
                key="zarr_selected_store",
                format_func=lambda value: next(
                    (
                        f"{Path(str(item['artifact_uri'])).name}  |  "
                        f"{_container_to_host_path_hint(str(item['artifact_uri'])) or str(item['artifact_uri'])}"
                        for item in artifacts
                        if str(item["artifact_uri"]) == value
                    ),
                    value,
                ),
            )
            selected_store_meta = next(
                (item for item in artifacts if str(item["artifact_uri"]) == selected_store),
                artifacts[0],
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
            date_from=_recent_jobs_cutoff(),
            page=1,
            page_size=RECENT_JOBS_FETCH_LIMIT,
        )
        jobs_rows = _filter_recent_job_rows(jobs_rows)
        st.caption(
            f"Showing recent jobs only (last {RECENT_JOBS_WINDOW_HOURS}h + active). "
            f"{len(jobs_rows)} / {jobs_total} rows."
        )
        if jobs_rows:
            st.dataframe(jobs_rows, use_container_width=True)
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
                st.dataframe(rows[:500], use_container_width=True, hide_index=True)
                selected = st.selectbox("Select a file", options=[r["path"] for r in rows], index=0)
                sel_path = dl_dir / selected
                if sel_path.exists() and sel_path.is_file():
                    st.download_button(
                        "⬇️ Download selected",
                        data=sel_path.read_bytes(),
                        file_name=sel_path.name,
                        mime="application/octet-stream",
                        use_container_width=True,
                    )

    with tab_set:
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
        st.markdown(
            """**Runtime notes**
- Legacy map/tile UX preserved.
- Downloads are executed via FastAPI jobs (`/v1/jobs`) and worker service.
- Job events consumed through `/v1/events` (SSE) with polling fallback.
- Reset/Unlock only clear UI runtime state, never downloaded files."""
        )


if __name__ == "__main__":
    main()
