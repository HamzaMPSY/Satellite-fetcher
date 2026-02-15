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
try:
    import signalx as signal  # optional, if present
except ImportError:
    import signal
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass

import geopandas as gpd
import shapely
import streamlit as st
import streamlit.components.v1 as components
from loguru import logger
from shapely.geometry import Polygon, shape, mapping, box
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


# ═══════════════════════════════════════════════════════════════════════════════
# PROJECT PATHS
# ═══════════════════════════════════════════════════════════════════════════════
# Streamlit can be launched from any working directory (VSCode, terminal, etc.).
# The CLI and relative file paths must therefore be resolved against the project
# root (the folder containing this script) to behave consistently.
PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOADS_DIR = PROJECT_ROOT / "downloads"
NOHUP_PATH = PROJECT_ROOT / "nohup.out"
PID_PATH = PROJECT_ROOT / "job_pid"

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
    # Resolve relative to PROJECT_ROOT so the app works regardless of the
    # directory Streamlit was launched from.
    S2_GEOJSON: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_grids.geojson")
    S2_NOCOV: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_no_coverage.geojson")
    S2_SHAPEFILE: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel_2_index_shapefile.shp")
    LANDSAT_SHAPEFILE: str = str(PROJECT_ROOT / "data" / "Landsat-tiles" / "WRS2_descending.shp")
    LANDSAT_GEOJSON: str = str(PROJECT_ROOT / "data" / "Landsat-tiles" / "wrs2_descending.geojson")


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
    "OpenTopography": [
        "SRTMGL3 (SRTM GL3 90m)", "SRTMGL1 (SRTM GL1 30m)",
        "SRTMGL1_E (SRTM GL1 Ellipsoidal 30m)",
        "AW3D30 (ALOS World 3D 30m)", "AW3D30_E (ALOS World 3D Ellipsoidal 30m)",
        "SRTM15Plus (Global Bathymetry SRTM15+ V2.1 500m)",
        "NASADEM (NASADEM Global DEM)",
        "COP30 (Copernicus Global DSM 30m)", "COP90 (Copernicus Global DSM 90m)",
        "EU_DTM (DTM 30m)", "GEDI_L3 (DTM 1000m)",
        "GEBCOIceTopo (Global Bathymetry 500m)",
        "GEBCOSubIceTopo (Global Bathymetry 500m)",
        "CA_MRDEM_DSM (DSM 30m)", "CA_MRDEM_DTM (DTM 30m)",
    ],
    "CDS": [],
    "GoogleEarthEngine": [
        "COPERNICUS/S2_SR", "LANDSAT/LC08/C02/T1_L2",
        "MODIS/006/MOD13Q1", "USGS/SRTMGL1_003",
    ],
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
    "landsat_ot_c2_l1": ["8L1TP", "8L1GT", "8L1GS", "9L1TP", "9L1GT", "9L1GS"],
    "landsat_ot_c2_l2": ["8L2SP", "8L2SR", "9L2SP", "9L2SR"],
}

# ── FIX: Explicit mapping from UI provider name to CLI --provider value ──
PROVIDER_CLI_MAP: Dict[str, str] = {
    "Copernicus": "copernicus",
    "USGS": "usgs",
    "OpenTopography": "opentopography",
    "CDS": "cds",
    "GoogleEarthEngine": "google_earth_engine",
}


# ═══════════════════════════════════════════════════════════════════════════════
# STYLING
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background: #060a14 !important;
    color: #e2e8f0 !important;
    font-family: 'DM Sans', system-ui, sans-serif !important;
}
[data-testid="stSidebar"] {
    background: #0b1120 !important;
    border-right: 1px solid rgba(56,120,200,0.10) !important;
}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label, [data-testid="stSidebar"] span {
    color: #e2e8f0 !important;
}
.stButton > button {
    background: rgba(56,189,248,0.08) !important;
    color: #38bdf8 !important;
    border: 1px solid rgba(56,189,248,0.2) !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    transition: all 0.2s ease !important;
}
.stButton > button:hover {
    background: rgba(56,189,248,0.16) !important;
    border-color: #38bdf8 !important;
    box-shadow: 0 0 12px rgba(56,189,248,0.15) !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #38bdf8, #2dd4bf) !important;
    border: none !important;
    color: #060a14 !important;
    font-weight: 700 !important;
}
.stTabs [data-baseweb="tab-list"] {
    background: #0b1120 !important;
    border-radius: 12px !important;
    padding: 4px !important;
    border: 1px solid rgba(56,120,200,0.10) !important;
    gap: 4px !important;
    width: 100% !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 8px !important;
    color: #94a3b8 !important;
    font-weight: 600 !important;
    flex: 1 1 0% !important;
    justify-content: center !important;
    padding: 8px 12px !important;
    font-size: 0.9rem !important;
}
.stTabs [aria-selected="true"] {
    background: rgba(56,189,248,0.14) !important;
    color: #38bdf8 !important;
}
[data-testid="stExpander"] {
    background: #111827 !important;
    border: 1px solid rgba(56,120,200,0.10) !important;
    border-radius: 10px !important;
}
pre, code {
    background: #0b1120 !important;
    color: #e2e8f0 !important;
    border-radius: 8px !important;
}
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: #060a14; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.18); border-radius: 3px; }
/* Hide the leaflet component iframe border */
iframe[title="leaflet_map"] {
    border: none !important;
    border-radius: 14px !important;
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
    const isInter = interNames.has(name);
    const isSel   = selNames.has(name);
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
    maybeSend({type:"tile_click", name:name});
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
        const b = t.b;
        features.push({
            type:"Feature",
            properties:{name:t.n},
            geometry:{
                type:"Polygon",
                coordinates:[[
                    [b[0],b[1]], [b[2],b[1]],
                    [b[2],b[3]], [b[0],b[3]],
                    [b[0],b[1]]
                ]]
            }
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
                if(e && e.originalEvent){ L.DomEvent.stopPropagation(e.originalEvent); }
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

    if(interChanged || selChanged){
        interNames = new Set(JSON.parse(newInter));
        selNames   = new Set(JSON.parse(newSel));
        prevInterHash = newInter;
        prevSelHash   = newSel;
        refreshGridStyles();
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
    if not text or not text.strip():
        return None
    t = text.strip()
    if t.startswith("{"):
        try:
            obj = json.loads(t)
            if obj.get("type") == "Feature":
                return shape(obj["geometry"])
            if obj.get("type") == "FeatureCollection":
                geoms = [shape(f["geometry"]) for f in obj.get("features", []) if f.get("geometry")]
                return safe_union(geoms) if geoms else None
            return shape(obj)
        except Exception:
            return None
    try:
        return shapely_wkt.loads(t)
    except Exception:
        return None


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


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading Sentinel-2 grid…")
def load_s2():
    tiles, nocov = None, None
    for p in [PATHS.S2_GEOJSON, PATHS.S2_SHAPEFILE]:
        if tiles is not None:
            break
        try:
            if Path(p).exists():
                tiles = ensure_4326(gpd.read_file(p))
                logger.info(f"Loaded Sentinel-2 grid from {p} ({len(tiles)} tiles)")
        except Exception as e:
            logger.warning(f"Failed to read Sentinel-2 grid '{p}': {e}")
    try:
        if Path(PATHS.S2_NOCOV).exists():
            nocov = ensure_4326(gpd.read_file(PATHS.S2_NOCOV))
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
    for p in [PATHS.LANDSAT_GEOJSON, PATHS.LANDSAT_SHAPEFILE]:
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
    bdf = gdf.geometry.bounds.round(4)
    features = []
    for name, minx, miny, maxx, maxy in zip(
        names, bdf["minx"], bdf["miny"], bdf["maxx"], bdf["maxy"]
    ):
        features.append({"n": name, "b": [minx, miny, maxx, maxy]})
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


def reset_downloads(dl_dir: Optional[str] = None):
    """Reset the downloads directory and UI tracking state."""
    logger.info("[DL] reset_downloads() called")
    dl_path = Path(dl_dir) if dl_dir else DOWNLOADS_DIR
    if dl_path.exists():
        import shutil
        shutil.rmtree(dl_path, ignore_errors=True)
    dl_path.mkdir(parents=True, exist_ok=True)

    # Close any open log file handle
    _close_log_fh()

    # Clear logs and PID file
    try:
        NOHUP_PATH.write_text("")
    except Exception:
        pass
    try:
        PID_PATH.unlink(missing_ok=True)
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


def _build_download_command(
    provider, satellite, product, start_date, end_date, aoi_file,
    selected_tiles=None,
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

    if logs.get("products_found", 0) > 0:
        st.session_state["dl_total_products"] = logs["products_found"]
    total_products = st.session_state.get("dl_total_products", 0)

    phase_info = {
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
        "provider": "Copernicus",
        "satellite": "SENTINEL-2",
        "product": "S2MSI2A",
        "dl_start_time": None,
        "dl_total_products": 0,
        "dl_completed": 0,
        "dl_running": False,
        "fly_to": None,
        "use_file_browser_component": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _ss(key, default=None):
    return st.session_state.get(key, default)


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

def render_sidebar(sat_tiles, gdf, nocov, ncol, skey):
    st.sidebar.markdown("""
    <div style="text-align:center;padding:.3rem 0 .6rem">
        <div style="font-size:1.6rem">🛰️</div>
        <div style="font-size:1rem;font-weight:700;color:#e2e8f0">Sat Downloader</div>
        <div style="font-size:.65rem;color:#64748b;letter-spacing:.06em">v2 — NATIVE LEAFLET</div>
    </div>""", unsafe_allow_html=True)

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
        st.session_state["geometry_text"] = st.sidebar.text_area("WKT/GeoJSON", value=_ss("geometry_text", ""), height=100, label_visibility="collapsed", placeholder="Paste WKT or GeoJSON…")
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
        q = st.sidebar.text_input("Search", placeholder="e.g. 34UED or 233062", label_visibility="collapsed", key=f"ts_{skey}")
        if q:
            matches = find_tiles(gdf, ncol, q, 50)
            mids = matches[ncol].astype(str).tolist() if not matches.empty else []
            if not mids:
                st.sidebar.caption("No matches.")
            else:
                pk = st.sidebar.selectbox("Results", mids, index=0, key=f"tm_{skey}", label_visibility="collapsed")
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
                        row = gdf[gdf[ncol].astype(str) == str(pk)]
                        if not row.empty:
                            ct = row.iloc[0].geometry.centroid
                            st.session_state["map_center"] = [float(ct.y), float(ct.x)]
                            st.session_state["fly_to"] = json.dumps([float(ct.y), float(ct.x), 10])
                            st.rerun()
        all_names = sorted(gdf[ncol].astype(str).unique().tolist()) if ncol else []
        all_names_set = set(all_names)
        valid_sel = [t for t in st.session_state["selected_tiles"] if t in all_names_set]
        if len(valid_sel) != len(st.session_state["selected_tiles"]):
            st.session_state["selected_tiles"] = valid_sel
        cur_sel = st.sidebar.multiselect("Selected", all_names, default=valid_sel, key=f"ms_{skey}", label_visibility="collapsed")
        st.session_state["selected_tiles"] = cur_sel
        if cur_sel:
            if st.sidebar.button("✕ Clear", use_container_width=True, key=f"tc_{skey}"):
                st.session_state["selected_tiles"] = []
                st.rerun()
    return provider, satellite, product, aoi_mode


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(page_title="Satellite Imagery Downloader", page_icon="🛰️", layout="wide", initial_sidebar_state="expanded")
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

    provider, satellite, product, aoi_mode = render_sidebar(sat_tiles, gdf, nocov, ncol, skey)

    st.markdown("""<div style='display:flex;align-items:center;gap:14px;margin-bottom:4px;'>
        <div style='font-size:1.6rem;background:linear-gradient(135deg,#38bdf8,#2dd4bf);width:44px;height:44px;border-radius:12px;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 12px rgba(56,189,248,0.35);'>🛰️</div>
        <div><div style='font-size:1.25rem;font-weight:700;color:#e2e8f0;'>Satellite Imagery Downloader</div><div style='font-size:0.72rem;color:#64748b;letter-spacing:.04em;'>v2 Native Leaflet · SVG Grid · No Lag</div></div>
    </div>""", unsafe_allow_html=True)

    tab_map, tab_dl, tab_res, tab_set = st.tabs(["🗺️ Map", "⬇️ Download", "📂 Results", "🔧 Settings"])

    # ── MAP TAB ───────────────────────────────────────────────────────
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

        options = json.dumps({
            "show_grid": st.session_state["show_grid"],
            "colorize": st.session_state["colorize"],
            "opacity": st.session_state["opacity"],
            "show_nocov": st.session_state.get("show_nocov", False),
            "show_inter": st.session_state["show_inter"],
            "show_sel": st.session_state["show_sel"],
            "click_select": st.session_state["click_sel"],
        })

        fly_to = st.session_state.pop("fly_to", None)

        st.markdown('<div style="border-radius:14px;overflow:hidden;border:1px solid rgba(56,120,200,0.10);box-shadow:0 4px 20px rgba(0,0,0,0.3);">', unsafe_allow_html=True)

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

        st.markdown('</div>', unsafe_allow_html=True)

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
                tid = comp_result.get("name", "")
                if tid and gdf is not None and ncol:
                    valid_names = set(gdf[ncol].astype(str).unique())
                    if tid in valid_names:
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        sel.symmetric_difference_update({tid})
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                    else:
                        logger.warning(f"Clicked tile '{tid}' not found in grid column '{ncol}'")

        ni = len(st.session_state["intersecting_tiles"])
        ns = len(st.session_state["selected_tiles"])
        grid_label = skey.split('-')[0].upper()
        st.markdown(f"""<div style='display:flex;gap:8px;margin-top:6px;'>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#2dd4bf;font-weight:700;'>{ni}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Intersecting</div></div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#e2e8f0;font-weight:700;'>{ns}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Selected</div></div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#38bdf8;font-weight:700;'>{grid_label}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Grid</div></div>
        </div>""", unsafe_allow_html=True)

        if gdf is not None and ncol:
            ca, cb = st.columns(2)
            with ca:
                if st.session_state["intersecting_tiles"]:
                    with st.expander(f"🔮 Intersecting ({ni})", expanded=False):
                        st.markdown("".join(
                            f'<span style="display:inline-block;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:16px;padding:4px 10px;margin:2px;font-size:11px;font-family:JetBrains Mono;color:#e48abf;">{t}</span>'
                            for t in st.session_state["intersecting_tiles"][:60]
                        ), unsafe_allow_html=True)
                        if ni > 60:
                            st.caption(f"…+{ni-60}")
                        st.download_button("📥 CSV", data="tile\n" + "\n".join(st.session_state["intersecting_tiles"]), file_name=f"{skey}_intersects.csv", mime="text/csv")
            with cb:
                if st.session_state["selected_tiles"]:
                    with st.expander(f"✅ Selected ({ns})", expanded=False):
                        st.markdown("".join(
                            f'<span style="display:inline-block;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:16px;padding:4px 10px;margin:2px;font-size:11px;font-family:JetBrains Mono;color:#f88cb0;">{t}</span>'
                            for t in st.session_state["selected_tiles"]
                        ), unsafe_allow_html=True)
                        st.download_button("📥 CSV", data="tile\n" + "\n".join(st.session_state["selected_tiles"]), file_name=f"{skey}_selected.csv", mime="text/csv")

    # ── DOWNLOAD TAB ──────────────────────────────────────────────────
    with tab_dl:
        st.markdown('<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>⬇️</span><span style="font-weight:600;font-size:.94rem;">Download Manager</span></div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Provider", provider)
        with c2:
            st.metric("Mission", satellite)
        with c3:
            st.metric("Product", product)
        st.markdown("---")

        np_ = len(_ss("selected_tiles", []))
        ni_ = len(_ss("intersecting_tiles", []))
        if _ss("dl_running"):
            st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">🔄 Download in progress…</div>', unsafe_allow_html=True)
        elif np_ > 0:
            st.markdown(f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">📦 {np_} tile(s) queued</div>', unsafe_allow_html=True)
        elif ni_ > 0:
            st.markdown(f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#38bdf8;">ℹ️ {ni_} intersecting — select tiles to download</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#fbbf24;">⚠️ No tiles — draw AOI or select tiles</div>', unsafe_allow_html=True)

        atxt = _ss("geometry_text", "").strip()
        afile = "example_aoi.geojson" if atxt.startswith("{") else "example_aoi.wkt"
        aoi_path = PROJECT_ROOT / afile

        selected_tiles_for_cmd = _ss("selected_tiles", [])

        cli_cmd, cli_err = _build_download_command(
            provider, satellite, product,
            st.session_state["start_date"], st.session_state["end_date"],
            str(aoi_path),
            selected_tiles=selected_tiles_for_cmd,
        )

        if cli_err:
            st.warning(f"⚠️ {cli_err}")
            import shlex
            cli_provider = PROVIDER_CLI_MAP.get(provider, provider.lower())
            _cli_path = _check_cli_exists() or "cli.py"
            _collection = str(satellite).split(" ")[0]
            _cmd_parts = [
                sys.executable, "-u", _cli_path,
                "--provider", cli_provider,
                "--collection", _collection,
            ]
            if product and str(product).strip():
                _cmd_parts.extend(["--product-type", str(product)])
            _cmd_parts.extend([
                "--start-date", str(st.session_state['start_date']),
                "--end-date", str(st.session_state['end_date']),
                "--aoi_file", str(aoi_path),
                "--log-type", "all",
            ])
            cli_cmd = " ".join(shlex.quote(str(p)) for p in _cmd_parts)

        st.code(cli_cmd or "# cli.py not found", language="bash")

        d1, d2, d3 = st.columns([2, 1, 1])
        with d1:
            if st.button("🚀 Start Download", use_container_width=True, type="primary"):
                logger.info("=" * 40)
                logger.info("[DL] ▶ START DOWNLOAD button pressed")
                logger.info(f"[DL]   provider={provider} satellite={satellite} product={product}")
                logger.info(f"[DL]   dates={st.session_state['start_date']} → {st.session_state['end_date']}")
                logger.info(f"[DL]   selected_tiles={selected_tiles_for_cmd}")
                logger.info(f"[DL]   AOI length={len(atxt)} chars")

                if not atxt:
                    st.error("Define an AOI first.")
                    logger.error("[DL] No AOI defined — aborting")
                elif cli_err:
                    st.error(f"Cannot start: {cli_err}")
                    logger.error(f"[DL] CLI error: {cli_err}")
                else:
                    reset_downloads()
                    st.session_state["dl_running"] = True
                    st.session_state["dl_start_time"] = time.time()

                    # Write AOI file
                    try:
                        aoi_path.write_text(atxt, encoding="utf-8")
                        logger.info(f"[DL] AOI written to {aoi_path} ({len(atxt)} chars)")
                    except Exception as e:
                        st.error(f"Failed to write AOI file: {e}")
                        logger.error(f"[DL] AOI write failed: {e}")
                        st.session_state["dl_running"] = False
                        st.stop()

                    # Clear log file
                    try:
                        NOHUP_PATH.write_text("")
                        logger.info(f"[DL] Cleared log file: {NOHUP_PATH}")
                    except Exception as e:
                        logger.warning(f"[DL] Could not clear log: {e}")

                    # Verify CLI exists
                    cli_real = _check_cli_exists()
                    if cli_real:
                        logger.info(f"[DL] CLI verified at: {cli_real}")
                    else:
                        logger.error("[DL] CLI NOT FOUND at launch time!")

                    logger.info(f"[DL] Full command: {cli_cmd}")

                    # ══════════════════════════════════════════════════════
                    # FIX: Use os.system("nohup ... &") — the ONLY method
                    # that worked in the old code.
                    #
                    # subprocess.Popen does fork() of the current Python
                    # process (with GDAL/fiona loaded) → SIGSEGV on macOS.
                    # os.system() goes through /bin/sh which spawns a FRESH
                    # Python process.  This is what the old code did.
                    # ══════════════════════════════════════════════════════
                    try:
                        # Build the command EXACTLY like the old working code:
                        #   os.system(f"nohup python cli.py --provider {provider.lower()} ... &")
                        # But with: full python path, cd to project root, PID capture
                        cli_provider = PROVIDER_CLI_MAP.get(provider, provider.lower())
                        collection = str(satellite).split(" ")[0]

                        # Build simple command string — NO shlex.quote
                        cmd_args = (
                            f"--provider {cli_provider} "
                            f"--collection {collection} "
                        )
                        if product and str(product).strip():
                            cmd_args += f"--product-type {product} "

                        cmd_args += (
                            f"--start-date {st.session_state['start_date']} "
                            f"--end-date {st.session_state['end_date']} "
                            f"--aoi_file {aoi_path} "
                            f"--log-type all"
                        )

                        # The shell command: cd to project root, then nohup + background
                        # This mirrors the old working code exactly.
                        shell_cmd = (
                            f"cd {PROJECT_ROOT} && "
                            f"nohup {sys.executable} -u cli.py {cmd_args} "
                            f"> nohup.out 2>&1 & "
                            f"echo $! > job_pid"
                        )
                        logger.info(f"[DL] Shell command: {shell_cmd}")

                        ret = os.system(shell_cmd)
                        logger.info(f"[DL] os.system returned: {ret}")

                        # Read the PID that was written by the shell
                        time.sleep(0.5)
                        pid = _read_pid()
                        if pid:
                            st.session_state["dl_pid"] = pid
                            logger.info(f"[DL] Background process PID={pid}")

                            # Give it a moment to start
                            time.sleep(0.5)
                            if _pid_is_running(pid):
                                st.success(f"✅ Download started (PID: {pid})")
                                logger.info(f"[DL] Process confirmed alive PID={pid}")
                            else:
                                # Process died — read its output
                                err_out = ""
                                if NOHUP_PATH.exists():
                                    err_out = NOHUP_PATH.read_text(errors="replace").strip()
                                logger.error(f"[DL] Process died. Output ({len(err_out)} chars): {err_out[-500:]}")
                                st.session_state["dl_running"] = False
                                if err_out:
                                    st.error("Download process died immediately:")
                                    st.code(err_out[-1500:], language="text")
                                else:
                                    st.error("Download process died with no output")
                        else:
                            logger.error("[DL] No PID captured")
                            st.error("Failed to start background process")
                            st.session_state["dl_running"] = False

                    except Exception as e:
                        st.error(f"Failed to start download: {e}")
                        st.session_state["dl_running"] = False
                        logger.error(f"[DL] Launch exception: {e}", exc_info=True)

                    st.rerun()
        with d2:
            if st.button("⏹️ Stop", use_container_width=True):
                logger.info("[DL] ⏹ STOP button pressed")
                pid = st.session_state.get("dl_pid") or _read_pid()
                if pid and _pid_is_running(pid):
                    try:
                        os.kill(pid, signal.SIGTERM)
                        logger.info(f"[DL] Sent SIGTERM to PID {pid}")
                    except Exception as e:
                        logger.error(f"[DL] Failed to kill PID {pid}: {e}")
                        # Try harder with SIGKILL
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except Exception:
                            pass
                else:
                    logger.info(f"[DL] PID {pid} not running, nothing to stop")
                _close_log_fh()
                st.session_state["dl_running"] = False
                try:
                    PID_PATH.unlink(missing_ok=True)
                except Exception:
                    pass
                st.warning("⏹️ Stopped.")
        with d3:
            if st.button("🗑️ Reset", use_container_width=True):
                logger.info("[DL] 🗑 RESET button pressed")
                reset_downloads()
                st.info("🗑️ Cleared.")
                st.rerun()
        st.markdown("---")
        render_download_progress()
        if _ss("dl_running"):
            pid = st.session_state.get("dl_pid") or _read_pid()
            still_running = _pid_is_running(pid)

            if not still_running:
                logger.info(f"[DL] Process PID={pid} is no longer running")
                _close_log_fh()  # FIX: close log handle when process ends
                st.session_state["dl_running"] = False
                try:
                    PID_PATH.unlink(missing_ok=True)
                except Exception:
                    pass
                final_logs = parse_download_logs()
                has_errors = bool(final_logs.get("errors"))
                final_phase = final_logs.get("phase", "")
                logger.info(f"[DL] Final phase={final_phase}, errors={len(final_logs.get('errors', []))}, "
                            f"products_found={final_logs.get('products_found', 0)}")
                if has_errors:
                    st.markdown('<div style="background:#111827;border:1px solid rgba(251,191,36,0.3);border-radius:10px;padding:8px;color:#fbbf24;">⚠️ Download finished with errors — check logs below</div>', unsafe_allow_html=True)
                elif final_phase == "done":
                    st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">✅ Download completed successfully!</div>', unsafe_allow_html=True)
                elif final_logs.get("products_found", 0) == 0:
                    st.markdown('<div style="background:#111827;border:1px solid rgba(251,191,36,0.3);border-radius:10px;padding:8px;color:#fbbf24;">ℹ️ No products found for the given parameters</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">✅ Download process finished</div>', unsafe_allow_html=True)
            else:
                time.sleep(2)
                st.rerun()

    # ── RESULTS TAB ───────────────────────────────────────────────────
    with tab_res:
        st.markdown('<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📂</span><span style="font-weight:600;font-size:.94rem;">Downloaded Products</span></div>', unsafe_allow_html=True)
        dl_dir = DOWNLOADS_DIR
        dl_dir.mkdir(exist_ok=True)
        n_files, total_mb = count_downloaded_products()
        st.markdown(f"""<div style='display:flex;gap:8px;margin:6px 0;'>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#e2e8f0;font-weight:700;'>{n_files}</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;'>Files</div></div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono;color:#2dd4bf;font-weight:700;'>{total_mb:.1f} MB</div>
                <div style='font-size:.7rem;color:#64748b;text-transform:uppercase;'>Total Size</div></div>
        </div>""", unsafe_allow_html=True)
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
                st.caption("If downloads don't appear immediately, click Reset/Refresh or disable the experimental browser.")
            except Exception as e:
                st.warning(f"File browser component failed: {e}. Falling back to built-in browser.")
                use_comp = False

        if (not use_comp) or (st_file_browser is None):
            files = [f for f in dl_dir.rglob("*") if f.is_file()]
            if not files:
                st.info("No files yet.")
            else:
                rows = []
                for f in sorted(files):
                    rel = str(f.relative_to(dl_dir))
                    stt = f.stat()
                    rows.append(
                        {
                            "path": rel,
                            "size_MB": round(stt.st_size / (1024 * 1024), 3),
                            "modified": dt.datetime.fromtimestamp(stt.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )

                ctop1, ctop2 = st.columns([1, 1])
                with ctop1:
                    st.dataframe(rows, use_container_width=True, hide_index=True)
                with ctop2:
                    selected = st.selectbox(
                        "Select a file",
                        options=[r["path"] for r in rows],
                        index=0,
                    )
                    sel_path = dl_dir / selected
                    if sel_path.exists() and sel_path.is_file():
                        st.caption(f"Selected: {selected}")
                        st.download_button(
                            "⬇️ Download selected",
                            data=sel_path.read_bytes(),
                            file_name=sel_path.name,
                            mime="application/octet-stream",
                            use_container_width=True,
                        )
                        if st.button("🗑️ Delete selected", use_container_width=True):
                            try:
                                sel_path.unlink()
                                st.success("Deleted.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Delete failed: {e}")
                    if st.button("🔄 Refresh list", use_container_width=True):
                        st.rerun()

    # ── SETTINGS TAB ──────────────────────────────────────────────────
    with tab_set:
        st.markdown('<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🔧</span><span style="font-weight:600;font-size:.94rem;">Configuration</span></div>', unsafe_allow_html=True)
        try:
            st.code((PROJECT_ROOT / "config.yaml").read_text(), language="yaml")
        except FileNotFoundError:
            st.info("config.yaml not found.")
        st.markdown("---")
        s1, s2, s3 = st.columns(3)
        with s1:
            st.metric("Center", f"{st.session_state['map_center'][0]:.4f}, {st.session_state['map_center'][1]:.4f}")
        with s2:
            st.metric("Zoom", st.session_state["map_zoom"])
        with s3:
            st.metric("System", skey)
        st.markdown("---")

        # ── App Debug Log viewer ──
        st.markdown('<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🔬</span><span style="font-weight:600;font-size:.94rem;">App Debug Log</span></div>', unsafe_allow_html=True)
        if _APP_LOG.exists():
            try:
                log_text = _APP_LOG.read_text(errors="replace")
                # Show last 5KB
                tail = log_text[-5000:] if len(log_text) > 5000 else log_text
                st.code(tail, language="text")
                st.caption(f"Log file: {_APP_LOG} ({_APP_LOG.stat().st_size / 1024:.1f} KB)")
            except Exception as e:
                st.warning(f"Cannot read debug log: {e}")
        else:
            st.info("No debug log yet.")

        st.markdown("---")
        st.markdown("""**Architecture v2 — Performance notes:**
- SVG renderer for grid tiles (reliable tooltips & click events)
- Client-side viewport filtering (no Python rerun on pan/zoom)
- Compact grid format (~2 MB vs 20 MB GeoJSON)
- Spatial index (R-tree) for intersection queries
- Zero `st.rerun()` loops — only explicit actions trigger reruns
- Debounced viewport updates (80ms) to avoid excessive redraws
""")


if __name__ == "__main__":
    main()