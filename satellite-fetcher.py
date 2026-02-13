"""
Satellite Imagery Downloader — Professional Edition
Inspired by Copernicus Data Space Ecosystem Browser.
"""

import os
import re
import math
import json
import time
import hashlib
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field

import folium
from folium import plugins
import geopandas as gpd
import shapely
import streamlit as st
from loguru import logger
from shapely.geometry import Polygon, shape, mapping, box
from shapely import wkt as shapely_wkt
from shapely.ops import unary_union
from streamlit_file_browser import st_file_browser
from streamlit_folium import st_folium

from utilities import ConfigLoader


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class TilePaths:
    """Immutable tile data paths."""
    S2_GEOJSON: str = "data/Sentinel-2-tiles/sentinel-2_grids.geojson"
    S2_NOCOV: str = "data/Sentinel-2-tiles/sentinel-2_no_coverage.geojson"
    S2_SHAPEFILE: str = "data/Sentinel-2-tiles/sentinel_2_index_shapefile.shp"
    LANDSAT_SHAPEFILE: str = "data/Landsat-tiles/WRS2_descending.shp"
    LANDSAT_GEOJSON: str = "data/Landsat-tiles/wrs2_descending.geojson"


@dataclass(frozen=True)
class MapConfig:
    """Map display constants."""
    MIN_GRID_ZOOM: int = 6
    AUTO_REFRESH_THROTTLE_SEC: float = 2.0
    DEFAULT_CENTER: Tuple[float, float] = (48.8566, 2.3522)
    DEFAULT_ZOOM: int = 8
    MAP_HEIGHT: int = 620
    MAX_FEATURES_DEFAULT: int = 800
    SIMPLIFY_TOL_DEFAULT: float = 0.002
    GRID_OPACITY_DEFAULT: float = 0.03


PATHS = TilePaths()
MAP_CFG = MapConfig()

# Satellite / provider registry
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


# ═══════════════════════════════════════════════════════════════════════════════
# STYLING — Copernicus‑inspired dark professional theme
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOM_CSS = """
<style>
/* ── Import fonts ── */
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    /* Copernicus-inspired palette */
    --bg-primary: #0a0e1a;
    --bg-secondary: #101729;
    --bg-card: #141d30;
    --bg-card-hover: #1a2540;
    --bg-input: #0d1322;
    --border-subtle: rgba(56, 120, 200, 0.12);
    --border-active: rgba(56, 120, 200, 0.35);
    --text-primary: #e8ecf4;
    --text-secondary: #8b99b5;
    --text-muted: #576380;
    --accent-blue: #2d7dd2;
    --accent-teal: #00b4d8;
    --accent-cyan: #48cae4;
    --accent-green: #06d6a0;
    --accent-amber: #ffb703;
    --accent-red: #ef476f;
    --accent-purple: #7b68ee;
    --gradient-primary: linear-gradient(135deg, #2d7dd2 0%, #00b4d8 50%, #48cae4 100%);
    --gradient-accent: linear-gradient(135deg, #7b68ee 0%, #2d7dd2 100%);
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.3);
    --shadow-md: 0 4px 16px rgba(0,0,0,0.35);
    --shadow-lg: 0 8px 32px rgba(0,0,0,0.4);
    --shadow-glow: 0 0 20px rgba(45, 125, 210, 0.15);
    --radius-sm: 6px;
    --radius-md: 10px;
    --radius-lg: 14px;
    --radius-xl: 20px;
}

/* ── Global overrides ── */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stApp"], .main, .block-container {
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
    color: var(--text-primary) !important;
}

[data-testid="stAppViewContainer"] {
    background: var(--bg-primary) !important;
}

[data-testid="stHeader"] {
    background: transparent !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--bg-secondary) !important;
    border-right: 1px solid var(--border-subtle) !important;
}

[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {
    color: var(--text-secondary) !important;
}

/* ── Cards ── */
.sat-card {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-lg);
    padding: 20px 22px 16px;
    margin-bottom: 16px;
    box-shadow: var(--shadow-md);
    transition: border-color 0.25s ease, box-shadow 0.25s ease;
}
.sat-card:hover {
    border-color: var(--border-active);
    box-shadow: var(--shadow-glow);
}

/* ── Headings ── */
.sat-header {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 4px;
}
.sat-header-icon {
    width: 42px;
    height: 42px;
    border-radius: var(--radius-md);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.2rem;
    flex-shrink: 0;
}
.sat-header-icon.blue   { background: linear-gradient(135deg, #2d7dd2 0%, #00b4d8 100%); }
.sat-header-icon.teal   { background: linear-gradient(135deg, #00b4d8 0%, #06d6a0 100%); }
.sat-header-icon.purple { background: linear-gradient(135deg, #7b68ee 0%, #2d7dd2 100%); }
.sat-header-icon.amber  { background: linear-gradient(135deg, #ffb703 0%, #fb8500 100%); }
.sat-header-icon.green  { background: linear-gradient(135deg, #06d6a0 0%, #118ab2 100%); }

.sat-title {
    font-weight: 700;
    font-size: 1.08rem;
    color: var(--text-primary);
    letter-spacing: -0.02em;
}
.sat-subtitle {
    font-size: 0.82rem;
    color: var(--text-muted);
    margin-top: 2px;
    margin-bottom: 14px;
    padding-left: 56px;
}

/* ── Inputs ── */
[data-testid="stTextInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stTextArea"] textarea,
[data-testid="stSelectbox"] > div > div,
.stSelectbox > div > div {
    background: var(--bg-input) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--radius-sm) !important;
    color: var(--text-primary) !important;
    font-family: 'DM Sans', sans-serif !important;
}

[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: var(--accent-blue) !important;
    box-shadow: 0 0 0 2px rgba(45, 125, 210, 0.2) !important;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: var(--radius-md) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
    letter-spacing: 0.01em !important;
    padding: 0.5rem 1rem !important;
    transition: all 0.2s ease !important;
    border: 1px solid var(--border-subtle) !important;
    background: var(--bg-card) !important;
    color: var(--text-primary) !important;
}
.stButton > button:hover {
    border-color: var(--accent-blue) !important;
    background: var(--bg-card-hover) !important;
    box-shadow: var(--shadow-glow) !important;
}

/* Primary button style for download */
.stButton > button[kind="primary"],
div[data-testid="stButton"]:has(button:contains("Download")) > button {
    background: var(--gradient-primary) !important;
    border: none !important;
    color: white !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--bg-card) !important;
    border-radius: var(--radius-lg) !important;
    padding: 4px !important;
    border: 1px solid var(--border-subtle) !important;
    gap: 2px !important;
}

.stTabs [data-baseweb="tab"] {
    border-radius: var(--radius-md) !important;
    color: var(--text-secondary) !important;
    font-weight: 500 !important;
    font-family: 'DM Sans', sans-serif !important;
    padding: 8px 20px !important;
}

.stTabs [aria-selected="true"] {
    background: var(--accent-blue) !important;
    color: white !important;
}

.stTabs [data-baseweb="tab-panel"] {
    padding-top: 16px !important;
}

/* ── Dividers ── */
hr {
    border-color: var(--border-subtle) !important;
    opacity: 0.5 !important;
}

/* ── Checkboxes ── */
[data-testid="stCheckbox"] label span {
    color: var(--text-secondary) !important;
    font-size: 0.88rem !important;
}

/* ── Code blocks ── */
code, .stCodeBlock {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.83rem !important;
}
pre {
    background: var(--bg-input) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--radius-md) !important;
}

/* ── Progress bars ── */
.stProgress > div > div {
    background: var(--gradient-primary) !important;
    border-radius: 100px !important;
}

/* ── Custom components ── */
.product-badge {
    background: var(--gradient-primary);
    color: white;
    padding: 14px 24px;
    border-radius: var(--radius-lg);
    text-align: center;
    font-weight: 700;
    font-size: 1.05rem;
    letter-spacing: -0.01em;
    margin: 12px 0;
    box-shadow: var(--shadow-md), 0 0 30px rgba(45, 125, 210, 0.2);
}

.tile-chip {
    display: inline-block;
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-sm);
    padding: 3px 10px;
    margin: 2px 3px;
    font-size: 0.78rem;
    color: var(--accent-cyan);
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
}

.stat-row {
    display: flex;
    gap: 12px;
    margin: 10px 0;
}
.stat-item {
    flex: 1;
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-md);
    padding: 14px 16px;
    text-align: center;
}
.stat-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--accent-cyan);
    font-family: 'JetBrains Mono', monospace;
}
.stat-label {
    font-size: 0.75rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 4px;
}

.color-legend-pro {
    background: var(--bg-card);
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-md);
    padding: 14px 16px;
    margin: 12px 0;
}
.legend-title {
    font-weight: 600;
    font-size: 0.82rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 10px;
}
.legend-item {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 7px 0;
    font-size: 0.82rem;
    color: var(--text-secondary);
}
.legend-swatch {
    width: 20px;
    height: 10px;
    border-radius: 3px;
    flex-shrink: 0;
}

/* ── Map container ── */
.map-wrapper {
    border-radius: var(--radius-lg);
    overflow: hidden;
    border: 1px solid var(--border-subtle);
    box-shadow: var(--shadow-lg);
}

/* ── Notification banners ── */
.info-banner {
    background: rgba(45, 125, 210, 0.08);
    border: 1px solid rgba(45, 125, 210, 0.2);
    border-radius: var(--radius-md);
    padding: 10px 16px;
    font-size: 0.85rem;
    color: var(--accent-cyan);
    margin: 8px 0;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb { background: var(--border-active); border-radius: 100px; }
::-webkit-scrollbar-thumb:hover { background: var(--accent-blue); }

/* ── Multiselect tags ── */
[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
    background: var(--bg-card-hover) !important;
    border: 1px solid var(--border-active) !important;
    border-radius: var(--radius-sm) !important;
    color: var(--accent-cyan) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.78rem !important;
}

/* ── Expander ── */
[data-testid="stExpander"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--radius-md) !important;
}

/* ── Captions ── */
.stCaption, [data-testid="stCaption"] {
    color: var(--text-muted) !important;
    font-size: 0.78rem !important;
}

/* ── Remove default padding ── */
.block-container {
    padding-top: 2rem !important;
    padding-bottom: 1rem !important;
}
</style>
"""


# ═══════════════════════════════════════════════════════════════════════════════
# GEO UTILITIES (pure functions — no Streamlit state)
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject to WGS‑84 if needed."""
    if gdf is None or gdf.empty:
        return gdf
    return gdf.set_crs(epsg=4326) if gdf.crs is None else gdf.to_crs(epsg=4326)


def get_name_column(gdf: Optional[gpd.GeoDataFrame], system: str) -> Optional[str]:
    """Resolve the tile‑name column for a GeoDataFrame."""
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
    """Shapely ≥2 / <2 compatible union."""
    try:
        return shapely.union_all(geoms)
    except Exception:
        return unary_union(geoms)


def bounds_from_leaflet(bounds) -> Optional[Tuple[float, float, float, float]]:
    """Extract (minx, miny, maxx, maxy) from Leaflet bounds dict."""
    if not bounds:
        return None

    def _f(x):
        try:
            return float(x) if x is not None else None
        except (TypeError, ValueError):
            return None

    if isinstance(bounds, dict):
        sw = bounds.get("_southWest") or bounds.get("southWest")
        ne = bounds.get("_northEast") or bounds.get("northEast")
        if not (sw and ne):
            return None
        vals = (_f(sw.get("lng")), _f(sw.get("lat")), _f(ne.get("lng")), _f(ne.get("lat")))
        if None in vals:
            return None
        minx, miny, maxx, maxy = vals
        if maxx < minx:
            minx, maxx = maxx, minx
        if maxy < miny:
            miny, maxy = maxy, miny
        return (minx, miny, maxx, maxy)
    return None


def fallback_bbox(lat: float, lng: float, zoom: int) -> Tuple[float, float, float, float]:
    w = 360.0 / (2 ** max(0, zoom))
    h = 180.0 / (2 ** max(0, zoom))
    return (lng - w * 0.6, lat - h * 0.6, lng + w * 0.6, lat + h * 0.6)


def bbox_key(bbox: Optional[Tuple], nd: int = 4):
    if bbox is None:
        return None
    try:
        return tuple(round(float(v), nd) for v in bbox)
    except Exception:
        return None


def filter_gdf_in_bbox(
    gdf: Optional[gpd.GeoDataFrame],
    bbox_ll: Optional[Tuple[float, float, float, float]],
    max_features: int,
    simplify_tol: float,
    keep_cols: Optional[List[str]] = None,
) -> Optional[gpd.GeoDataFrame]:
    """Clip GeoDataFrame to bounding box, simplify, and limit features."""
    if gdf is None or gdf.empty or bbox_ll is None:
        return None

    bb = box(*bbox_ll)
    try:
        sub = gdf[gdf.intersects(bb)].copy()
    except Exception:
        sub = gdf.copy()

    if sub.empty:
        return sub

    cols = ([c for c in (keep_cols or []) if c in sub.columns]) + ["geometry"]
    cols = list(dict.fromkeys(cols))  # deduplicate, preserve order
    sub = sub[cols].copy() if keep_cols else sub[["geometry"]].copy()

    if simplify_tol > 0:
        try:
            sub["geometry"] = sub.geometry.simplify(simplify_tol, preserve_topology=True)
        except Exception:
            pass

    return sub.iloc[:max_features].copy() if len(sub) > max_features else sub


def parse_text_geometry(text: str) -> Optional[shapely.Geometry]:
    """Parse WKT or GeoJSON text into a Shapely geometry."""
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


def make_square_wkt(lat: float, lng: float, side_km: float) -> str:
    half = side_km / 2.0
    dlat = half / 111.0
    dlon = half / (111.0 * max(0.05, abs(math.cos(math.radians(lat)))))
    poly = Polygon([
        (lng - dlon, lat - dlat), (lng + dlon, lat - dlat),
        (lng + dlon, lat + dlat), (lng - dlon, lat + dlat),
        (lng - dlon, lat - dlat),
    ])
    return shapely_wkt.dumps(poly, rounding_precision=6)


def compute_intersections(
    aoi_polys: List[Polygon],
    tiles_gdf: Optional[gpd.GeoDataFrame],
    name_col: Optional[str],
) -> Tuple[List[str], Optional[gpd.GeoDataFrame]]:
    if tiles_gdf is None or tiles_gdf.empty or not aoi_polys or not name_col:
        return [], None
    aoi_union = safe_union(aoi_polys)
    try:
        candidates = tiles_gdf[tiles_gdf.intersects(aoi_union)].copy()
        if candidates.empty:
            return [], candidates
        candidates = candidates[[name_col, "geometry"]].copy()
        names = sorted(candidates[name_col].astype(str).unique().tolist())
        return names, candidates
    except Exception as e:
        logger.error(f"Intersection error: {e}")
        return [], None


def find_tiles_by_query(gdf: gpd.GeoDataFrame, col: str, query: str, limit: int = 50) -> gpd.GeoDataFrame:
    q = (query or "").strip()
    if not q:
        return gdf.iloc[0:0]
    s = gdf[col].astype(str)
    exact = gdf[s.str.upper() == q.upper()]
    if not exact.empty:
        return exact[[col, "geometry"]].copy()
    contains = gdf[s.str.contains(q, case=False, na=False)]
    return contains[[col, "geometry"]].iloc[:limit].copy()


def parse_map_drawings(map_data) -> List[Polygon]:
    polys: List[Polygon] = []
    if not map_data:
        return polys
    for feat in (map_data.get("all_drawings") or []):
        try:
            geom = feat.get("geometry")
            if not geom:
                continue
            coords = geom.get("coordinates", [[]])[0]
            poly = Polygon(coords)
            if poly.is_valid and not poly.is_empty:
                polys.append(poly)
        except Exception as e:
            logger.warning(f"Drawing parse failed: {e}")
    return polys


# ═══════════════════════════════════════════════════════════════════════════════
# TILE COLORING
# ═══════════════════════════════════════════════════════════════════════════════

def _sentinel_color(name: str) -> str:
    try:
        zone = max(1, min(60, int(str(name)[:2])))
        hue = 190 + int((zone - 1) * (40 / 60))
        return f"hsl({hue}, 70%, 52%)"
    except Exception:
        return "#2d7dd2"


def _landsat_color(name: str) -> str:
    try:
        path = max(1, min(233, int(str(name)[:3])))
        hue = 20 + int((path - 1) * (25 / 233))
        return f"hsl({hue}, 80%, 55%)"
    except Exception:
        return "#ffb703"


def tile_style_fn(colorize: bool, opacity: float, system: str):
    def _fn(feat):
        props = feat.get("properties", {}) or {}
        if system == "landsat":
            name = props.get("PR") or props.get("PATH_ROW") or props.get("name") or ""
            c = _landsat_color(name) if colorize else "#d97706"
        else:
            name = props.get("Name") or props.get("name") or ""
            c = _sentinel_color(name) if colorize else "#2d7dd2"
        return {"color": c, "weight": 1, "fillOpacity": opacity}
    return _fn


def selected_style(_feat):
    return {"color": "#ef476f", "weight": 3.5, "fillOpacity": 0.12, "dashArray": "6, 4"}


def intersect_style(_feat):
    return {"color": "#7b68ee", "weight": 2, "fillOpacity": 0.08}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING (cached)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading Sentinel-2 grid…")
def load_s2_data() -> Tuple[Optional[gpd.GeoDataFrame], Optional[gpd.GeoDataFrame]]:
    tiles, nocov = None, None
    for p in [PATHS.S2_GEOJSON, PATHS.S2_SHAPEFILE]:
        if Path(p).exists() and tiles is None:
            tiles = ensure_4326(gpd.read_file(p))
    if Path(PATHS.S2_NOCOV).exists():
        nocov = ensure_4326(gpd.read_file(PATHS.S2_NOCOV))
    return tiles, nocov


@st.cache_data(show_spinner="Loading Landsat WRS-2 grid…")
def load_landsat_data() -> Optional[gpd.GeoDataFrame]:
    for p in [PATHS.LANDSAT_GEOJSON, PATHS.LANDSAT_SHAPEFILE]:
        if Path(p).exists():
            return ensure_4326(gpd.read_file(p))
    return None


def load_all_tiles() -> Dict[str, Dict]:
    s2_tiles, s2_nocov = load_s2_data()
    landsat = load_landsat_data()
    return {
        "sentinel-2": {"tiles": s2_tiles, "nocov": s2_nocov},
        "landsat": {"tiles": landsat, "nocov": None},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAP BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_map(
    center: Tuple[float, float],
    zoom: int,
    aoi_geom: Optional[shapely.Geometry],
    tiles_vis: Optional[gpd.GeoDataFrame],
    nocov_vis: Optional[gpd.GeoDataFrame],
    intersects_gdf: Optional[gpd.GeoDataFrame],
    selected_gdf: Optional[gpd.GeoDataFrame],
    opts: Dict[str, Any],
    name_col: Optional[str],
    tile_system: str,
) -> folium.Map:
    m = folium.Map(
        location=list(center),
        zoom_start=zoom,
        tiles="CartoDB dark_matter",
        attr="CartoDB",
    )

    # Satellite layer
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri", name="Satellite", overlay=False, control=True,
    ).add_to(m)

    # OpenStreetMap
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", overlay=False, control=True).add_to(m)

    plugins.Fullscreen(position="topleft").add_to(m)
    plugins.MousePosition(position="bottomleft", prefix="<span style='color:#48cae4'>").add_to(m)

    # AOI layer
    if aoi_geom and not aoi_geom.is_empty:
        folium.GeoJson(
            mapping(aoi_geom), name="AOI",
            style_function=lambda _: {
                "color": "#ffb703", "weight": 2.5,
                "fillOpacity": 0.08, "dashArray": "4, 4",
            },
        ).add_to(m)

    # Grid tiles
    if opts.get("show_grid") and tiles_vis is not None and not getattr(tiles_vis, "empty", True):
        kw = {}
        if name_col and name_col in tiles_vis.columns:
            kw["tooltip"] = folium.GeoJsonTooltip(
                fields=[name_col], aliases=["Tile"], sticky=False,
                style="background:rgba(10,14,26,0.9);color:#48cae4;border:1px solid rgba(56,120,200,0.3);border-radius:6px;padding:6px 10px;font-family:JetBrains Mono,monospace;font-size:12px",
            )
            kw["popup"] = folium.GeoJsonPopup(fields=[name_col], aliases=["Tile"])
        label = "Landsat WRS-2" if tile_system == "landsat" else "Sentinel-2 MGRS"
        folium.GeoJson(
            tiles_vis, name=label,
            style_function=tile_style_fn(opts.get("colorize", True), opts.get("opacity", 0.03), tile_system),
            **kw,
        ).add_to(m)

    # No‑coverage
    if opts.get("show_nocov") and nocov_vis is not None and not getattr(nocov_vis, "empty", True):
        folium.GeoJson(
            nocov_vis, name="No Coverage",
            style_function=lambda _: {"color": "#ef476f", "weight": 1.5, "fillOpacity": 0.03},
        ).add_to(m)

    # Intersecting tiles
    if opts.get("show_intersects") and intersects_gdf is not None and not getattr(intersects_gdf, "empty", True):
        folium.GeoJson(intersects_gdf, name="Intersecting", style_function=intersect_style).add_to(m)

    # Selected tiles
    if opts.get("show_selected") and selected_gdf is not None and not getattr(selected_gdf, "empty", True):
        folium.GeoJson(selected_gdf, name="Selected", style_function=selected_style).add_to(m)

    # Draw controls
    plugins.Draw(
        export=False, position="topleft",
        draw_options={
            "polyline": False, "rectangle": True, "polygon": True,
            "circle": False, "marker": False, "circlemarker": False,
        },
        edit_options={"edit": True, "remove": True},
    ).add_to(m)

    folium.LayerControl(position="topright", collapsed=True).add_to(m)
    return m


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE LOG DISPLAY
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every="2000ms")
def show_live_logs(log_path: str = "nohup.out"):
    lp = Path(log_path)
    batch_re = re.compile(
        r"Concurrent Downloads:\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>\d+)/(?P<t>\d+)"
    )
    dl_re = re.compile(
        r"Downloading\s+(?P<fn>.+?):\s*(?P<pct>\d+)%\|.*?\|\s*"
        r"(?P<d>[\d.]+[kMGTP]?)/(?P<t>[\d.]+[kMGTP]?)\s*\[(?P<el>[0-9:]+)<(?P<eta>[0-9:?\-]+)\]"
    )

    bars, logs = {}, []
    if lp.exists():
        for line in lp.read_text().splitlines():
            line = line.strip()
            mb = batch_re.search(line)
            if mb:
                bars["batch"] = {"label": f"Concurrent ({mb.group('d')}/{mb.group('t')})", "pct": int(mb.group("pct"))}
                continue
            md = dl_re.search(line)
            if md:
                bars[md.group("fn")] = {
                    "label": f"{md.group('fn')} ({md.group('d')}/{md.group('t')}) ETA {md.group('eta')}",
                    "pct": int(md.group("pct")),
                }
                continue
            if line:
                logs.append(line)

    for pb in bars.values():
        st.caption(pb["label"])
        st.progress(pb["pct"])
    if logs:
        for l in logs[-5:]:
            st.text(l)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

def init_state():
    defaults = {
        "tile_system": "sentinel-2",
        "geometry_text": "",
        "intersecting_tiles": [],
        "selected_tiles": [],
        "start_date": dt.date.today() - dt.timedelta(days=7),
        "end_date": dt.date.today(),
        "map_center": MAP_CFG.DEFAULT_CENTER,
        "map_zoom": MAP_CFG.DEFAULT_ZOOM,
        "map_bounds": None,
        "last_click_popup": None,
        "last_aoi_wkt": "",
        "last_drawings_hash": None,
        "show_grid": True,
        "show_nocov": False,
        "show_intersects": True,
        "show_selected": True,
        "colorize_grid": True,
        "grid_opacity": MAP_CFG.GRID_OPACITY_DEFAULT,
        "max_grid_features": MAP_CFG.MAX_FEATURES_DEFAULT,
        "simplify_tol": MAP_CFG.SIMPLIFY_TOL_DEFAULT,
        "click_to_select": False,
        "grid_cache_params": None,
        "grid_auto_refresh": False,
        "grid_last_refresh_ts": 0.0,
    }
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

    for sys in ("sentinel-2", "landsat"):
        for suffix in ("bbox_key", "tiles", "nocov", "updated_ts"):
            val = 0.0 if suffix == "updated_ts" else None
            st.session_state.setdefault(f"gc_{sys}_{suffix}", val)


def update_grid_cache(tiles_gdf, nocov_gdf, name_col, bbox, system):
    pfx = f"gc_{system}"
    st.session_state[f"{pfx}_bbox_key"] = bbox_key(bbox)
    st.session_state[f"{pfx}_updated_ts"] = time.time()
    st.session_state[f"{pfx}_tiles"] = filter_gdf_in_bbox(
        tiles_gdf, bbox,
        max_features=int(st.session_state.get("max_grid_features", MAP_CFG.MAX_FEATURES_DEFAULT)),
        simplify_tol=float(st.session_state.get("simplify_tol", MAP_CFG.SIMPLIFY_TOL_DEFAULT)),
        keep_cols=[name_col],
    )
    if system == "sentinel-2" and nocov_gdf is not None and st.session_state.get("show_nocov"):
        st.session_state[f"{pfx}_nocov"] = filter_gdf_in_bbox(
            nocov_gdf, bbox, max_features=600,
            simplify_tol=float(st.session_state.get("simplify_tol", MAP_CFG.SIMPLIFY_TOL_DEFAULT)),
        )
    else:
        st.session_state[f"{pfx}_nocov"] = None


def sync_map_viewport(map_data, eps: float = 0.01):
    """Sync viewport with anti‑jitter tolerance."""
    if not map_data:
        return
    z = map_data.get("zoom")
    if z is not None:
        try:
            z = int(z)
            if abs(z - int(st.session_state.get("map_zoom", z))) >= 1:
                st.session_state["map_zoom"] = z
        except Exception:
            pass
    c = map_data.get("center")
    if c and "lat" in c and "lng" in c:
        try:
            lat, lng = float(c["lat"]), float(c["lng"])
            old = st.session_state.get("map_center", (lat, lng))
            if abs(lat - old[0]) > eps or abs(lng - old[1]) > eps:
                st.session_state["map_center"] = (round(lat, 4), round(lng, 4))
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# HTML HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def card_header(icon: str, title: str, subtitle: str, color: str = "blue"):
    st.markdown(f"""
        <div class="sat-header">
            <div class="sat-header-icon {color}">{icon}</div>
            <div class="sat-title">{title}</div>
        </div>
        <div class="sat-subtitle">{subtitle}</div>
    """, unsafe_allow_html=True)


def card_open():
    st.markdown('<div class="sat-card">', unsafe_allow_html=True)

def card_close():
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="Satellite Imagery Downloader",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    init_state()

    configuration = ConfigLoader(config_file_path="config.yaml")
    sat_tiles = load_all_tiles()
    today = dt.date.today()

    # ── Header ──
    st.markdown("""
        <div style="display:flex;align-items:center;gap:16px;margin-bottom:6px">
            <div style="
                width:48px;height:48px;border-radius:12px;
                background:linear-gradient(135deg,#2d7dd2 0%,#00b4d8 50%,#48cae4 100%);
                display:flex;align-items:center;justify-content:center;
                font-size:1.5rem;box-shadow:0 4px 20px rgba(45,125,210,0.35);
            ">🛰️</div>
            <div>
                <div style="font-size:1.4rem;font-weight:700;letter-spacing:-0.03em;color:#e8ecf4">
                    Satellite Imagery Downloader
                </div>
                <div style="font-size:0.82rem;color:#576380;margin-top:1px">
                    Sentinel-2 &nbsp;·&nbsp; Landsat &nbsp;·&nbsp; DEM &nbsp;·&nbsp; Grid Explorer &nbsp;·&nbsp; CLI Download
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # ── Current tile system ──
    sys_key = st.session_state.get("tile_system", "sentinel-2")
    tiles_data = sat_tiles.get(sys_key, {})
    tiles_gdf = tiles_data.get("tiles")
    nocov_gdf = tiles_data.get("nocov")
    name_col = get_name_column(tiles_gdf, sys_key) if tiles_gdf is not None else None

    # ── Tabs ──
    tab_config, tab_results, tab_settings = st.tabs(["⚙  Configuration", "📂  Results", "🔧  Settings"])

    # ══════════════════════════════════════════════════════════════
    # TAB: Configuration
    # ══════════════════════════════════════════════════════════════
    with tab_config:

        # ── Provider & Satellite card ──
        card_open()
        card_header("📡", "Data Source", "Select your provider, satellite mission, and product type.", "blue")

        c1, c2, c3 = st.columns(3)
        with c1:
            provider = st.selectbox("Provider", list(PROVIDERS.keys()), label_visibility="collapsed",
                                     help="Data provider")
        with c2:
            satellite = st.selectbox("Mission", PROVIDERS.get(provider, []), label_visibility="collapsed",
                                      help="Satellite mission")
        with c3:
            product_type = st.selectbox("Product", PRODUCT_TYPES.get(satellite, []), label_visibility="collapsed",
                                         help="Product type")
        card_close()

        # ── AOI + Grid Explorer ──
        card_open()
        card_header("🗺️", "Area of Interest & Grid Explorer",
                     "Define your AOI and explore the satellite tile grid interactively.", "teal")

        map_col, ctrl_col = st.columns([2.2, 1.0], gap="large")

        refresh_grid = False

        with ctrl_col:
            # Tile system selector
            with st.expander("🛰️ **Tile System**", expanded=True):
                sys_options = []
                sys_labels = {}
                if sat_tiles.get("sentinel-2", {}).get("tiles") is not None:
                    sys_options.append("sentinel-2")
                    sys_labels["sentinel-2"] = "Sentinel-2 (MGRS)"
                if sat_tiles.get("landsat", {}).get("tiles") is not None:
                    sys_options.append("landsat")
                    sys_labels["landsat"] = "Landsat (WRS-2)"

                if sys_options:
                    new_sys = st.radio(
                        "Grid", sys_options,
                        format_func=lambda x: sys_labels.get(x, x),
                        index=sys_options.index(sys_key) if sys_key in sys_options else 0,
                        label_visibility="collapsed",
                        horizontal=True,
                    )
                    if new_sys != sys_key:
                        st.session_state["tile_system"] = new_sys
                        st.session_state["selected_tiles"] = []
                        st.session_state["intersecting_tiles"] = []
                        st.rerun()

                # Color legend
                st.markdown("""
                <div class="color-legend-pro">
                    <div class="legend-title">Layer Colors</div>
                    <div class="legend-item">
                        <div class="legend-swatch" style="background:linear-gradient(90deg,hsl(190,70%,52%),hsl(230,70%,52%))"></div>
                        Sentinel-2 tiles
                    </div>
                    <div class="legend-item">
                        <div class="legend-swatch" style="background:linear-gradient(90deg,hsl(20,80%,55%),hsl(45,80%,55%))"></div>
                        Landsat tiles
                    </div>
                    <div class="legend-item">
                        <div class="legend-swatch" style="background:#7b68ee"></div>
                        AOI intersections
                    </div>
                    <div class="legend-item">
                        <div class="legend-swatch" style="background:#ef476f"></div>
                        Selected tiles
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # AOI input
            with st.expander("📐 **AOI Input**", expanded=True):
                aoi_mode = st.radio(
                    "Method", ["Draw on map", "Preset square", "Paste WKT / GeoJSON"],
                    horizontal=False, label_visibility="collapsed",
                )
                if aoi_mode == "Preset square":
                    pc1, pc2 = st.columns(2)
                    with pc1:
                        sq_lat = st.number_input("Lat", value=float(st.session_state["map_center"][0]), format="%.6f")
                    with pc2:
                        sq_lng = st.number_input("Lng", value=float(st.session_state["map_center"][1]), format="%.6f")
                    sq_side = st.number_input("Side (km)", min_value=0.1, value=25.0, step=5.0)
                    if st.button("Apply square AOI", use_container_width=True):
                        st.session_state["geometry_text"] = make_square_wkt(sq_lat, sq_lng, sq_side)
                        st.session_state["map_center"] = (sq_lat, sq_lng)
                        st.rerun()

                if aoi_mode == "Paste WKT / GeoJSON":
                    st.session_state["geometry_text"] = st.text_area(
                        "WKT or GeoJSON", value=st.session_state.get("geometry_text", ""), height=120,
                        label_visibility="collapsed", placeholder="Paste WKT or GeoJSON…",
                    )

            # Grid options
            with st.expander("🔲 **Grid Options**", expanded=False):
                if tiles_gdf is None or name_col is None:
                    st.error(f"No grid data for **{sys_key}**.")
                else:
                    st.session_state["show_grid"] = st.checkbox("Show grid", value=st.session_state["show_grid"])
                    st.session_state["colorize_grid"] = st.checkbox("Colorize", value=st.session_state["colorize_grid"])
                    if sys_key == "sentinel-2":
                        st.session_state["show_nocov"] = st.checkbox("No-coverage zones", value=st.session_state["show_nocov"], disabled=(nocov_gdf is None))
                    st.session_state["show_intersects"] = st.checkbox("Intersecting tiles", value=st.session_state["show_intersects"])
                    st.session_state["show_selected"] = st.checkbox("Selected tiles", value=st.session_state["show_selected"])
                    st.session_state["click_to_select"] = st.checkbox("Click to select", value=st.session_state["click_to_select"])

                    st.session_state["max_grid_features"] = int(st.number_input(
                        "Max features", 200, 8000, int(st.session_state["max_grid_features"]), step=200))
                    st.session_state["grid_opacity"] = float(st.slider(
                        "Fill opacity", 0.0, 0.2, float(st.session_state["grid_opacity"]), step=0.01))
                    st.session_state["simplify_tol"] = float(st.slider(
                        "Simplify (°)", 0.0, 0.02, float(st.session_state["simplify_tol"]), step=0.001))

                    # Invalidate cache on param change
                    cur_params = (int(st.session_state["max_grid_features"]), float(st.session_state["simplify_tol"]), sys_key)
                    if st.session_state.get("grid_cache_params") != cur_params:
                        st.session_state["grid_cache_params"] = cur_params
                        for s in ("sentinel-2", "landsat"):
                            for suf in ("tiles", "nocov", "bbox_key"):
                                st.session_state[f"gc_{s}_{suf}"] = None

                    refresh_grid = st.button("🔄 Update Grid Overlay", use_container_width=True)
                    st.session_state["grid_auto_refresh"] = st.checkbox(
                        "Auto-refresh (throttled)", value=st.session_state["grid_auto_refresh"])

                    pfx = f"gc_{sys_key}"
                    if st.session_state.get(f"{pfx}_bbox_key") is None:
                        st.caption("Cache empty — click **Update Grid Overlay**")
                    else:
                        age = time.time() - float(st.session_state.get(f"{pfx}_updated_ts", 0))
                        st.caption(f"Cache age: {age:.0f}s")

            # Tile search
            with st.expander("🔍 **Tile Search & Selection**", expanded=True):
                if tiles_gdf is not None and name_col:
                    query = st.text_input(
                        "Search", placeholder="e.g. 34UED or 233062",
                        label_visibility="collapsed", key=f"tsearch_{sys_key}")

                    if query:
                        matches = find_tiles_by_query(tiles_gdf, name_col, query, 50)
                        match_ids = matches[name_col].astype(str).tolist() if not matches.empty else []
                        if not match_ids:
                            st.caption("No matches found.")
                        else:
                            picked = st.selectbox("Results", match_ids, index=0, key=f"tmatch_{sys_key}",
                                                   label_visibility="collapsed")
                            bc1, bc2, bc3 = st.columns(3)
                            with bc1:
                                if st.button("➕ Add", use_container_width=True, key=f"tadd_{sys_key}"):
                                    sel = set(map(str, st.session_state["selected_tiles"]))
                                    sel.add(picked)
                                    st.session_state["selected_tiles"] = sorted(sel)
                                    st.rerun()
                            with bc2:
                                if st.button("🔄 Replace", use_container_width=True, key=f"trep_{sys_key}"):
                                    st.session_state["selected_tiles"] = [picked]
                                    st.rerun()
                            with bc3:
                                if st.button("🎯 Zoom", use_container_width=True, key=f"tzoom_{sys_key}"):
                                    row = tiles_gdf[tiles_gdf[name_col].astype(str) == str(picked)]
                                    if not row.empty:
                                        c = row.iloc[0].geometry.centroid
                                        st.session_state["map_center"] = (float(c.y), float(c.x))
                                        st.session_state["map_zoom"] = max(int(st.session_state["map_zoom"]), 10)
                                        st.rerun()

                    current_sel = st.multiselect(
                        "Selected", tiles_gdf[name_col].astype(str).unique().tolist(),
                        default=st.session_state["selected_tiles"], key=f"msel_{sys_key}",
                        label_visibility="collapsed",
                    )
                    st.session_state["selected_tiles"] = current_sel
                    if st.button("✕ Clear selection", use_container_width=True, key=f"tclear_{sys_key}"):
                        st.session_state["selected_tiles"] = []
                        st.rerun()

            # AOI preview
            with st.expander("📋 **AOI Preview**", expanded=False):
                st.text_area(
                    "AOI", value=st.session_state.get("geometry_text", "") or "No AOI defined.",
                    height=100, key="aoi_ro", label_visibility="collapsed", disabled=True,
                )

        # ── Parse AOI ──
        aoi_geom = parse_text_geometry(st.session_state.get("geometry_text", ""))
        aoi_polys: List[Polygon] = []
        if aoi_geom and not aoi_geom.is_empty:
            if aoi_geom.geom_type == "Polygon":
                aoi_polys = [aoi_geom]
            elif aoi_geom.geom_type == "MultiPolygon":
                aoi_polys = list(aoi_geom.geoms)

        tile_names, intersects_gdf = compute_intersections(aoi_polys, tiles_gdf, name_col)
        st.session_state["intersecting_tiles"] = tile_names or []

        selected_gdf = None
        if tiles_gdf is not None and name_col and st.session_state["selected_tiles"]:
            selset = set(map(str, st.session_state["selected_tiles"]))
            selected_gdf = tiles_gdf[tiles_gdf[name_col].astype(str).isin(selset)][[name_col, "geometry"]].copy()
            if selected_gdf.empty:
                selected_gdf = None

        # Manual refresh
        if refresh_grid and tiles_gdf is not None and name_col and st.session_state.get("show_grid"):
            bb = bounds_from_leaflet(st.session_state.get("map_bounds"))
            if bb is None:
                clat, clng = st.session_state["map_center"]
                bb = fallback_bbox(clat, clng, int(st.session_state["map_zoom"]))
            update_grid_cache(tiles_gdf, nocov_gdf, name_col, bb, sys_key)

        pfx = f"gc_{sys_key}"
        tiles_visible = st.session_state.get(f"{pfx}_tiles")
        nocov_visible = st.session_state.get(f"{pfx}_nocov")

        center = st.session_state["map_center"]
        zoom = int(st.session_state["map_zoom"])

        show_grid_eff = (
            st.session_state.get("show_grid", False)
            and zoom >= MAP_CFG.MIN_GRID_ZOOM
            and tiles_visible is not None
            and not getattr(tiles_visible, "empty", True)
        )
        show_nocov_eff = (
            sys_key == "sentinel-2"
            and st.session_state.get("show_nocov", False)
            and nocov_visible is not None
            and not getattr(nocov_visible, "empty", True)
        )

        # ── Map ──
        with map_col:
            if st.session_state.get("show_grid") and zoom < MAP_CFG.MIN_GRID_ZOOM:
                st.markdown(f'<div class="info-banner">Zoom in to level {MAP_CFG.MIN_GRID_ZOOM}+ to render the grid (current: {zoom})</div>', unsafe_allow_html=True)

            if st.session_state.get("show_grid") and st.session_state.get(f"{pfx}_bbox_key") is None:
                st.markdown('<div class="info-banner">Grid cache empty — click <b>Update Grid Overlay</b> in the sidebar</div>', unsafe_allow_html=True)

            opts = {
                "show_grid": show_grid_eff,
                "show_nocov": show_nocov_eff,
                "show_intersects": st.session_state["show_intersects"],
                "show_selected": st.session_state["show_selected"],
                "colorize": st.session_state["colorize_grid"],
                "opacity": st.session_state["grid_opacity"],
            }

            m = build_map(
                center=center, zoom=zoom, aoi_geom=aoi_geom,
                tiles_vis=tiles_visible if show_grid_eff else None,
                nocov_vis=nocov_visible if show_nocov_eff else None,
                intersects_gdf=intersects_gdf, selected_gdf=selected_gdf,
                opts=opts, name_col=name_col, tile_system=sys_key,
            )

            returned = ["all_drawings", "bounds", "zoom", "center"]
            if st.session_state["click_to_select"]:
                returned.append("last_object_clicked_popup")

            st.markdown('<div class="map-wrapper">', unsafe_allow_html=True)
            map_data = st_folium(m, key="main_map", width="100%", height=MAP_CFG.MAP_HEIGHT, returned_objects=returned)
            st.markdown('</div>', unsafe_allow_html=True)

            # Sync viewport
            if map_data and map_data.get("bounds"):
                b = bounds_from_leaflet(map_data["bounds"])
                if b is not None:
                    st.session_state["map_bounds"] = map_data["bounds"]
            sync_map_viewport(map_data)

            # Auto‑refresh grid
            if (tiles_gdf is not None and name_col and st.session_state.get("show_grid")
                    and st.session_state.get("grid_auto_refresh")):
                bbox_now = bounds_from_leaflet(map_data.get("bounds") if map_data else None)
                if bbox_now is not None:
                    k_now = bbox_key(bbox_now)
                    k_cached = st.session_state.get(f"{pfx}_bbox_key")
                    now = time.time()
                    if (k_now != k_cached and
                            (now - float(st.session_state.get("grid_last_refresh_ts", 0))) >= MAP_CFG.AUTO_REFRESH_THROTTLE_SEC):
                        st.session_state["grid_last_refresh_ts"] = now
                        update_grid_cache(tiles_gdf, nocov_gdf, name_col, bbox_now, sys_key)
                        st.rerun()

            # Draw‑on‑map AOI
            if aoi_mode == "Draw on map":
                drawn = parse_map_drawings(map_data)
                if drawn:
                    union = safe_union(drawn)
                    new_wkt = shapely_wkt.dumps(union, rounding_precision=6)
                    drawings = map_data.get("all_drawings", []) if map_data else []
                    dhash = hash(json.dumps(drawings, sort_keys=True))
                    if (new_wkt and dhash != st.session_state.get("last_drawings_hash")
                            and new_wkt != st.session_state.get("last_aoi_wkt", "")):
                        st.session_state.update({"last_aoi_wkt": new_wkt, "last_drawings_hash": dhash, "geometry_text": new_wkt})
                        st.rerun()

            # Click‑to‑select
            if st.session_state["click_to_select"] and map_data:
                popup = map_data.get("last_object_clicked_popup")
                if popup and popup != st.session_state.get("last_click_popup"):
                    st.session_state["last_click_popup"] = popup
                    pat = r"\b\d{2}[A-Z]{3}\b" if sys_key == "sentinel-2" else r"\b\d{6}\b"
                    m_id = re.search(pat, str(popup).upper() if sys_key == "sentinel-2" else str(popup))
                    if m_id:
                        tid = m_id.group(0)
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        sel.symmetric_difference_update({tid})
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()

            # ── Stats row ──
            n_inter = len(st.session_state["intersecting_tiles"])
            n_sel = len(st.session_state["selected_tiles"])
            st.markdown(f"""
            <div class="stat-row">
                <div class="stat-item">
                    <div class="stat-value">{n_inter}</div>
                    <div class="stat-label">Intersecting</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{n_sel}</div>
                    <div class="stat-label">Selected</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{sys_key.split('-')[0].upper()}</div>
                    <div class="stat-label">Grid System</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Tile chips
            if tiles_gdf is not None and name_col:
                if st.session_state["intersecting_tiles"]:
                    with st.expander(f"Intersecting tiles ({n_inter})", expanded=False):
                        chips = "".join(f'<span class="tile-chip">{t}</span>' for t in st.session_state["intersecting_tiles"][:40])
                        st.markdown(chips, unsafe_allow_html=True)
                        if n_inter > 40:
                            st.caption(f"… and {n_inter - 40} more")
                        csv = "tile\n" + "\n".join(st.session_state["intersecting_tiles"])
                        st.download_button("Download CSV", data=csv,
                                            file_name=f"{sys_key}_intersects.csv", mime="text/csv")

                if st.session_state["selected_tiles"]:
                    with st.expander(f"Selected tiles ({n_sel})", expanded=False):
                        chips = "".join(f'<span class="tile-chip">{t}</span>' for t in st.session_state["selected_tiles"])
                        st.markdown(chips, unsafe_allow_html=True)
                        csv2 = "tile\n" + "\n".join(map(str, st.session_state["selected_tiles"]))
                        st.download_button("Download CSV", data=csv2,
                                            file_name=f"{sys_key}_selected.csv", mime="text/csv")

        card_close()

        # ── Time Range ──
        card_open()
        card_header("📅", "Time Range", "Define the temporal window for your data search.", "purple")
        d1, d2 = st.columns(2)
        with d1:
            start_date = st.date_input("Start", value=st.session_state["start_date"], max_value=today, key="sd")
        with d2:
            end_date = st.date_input("End", value=st.session_state["end_date"], min_value=start_date, max_value=today, key="ed")
        if end_date < start_date:
            end_date = start_date
            st.warning("End date adjusted to match start date.")
        st.session_state["start_date"] = start_date
        st.session_state["end_date"] = end_date
        card_close()

        # ── Download ──
        card_open()
        card_header("⬇️", "Download", "Execute the CLI download command. Logs stream from nohup.out.", "green")

        n_products = len(st.session_state.get("selected_tiles", []))
        if n_products > 0:
            st.markdown(f'<div class="product-badge">📦  {n_products} product{"s" if n_products != 1 else ""} queued for download</div>', unsafe_allow_html=True)

        aoi_text = st.session_state.get("geometry_text", "").strip()
        aoi_file = "example_aoi.geojson" if aoi_text.startswith("{") else "example_aoi.wkt"
        cli_cmd = (
            f"python cli.py --provider {provider.lower()} "
            f"--collection {satellite.split(' ')[0]} --product-type {product_type} "
            f"--start-date {start_date} --end-date {end_date} --aoi_file {aoi_file}"
        )
        st.code(cli_cmd, language="bash")

        if st.button("🚀 Start Download", use_container_width=True, type="primary"):
            if not aoi_text:
                st.error("Please define an AOI first.")
            elif not start_date or not end_date:
                st.error("Please set both start and end dates.")
            else:
                Path(aoi_file).write_text(aoi_text)
                Path("nohup.out").write_text("")
                os.system(f"nohup {cli_cmd} &")
                show_live_logs()

        card_close()

    # ══════════════════════════════════════════════════════════════
    # TAB: Results
    # ══════════════════════════════════════════════════════════════
    with tab_results:
        card_open()
        card_header("📂", "Downloaded Products", "Browse, manage and download your satellite data files.", "amber")

        def sort_by_size(files):
            return sorted(files, key=lambda x: x["size"])

        _ = st_file_browser(
            os.path.join("downloads"),
            file_ignores=None, key="file_browser",
            show_choose_file=True, show_choose_folder=True,
            show_delete_file=True, show_download_file=True,
            show_new_folder=True, show_upload_file=True,
            show_rename_file=True, show_rename_folder=True,
            use_cache=True, sort=sort_by_size,
        )
        card_close()

    # ══════════════════════════════════════════════════════════════
    # TAB: Settings
    # ══════════════════════════════════════════════════════════════
    with tab_settings:
        card_open()
        card_header("🔧", "Configuration", "View and manage your config.yaml settings.", "purple")
        try:
            config_content = Path("config.yaml").read_text()
            st.code(config_content, language="yaml")
        except FileNotFoundError:
            st.warning("config.yaml not found.")
        card_close()


if __name__ == "__main__":
    main()