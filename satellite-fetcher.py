"""
Satellite Imagery Downloader — Professional Edition

This script provides an interactive Streamlit application that allows users to
explore Sentinel‑2 and Landsat tile grids, define an area of interest (AOI)
either by drawing on the map, pasting WKT/GeoJSON, or specifying a preset
square, and launch downloads of satellite products via a command‑line
interface (CLI). The app features a modern, dark user interface inspired by
Copernicus Data Space and includes performance optimisations such as batched
GeoJSON rendering and optional auto‑refresh of the grid overlay.  The
configuration loader is patched to accept an optional default parameter to
avoid exceptions when missing keys are requested.
"""

import os
import re
import math
import json
import time
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass

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

try:
    # Attempt to import the configuration loader.  If it is present
    # monkey‑patch its get_var method to accept an optional default value.
    from utilities import ConfigLoader  # type: ignore
    _original_get_var = ConfigLoader.get_var
    def _patched_get_var(self, key, default=None):  # type: ignore
        """Return configuration value or default when missing.

        The original ConfigLoader.get_var() did not accept a default
        argument and raised errors for missing keys.  This wrapper adds
        support for a second parameter and gracefully returns the default
        when any exception is encountered.
        """
        try:
            val = _original_get_var(self, key)
            return val if val is not None else default
        except Exception:
            return default
    ConfigLoader.get_var = _patched_get_var  # type: ignore
except Exception:
    # ConfigLoader is optional; if missing there is nothing to patch.
    ConfigLoader = None  # type: ignore


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
    AUTO_REFRESH_THROTTLE: float = 2.0  # seconds between auto refreshes
    DEFAULT_CENTER: Tuple[float, float] = (48.8566, 2.3522)
    DEFAULT_ZOOM: int = 8
    MAP_HEIGHT: int = 700
    MAX_FEATURES: int = 1200
    SIMPLIFY_TOL: float = 0.001
    GRID_OPACITY: float = 0.04


PATHS = TilePaths()
MCFG = MapConfig()

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
# STYLING
# ═══════════════════════════════════════════════════════════════════════════════

# A minimal dark theme loosely inspired by Copernicus Data Space.  The colours
# are defined via CSS variables to allow easy tweaking.  The palette is
# designed to be colour‑blind friendly (using the Wong palette) and avoids
# strong contrast that might distract from the map and controls.

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background: #060a14 !important;
    color: #e2e8f0 !important;
    font-family: 'DM Sans', system-ui, sans-serif !important;
}
/* Sidebar styling */
[data-testid="stSidebar"] {
    background: #0b1120 !important;
    border-right: 1px solid rgba(56,120,200,0.10) !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span {
    color: #e2e8f0 !important;
}
/* Buttons */
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
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 0 20px rgba(56,189,248,0.3) !important;
}
/* Tabs */
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
    min-width: 0 !important;
    padding: 8px 12px !important;
    font-size: 0.9rem !important;
}
.stTabs [aria-selected="true"] {
    background: rgba(56,189,248,0.14) !important;
    color: #38bdf8 !important;
    box-shadow: 0 0 8px rgba(56,189,248,0.08) !important;
}
.stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {
    background: rgba(56,189,248,0.06) !important;
    color: #e2e8f0 !important;
}
/* Expander */
[data-testid="stExpander"] {
    background: #111827 !important;
    border: 1px solid rgba(56,120,200,0.10) !important;
    border-radius: 10px !important;
}
/* Code blocks */
pre, code {
    background: #0b1120 !important;
    color: #e2e8f0 !important;
    border-radius: 8px !important;
}
/* Scrollbars */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: #060a14; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.18); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(56,189,248,0.3); }
</style>
"""


# ═══════════════════════════════════════════════════════════════════════════════
# GEO UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_4326(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    """Reproject a GeoDataFrame to WGS‑84 (EPSG:4326) if needed."""
    if gdf is None or gdf.empty:
        return gdf
    return gdf.set_crs(epsg=4326) if gdf.crs is None else gdf.to_crs(epsg=4326)


def get_name_col(gdf: Optional[gpd.GeoDataFrame], system: str) -> Optional[str]:
    """Return the column name that holds the tile identifier for a grid system."""
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


def safe_union(geoms: List[shapely.geometry.base.BaseGeometry]):
    """Compute the union of multiple geometries in a Shapely 1.x/2.x compatible way."""
    try:
        return shapely.union_all(geoms)
    except Exception:
        return unary_union(geoms)


def bounds_from_leaflet(bounds: Any) -> Optional[Tuple[float, float, float, float]]:
    """Extract a bounding box from Leaflet bounds structure.

    Parameters
    ----------
    bounds: dict or None
        The bounds returned by st_folium.  It should contain either
        `_southWest`/`_northEast` keys or `southWest`/`northEast` keys.

    Returns
    -------
    tuple or None
        (minx, miny, maxx, maxy) if valid, otherwise None.
    """
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
        # normalise if reversed
        if maxx < minx:
            minx, maxx = maxx, minx
        if maxy < miny:
            miny, maxy = maxy, miny
        return (minx, miny, maxx, maxy)
    return None


def fallback_bbox(lat: float, lng: float, zoom: int) -> Tuple[float, float, float, float]:
    """Generate a fallback bounding box around a point when map bounds are unknown."""
    w = 360.0 / (2 ** max(0, zoom))
    h = 180.0 / (2 ** max(0, zoom))
    return (lng - w * 0.6, lat - h * 0.6, lng + w * 0.6, lat + h * 0.6)


def bbox_key(bbox: Optional[Tuple], nd: int = 4) -> Optional[Tuple]:
    """Round a bounding box to a fixed precision so that small jitters are ignored."""
    if bbox is None:
        return None
    try:
        return tuple(round(float(v), nd) for v in bbox)
    except Exception:
        return None


def filter_gdf_bbox(
    gdf: Optional[gpd.GeoDataFrame],
    bbox: Optional[Tuple[float, float, float, float]],
    max_features: int,
    simplify_tol: float,
    keep_cols: Optional[List[str]] = None,
) -> Optional[gpd.GeoDataFrame]:
    """Clip a GeoDataFrame to a bounding box, simplify shapes and limit features."""
    if gdf is None or gdf.empty or bbox is None:
        return None
    bb = box(*bbox)
    try:
        sub = gdf[gdf.intersects(bb)].copy()
    except Exception:
        sub = gdf.copy()
    if sub.empty:
        return sub
    cols = ([c for c in (keep_cols or []) if c in sub.columns]) + ["geometry"]
    cols = list(dict.fromkeys(cols))
    sub = sub[cols].copy() if keep_cols else sub[["geometry"]].copy()
    if simplify_tol > 0:
        try:
            sub["geometry"] = sub.geometry.simplify(simplify_tol, preserve_topology=True)
        except Exception:
            pass
    return sub.iloc[:max_features].copy() if len(sub) > max_features else sub


def parse_geometry(text: str) -> Optional[shapely.geometry.base.BaseGeometry]:
    """Parse a WKT or GeoJSON string into a Shapely geometry."""
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


def make_square_wkt(lat: float, lng: float, km: float) -> str:
    """Return a WKT square centred on (lat, lng) with a side length in km."""
    half = km / 2.0
    dlat = half / 111.0
    dlon = half / (111.0 * max(0.05, abs(math.cos(math.radians(lat)))))
    p = Polygon([
        (lng - dlon, lat - dlat), (lng + dlon, lat - dlat),
        (lng + dlon, lat + dlat), (lng - dlon, lat + dlat),
        (lng - dlon, lat - dlat),
    ])
    return shapely_wkt.dumps(p, rounding_precision=6)


def compute_intersections(
    polys: List[Polygon],
    gdf: Optional[gpd.GeoDataFrame],
    ncol: Optional[str],
) -> Tuple[List[str], Optional[gpd.GeoDataFrame]]:
    """Find tiles intersecting a set of polygons and return their names and subset."""
    if gdf is None or gdf.empty or not polys or not ncol:
        return [], None
    try:
        au = safe_union(polys)
        c = gdf[gdf.intersects(au)].copy()
        if c.empty:
            return [], c
        c = c[[ncol, "geometry"]].copy()
        return sorted(c[ncol].astype(str).unique().tolist()), c
    except Exception as e:
        logger.error(f"Intersection: {e}")
        return [], None


def find_tiles(gdf: gpd.GeoDataFrame, col: str, query: str, limit: int = 50) -> gpd.GeoDataFrame:
    """Search for tiles whose names match a query string (case insensitive)."""
    q = (query or "").strip()
    if not q:
        return gdf.iloc[0:0]
    s = gdf[col].astype(str)
    exact = gdf[s.str.upper() == q.upper()]
    if not exact.empty:
        return exact[[col, "geometry"]].copy()
    return gdf[s.str.contains(q, case=False, na=False)][[col, "geometry"]].iloc[:limit].copy()


def parse_drawings(md: Dict[str, Any]) -> List[Polygon]:
    """Extract drawn polygons from the map draw tool metadata."""
    polys: List[Polygon] = []
    if not md:
        return polys
    for feat in (md.get("all_drawings") or []):
        try:
            g = feat.get("geometry")
            if not g:
                continue
            coords = g.get("coordinates", [[]])[0]
            if len(coords) < 3:
                continue
            p = Polygon(coords)
            if p.is_valid and not p.is_empty:
                polys.append(p)
        except Exception:
            pass
    return polys


# ═══════════════════════════════════════════════════════════════════════════════
# TILE COLOURING
# ═══════════════════════════════════════════════════════════════════════════════

def _s2_color(n: str) -> str:
    """Compute a Sentinel‑2 tile colour from its zone number to ensure variety."""
    try:
        z = max(1, min(60, int(str(n)[:2])))
        hue = 195 + int((z - 1) * 30 / 60)
        return f"hsl({hue},75%,48%)"
    except Exception:
        return "#0077BB"


def _ls_color(n: str) -> str:
    """Compute a Landsat tile colour from its path number for variety."""
    try:
        p = max(1, min(233, int(str(n)[:3])))
        hue = 18 + int((p - 1) * 28 / 233)
        return f"hsl({hue},85%,52%)"
    except Exception:
        return "#EE7733"


def grid_style(colorize: bool, opacity: float, sys: str):
    """Return a Folium style function for grid cells."""
    def fn(f):
        pr = f.get("properties", {}) or {}
        if sys == "landsat":
            n = pr.get("PR") or pr.get("PATH_ROW") or pr.get("name") or ""
            c = _ls_color(n) if colorize else "#EE7733"
        else:
            n = pr.get("Name") or pr.get("name") or ""
            c = _s2_color(n) if colorize else "#0077BB"
        return {"color": c, "weight": 1.2, "fillOpacity": opacity}
    return fn


def sel_style(_: Dict[str, Any]) -> Dict[str, Any]:
    """Style for selected tiles."""
    return {"color": "#EE3377", "weight": 3, "fillOpacity": 0.12, "dashArray": "6,4"}


def int_style(_: Dict[str, Any]) -> Dict[str, Any]:
    """Style for intersecting tiles."""
    return {"color": "#AA3377", "weight": 2.2, "fillOpacity": 0.09}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING (cached)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading Sentinel-2 grid…")
def load_s2() -> Tuple[Optional[gpd.GeoDataFrame], Optional[gpd.GeoDataFrame]]:
    """Load Sentinel‑2 tiles and no‑coverage zones from GeoJSON or shapefile."""
    tiles, nocov = None, None
    for p in [PATHS.S2_GEOJSON, PATHS.S2_SHAPEFILE]:
        if Path(p).exists() and tiles is None:
            tiles = ensure_4326(gpd.read_file(p))
    if Path(PATHS.S2_NOCOV).exists():
        nocov = ensure_4326(gpd.read_file(PATHS.S2_NOCOV))
    return tiles, nocov


@st.cache_data(show_spinner="Loading Landsat WRS-2 grid…")
def load_landsat() -> Optional[gpd.GeoDataFrame]:
    """Load Landsat WRS-2 tiles from GeoJSON or shapefile."""
    for p in [PATHS.LANDSAT_GEOJSON, PATHS.LANDSAT_SHAPEFILE]:
        if Path(p).exists():
            return ensure_4326(gpd.read_file(p))
    return None


def load_tiles() -> Dict[str, Dict[str, Any]]:
    """Load both Sentinel‑2 and Landsat tiles into a dictionary."""
    s2, s2n = load_s2()
    ls = load_landsat()
    return {"sentinel-2": {"tiles": s2, "nocov": s2n}, "landsat": {"tiles": ls, "nocov": None}}


# ═══════════════════════════════════════════════════════════════════════════════
# MAP BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_map(
    center: Tuple[float, float],
    zoom: int,
    aoi_geom: Optional[shapely.geometry.base.BaseGeometry],
    tiles_vis: Optional[gpd.GeoDataFrame],
    nocov_vis: Optional[gpd.GeoDataFrame],
    inter_gdf: Optional[gpd.GeoDataFrame],
    sel_gdf: Optional[gpd.GeoDataFrame],
    opts: Dict[str, Any],
    ncol: Optional[str],
    tilesys: str,
) -> folium.Map:
    """Build a Folium map with base layers and optional overlays for tiles, AOI, etc."""
    m = folium.Map(
        location=list(center),
        zoom_start=zoom,
        tiles=None,
        control_scale=True,
        prefer_canvas=True,
    )
    # Satellite base layer
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri", name="Satellite", overlay=False, control=True,
        max_native_zoom=19, max_zoom=22,
    ).add_to(m)
    # Dark base layer
    folium.TileLayer(
        "CartoDB dark_matter", name="Dark", overlay=False, control=True,
    ).add_to(m)
    # Streets base layer
    folium.TileLayer(
        "OpenStreetMap", name="Streets", overlay=False, control=True,
    ).add_to(m)
    # Fullscreen control
    plugins.Fullscreen(position="topleft").add_to(m)

    # AOI polygon (yellow‑green)
    if aoi_geom and not aoi_geom.is_empty:
        folium.GeoJson(
            mapping(aoi_geom), name="AOI",
            style_function=lambda _: {
                "color": "#CCBB44", "weight": 2.5, "fillOpacity": 0.10, "dashArray": "5,5",
            },
        ).add_to(m)

    # Grid layer
    if opts.get("show_grid") and tiles_vis is not None and not getattr(tiles_vis, "empty", True):
        kw: Dict[str, Any] = {}
        if ncol and ncol in tiles_vis.columns:
            kw["tooltip"] = folium.GeoJsonTooltip(
                fields=[ncol], aliases=["Tile"], sticky=False,
                style="background:rgba(6,10,20,.9);color:#48cae4;border:1px solid rgba(56,120,200,.25);border-radius:6px;padding:4px 8px;font-size:11px;font-family:'JetBrains Mono',monospace;",
            )
            kw["popup"] = folium.GeoJsonPopup(fields=[ncol], aliases=["Tile"])
        folium.GeoJson(
            tiles_vis,
            name="Landsat WRS-2" if tilesys == "landsat" else "Sentinel-2 MGRS",
            style_function=grid_style(opts.get("colorize", True), opts.get("opacity", 0.03), tilesys),
            **kw,
        ).add_to(m)

    # No coverage zones
    if opts.get("show_nocov") and nocov_vis is not None and not getattr(nocov_vis, "empty", True):
        folium.GeoJson(nocov_vis, name="No Coverage",
            style_function=lambda _: {"color": "#CC3311", "weight": 1.5, "fillOpacity": 0.04},
        ).add_to(m)

    # Intersecting tiles
    if opts.get("show_inter") and inter_gdf is not None and not getattr(inter_gdf, "empty", True):
        kw2: Dict[str, Any] = {}
        if ncol and ncol in inter_gdf.columns:
            kw2["tooltip"] = folium.GeoJsonTooltip(
                fields=[ncol], aliases=["Tile"], sticky=False,
                style="background:rgba(6,10,20,.9);color:#e48abf;border:1px solid rgba(56,120,200,.25);border-radius:6px;padding:4px 8px;font-size:11px;font-family:'JetBrains Mono',monospace;",
            )
        folium.GeoJson(inter_gdf, name="Intersecting", style_function=int_style, **kw2).add_to(m)

    # Selected tiles
    if opts.get("show_sel") and sel_gdf is not None and not getattr(sel_gdf, "empty", True):
        kw3: Dict[str, Any] = {}
        if ncol and ncol in sel_gdf.columns:
            kw3["tooltip"] = folium.GeoJsonTooltip(
                fields=[ncol], aliases=["Selected"], sticky=False,
                style="background:rgba(6,10,20,.9);color:#f88cb0;border:1px solid rgba(56,120,200,.25);border-radius:6px;padding:4px 8px;font-size:11px;font-family:'JetBrains Mono',monospace;",
            )
        folium.GeoJson(sel_gdf, name="Selected", style_function=sel_style, **kw3).add_to(m)

    # Draw controls
    plugins.Draw(
        export=False,
        position="topleft",
        draw_options={
            "polyline": False,
            "rectangle": {"shapeOptions": {"color": "#CCBB44", "weight": 2, "fillOpacity": 0.08}},
            "polygon": {"shapeOptions": {"color": "#CCBB44", "weight": 2, "fillOpacity": 0.08}},
            "circle": False,
            "marker": False,
            "circlemarker": False,
        },
        edit_options={"edit": True, "remove": True},
    ).add_to(m)

    folium.LayerControl(position="topright", collapsed=True).add_to(m)
    return m


# ═══════════════════════════════════════════════════════════════════════════════
# DOWNLOAD MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def reset_downloads(dl_dir: str = "downloads") -> None:
    """Clear all previous downloads and logs before a new run."""
    dl_path = Path(dl_dir)
    if dl_path.exists():
        # Use shutil.rmtree to remove entire directory tree
        import shutil
        shutil.rmtree(dl_path, ignore_errors=True)
    dl_path.mkdir(parents=True, exist_ok=True)
    log_path = Path("nohup.out")
    if log_path.exists():
        # Clear previous log output
        log_path.write_text("")
    # Remove session state keys related to downloads
    for key in list(st.session_state.keys()):
        if key.startswith("dl_"):
            del st.session_state[key]
    st.session_state["dl_start_time"] = None
    st.session_state["dl_total_products"] = 0
    st.session_state["dl_completed"] = 0
    st.session_state["dl_running"] = False


def count_downloaded_products(dl_dir: str = "downloads") -> Tuple[int, float]:
    """Count downloaded files and total size in MB in the specified directory."""
    dl_path = Path(dl_dir)
    if not dl_path.exists():
        return 0, 0.0
    real_files = [f for f in dl_path.rglob("*") if f.is_file()]
    total_size = sum(f.stat().st_size for f in real_files) / (1024 * 1024)
    return len(real_files), total_size


def parse_download_logs(path: str = "nohup.out") -> Dict[str, Any]:
    """Parse nohup.out for progress bars, file bars, product count and errors."""
    lp = Path(path)
    if not lp.exists():
        return {"batch": None, "files": {}, "logs": [], "products_found": 0, "errors": []}
    brx = re.compile(
        r"Concurrent Downloads:\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>\d+)/(?:\d+)")
    drx = re.compile(
        r"Downloading\s+(?P<fn>.+?):\s*(?P<pct>\d+)%\|.*?\|\s*(?P<d>[\d.]+\S*)/(?P<t>[\d.]+\S*)\s*\[(?:.+?)<(?P<eta>[0-9:?\-]+)\]"
    )
    prx = re.compile(r"Found\s+(?P<n>\d+)\s+products?", re.IGNORECASE)
    erx = re.compile(r"(error|exception|failed|traceback)", re.IGNORECASE)
    result: Dict[str, Any] = {"batch": None, "files": {}, "logs": [], "products_found": 0, "errors": []}
    try:
        text = lp.read_text(errors="replace")
    except Exception:
        return result
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        mb = brx.search(line)
        if mb:
            result["batch"] = {
                "done": int(mb.group("d")),
                "pct": int(mb.group("pct")),
            }
            continue
        md = drx.search(line)
        if md:
            result["files"][md.group("fn")] = {
                "pct": int(md.group("pct")),
                "done": md.group("d"),
                "total": md.group("t"),
                "eta": md.group("eta"),
            }
            continue
        mp = prx.search(line)
        if mp:
            result["products_found"] = max(result["products_found"], int(mp.group("n")))
            continue
        if erx.search(line):
            result["errors"].append(line)
            continue
        if line:
            result["logs"].append(line)
    return result


def _format_eta(seconds: float) -> str:
    """Format seconds into a human‑readable ETA string."""
    if seconds > 3600:
        return f"{seconds / 3600:.1f}h"
    elif seconds > 60:
        return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
    else:
        return f"{seconds:.0f}s"


def render_download_progress() -> None:
    """Render styled download progress indicators from the parsed logs."""
    logs = parse_download_logs()
    n_files, total_mb = count_downloaded_products()
    if logs.get("products_found", 0) > 0:
        st.session_state["dl_total_products"] = logs["products_found"]
    total_products = st.session_state.get("dl_total_products", 0)
    batch = logs.get("batch")
    if batch:
        done, pct = batch.get("done", 0), batch.get("pct", 0)
        st.session_state["dl_completed"] = done
        start_ts = st.session_state.get("dl_start_time")
        eta_str = "calculating…"
        if start_ts and done > 0:
            elapsed = time.time() - start_ts
            remaining = (elapsed / done) * (max(total_products, done) - done)
            eta_str = _format_eta(remaining)
        st.markdown(f"""<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;margin-bottom:8px;'>
            <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;'>
                <div style='font-family:JetBrains Mono,monospace;font-size:0.78rem;color:#e2e8f0;font-weight:600;'>Batch Progress</div>
                <div style='font-family:JetBrains Mono,monospace;font-size:0.7rem;color:#fbbf24;'>ETA: {eta_str}</div>
            </div>
            <div style='height:6px;background:#1a2236;border-radius:3px;overflow:hidden;margin-bottom:4px;'>
                <div style='height:100%;width:{pct}%;background:linear-gradient(90deg,#38bdf8,#2dd4bf);border-radius:3px;transition:width 0.3s ease;'></div>
            </div>
            <div style='display:flex;justify-content:space-between;font-family:JetBrains Mono,monospace;font-size:0.65rem;color:#64748b;'>
                <span>{done}/{total_products or '—'} products</span><span>{pct}%</span>
            </div>
        </div>""", unsafe_allow_html=True)
    for fname, info in logs.get("files", {}).items():
        short = fname if len(fname) < 40 else fname[:18] + "…" + fname[-18:]
        pct = info.get("pct", 0)
        st.markdown(f"""<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;margin-bottom:6px;'>
            <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;'>
                <div style='font-family:JetBrains Mono,monospace;font-size:0.75rem;color:#e2e8f0;font-weight:600;'>{short}</div>
                <div style='font-family:JetBrains Mono,monospace;font-size:0.68rem;color:#fbbf24;'>ETA: {info.get('eta')}</div>
            </div>
            <div style='height:6px;background:#1a2236;border-radius:3px;overflow:hidden;margin-bottom:4px;'>
                <div style='height:100%;width:{pct}%;background:linear-gradient(90deg,#a78bfa,#fb7185);border-radius:3px;transition:width 0.3s ease;'></div>
            </div>
            <div style='display:flex;justify-content:space-between;font-family:JetBrains Mono,monospace;font-size:0.65rem;color:#64748b;'>
                <span>{info.get('done')}/{info.get('total')}</span><span>{pct}%</span>
            </div>
        </div>""", unsafe_allow_html=True)
    # Stats row
    completed = st.session_state.get("dl_completed", 0)
    products_display = total_products if total_products else "—"
    st.markdown(f"""<div style='display:flex;gap:8px;margin-top:6px;'>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono,monospace;color:#2dd4bf;font-weight:700;'>{products_display}</div>
            <div style='font-size:0.68rem;color:#64748b;text-transform:uppercase;letter-spacing:0.06em;'>Products Found</div>
        </div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono,monospace;color:#e2e8f0;font-weight:700;'>{completed}</div>
            <div style='font-size:0.68rem;color:#64748b;text-transform:uppercase;letter-spacing:0.06em;'>Downloaded</div>
        </div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono,monospace;color:#a78bfa;font-weight:700;'>{n_files}</div>
            <div style='font-size:0.68rem;color:#64748b;text-transform:uppercase;letter-spacing:0.06em;'>Files</div>
        </div>
        <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:10px;text-align:center;'>
            <div style='font-size:1.3rem;font-family:JetBrains Mono,monospace;color:#fbbf24;font-weight:700;'>{total_mb:.1f} MB</div>
            <div style='font-size:0.68rem;color:#64748b;text-transform:uppercase;letter-spacing:0.06em;'>Total Size</div>
        </div>
    </div>""", unsafe_allow_html=True)
    # Error log and recent log lines
    if logs.get("errors"):
        with st.expander(f"⚠️ Errors ({len(logs['errors'])})", expanded=False):
            for err in logs["errors"][-10:]:
                st.text(err)
    if logs.get("logs"):
        with st.expander("📜 Recent Logs", expanded=False):
            for line in logs["logs"][-15:]:
                st.text(line)
    if not batch and not logs.get("files") and not logs.get("logs"):
        st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#38bdf8;">ℹ️ Waiting for download output…</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════

def init_state() -> None:
    """Initialise session state with default values without overriding existing keys."""
    defaults = {
        "tile_system": "sentinel-2",
        "geometry_text": "",
        "intersecting_tiles": [],
        "selected_tiles": [],
        "start_date": dt.date.today() - dt.timedelta(days=7),
        "end_date": dt.date.today(),
        "map_center": MCFG.DEFAULT_CENTER,
        "map_zoom": MCFG.DEFAULT_ZOOM,
        "map_bounds": None,
        "last_click_popup": None,
        "last_aoi_wkt": "",
        "last_drawings_hash": None,
        "show_grid": True,
        "show_nocov": False,
        "show_inter": True,
        "show_sel": True,
        "colorize": True,
        "opacity": MCFG.GRID_OPACITY,
        "max_feat": MCFG.MAX_FEATURES,
        "simp_tol": MCFG.SIMPLIFY_TOL,
        "click_sel": False,
        "gc_params": None,
        "auto_refresh": False,  # auto refresh disabled by default for stability
        "gc_last_ts": 0.0,
        "provider": "Copernicus",
        "satellite": "SENTINEL-2",
        "product": "S2MSI2A",
        "dl_start_time": None,
        "dl_total_products": 0,
        "dl_completed": 0,
        "dl_running": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    # Separate cache keys for each tile system: sentinel-2 and landsat
    for s in ("sentinel-2", "landsat"):
        for x in ("bk", "tiles", "nocov", "ts"):
            key = f"gc_{s}_{x}"
            if key not in st.session_state:
                st.session_state[key] = 0.0 if x == "ts" else None


def _ss_get(key: str, default: Any = None) -> Any:
    """Safe getter for session_state that never raises."""
    return st.session_state.get(key, default)


def update_cache(gdf: gpd.GeoDataFrame, nocov: Optional[gpd.GeoDataFrame], ncol: Optional[str], bbox: Tuple[float, float, float, float], sys: str) -> None:
    """Update the cached grid overlay for the given tile system and bounding box."""
    pfx = f"gc_{sys}"
    st.session_state[f"{pfx}_bk"] = bbox_key(bbox)
    st.session_state[f"{pfx}_ts"] = time.time()
    st.session_state[f"{pfx}_tiles"] = filter_gdf_bbox(
        gdf, bbox,
        int(_ss_get("max_feat", MCFG.MAX_FEATURES)),
        float(_ss_get("simp_tol", MCFG.SIMPLIFY_TOL)),
        keep_cols=[ncol] if ncol else None,
    )
    if sys == "sentinel-2" and nocov is not None and _ss_get("show_nocov"):
        st.session_state[f"{pfx}_nocov"] = filter_gdf_bbox(
            nocov, bbox, 600,
            float(_ss_get("simp_tol", MCFG.SIMPLIFY_TOL)),
        )
    else:
        st.session_state[f"{pfx}_nocov"] = None


def sync_viewport(md: Dict[str, Any], eps: float = 0.01) -> None:
    """Synchronise the map viewport stored in session state with the returned metadata."""
    if not md:
        return
    z = md.get("zoom")
    if z is not None:
        try:
            z = int(z)
            cur_z = int(_ss_get("map_zoom", z))
            if abs(z - cur_z) >= 1:
                st.session_state["map_zoom"] = z
        except Exception:
            pass
    c = md.get("center")
    if c and isinstance(c, dict) and "lat" in c and "lng" in c:
        try:
            lat, lng = float(c["lat"]), float(c["lng"])
            old = _ss_get("map_center", (lat, lng))
            if abs(lat - old[0]) > eps or abs(lng - old[1]) > eps:
                st.session_state["map_center"] = (round(lat, 4), round(lng, 4))
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR RENDERING
# ═══════════════════════════════════════════════════════════════════════════════

def render_sidebar(sat_tiles: Dict[str, Dict[str, Any]], gdf: Optional[gpd.GeoDataFrame], nocov: Optional[gpd.GeoDataFrame], ncol: Optional[str], skey: str) -> Tuple[str, str, str, str, bool]:
    """Render the sidebar controls and return selections and refresh flag."""
    refresh = False
    # Logo and title
    st.sidebar.markdown("""
    <div style="text-align:center;padding:.3rem 0 .6rem">
        <div style="font-size:1.6rem">🛰️</div>
        <div style="font-size:1rem;font-weight:700;color:#e2e8f0">Sat Downloader</div>
        <div style="font-size:.65rem;color:#64748b;letter-spacing:.06em">PROFESSIONAL EDITION</div>
    </div>""", unsafe_allow_html=True)
    # Provider selection
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>📡</span><span style="font-weight:600;font-size:0.88rem;">Data Source</span></div>',
        unsafe_allow_html=True,
    )
    provider = st.sidebar.selectbox(
        "Provider", list(PROVIDERS.keys()),
        index=list(PROVIDERS.keys()).index(_ss_get("provider", "Copernicus")),
        key="sb_prov",
    )
    st.session_state["provider"] = provider
    missions = PROVIDERS.get(provider, [])
    if missions:
        ds = _ss_get("satellite", missions[0])
        satellite = st.sidebar.selectbox(
            "Mission", missions,
            index=missions.index(ds) if ds in missions else 0,
            key="sb_sat",
        )
    else:
        satellite = st.sidebar.text_input("Mission", value="", key="sb_sat_t")
    st.session_state["satellite"] = satellite
    prods = PRODUCT_TYPES.get(satellite, [])
    if prods:
        dp = _ss_get("product", prods[0])
        product = st.sidebar.selectbox(
            "Product", prods,
            index=prods.index(dp) if dp in prods else 0,
            key="sb_prod",
        )
    else:
        product = st.sidebar.text_input("Product", value="", key="sb_prod_t")
    st.session_state["product"] = product
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)
    # Tile system
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>🛰️</span><span style="font-weight:600;font-size:0.88rem;">Tile System</span></div>',
        unsafe_allow_html=True,
    )
    opts_list: List[str] = []
    labs: Dict[str, str] = {}
    if sat_tiles.get("sentinel-2", {}).get("tiles") is not None:
        opts_list.append("sentinel-2")
        labs["sentinel-2"] = "Sentinel-2 (MGRS)"
    if sat_tiles.get("landsat", {}).get("tiles") is not None:
        opts_list.append("landsat")
        labs["landsat"] = "Landsat (WRS-2)"
    if opts_list:
        ns = st.sidebar.radio(
            "Grid", opts_list, format_func=lambda x: labs.get(x, x),
            index=opts_list.index(skey) if skey in opts_list else 0,
            horizontal=True, label_visibility="collapsed",
        )
        if ns != skey:
            st.session_state["tile_system"] = ns
            st.session_state["selected_tiles"] = []
            st.session_state["intersecting_tiles"] = []
            st.rerun()
    # Legend (colour‑blind friendly)
    st.sidebar.markdown("""
    <div style="margin:.3rem 0">
        <div style="display:flex;align-items:center;gap:10px;font-size:0.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#0077BB"></div>Sentinel-2</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:0.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#EE7733"></div>Landsat</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:0.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#CCBB44"></div>AOI</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:0.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#AA3377"></div>Intersecting</div>
        <div style="display:flex;align-items:center;gap:10px;font-size:0.8rem;color:#94a3b8;margin:3px 0;"><div style="width:18px;height:10px;border-radius:3px;background:#EE3377"></div>Selected</div>
    </div>
    """, unsafe_allow_html=True)
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)
    # AOI mode
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>📐</span><span style="font-weight:600;font-size:0.88rem;">Area of Interest</span></div>',
        unsafe_allow_html=True,
    )
    aoi_choices = ["Draw on map", "Preset square", "Paste WKT / GeoJSON"]
    aoi_mode = st.sidebar.radio(
        "AOI", aoi_choices,
        horizontal=False, label_visibility="collapsed",
        index=aoi_choices.index(_ss_get("aoi_mode", "Draw on map")),
    )
    st.session_state["aoi_mode"] = aoi_mode
    if aoi_mode == "Preset square":
        c1, c2 = st.sidebar.columns(2)
        with c1:
            sq_lat = st.number_input(
                "Lat", value=float(st.session_state["map_center"][0]),
                format="%.4f", key="sq_lat",
            )
        with c2:
            sq_lng = st.number_input(
                "Lng", value=float(st.session_state["map_center"][1]),
                format="%.4f", key="sq_lng",
            )
        sq_km = st.sidebar.number_input(
            "Side (km)", min_value=0.1, value=25.0, step=5.0, key="sq_km",
        )
        if st.sidebar.button("✅ Apply", use_container_width=True):
            st.session_state["geometry_text"] = make_square_wkt(sq_lat, sq_lng, sq_km)
            st.session_state["map_center"] = (sq_lat, sq_lng)
            st.rerun()
    elif aoi_mode == "Paste WKT / GeoJSON":
        st.session_state["geometry_text"] = st.sidebar.text_area(
            "WKT/GeoJSON",
            value=_ss_get("geometry_text", ""),
            height=100, label_visibility="collapsed",
            placeholder="Paste WKT or GeoJSON…",
        )
    else:
        st.sidebar.caption("Draw rectangle/polygon on the map.")
    atxt = _ss_get("geometry_text", "")
    if atxt:
        with st.sidebar.expander("📋 AOI Preview", expanded=False):
            st.code(atxt[:400] + ("…" if len(atxt) > 400 else ""), language="text")
            if st.button("🗑️ Clear", use_container_width=True, key="clr_aoi"):
                st.session_state["geometry_text"] = ""
                st.session_state["last_aoi_wkt"] = ""
                st.session_state["intersecting_tiles"] = []
                st.rerun()
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)
    # Time range
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>📅</span><span style="font-weight:600;font-size:0.88rem;">Time Range</span></div>',
        unsafe_allow_html=True,
    )
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
    # Grid display settings
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>🔲</span><span style="font-weight:600;font-size:0.88rem;">Grid & Display</span></div>',
        unsafe_allow_html=True,
    )
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
                st.session_state["show_nocov"] = st.checkbox(
                    "No-cov", value=st.session_state["show_nocov"],
                    disabled=(nocov is None), key="cn",
                )
        with st.sidebar.expander("⚙️ Advanced", expanded=False):
            st.session_state["max_feat"] = int(st.number_input(
                "Max features", 200, 8000, int(st.session_state["max_feat"]), step=200, key="mf",
            ))
            st.session_state["opacity"] = float(st.slider(
                "Fill opacity", 0.0, 0.2, float(st.session_state["opacity"]), step=0.01, key="op",
            ))
            st.session_state["simp_tol"] = float(st.slider(
                "Simplify (°)", 0.0, 0.02, float(st.session_state["simp_tol"]), step=0.001, key="si",
            ))
        # Invalidate cache if parameters changed
        cur = (int(st.session_state["max_feat"]), float(st.session_state["simp_tol"]), skey)
        if _ss_get("gc_params") != cur:
            st.session_state["gc_params"] = cur
            for s in ("sentinel-2", "landsat"):
                for x in ("tiles", "nocov", "bk"):
                    st.session_state[f"gc_{s}_{x}"] = None
        # Refresh and auto refresh options
        a, b = st.sidebar.columns(2)
        with a:
            refresh = st.button("🔄 Refresh", use_container_width=True, key="ref")
        with b:
            st.session_state["auto_refresh"] = st.checkbox(
                "Auto", value=st.session_state["auto_refresh"], key="ar",
            )
        pfx = f"gc_{skey}"
        if _ss_get(f"{pfx}_bk") is None:
            st.sidebar.caption("⏳ Cache empty — refresh")
        else:
            age = time.time() - float(_ss_get(f"{pfx}_ts", 0))
            st.sidebar.caption(f"Cache: {age:.0f}s ago")
    else:
        st.sidebar.info(f"No grid data for {skey}.")
    st.sidebar.markdown('<hr style="border-color:rgba(56,120,200,0.10)">', unsafe_allow_html=True)
    # Tile search and selection
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:6px;padding-top:0.3rem"><span>🔍</span><span style="font-weight:600;font-size:0.88rem;">Tile Search</span></div>',
        unsafe_allow_html=True,
    )
    if gdf is not None and ncol:
        q = st.sidebar.text_input(
            "Search", placeholder="e.g. 34UED or 233062",
            label_visibility="collapsed", key=f"ts_{skey}",
        )
        if q:
            matches = find_tiles(gdf, ncol, q, 50)
            mids = matches[ncol].astype(str).tolist() if not matches.empty else []
            if not mids:
                st.sidebar.caption("No matches.")
            else:
                pk = st.sidebar.selectbox(
                    "Results", mids, index=0, key=f"tm_{skey}",
                    label_visibility="collapsed",
                )
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
                            st.session_state["map_center"] = (float(ct.y), float(ct.x))
                            st.session_state["map_zoom"] = max(int(st.session_state["map_zoom"]), 10)
                            st.rerun()
        all_names = sorted(gdf[ncol].astype(str).unique().tolist()) if ncol else []
        cur_sel = st.sidebar.multiselect(
            "Selected", all_names,
            default=st.session_state["selected_tiles"],
            key=f"ms_{skey}", label_visibility="collapsed",
        )
        st.session_state["selected_tiles"] = cur_sel
        if cur_sel:
            if st.sidebar.button("✕ Clear", use_container_width=True, key=f"tc_{skey}"):
                st.session_state["selected_tiles"] = []
                st.rerun()
    return provider, satellite, product, aoi_mode, refresh


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Entry point for the Streamlit application."""
    st.set_page_config(
        page_title="Satellite Imagery Downloader",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    # Apply custom CSS styling
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    # Initialise session state
    init_state()
    # Load tiles
    sat_tiles = load_tiles()
    # Determine current system key and relevant data
    skey = _ss_get("tile_system", "sentinel-2")
    td = sat_tiles.get(skey, {})
    gdf = td.get("tiles")
    nocov = td.get("nocov")
    ncol = get_name_col(gdf, skey) if gdf is not None else None
    # Render sidebar controls
    provider, satellite, product, aoi_mode, do_refresh = render_sidebar(
        sat_tiles, gdf, nocov, ncol, skey,
    )
    # Page header
    st.markdown("""<div style='display:flex;align-items:center;gap:14px;margin-bottom:4px;'>
        <div style='font-size:1.6rem;background:linear-gradient(135deg,#38bdf8,#2dd4bf);width:44px;height:44px;border-radius:12px;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 12px rgba(56,189,248,0.35);'>🛰️</div>
        <div><div style='font-size:1.25rem;font-weight:700;color:#e2e8f0;'>Satellite Imagery Downloader</div><div style='font-size:0.72rem;color:#64748b;letter-spacing:.04em;'>Sentinel-2 · Landsat · DEM · Grid Explorer · CLI Download</div></div>
    </div>""", unsafe_allow_html=True)
    # Tabs: Map, Download, Results, Settings
    tab_map, tab_dl, tab_res, tab_set = st.tabs(["🗺️ Map", "⬇️ Download", "📂 Results", "🔧 Settings"])
    # Map tab
    with tab_map:
        aoi_geom = parse_geometry(_ss_get("geometry_text", ""))
        aoi_polys: List[Polygon] = []
        if aoi_geom and not aoi_geom.is_empty:
            if aoi_geom.geom_type == "Polygon":
                aoi_polys = [aoi_geom]
            elif aoi_geom.geom_type == "MultiPolygon":
                aoi_polys = list(aoi_geom.geoms)
        tnames, inter_gdf = compute_intersections(aoi_polys, gdf, ncol)
        st.session_state["intersecting_tiles"] = tnames or []
        sel_gdf: Optional[gpd.GeoDataFrame] = None
        if gdf is not None and ncol and st.session_state["selected_tiles"]:
            ss = set(map(str, st.session_state["selected_tiles"]))
            sel_gdf = gdf[gdf[ncol].astype(str).isin(ss)][[ncol, "geometry"]].copy()
            if sel_gdf.empty:
                sel_gdf = None
        pfx = f"gc_{skey}"
        # Manual refresh
        if do_refresh and gdf is not None and ncol and _ss_get("show_grid"):
            bb = bounds_from_leaflet(_ss_get("map_bounds"))
            if not bb:
                lat, lng = st.session_state["map_center"]
                bb = fallback_bbox(lat, lng, int(st.session_state["map_zoom"]))
            update_cache(gdf, nocov, ncol, bb, skey)
        # Retrieve cached overlays
        tv = _ss_get(f"{pfx}_tiles")
        nv = _ss_get(f"{pfx}_nocov")
        center = st.session_state["map_center"]
        zoom = int(st.session_state["map_zoom"])
        sg = (
            _ss_get("show_grid")
            and zoom >= MCFG.MIN_GRID_ZOOM
            and tv is not None
            and not getattr(tv, "empty", True)
        )
        sn = (
            skey == "sentinel-2"
            and _ss_get("show_nocov")
            and nv is not None
            and not getattr(nv, "empty", True)
        )
        if _ss_get("show_grid") and zoom < MCFG.MIN_GRID_ZOOM:
            st.markdown(
                f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#fbbf24;">⚠️ Zoom to {MCFG.MIN_GRID_ZOOM}+ for grid (current: {zoom})</div>',
                unsafe_allow_html=True,
            )
        if _ss_get("show_grid") and _ss_get(f"{pfx}_bk") is None:
            st.markdown(
                '<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#38bdf8;">ℹ️ Grid cache empty — click Refresh in sidebar</div>',
                unsafe_allow_html=True,
            )
        opts = {
            "show_grid": sg,
            "show_nocov": sn,
            "show_inter": st.session_state["show_inter"],
            "show_sel": st.session_state["show_sel"],
            "colorize": st.session_state["colorize"],
            "opacity": st.session_state["opacity"],
        }
        m = build_map(
            center, zoom, aoi_geom,
            tv if sg else None,
            nv if sn else None,
            inter_gdf, sel_gdf, opts, ncol, skey,
        )
        ret = ["all_drawings", "bounds", "zoom", "center"]
        if st.session_state["click_sel"]:
            ret.append("last_object_clicked_popup")
        st.markdown('<div style="border-radius:14px;overflow:hidden;border:1px solid rgba(56,120,200,0.10);box-shadow:0 4px 20px rgba(0,0,0,0.3);">', unsafe_allow_html=True)
        md = st_folium(m, key="mm", width="100%", height=MCFG.MAP_HEIGHT, returned_objects=ret)
        st.markdown('</div>', unsafe_allow_html=True)
        if md and md.get("bounds"):
            b = bounds_from_leaflet(md["bounds"])
            if b:
                st.session_state["map_bounds"] = md["bounds"]
            sync_viewport(md)
        # Auto refresh grid overlay when panning/zooming
        if gdf is not None and ncol and _ss_get("show_grid") and _ss_get("auto_refresh"):
            bnow = bounds_from_leaflet(md.get("bounds") if md else None)
            if bnow:
                kn = bbox_key(bnow)
                kc = _ss_get(f"{pfx}_bk")
                now = time.time()
                # Only refresh if bounding box has changed and throttle interval has passed
                if kn != kc and (now - float(_ss_get("gc_last_ts", 0))) >= MCFG.AUTO_REFRESH_THROTTLE:
                    st.session_state["gc_last_ts"] = now
                    update_cache(gdf, nocov, ncol, bnow, skey)
                    st.rerun()
        # Draw on map AOI handling
        if aoi_mode == "Draw on map":
            drawn = parse_drawings(md)
            if drawn:
                u = safe_union(drawn)
                nw = shapely_wkt.dumps(u, rounding_precision=6)
                drs = md.get("all_drawings", []) if md else []
                dh = hash(json.dumps(drs, sort_keys=True))
                if (
                    nw
                    and dh != _ss_get("last_drawings_hash")
                    and nw != _ss_get("last_aoi_wkt", "")
                ):
                    st.session_state.update({
                        "last_aoi_wkt": nw,
                        "last_drawings_hash": dh,
                        "geometry_text": nw,
                    })
                    st.rerun()
        # Click select/deselect
        if st.session_state["click_sel"] and md:
            popup = md.get("last_object_clicked_popup")
            if popup and popup != _ss_get("last_click_popup"):
                st.session_state["last_click_popup"] = popup
                pat = r"\b\d{2}[A-Z]{3}\b" if skey == "sentinel-2" else r"\b\d{6}\b"
                mi = re.search(pat, str(popup).upper() if skey == "sentinel-2" else str(popup))
                if mi:
                    tid = mi.group(0)
                    sel = set(map(str, st.session_state["selected_tiles"]))
                    sel.symmetric_difference_update({tid})
                    st.session_state["selected_tiles"] = sorted(sel)
                    st.rerun()
        # Stats and lists
        ni = len(st.session_state["intersecting_tiles"])
        ns = len(st.session_state["selected_tiles"])
        grid_label = skey.split('-')[0].upper()
        st.markdown(f"""<div style='display:flex;gap:8px;margin-top:6px;'>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#2dd4bf;font-weight:700;'>{ni}</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Intersecting</div>
            </div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#e2e8f0;font-weight:700;'>{ns}</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Selected</div>
            </div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#38bdf8;font-weight:700;'>{grid_label}</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Grid</div>
            </div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#fbbf24;font-weight:700;'>{zoom}</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Zoom</div>
            </div>
        </div>""", unsafe_allow_html=True)
        # Show lists in expanders with chips
        if gdf is not None and ncol:
            ca, cb = st.columns(2)
            with ca:
                if st.session_state["intersecting_tiles"]:
                    with st.expander(f"🔮 Intersecting ({ni})", expanded=False):
                        st.markdown(
                            "".join(
                                f'<span style="display:inline-block;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:16px;padding:4px 10px;margin:2px;font-size:11px;font-family:\'JetBrains Mono\',monospace;color:#e48abf;">{t}</span>'
                                for t in st.session_state["intersecting_tiles"][:60]
                            ),
                            unsafe_allow_html=True,
                        )
                        if ni > 60:
                            st.caption(f"…+{ni - 60}")
                        st.download_button(
                            "📥 CSV",
                            data="tile\n" + "\n".join(st.session_state["intersecting_tiles"]),
                            file_name=f"{skey}_intersects.csv", mime="text/csv",
                        )
            with cb:
                if st.session_state["selected_tiles"]:
                    with st.expander(f"✅ Selected ({ns})", expanded=False):
                        st.markdown(
                            "".join(
                                f'<span style="display:inline-block;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:16px;padding:4px 10px;margin:2px;font-size:11px;font-family:\'JetBrains Mono\',monospace;color:#f88cb0;">{t}</span>'
                                for t in st.session_state["selected_tiles"]
                            ),
                            unsafe_allow_html=True,
                        )
                        st.download_button(
                            "📥 CSV",
                            data="tile\n" + "\n".join(map(str, st.session_state["selected_tiles"])),
                            file_name=f"{skey}_selected.csv", mime="text/csv",
                        )
    # Download tab
    with tab_dl:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>⬇️</span><span style="font-weight:600;font-size:0.94rem;">Download Manager</span><span style="font-size:0.72rem;color:#64748b;margin-left:auto;">CLI execution</span></div>',
            unsafe_allow_html=True,
        )
        np_ = len(_ss_get("selected_tiles", []))
        ni_ = len(_ss_get("intersecting_tiles", []))
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Provider", provider)
        with c2:
            st.metric("Mission", satellite)
        with c3:
            st.metric("Product", product)
        st.markdown("---")
        if _ss_get("dl_running"):
            st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">🔄 Download in progress…</div>', unsafe_allow_html=True)
        elif np_ > 0:
            tile_word = "tiles" if np_ != 1 else "tile"
            st.markdown(f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">📦 {np_} {tile_word} queued for download</div>', unsafe_allow_html=True)
        elif ni_ > 0:
            st.markdown(f'<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#38bdf8;">ℹ️ {ni_} intersecting — select tiles to download</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#fbbf24;">⚠️ No tiles — draw AOI or select tiles first</div>', unsafe_allow_html=True)
        atxt = _ss_get("geometry_text", "").strip()
        afile = "example_aoi.geojson" if atxt.startswith("{") else "example_aoi.wkt"
        cli_cmd = (
            f"python cli.py --provider {provider.lower()} "
            f"--collection {satellite.split(' ')[0]} --product-type {product} "
            f"--start-date {st.session_state['start_date']} "
            f"--end-date {st.session_state['end_date']} --aoi_file {afile}"
        )
        st.code(cli_cmd, language="bash")
        d1, d2, d3 = st.columns([2, 1, 1])
        with d1:
            if st.button("🚀 Start Download", use_container_width=True, type="primary"):
                if not atxt:
                    st.error("Define an AOI first.")
                else:
                    reset_downloads()
                    st.session_state["dl_running"] = True
                    st.session_state["dl_start_time"] = time.time()
                    Path(afile).write_text(atxt)
                    Path("nohup.out").write_text("")
                    # Launch the CLI download in background.  Use nohup to detach.
                    os.system(f"nohup {cli_cmd} > nohup.out 2>&1 &")
                    st.success("✅ Download started! Previous downloads cleared.")
                    st.rerun()
        with d2:
            if st.button("⏹️ Stop", use_container_width=True):
                # Attempt to stop the CLI by searching for python cli.py processes
                os.system("pkill -f 'python cli.py'")
                st.session_state["dl_running"] = False
                st.warning("⏹️ Stopped.")
        with d3:
            if st.button("🗑️ Reset", use_container_width=True):
                reset_downloads()
                st.session_state["dl_running"] = False
                st.info("🗑️ Downloads cleared.")
                st.rerun()
        st.markdown("---")
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📊</span><span style="font-weight:600;font-size:0.94rem;">Live Progress</span></div>',
            unsafe_allow_html=True,
        )
        render_download_progress()
        # Poll CLI process to determine if download has finished
        if _ss_get("dl_running"):
            result = os.popen("pgrep -f 'python cli.py'").read()
            if not result.strip():
                st.session_state["dl_running"] = False
                st.markdown('<div style="background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:8px;color:#06d6a0;">✅ Download complete!</div>', unsafe_allow_html=True)
            else:
                # Brief pause then rerun to update progress
                time.sleep(2)
                st.rerun()
    # Results tab
    with tab_res:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>📂</span><span style="font-weight:600;font-size:0.94rem;">Downloaded Products</span></div>',
            unsafe_allow_html=True,
        )
        dl_dir = "downloads"
        Path(dl_dir).mkdir(exist_ok=True)
        n_files, total_mb = count_downloaded_products(dl_dir)
        st.markdown(f"""<div style='display:flex;gap:8px;margin:6px 0;'>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#e2e8f0;font-weight:700;'>{n_files}</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Files</div>
            </div>
            <div style='flex:1;background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;text-align:center;'>
                <div style='font-size:1.4rem;font-family:JetBrains Mono,monospace;color:#2dd4bf;font-weight:700;'>{total_mb:.1f} MB</div>
                <div style='font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;'>Total Size</div>
            </div>
        </div>""", unsafe_allow_html=True)
        # Use the file browser if available
        if st_file_browser is not None:
            st_file_browser(
                dl_dir, key="fb",
                show_choose_file=True, show_choose_folder=True,
                show_delete_file=True, show_download_file=True,
                show_new_folder=True, show_upload_file=True,
                show_rename_file=True, show_rename_folder=True,
                use_cache=True,
            )
        else:
            files = [f for f in Path(dl_dir).rglob("*") if f.is_file()]
            if files:
                for f in sorted(files):
                    sz = f.stat().st_size / (1024 * 1024)
                    st.text(f"📄 {f.relative_to(dl_dir)} ({sz:.2f} MB)")
            else:
                st.info("No files yet.")
    # Settings tab
    with tab_set:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🔧</span><span style="font-weight:600;font-size:0.94rem;">Configuration</span></div>',
            unsafe_allow_html=True,
        )
        try:
            st.code(Path("config.yaml").read_text(), language="yaml")
        except FileNotFoundError:
            st.info("config.yaml not found.")
        st.markdown("---")
        s1, s2, s3 = st.columns(3)
        with s1:
            st.metric(
                "Center",
                f"{st.session_state['map_center'][0]:.4f}, {st.session_state['map_center'][1]:.4f}",
            )
        with s2:
            st.metric("Zoom", st.session_state["map_zoom"])
        with s3:
            st.metric("System", skey)


if __name__ == "__main__":
    main()