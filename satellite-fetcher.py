# satellite-fetcher.py
import os
import re
import math
import json
import time
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple

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


# -------------------------
# Constants (Grid Explorer data paths)
# -------------------------
S2_TILES_GEOJSON = "data/Sentinel-2-tiles/sentinel-2_grids.geojson"
S2_NOCOV_GEOJSON = "data/Sentinel-2-tiles/sentinel-2_no_coverage.geojson"
S2_FALLBACK_SHAPEFILE = "data/Sentinel-2-tiles/sentinel_2_index_shapefile.shp"  # optional fallback

MIN_GRID_ZOOM = 6  # don't render grid below this zoom
GRID_AUTO_REFRESH_THROTTLE_SEC = 1.0  # throttling to avoid rerun loops


# -------------------------
# Live log (tail -f)
# -------------------------
@st.fragment(run_every="2000ms")  # refresh every 2s
def show_live_logs(log_path="nohup.out"):
    log_path = Path(log_path)

    batch_re = re.compile(
        r"^Concurrent Downloads:\s*"
        r"(?P<percent>\d+)%\|\s*[^\|]*\|\s*"
        r"(?P<done>\d+)/(?P<total>\d+)\s*"
        r"\[\s*(?P<elapsed>[0-9:?]+)<(?P<eta>[^\]]+)\]\s*"
        r"(?P<rate>[^\s]*/?[^\s]*?)?\s*$"
    )

    download_re = re.compile(
        r"^Downloading\s+(?P<filename>.+?):\s*"
        r"(?P<percent>\d+)%\|\s*.*?\|\s*"
        r"(?P<done>[\d\.]+[kMGTP]?)/(?P<total>[\d\.]+[kMGTP]?)\s*"
        r"\[(?P<elapsed>[0-9:]+)<(?P<eta>[0-9:?\-]+)\]"
    )

    with st.container():
        progress_bars_info = {}
        non_progress_lines = []

        if log_path.exists():
            with log_path.open("r") as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()

                m = batch_re.search(line)
                if m:
                    desc = "Concurrent Downloads"
                    percent = int(m.group("percent"))
                    done, total = int(m.group("done")), int(m.group("total"))
                    progress_bars_info[desc] = {
                        "label": f"🌐 {desc} ({done}/{total})",
                        "percent": percent,
                    }
                    continue

                m = download_re.search(line)
                if m:
                    desc = m.group("filename").strip()
                    percent = int(m.group("percent"))
                    done, total = m.group("done"), m.group("total")
                    elapsed = m.group("elapsed").strip()
                    eta = m.group("eta").strip()
                    progress_bars_info[desc] = {
                        "label": f"📥 {desc} ({done}/{total}) | Elapsed: {elapsed} | ETA: {eta}",
                        "percent": percent,
                    }
                    continue

                if line:
                    non_progress_lines.append(line)

        for _, pb in progress_bars_info.items():
            st.write(pb["label"])
            st.progress(pb["percent"])

        if non_progress_lines:
            st.markdown("#### Recent Logs")
            for l in non_progress_lines[-6:]:
                st.write(l)


# -------------------------
# Grid Explorer helpers
# -------------------------
def _ensure_4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        return gdf.set_crs(epsg=4326)
    return gdf.to_crs(epsg=4326)


def _tile_name_column(gdf: Optional[gpd.GeoDataFrame]) -> Optional[str]:
    if gdf is None or gdf.empty:
        return None
    candidates = ["Name", "name", "TILE_ID", "tile_id", "MGRS_TILE", "mgrs"]
    for c in candidates:
        if c in gdf.columns:
            return c
    for c in gdf.columns:
        if c != "geometry" and gdf[c].dtype == object:
            return c
    return None


@st.cache_data(show_spinner="Loading Sentinel-2 Grid Explorer data…")
def _load_s2_geojson(
    tiles_path: str, nocov_path: str
) -> Tuple[Optional[gpd.GeoDataFrame], Optional[gpd.GeoDataFrame]]:
    tiles_p = Path(tiles_path)
    nocov_p = Path(nocov_path)

    tiles = None
    nocov = None

    if tiles_p.exists():
        tiles = gpd.read_file(tiles_p)
        tiles = _ensure_4326(tiles)

    if nocov_p.exists():
        nocov = gpd.read_file(nocov_p)
        nocov = _ensure_4326(nocov)

    return tiles, nocov


@st.cache_data(show_spinner=False)
def _load_s2_fallback_shapefile(path: str) -> Optional[gpd.GeoDataFrame]:
    p = Path(path)
    if not p.exists():
        return None
    gdf = gpd.read_file(p)
    return _ensure_4326(gdf)


def _union_geoms(geoms):
    try:
        return shapely.union_all(geoms)  # shapely>=2
    except Exception:
        return unary_union(geoms)  # shapely<2 fallback


def _bounds_from_leaflet(bounds) -> Optional[Tuple[float, float, float, float]]:
    if not bounds:
        return None

    def _f(x):
        try:
            if x is None:
                return None
            return float(x)
        except (TypeError, ValueError):
            return None

    if isinstance(bounds, dict):
        sw = bounds.get("_southWest") or bounds.get("southWest")
        ne = bounds.get("_northEast") or bounds.get("northEast")
        if not (sw and ne):
            return None

        minx = _f(sw.get("lng"))
        miny = _f(sw.get("lat"))
        maxx = _f(ne.get("lng"))
        maxy = _f(ne.get("lat"))

        if None in (minx, miny, maxx, maxy):
            return None

        if maxx < minx:
            minx, maxx = maxx, minx
        if maxy < miny:
            miny, maxy = maxy, miny

        return (minx, miny, maxx, maxy)

    return None


def _fallback_bbox(center_lat: float, center_lng: float, zoom: int) -> Tuple[float, float, float, float]:
    width = 360.0 / (2 ** max(0, zoom))
    height = 180.0 / (2 ** max(0, zoom))
    half_w = width * 0.6
    half_h = height * 0.6
    return (center_lng - half_w, center_lat - half_h, center_lng + half_w, center_lat + half_h)


def _bbox_key(bbox: Optional[Tuple[float, float, float, float]], nd: int = 4):
    if bbox is None:
        return None
    try:
        return tuple(round(float(v), nd) for v in bbox)
    except Exception:
        return None


def _maybe_update_center_zoom(map_data, eps: float = 1e-4):
    # Update center/zoom with tolerance to avoid jitter loops
    if map_data and map_data.get("zoom") is not None:
        try:
            z = int(map_data["zoom"])
            if z != int(st.session_state.get("map_zoom", z)):
                st.session_state["map_zoom"] = z
        except Exception:
            pass

    c = map_data.get("center") if map_data else None
    if c and "lat" in c and "lng" in c:
        try:
            lat = float(c["lat"])
            lng = float(c["lng"])
            old_lat, old_lng = st.session_state.get("map_center", (lat, lng))
            if abs(lat - old_lat) > eps or abs(lng - old_lng) > eps:
                st.session_state["map_center"] = (round(lat, 6), round(lng, 6))
        except Exception:
            pass


def _filter_gdf_visible(
    gdf: Optional[gpd.GeoDataFrame],
    bbox_ll: Optional[Tuple[float, float, float, float]],
    max_features: int,
    simplify_tol: float,
    keep_cols: Optional[List[str]] = None,
) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty or bbox_ll is None:
        return None

    minx, miny, maxx, maxy = bbox_ll
    bb = box(minx, miny, maxx, maxy)

    try:
        sub = gdf[gdf.intersects(bb)]
    except Exception:
        sub = gdf

    if sub.empty:
        return sub

    if keep_cols:
        cols = [c for c in keep_cols if c in sub.columns]
        if "geometry" not in cols:
            cols.append("geometry")
        sub = sub[cols].copy()
    else:
        sub = sub[["geometry"]].copy()

    if simplify_tol > 0:
        try:
            sub["geometry"] = sub["geometry"].simplify(simplify_tol, preserve_topology=True)
        except Exception:
            pass

    if len(sub) > max_features:
        sub = sub.iloc[:max_features].copy()

    return sub


def _utm_color_from_tile(tile_name: str) -> str:
    try:
        col = int(str(tile_name)[:2])
        col = max(1, min(60, col))
        hue = int((col - 1) * (360 / 60))
        return f"hsl({hue}, 70%, 45%)"
    except Exception:
        return "#64748b"


def _style_tiles(colorize: bool, opacity: float):
    def _fn(feat):
        props = feat.get("properties", {}) or {}
        name = props.get("Name") or props.get("name") or ""
        c = _utm_color_from_tile(name) if colorize else "#64748b"
        return {"color": c, "weight": 1, "fillOpacity": opacity}

    return _fn


def _style_selected(_feat):
    return {"color": "#ef4444", "weight": 3, "fillOpacity": 0.08}


def _style_intersects(_feat):
    return {"color": "#3b82f6", "weight": 2, "fillOpacity": 0.06}


def _parse_map_drawings(map_data) -> List[Polygon]:
    polys: List[Polygon] = []
    if not map_data:
        return polys
    drawings = map_data.get("all_drawings") or []
    for feat in drawings:
        try:
            geom = feat.get("geometry")
            if not geom:
                continue
            if geom.get("type") == "Polygon":
                coords = geom["coordinates"][0]  # [lng,lat]
                poly = Polygon(coords)
                if poly.is_valid and not poly.is_empty:
                    polys.append(poly)
        except Exception:
            continue
    return polys


def _parse_text_geometry(text: str) -> Optional[shapely.Geometry]:
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
                return _union_geoms(geoms) if geoms else None
            return shape(obj)
        except Exception:
            return None

    try:
        return shapely_wkt.loads(t)
    except Exception:
        return None


def _square_wkt(center_lat: float, center_lng: float, side_km: float) -> str:
    half_km = side_km / 2.0
    dlat = half_km / 111.0
    dlon = half_km / (111.0 * max(0.05, abs(math.cos(math.radians(center_lat)))))

    lng1, lng2 = center_lng - dlon, center_lng + dlon
    lat1, lat2 = center_lat - dlat, center_lat + dlat

    poly = Polygon([(lng1, lat1), (lng2, lat1), (lng2, lat2), (lng1, lat2), (lng1, lat1)])
    return shapely_wkt.dumps(poly, rounding_precision=6)


def _compute_intersections(
    aoi_polys: List[Polygon],
    tiles_gdf: Optional[gpd.GeoDataFrame],
) -> Tuple[List[str], Optional[gpd.GeoDataFrame]]:
    if tiles_gdf is None or tiles_gdf.empty or not aoi_polys:
        return [], None

    name_col = _tile_name_column(tiles_gdf)
    if name_col is None:
        return [], None

    aoi_union = _union_geoms(aoi_polys)

    try:
        candidates = tiles_gdf[tiles_gdf.intersects(aoi_union)]
        if candidates.empty:
            return [], candidates

        candidates = candidates[[name_col, "geometry"]].copy()
        tile_names = candidates[name_col].astype(str).unique().tolist()
        tile_names.sort()
        return tile_names, candidates
    except Exception:
        return [], None


def _find_tiles_by_query(
    tiles_gdf: gpd.GeoDataFrame,
    name_col: str,
    query: str,
    limit: int = 50,
) -> gpd.GeoDataFrame:
    q = (query or "").strip()
    if not q:
        return tiles_gdf.iloc[0:0]

    s = tiles_gdf[name_col].astype(str)

    exact = tiles_gdf[s.str.upper() == q.upper()]
    if not exact.empty:
        return exact[[name_col, "geometry"]].copy()

    contains = tiles_gdf[s.str.contains(q, case=False, na=False)]
    if len(contains) > limit:
        contains = contains.iloc[:limit].copy()
    return contains[[name_col, "geometry"]].copy()


def _update_grid_cache(
    tiles_gdf: gpd.GeoDataFrame,
    nocov_gdf: Optional[gpd.GeoDataFrame],
    name_col: str,
    bbox_use: Tuple[float, float, float, float],
):
    st.session_state["grid_cache_bbox_key"] = _bbox_key(bbox_use)
    st.session_state["grid_cache_updated_ts"] = time.time()

    st.session_state["grid_cache_tiles"] = _filter_gdf_visible(
        tiles_gdf,
        bbox_use,
        max_features=int(st.session_state["max_grid_features"]),
        simplify_tol=float(st.session_state["simplify_tol"]),
        keep_cols=[name_col],
    )

    if nocov_gdf is not None and st.session_state.get("show_nocov", False):
        st.session_state["grid_cache_nocov"] = _filter_gdf_visible(
            nocov_gdf,
            bbox_use,
            max_features=600,
            simplify_tol=float(st.session_state["simplify_tol"]),
            keep_cols=[],
        )
    else:
        st.session_state["grid_cache_nocov"] = None


def _build_map(
    center_lat: float,
    center_lng: float,
    zoom: int,
    aoi_geom: Optional[shapely.Geometry],
    tiles_visible: Optional[gpd.GeoDataFrame],
    nocov_visible: Optional[gpd.GeoDataFrame],
    intersects_gdf: Optional[gpd.GeoDataFrame],
    selected_gdf: Optional[gpd.GeoDataFrame],
    show_grid: bool,
    show_nocov: bool,
    show_intersects: bool,
    show_selected: bool,
    colorize_grid: bool,
    grid_opacity: float,
    name_col: Optional[str],
) -> folium.Map:
    m = folium.Map(location=[center_lat, center_lng], zoom_start=zoom, tiles="OpenStreetMap")

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite",
        overlay=False,
        control=True,
    ).add_to(m)

    plugins.Fullscreen(position="topleft").add_to(m)
    plugins.MousePosition(position="bottomleft").add_to(m)
    plugins.Geocoder(position="topright", collapsed=True).add_to(m)

    if aoi_geom is not None and not aoi_geom.is_empty:
        folium.GeoJson(
            mapping(aoi_geom),
            name="AOI",
            style_function=lambda _: {"color": "#fbbf24", "weight": 3, "fillOpacity": 0.12},
        ).add_to(m)

    if show_grid and tiles_visible is not None and not getattr(tiles_visible, "empty", True):
        tooltip = None
        popup = None
        if name_col and name_col in tiles_visible.columns:
            tooltip = folium.GeoJsonTooltip(fields=[name_col], aliases=["Tile:"], sticky=False)
            popup = folium.GeoJsonPopup(fields=[name_col], aliases=["Tile:"])
        folium.GeoJson(
            tiles_visible,
            name="S2 grid (cached visible)",
            style_function=_style_tiles(colorize_grid, grid_opacity),
            tooltip=tooltip,
            popup=popup,
        ).add_to(m)

    if show_nocov and nocov_visible is not None and not getattr(nocov_visible, "empty", True):
        folium.GeoJson(
            nocov_visible,
            name="No coverage",
            style_function=lambda _: {"color": "#ef4444", "weight": 2, "fillOpacity": 0.04},
        ).add_to(m)

    if show_intersects and intersects_gdf is not None and not getattr(intersects_gdf, "empty", True):
        folium.GeoJson(intersects_gdf, name="Intersecting tiles", style_function=_style_intersects).add_to(m)

    if show_selected and selected_gdf is not None and not getattr(selected_gdf, "empty", True):
        folium.GeoJson(selected_gdf, name="Selected tiles", style_function=_style_selected).add_to(m)

    draw = plugins.Draw(
        export=False,
        position="topleft",
        draw_options={
            "polyline": False,
            "rectangle": True,
            "polygon": True,
            "circle": False,
            "marker": False,
            "circlemarker": False,
        },
        edit_options={"edit": True, "remove": True},
    )
    draw.add_to(m)

    folium.LayerControl().add_to(m)
    return m


# -------------------------
# Init
# -------------------------
def init():
    tiles, nocov = _load_s2_geojson(S2_TILES_GEOJSON, S2_NOCOV_GEOJSON)
    if tiles is not None:
        return {"SENTINEL-2": tiles, "_S2_NOCOV": nocov}

    fallback = _load_s2_fallback_shapefile(S2_FALLBACK_SHAPEFILE)
    return {"SENTINEL-2": fallback, "_S2_NOCOV": None}


# -------------------------
# Page config + state
# -------------------------
st.set_page_config(page_title="Satellite Imagery Downloader", layout="wide")
configuration = ConfigLoader(config_file_path="config.yaml")
logger.info("Configuration loaded successfully()")

today = dt.date.today()

st.session_state.setdefault("geometry_text", "")
st.session_state.setdefault("intersecting_tiles", [])
st.session_state.setdefault("selected_tiles", [])
st.session_state.setdefault("start_date", today - dt.timedelta(days=7))
st.session_state.setdefault("end_date", today)
st.session_state.setdefault("map_center", (48.8566, 2.3522))
st.session_state.setdefault("map_zoom", 8)
st.session_state.setdefault("map_bounds", None)

# Anti-loop guards
st.session_state.setdefault("last_click_popup", None)
st.session_state.setdefault("last_aoi_wkt", "")

# Grid options (safer defaults)
st.session_state.setdefault("show_grid", True)
st.session_state.setdefault("show_nocov", False)
st.session_state.setdefault("show_intersects", True)
st.session_state.setdefault("show_selected", True)
st.session_state.setdefault("colorize_grid", True)
st.session_state.setdefault("grid_opacity", 0.03)
st.session_state.setdefault("max_grid_features", 800)     # lower default to reduce lag
st.session_state.setdefault("simplify_tol", 0.002)        # slightly higher default to reduce lag
st.session_state.setdefault("click_to_select", False)

# Grid cache + auto refresh control (prevents zoom/pan loops)
st.session_state.setdefault("grid_cache_bbox_key", None)
st.session_state.setdefault("grid_cache_tiles", None)
st.session_state.setdefault("grid_cache_nocov", None)
st.session_state.setdefault("grid_cache_updated_ts", 0.0)
st.session_state.setdefault("grid_cache_params", None)
st.session_state.setdefault("grid_auto_refresh", False)
st.session_state.setdefault("grid_last_refresh_ts", 0.0)


# -------------------------
# UI styling
# -------------------------
st.markdown(
    """
    <style>
      :root { --card: rgba(255,255,255,0.92); --border: rgba(0,0,0,0.08); }
      .app-title { font-size: 1.35rem; font-weight: 700; }
      .app-sub { color: rgba(0,0,0,0.55); margin-top: -6px; }
      .card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 16px 16px 12px 16px;
        box-shadow: 0 6px 20px rgba(0,0,0,0.06);
        margin-bottom: 14px;
      }
      .section-title { font-weight: 650; font-size: 1.05rem; margin-bottom: 6px; }
      .section-subtitle { font-size: 0.85rem; color: rgba(0,0,0,0.55); margin-bottom: 10px; }
      .muted { color: rgba(0,0,0,0.6); font-size: 0.9rem; }
      .stTextInput input, .stDateInput input, textarea, select { border-radius: 10px !important; }
      .stButton > button { border-radius: 12px; padding: 0.55rem 0.9rem; }
      code { font-size: 0.9em; }
    </style>
    """,
    unsafe_allow_html=True,
)

hdr_l, hdr_r = st.columns([3, 1])
with hdr_l:
    st.markdown('<div class="app-title">Satellite Imagery Downloader</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-sub">AOI + Grid Explorer (Sentinel-2) + download via CLI.</div>',
        unsafe_allow_html=True,
    )
with hdr_r:
    st.write("")


# -------------------------
# Providers / satellites
# -------------------------
satellite_options = {
    "Copernicus": ["SENTINEL-1", "SENTINEL-2", "SENTINEL-3", "SENTINEL-5P"],
    "USGS": ["landsat_ot_c2_l1", "landsat_ot_c2_l2"],
    "OpenTopography": [
        "SRTMGL3 (SRTM GL3 90m)",
        "SRTMGL1 (SRTM GL1 30m)",
        "SRTMGL1_E (SRTM GL1 Ellipsoidal 30m)",
        "AW3D30 (ALOS World 3D 30m)",
        "AW3D30_E (ALOS World 3D Ellipsoidal 30m)",
        "SRTM15Plus (Global Bathymetry SRTM15+ V2.1 500m)",
        "NASADEM (NASADEM Global DEM)",
        "COP30 (Copernicus Global DSM 30m)",
        "COP90 (Copernicus Global DSM 90m)",
        "EU_DTM (DTM 30m)",
        "GEDI_L3 (DTM 1000m)",
        "GEBCOIceTopo (Global Bathymetry 500m)",
        "GEBCOSubIceTopo (Global Bathymetry 500m)",
        "CA_MRDEM_DSM (DSM 30m)",
        "CA_MRDEM_DTM (DTM 30m)",
    ],
    "CDS": [],
    "GoogleEarthEngine": [
        "COPERNICUS/S2_SR",
        "LANDSAT/LC08/C02/T1_L2",
        "MODIS/006/MOD13Q1",
        "USGS/SRTMGL1_003",
    ],
}

product_types_options = {
    "SENTINEL-1": ["RAW", "GRD", "SLC", "IW_SLC__1S"],
    "SENTINEL-2": ["S2MSI1C", "S2MSI2A"],
    "SENTINEL-3": [
        "S3OL1EFR",
        "S3OL1ERR",
        "S3SL1RBT",
        "S3OL2WFR",
        "S3OL2WRR",
        "S3OL2LFR",
        "S3OL2LRR",
        "S3SL2LST",
        "S3SL2FRP",
        "S3SR2LAN",
        "S3SY2SYN",
        "S3SY2VGP",
        "S3SY2VG1",
        "S3SY2V10",
        "S3SY2AOD",
    ],
    "SENTINEL-5P": ["L2__NO2___", "L2__CH4___", "L2__CO____", "L2__O3____", "L2__SO2___", "L2__HCHO__"],
    "landsat_ot_c2_l1": ["8L1TP", "8L1GT", "8L1GS", "9L1TP", "9L1GT", "9L1GS"],
    "landsat_ot_c2_l2": ["8L2SP", "8L2SR", "9L2SP", "9L2SR"],
}

sat_tiles = init()

tabs = st.tabs(["Configuration", "Results", "Settings"])

with tabs[0]:
    # Provider & Satellite
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Provider & Satellite</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Choose provider, satellite and product type.</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        provider = st.selectbox("Provider", list(satellite_options.keys()))
    with c2:
        satellite = st.selectbox("Satellite", satellite_options.get(provider, []))
    with c3:
        product_type = st.selectbox("Product Type", product_types_options.get(satellite, []))
    st.markdown("</div>", unsafe_allow_html=True)

    tiles_gdf = sat_tiles.get("SENTINEL-2") if satellite == "SENTINEL-2" else None
    nocov_gdf = sat_tiles.get("_S2_NOCOV") if satellite == "SENTINEL-2" else None
    name_col = _tile_name_column(tiles_gdf) if satellite == "SENTINEL-2" else None

    # AOI + Grid Explorer
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Area of Interest & Grid Explorer (Sentinel-2)</div>', unsafe_allow_html=True)

    left, right = st.columns([2.1, 1.0], gap="large")

    refresh_grid = False  # defined here so it's visible later

    with right:
        st.markdown(
            '<div class="section-subtitle">AOI input + grid options + tile search/selection.</div>',
            unsafe_allow_html=True,
        )

        aoi_mode = st.radio(
            "AOI input method",
            ["Draw on map", "Preset square", "Paste WKT/GeoJSON"],
            horizontal=False,
        )

        if aoi_mode == "Preset square":
            cc1, cc2 = st.columns(2)
            with cc1:
                center_lat = st.number_input(
                    "Center lat",
                    value=float(st.session_state["map_center"][0]),
                    format="%.6f",
                )
            with cc2:
                center_lng = st.number_input(
                    "Center lng",
                    value=float(st.session_state["map_center"][1]),
                    format="%.6f",
                )
            side_km = st.number_input("Square side (km)", min_value=0.1, value=25.0, step=5.0)
            if st.button("Use square AOI", use_container_width=True):
                sq_wkt = _square_wkt(center_lat, center_lng, side_km)
                st.session_state["geometry_text"] = sq_wkt
                st.session_state["map_center"] = (center_lat, center_lng)
                st.rerun()

        if aoi_mode == "Paste WKT/GeoJSON":
            st.session_state["geometry_text"] = st.text_area(
                "WKT or GeoJSON",
                value=st.session_state.get("geometry_text", ""),
                height=140,
            )

        st.divider()
        st.markdown("**Grid Explorer options (Sentinel-2)**")
        if satellite != "SENTINEL-2":
            st.info("Select **SENTINEL-2** to enable the grid explorer.")
        else:
            if tiles_gdf is None or name_col is None:
                st.error(
                    "Sentinel-2 grid data not loaded.\n\n"
                    f"Expected GeoJSON: `{S2_TILES_GEOJSON}`\n"
                    f"Optional: `{S2_NOCOV_GEOJSON}`"
                )
            else:
                st.session_state["show_grid"] = st.checkbox(
                    "Show grid overlay (cached)",
                    value=st.session_state["show_grid"],
                    help="Overlay is cached for the last refreshed view to avoid flicker/loops.",
                )
                st.session_state["colorize_grid"] = st.checkbox(
                    "Colorize by UTM column",
                    value=st.session_state["colorize_grid"],
                )
                st.session_state["show_nocov"] = st.checkbox(
                    "Show no-coverage",
                    value=st.session_state["show_nocov"],
                    disabled=(nocov_gdf is None),
                )
                st.session_state["show_intersects"] = st.checkbox(
                    "Show intersecting tiles",
                    value=st.session_state["show_intersects"],
                )
                st.session_state["show_selected"] = st.checkbox(
                    "Show selected tiles",
                    value=st.session_state["show_selected"],
                )
                st.session_state["click_to_select"] = st.checkbox(
                    "Click a tile to select (popup)",
                    value=st.session_state["click_to_select"],
                    help="Uses popup text; selection is toggled on click.",
                )

                st.session_state["max_grid_features"] = int(
                    st.number_input(
                        "Max grid features rendered",
                        min_value=200,
                        max_value=8000,
                        value=int(st.session_state["max_grid_features"]),
                        step=200,
                    )
                )
                st.session_state["grid_opacity"] = float(
                    st.slider(
                        "Grid fill opacity",
                        min_value=0.0,
                        max_value=0.2,
                        value=float(st.session_state["grid_opacity"]),
                        step=0.01,
                    )
                )
                st.session_state["simplify_tol"] = float(
                    st.slider(
                        "Geometry simplify (deg)",
                        min_value=0.0,
                        max_value=0.02,
                        value=float(st.session_state["simplify_tol"]),
                        step=0.001,
                    )
                )

                # Invalidate cache if important params changed (prevents mismatched overlays)
                curr_params = (int(st.session_state["max_grid_features"]), float(st.session_state["simplify_tol"]))
                if st.session_state.get("grid_cache_params") != curr_params:
                    st.session_state["grid_cache_params"] = curr_params
                    st.session_state["grid_cache_tiles"] = None
                    st.session_state["grid_cache_nocov"] = None
                    st.session_state["grid_cache_bbox_key"] = None

                refresh_grid = st.button("Update grid overlay for current view", use_container_width=True)
                st.session_state["grid_auto_refresh"] = st.checkbox(
                    "Auto refresh grid (throttled)",
                    value=st.session_state["grid_auto_refresh"],
                    help="Refreshes at most once per second when bounds change.",
                )

                if st.session_state.get("grid_cache_bbox_key") is None:
                    st.caption("Grid cache: empty (click **Update grid overlay**).")
                else:
                    age = time.time() - float(st.session_state.get("grid_cache_updated_ts", 0.0))
                    st.caption(f"Grid cache: bbox={st.session_state['grid_cache_bbox_key']} (age {age:.1f}s)")

        st.divider()
        st.markdown("**Tile search & selection**")

        if satellite == "SENTINEL-2" and tiles_gdf is not None and name_col:
            query = st.text_input("Search tile id (e.g. 34UED)", value="", placeholder="Type an ID or partial…")

            matches = _find_tiles_by_query(tiles_gdf, name_col, query, limit=50) if query else tiles_gdf.iloc[0:0]
            match_ids = matches[name_col].astype(str).tolist() if not matches.empty else []

            if query and not match_ids:
                st.caption("No match.")
            elif query and match_ids:
                picked = st.selectbox("Matches", match_ids, index=0)
                b1, b2, b3 = st.columns(3)
                with b1:
                    if st.button("Add", use_container_width=True):
                        sel = set(map(str, st.session_state["selected_tiles"]))
                        sel.add(picked)
                        st.session_state["selected_tiles"] = sorted(sel)
                        st.rerun()
                with b2:
                    if st.button("Replace", use_container_width=True):
                        st.session_state["selected_tiles"] = [picked]
                        st.rerun()
                with b3:
                    if st.button("Zoom", use_container_width=True):
                        row = tiles_gdf[tiles_gdf[name_col].astype(str) == str(picked)]
                        if not row.empty:
                            c = row.iloc[0].geometry.centroid
                            st.session_state["map_center"] = (float(c.y), float(c.x))
                            st.session_state["map_zoom"] = max(int(st.session_state["map_zoom"]), 10)
                            st.rerun()

            current_sel = st.multiselect(
                "Selected tiles",
                options=tiles_gdf[name_col].astype(str).unique().tolist(),
                default=st.session_state["selected_tiles"],
            )
            st.session_state["selected_tiles"] = current_sel

            if st.button("Clear selection", use_container_width=True):
                st.session_state["selected_tiles"] = []
                st.rerun()

        st.divider()
        st.markdown("**AOI used for download**")
        st.text_area(
            "AOI (WKT/GeoJSON)",
            value=st.session_state.get("geometry_text", "") or "No AOI yet.",
            height=120,
        )

    # AOI geometry
    aoi_geom = _parse_text_geometry(st.session_state.get("geometry_text", ""))
    aoi_polys: List[Polygon] = []
    if aoi_geom is not None and not aoi_geom.is_empty:
        if aoi_geom.geom_type == "Polygon":
            aoi_polys = [aoi_geom]
        elif aoi_geom.geom_type == "MultiPolygon":
            aoi_polys = list(aoi_geom.geoms)

    tile_names, intersects_gdf = (
        _compute_intersections(aoi_polys, tiles_gdf) if satellite == "SENTINEL-2" else ([], None)
    )
    st.session_state["intersecting_tiles"] = tile_names or []

    # Selected GeoDataFrame (minimal columns)
    selected_gdf = None
    if satellite == "SENTINEL-2" and tiles_gdf is not None and name_col and st.session_state["selected_tiles"]:
        selset = set(map(str, st.session_state["selected_tiles"]))
        selected_gdf = tiles_gdf[tiles_gdf[name_col].astype(str).isin(selset)][[name_col, "geometry"]].copy()
        if selected_gdf.empty:
            selected_gdf = None

    # If user clicked refresh grid: compute cache immediately from last known bounds (no extra rerun loop)
    if (
        refresh_grid
        and satellite == "SENTINEL-2"
        and tiles_gdf is not None
        and name_col is not None
        and st.session_state.get("show_grid", False)
    ):
        bbox_pre = _bounds_from_leaflet(st.session_state.get("map_bounds"))
        if bbox_pre is None:
            center_lat, center_lng = st.session_state["map_center"]
            bbox_pre = _fallback_bbox(center_lat, center_lng, int(st.session_state["map_zoom"]))
        _update_grid_cache(tiles_gdf, nocov_gdf, name_col, bbox_pre)

    # Use cached subsets only (prevents flicker + heavy recompute on every pan/zoom)
    tiles_visible = st.session_state.get("grid_cache_tiles")
    nocov_visible = st.session_state.get("grid_cache_nocov")

    center_lat, center_lng = st.session_state["map_center"]
    zoom = int(st.session_state["map_zoom"])

    show_grid_effective = (
        satellite == "SENTINEL-2"
        and bool(st.session_state.get("show_grid", False))
        and zoom >= MIN_GRID_ZOOM
        and tiles_visible is not None
        and not getattr(tiles_visible, "empty", True)
    )
    show_nocov_effective = (
        satellite == "SENTINEL-2"
        and bool(st.session_state.get("show_nocov", False))
        and nocov_visible is not None
        and not getattr(nocov_visible, "empty", True)
    )

    with left:
        if satellite == "SENTINEL-2" and st.session_state.get("show_grid", False) and zoom < MIN_GRID_ZOOM:
            st.info(f"Zoom in to at least {MIN_GRID_ZOOM} to render the grid (current zoom: {zoom}).")

        if satellite == "SENTINEL-2" and st.session_state.get("show_grid", False) and st.session_state.get("grid_cache_bbox_key") is None:
            st.info("Grid overlay is cached. Click **Update grid overlay for current view** to render it.")

        m = _build_map(
            center_lat=center_lat,
            center_lng=center_lng,
            zoom=zoom,
            aoi_geom=aoi_geom,
            tiles_visible=tiles_visible if show_grid_effective else None,
            nocov_visible=nocov_visible if show_nocov_effective else None,
            intersects_gdf=intersects_gdf,
            selected_gdf=selected_gdf,
            show_grid=show_grid_effective,
            show_nocov=show_nocov_effective,
            show_intersects=bool(st.session_state["show_intersects"]) and satellite == "SENTINEL-2",
            show_selected=bool(st.session_state["show_selected"]) and satellite == "SENTINEL-2",
            colorize_grid=bool(st.session_state["colorize_grid"]),
            grid_opacity=float(st.session_state["grid_opacity"]),
            name_col=name_col,
        )

        returned = ["all_drawings", "bounds", "zoom", "center"]
        if satellite == "SENTINEL-2" and st.session_state["click_to_select"]:
            returned.append("last_object_clicked_popup")

        map_data = st_folium(
            m,
            key="drawing_map",
            width="100%",
            height=560,
            returned_objects=returned,
        )

        # Persist view info (tolerant; avoids jitter loops)
        if map_data and map_data.get("bounds"):
            b = _bounds_from_leaflet(map_data["bounds"])
            if b is not None:
                st.session_state["map_bounds"] = map_data["bounds"]

        _maybe_update_center_zoom(map_data, eps=1e-4)

        # Auto-refresh grid cache (throttled) AFTER map event only if enabled
        if (
            satellite == "SENTINEL-2"
            and tiles_gdf is not None
            and name_col is not None
            and st.session_state.get("show_grid", False)
            and st.session_state.get("grid_auto_refresh", False)
        ):
            bbox_now = _bounds_from_leaflet(map_data.get("bounds") if map_data else None)
            if bbox_now is not None:
                k_now = _bbox_key(bbox_now)
                k_cached = st.session_state.get("grid_cache_bbox_key")
                now = time.time()
                if k_now != k_cached and (now - float(st.session_state.get("grid_last_refresh_ts", 0.0))) >= GRID_AUTO_REFRESH_THROTTLE_SEC:
                    st.session_state["grid_last_refresh_ts"] = now
                    _update_grid_cache(tiles_gdf, nocov_gdf, name_col, bbox_now)
                    # Single rerun to re-render with updated overlay (throttled)
                    st.rerun()

        # Draw AOI on map -> update geometry_text with stable WKT and rerun (only if changed)
        if aoi_mode == "Draw on map":
            drawn_polys = _parse_map_drawings(map_data)
            if drawn_polys:
                aoi_union = _union_geoms(drawn_polys)
                new_wkt = shapely_wkt.dumps(aoi_union, rounding_precision=6)
                if new_wkt and new_wkt != st.session_state.get("last_aoi_wkt", ""):
                    st.session_state["last_aoi_wkt"] = new_wkt
                    st.session_state["geometry_text"] = new_wkt
                    st.rerun()

        # Click-to-select (anti-loop: process only if popup value changed)
        if satellite == "SENTINEL-2" and st.session_state["click_to_select"] and map_data:
            popup = map_data.get("last_object_clicked_popup")
            if popup and popup != st.session_state.get("last_click_popup"):
                st.session_state["last_click_popup"] = popup
                m_id = re.search(r"\b\d{2}[A-Z]{3}\b", str(popup).upper())
                if m_id:
                    tid = m_id.group(0)
                    sel = set(map(str, st.session_state["selected_tiles"]))
                    if tid in sel:
                        sel.remove(tid)
                    else:
                        sel.add(tid)
                    st.session_state["selected_tiles"] = sorted(sel)
                    st.rerun()

        # Intersections + exports
        if satellite == "SENTINEL-2" and tiles_gdf is not None and name_col:
            st.markdown("**Intersecting tiles (current AOI)**")
            if st.session_state["intersecting_tiles"]:
                st.write(", ".join(st.session_state["intersecting_tiles"]))
                csv = "tile\n" + "\n".join(st.session_state["intersecting_tiles"])
                st.download_button(
                    "Download intersects CSV",
                    data=csv,
                    file_name="s2_intersecting_tiles.csv",
                    mime="text/csv",
                )
            else:
                st.caption("No intersections (or no AOI).")

            if st.session_state["selected_tiles"]:
                st.markdown("**Selected tiles exports**")
                csv2 = "tile\n" + "\n".join(map(str, st.session_state["selected_tiles"]))
                st.download_button(
                    "Download selection CSV",
                    data=csv2,
                    file_name="s2_selected_tiles.csv",
                    mime="text/csv",
                )

    st.markdown("</div>", unsafe_allow_html=True)

    # Time Range
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Time Range</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Start date ≤ end date, and no future dates.</div>',
        unsafe_allow_html=True,
    )

    d1, d2 = st.columns(2)
    with d1:
        start_date = st.date_input(
            "Start Date",
            value=st.session_state["start_date"],
            max_value=today,
            key="start_date_input",
        )
    with d2:
        end_date = st.date_input(
            "End Date",
            value=st.session_state["end_date"],
            min_value=start_date,
            max_value=today,
            key="end_date_input",
        )

    if end_date < start_date:
        end_date = start_date
        st.session_state["end_date_input"] = end_date
        st.warning("End Date was before Start Date; adjusted automatically.")

    st.session_state["start_date"] = start_date
    st.session_state["end_date"] = end_date
    st.markdown("</div>", unsafe_allow_html=True)

    # Download
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Download</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Runs CLI in background and streams logs from <code>nohup.out</code>.</div>',
        unsafe_allow_html=True,
    )

    aoi_text = st.session_state.get("geometry_text", "").strip()
    aoi_is_geojson = aoi_text.startswith("{")
    aoi_file = "example_aoi.geojson" if aoi_is_geojson else "example_aoi.wkt"

    cli_cmd = (
        f"python cli.py "
        f"--provider {provider.lower()} "
        f"--collection {satellite.split(' ')[0]} "
        f"--product-type {product_type} "
        f"--start-date {start_date} "
        f"--end-date {end_date} "
        f"--aoi_file {aoi_file}"
    )

    st.code(cli_cmd, language="bash")

    if st.button("Download Products", use_container_width=True):
        if not aoi_text:
            st.error("Please provide a valid AOI (draw/paste/preset square).")
        elif not start_date or not end_date:
            st.error("Please specify both start and end dates.")
        else:
            with open(aoi_file, "w") as f:
                f.write(aoi_text)

            open("nohup.out", "w").close()
            os.system(f"nohup {cli_cmd} &")
            show_live_logs()

    st.markdown("</div>", unsafe_allow_html=True)

with tabs[1]:
    def sort(files):
        return sorted(files, key=lambda x: x["size"])

    _ = st_file_browser(
        os.path.join("downloads"),
        file_ignores=None,
        key="A",
        show_choose_file=True,
        show_choose_folder=True,
        show_delete_file=True,
        show_download_file=True,
        show_new_folder=True,
        show_upload_file=True,
        show_rename_file=True,
        show_rename_folder=True,
        use_cache=True,
        sort=sort,
    )

with tabs[2]:
    with open("config.yaml", "r") as config_file:
        config_content = config_file.read()
    st.code(config_content, language="yaml")
