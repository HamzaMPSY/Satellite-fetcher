# satellite-fetcher.py
import os
import re
import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple

import folium
from folium import plugins
import geopandas as gpd
import shapely
import streamlit as st
from loguru import logger
from shapely.geometry import Polygon, shape, mapping
from shapely import wkt as shapely_wkt
from streamlit_file_browser import st_file_browser
from streamlit_folium import st_folium

from utilities import ConfigLoader


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
# Tiles + geometry helpers
# -------------------------
def _load_sentinel2_tiles(shapefile_path: str) -> Optional[gpd.GeoDataFrame]:
    p = Path(shapefile_path)
    if not p.exists():
        st.warning(
            "Sentinel-2 tiles shapefile not found.\n\n"
            f"Expected: `{shapefile_path}`\n\n"
            "Create it by placing the shapefile pack here (at minimum: .shp .shx .dbf .prj)."
        )
        return None

    try:
        gdf = gpd.read_file(shapefile_path)

        # Ensure geometry
        if gdf is None or gdf.empty:
            st.warning("Sentinel-2 tiles file loaded but appears empty.")
            return None

        # Ensure CRS is EPSG:4326 for folium/shapely lat/lon
        if gdf.crs is None:
            # If unknown, assume WGS84; adjust if your dataset is different.
            gdf = gdf.set_crs(epsg=4326)
        else:
            gdf = gdf.to_crs(epsg=4326)

        return gdf
    except Exception as e:
        st.warning(f"Failed to load Sentinel-2 tiles shapefile: {e}")
        return None


def _tile_name_column(gdf: gpd.GeoDataFrame) -> Optional[str]:
    candidates = ["Name", "name", "TILE_ID", "tile_id", "utm_zone", "MGRS_TILE", "mgrs"]
    for c in candidates:
        if c in gdf.columns:
            return c
    # last resort: any string column
    for c in gdf.columns:
        if c != "geometry" and gdf[c].dtype == object:
            return c
    return None


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

            gtype = geom.get("type")
            if gtype == "Polygon":
                # GeoJSON coords are [lng, lat]
                coords = geom["coordinates"][0]
                poly = Polygon(coords)
                if poly.is_valid and not poly.is_empty:
                    polys.append(poly)

            # Some Draw plugins may label rectangles as Polygon anyway; ignore custom "Rectangle" type.
        except Exception:
            continue

    return polys


def _compute_intersections(
    aoi_polys: List[Polygon],
    tiles_gdf: Optional[gpd.GeoDataFrame],
) -> Tuple[List[str], Optional[gpd.GeoDataFrame]]:
    if tiles_gdf is None or not aoi_polys:
        return [], None

    name_col = _tile_name_column(tiles_gdf)
    if name_col is None:
        st.warning("Tiles file has no usable name/id column; cannot list tile names.")
        return [], None

    # Union AOI to reduce intersection calls
    aoi_union = shapely.union_all(aoi_polys)

    try:
        # Spatial filter (fast path if spatial index available)
        candidates = tiles_gdf[tiles_gdf.intersects(aoi_union)]
        if candidates.empty:
            return [], candidates

        tile_names = candidates[name_col].astype(str).unique().tolist()
        tile_names.sort()
        return tile_names, candidates
    except Exception:
        return [], None


def _square_wkt(center_lat: float, center_lng: float, side_km: float) -> str:
    # Approx conversion: 1 deg lat ~ 111 km; 1 deg lon ~ 111 km * cos(lat)
    half_km = side_km / 2.0
    dlat = half_km / 111.0
    dlon = half_km / (111.0 * max(0.05, abs(shapely.cos(shapely.radians(center_lat)))))

    # WKT expects (lng lat)
    lng1, lng2 = center_lng - dlon, center_lng + dlon
    lat1, lat2 = center_lat - dlat, center_lat + dlat
    poly = Polygon([(lng1, lat1), (lng2, lat1), (lng2, lat2), (lng1, lat2), (lng1, lat1)])
    return poly.wkt


def _parse_text_geometry(text: str) -> Optional[shapely.Geometry]:
    if not text or not text.strip():
        return None
    t = text.strip()

    # GeoJSON
    if t.startswith("{"):
        try:
            import json
            obj = json.loads(t)
            # Feature / FeatureCollection / Geometry
            if "type" in obj and obj["type"] == "Feature":
                return shape(obj["geometry"])
            if "type" in obj and obj["type"] == "FeatureCollection":
                # take union of all geometries
                geoms = [shape(f["geometry"]) for f in obj.get("features", []) if f.get("geometry")]
                if not geoms:
                    return None
                return shapely.union_all(geoms)
            return shape(obj)
        except Exception:
            return None

    # WKT
    try:
        return shapely_wkt.loads(t)
    except Exception:
        return None


def _build_map(
    center_lat: float,
    center_lng: float,
    zoom: int,
    aoi_geom: Optional[shapely.Geometry],
    tiles_intersects_gdf: Optional[gpd.GeoDataFrame],
    show_tiles_layer: bool,
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

    # AOI (current)
    if aoi_geom is not None and not aoi_geom.is_empty:
        folium.GeoJson(
            mapping(aoi_geom),
            name="AOI",
            style_function=lambda _: {"color": "#fbbf24", "weight": 3, "fillOpacity": 0.15},
        ).add_to(m)

    # Intersecting tiles only (recommended)
    if tiles_intersects_gdf is not None and not tiles_intersects_gdf.empty and show_tiles_layer:
        folium.GeoJson(
            tiles_intersects_gdf,
            name="Intersecting Tiles",
            style_function=lambda _: {"color": "#3b82f6", "weight": 2, "fillOpacity": 0.05},
        ).add_to(m)

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
    # Keep EXACT same path your code expects (no path change)
    shapefile_path = "data/Sentinel-2-tiles/sentinel_2_index_shapefile.shp"
    sentinel2_tiles = _load_sentinel2_tiles(shapefile_path)
    return {"SENTINEL-2": sentinel2_tiles}


# -------------------------
# Page config + state
# -------------------------
st.set_page_config(page_title="Satellite Imagery Downloader", layout="wide")

configuration = ConfigLoader(config_file_path="config.yaml")
logger.info("Configuration loaded successfully.")

today = dt.date.today()

if "geometry_text" not in st.session_state:
    st.session_state["geometry_text"] = ""
if "intersecting_tiles" not in st.session_state:
    st.session_state["intersecting_tiles"] = []
if "start_date" not in st.session_state:
    st.session_state["start_date"] = today - dt.timedelta(days=7)
if "end_date" not in st.session_state:
    st.session_state["end_date"] = today
if "map_center" not in st.session_state:
    st.session_state["map_center"] = (48.8566, 2.3522)  # default: Paris
if "map_zoom" not in st.session_state:
    st.session_state["map_zoom"] = 8
if "last_aoi_wkt" not in st.session_state:
    st.session_state["last_aoi_wkt"] = ""


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
        padding: 16px 16px 10px 16px;
        box-shadow: 0 6px 20px rgba(0,0,0,0.06);
        margin-bottom: 14px;
      }
      .section-title { font-weight: 650; font-size: 1.05rem; margin-bottom: 6px; }
      .section-subtitle { font-size: 0.85rem; color: rgba(0,0,0,0.55); margin-bottom: 10px; }
      .muted { color: rgba(0,0,0,0.6); font-size: 0.9rem; }
      .stTextInput input, .stDateInput input, textarea, select { border-radius: 10px !important; }
      .stButton > button { border-radius: 12px; padding: 0.55rem 0.9rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

hdr_l, hdr_r = st.columns([3, 1])
with hdr_l:
    st.markdown('<div class="app-title">Satellite Imagery Downloader</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-sub">Draw or paste an AOI, pick a time range, and download products.</div>',
        unsafe_allow_html=True,
    )
with hdr_r:
    st.write("")
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
        "AW3D30_E (ALOS World 3D Ellipsoidal, 30m)",
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
    "SENTINEL-5P": [
        "L2__NO2___",
        "L2__CH4___",
        "L2__CO____",
        "L2__O3____",
        "L2__SO2___",
        "L2__HCHO__",
    ],
    "landsat_ot_c2_l1": ["8L1TP", "8L1GT", "8L1GS", "9L1TP", "9L1GT", "9L1GS"],
    "landsat_ot_c2_l2": ["8L2SP", "8L2SR", "9L2SP", "9L2SR"],
}


# -------------------------
# Load tiles
# -------------------------
sat_tiles = init()


# -------------------------
# Tabs
# -------------------------
tabs = st.tabs(["Configuration", "Results", "Settings"])

with tabs[0]:
    # Provider & Satellite
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Provider & Satellite</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Choose your provider, satellite and product type.</div>',
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns(3)
    with c1:
        provider = st.selectbox("Provider", list(satellite_options.keys()))
    with c2:
        satellite = st.selectbox("Satellite", satellite_options.get(provider, []))
    with c3:
        product_type = st.selectbox("Product Type", product_types_options.get(satellite, []))
    st.markdown("</div>", unsafe_allow_html=True)

    # AOI
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Area of Interest</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Draw on map, paste WKT/GeoJSON, or create a preset square (Copernicus-like).</div>',
        unsafe_allow_html=True,
    )

    aoi_mode = st.radio(
        "AOI input method",
        ["Draw on map", "Preset square", "Paste WKT/GeoJSON"],
        horizontal=True,
    )

    # Tiles only for Sentinel-2
    tiles_gdf = sat_tiles.get("SENTINEL-2") if satellite == "SENTINEL-2" else None

    # Preset square
    if aoi_mode == "Preset square":
        cc1, cc2, cc3, cc4 = st.columns([1, 1, 1, 1])
        with cc1:
            center_lat = st.number_input("Center lat", value=float(st.session_state["map_center"][0]), format="%.6f")
        with cc2:
            center_lng = st.number_input("Center lng", value=float(st.session_state["map_center"][1]), format="%.6f")
        with cc3:
            side_km = st.number_input("Square side (km)", min_value=0.1, value=25.0, step=5.0)
        with cc4:
            if st.button("Use this square"):
                sq_wkt = _square_wkt(center_lat, center_lng, side_km)
                st.session_state["geometry_text"] = sq_wkt
                st.session_state["last_aoi_wkt"] = sq_wkt

        st.session_state["map_center"] = (center_lat, center_lng)

    # Paste
    if aoi_mode == "Paste WKT/GeoJSON":
        st.session_state["geometry_text"] = st.text_area(
            "WKT or GeoJSON",
            value=st.session_state.get("geometry_text", ""),
            height=110,
        )

    # Draw on map
    aoi_geom = _parse_text_geometry(st.session_state.get("geometry_text", ""))

    show_tiles_layer = st.toggle(
        "Show intersecting Sentinel-2 tiles on the map",
        value=True,
        help="Only works for Sentinel-2 if tiles shapefile is available.",
    )

    center_lat, center_lng = st.session_state["map_center"]
    zoom = st.session_state["map_zoom"]

    # If we already have an AOI text geometry, compute intersects
    tile_names, intersects_gdf = _compute_intersections(
        [aoi_geom] if aoi_geom is not None and aoi_geom.geom_type in ["Polygon", "MultiPolygon"] else [],
        tiles_gdf,
    )

    # Map (draw mode)
    if aoi_mode == "Draw on map":
        # Build map without drawings first
        m = _build_map(
            center_lat=center_lat,
            center_lng=center_lng,
            zoom=zoom,
            aoi_geom=aoi_geom,
            tiles_intersects_gdf=intersects_gdf,
            show_tiles_layer=show_tiles_layer,
        )

        map_data = st_folium(
            m,
            key="drawing_map",
            width="100%",
            height=520,
            returned_objects=["all_drawings"],
        )

        # Parse drawings into polygons
        drawn_polys = _parse_map_drawings(map_data)

        if drawn_polys:
            # Persist AOI = union of drawn polygons
            aoi_union = shapely.union_all(drawn_polys)
            st.session_state["last_aoi_wkt"] = aoi_union.wkt
            st.session_state["geometry_text"] = aoi_union.wkt
            aoi_geom = aoi_union

            tile_names, intersects_gdf = _compute_intersections(drawn_polys, tiles_gdf)

    # Geometry text area (always visible, reflects current AOI)
    if st.session_state.get("geometry_text"):
        st.text_area(
            "AOI (WKT/GeoJSON used for download)",
            value=st.session_state["geometry_text"],
            height=110,
            key="aoi_display",
        )
    else:
        st.text_area(
            "AOI (WKT/GeoJSON used for download)",
            value="No AOI yet. Draw a polygon/rectangle, paste WKT/GeoJSON, or use a preset square.",
            height=110,
            key="aoi_empty",
        )

    # Tile results (Sentinel-2)
    if satellite == "SENTINEL-2":
        if tiles_gdf is None:
            st.info("Sentinel-2 tiles overlay is disabled because the tiles shapefile is missing or failed to load.")
        else:
            if tile_names:
                st.markdown("**Intersecting tiles**")
                st.write(", ".join(tile_names))
            else:
                st.markdown('<span class="muted">No intersecting tiles detected (or no AOI).</span>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # Time Range
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Time Range</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">Start date must be ≤ end date. Dates cannot be in the future.</div>',
        unsafe_allow_html=True,
    )

    # Date constraints:
    # - start_date max = today
    # - end_date max = today
    # - end_date min = start_date
    # - if start_date > end_date, auto-fix end_date = start_date
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

    # Persist + safety
    if end_date < start_date:
        end_date = start_date
        st.session_state["end_date_input"] = end_date
        st.warning("End Date was before Start Date; it has been adjusted.")

    st.session_state["start_date"] = start_date
    st.session_state["end_date"] = end_date

    st.markdown("</div>", unsafe_allow_html=True)

    # Download
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Download</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-subtitle">This runs the CLI in background and streams logs from <code>nohup.out</code>.</div>',
        unsafe_allow_html=True,
    )

    # Build CLI command preview (Copernicus-like)
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
            # Save AOI to file (WKT or GeoJSON)
            if aoi_is_geojson:
                with open(aoi_file, "w") as f:
                    f.write(aoi_text)
            else:
                with open(aoi_file, "w") as f:
                    f.write(aoi_text)

            # Reset log file
            open("nohup.out", "w").close()

            # Run CLI in background
            os.system(f"nohup {cli_cmd} &")

            # Live logs
            show_live_logs()

    st.markdown("</div>", unsafe_allow_html=True)

with tabs[1]:

    def sort(files):
        return sorted(files, key=lambda x: x["size"])

    event = st_file_browser(
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
