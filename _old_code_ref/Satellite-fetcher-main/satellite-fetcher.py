import os
import re
from pathlib import Path

import folium
import geopandas as gpd
import shapely
import streamlit as st
from loguru import logger
from shapely.geometry import Polygon
from streamlit_file_browser import st_file_browser
from streamlit_folium import st_folium
from utilities import ConfigLoader


# --- Live log function (tail -f alike for Streamlit) ---
@st.fragment(run_every="2000ms")  # refresh every 2s
def show_live_logs(log_path="nohup.out"):
    log_path = Path(log_path)
    batch_re = re.compile(
        r"^Concurrent Downloads:\s*"  # Description before colon
        r"(?P<percent>\d+)%\|\s*[^\|]*\|\s*"  # Percent and bar (allowing any content between |)
        r"(?P<done>\d+)/(?P<total>\d+)\s*"  # Done/Total tasks
        r"\[\s*(?P<elapsed>[0-9:?]+)<(?P<eta>[^\]]+)\]\s*"  # Elapsed and ETA/remaining (anything until ])
        r"(?P<rate>[^\s]*/?[^\s]*?)?\s*$"  # Optional rate like '?it/s' or '5.00it/s'
    )

    # New regex for download progress bars (the ones in your log)
    download_re = re.compile(
        r"^Downloading\s+(?P<filename>.+?):\s*"  # Filename
        r"(?P<percent>\d+)%\|\s*.*?\|\s*"  # Percent + bar
        r"(?P<done>[\d\.]+[kMGTP]?)/(?P<total>[\d\.]+[kMGTP]?)\s*"  # Done/Total with units
        r"\[(?P<elapsed>[0-9:]+)<(?P<eta>[0-9:?\-]+)\]"  # Elapsed and ETA
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
                # Collect non-matching lines to display as plain logs if wanted
                if line:
                    non_progress_lines.append(line)
        # Render all detected progress bars
        for desc, pb in progress_bars_info.items():
            st.write(pb["label"])
            st.progress(pb["percent"])
        # Optionally, display last 4 non-progress lines for context
        if non_progress_lines:
            st.markdown("#### Recent Logs")
            for l in non_progress_lines[-4:]:
                st.write(l)


def create_drawing_map(center_lat=0.0, center_lng=0.0, zoom=10, tiles_gdf=None):
    # Create the base map
    m = folium.Map(
        location=[center_lat, center_lng], zoom_start=zoom, tiles="OpenStreetMap"
    )
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite",
        overlay=False,
        control=True,
    ).add_to(m)

    # Add tile boundaries if available
    if tiles_gdf is not None:
        folium.GeoJson(
            tiles_gdf,
            name="All Tiles",
            style_function=lambda x: {"color": "gray", "weight": 1, "fillOpacity": 0},
        ).add_to(m)

    # Add drawing tools
    draw = folium.plugins.Draw(
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

    # Add layer control
    folium.LayerControl().add_to(m)

    return m


def init():
    # Load the shapefile
    shapefile_path = "data/Sentinel-2-tiles/sentinel_2_index_shapefile.shp"
    sentinel2_tiles = gpd.read_file(shapefile_path)
    return {"SENTINEL-2": sentinel2_tiles}


# ---------- PAGE CONFIG ----------
st.set_page_config(page_title="Satellite Imagery Downloader", layout="wide")
# Here you would call the function to download products based on the selected options
configuration = ConfigLoader(config_file_path="config.yaml")
logger.info("Configuration loaded successfully.")
# Initialize session state
if "geometry" not in st.session_state:
    st.session_state["geometry"] = ""

# ---------- CUSTOM SVG ----------
satellite_icon_svg = """
    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" 
    viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" 
    stroke-linecap="round" stroke-linejoin="round">
    <path d="M13 7 9 3 5 7l4 4"></path>
    <path d="m17 11 4 4-4 4-4-4"></path>
    <path d="m8 12 4 4 6-6-4-4Z"></path>
    <path d="m16 8 3-3"></path>
    <path d="M9 21a6 6 0 0 0-6-6"></path>
    </svg>
"""

geometry_icon_svg = """
<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-map-pin h-5 w-5"><path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"></path><circle cx="12" cy="10" r="3"></circle></svg>
"""

calendar_icon_svg = """
<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-calendar h-5 w-5"><path d="M8 2v4"></path><path d="M16 2v4"></path><rect width="18" height="18" x="3" y="4" rx="2"></rect><path d="M3 10h18"></path></svg>"""

sat_tiles = init()

# ---------- CSS STYLES ----------
st.markdown(
    """
    <style>
    /* Card styling */
    .card {
        background-color: white;
        border-radius: 15px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0px 2px 5px rgba(0,0,0,0.5);
    }

    /* Section headers */
    .section-title {
        font-weight: 600;
        font-size: 1.1rem;
        margin-bottom: 8px;
    }
    .section-subtitle {
        font-size: 0.85rem;
        color: #777;
        margin-bottom: 15px;
    }

    /* Input fields */
    .stTextInput input, .stDateInput input, textarea, select {
        border-radius: 8px !important;
    }
    </style>
""",
    unsafe_allow_html=True,
)

# ---------- VARIABLES ----------
# Provider → Satellite mapping
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
# Product types for each satellite
# This can be extended based on actual product types available for each satellite
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

# ---------- TABS ----------
tabs = st.tabs(["Configuration", "Results", "Settings"])

with tabs[0]:
    with st.container(border=True):
        # Provider & Satellite Selection
        st.markdown(
            f'<div class="section-title">{satellite_icon_svg} Provider & Satellite Selection</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="section-subtitle">Choose your satellite data provider and specific satellite</div>',
            unsafe_allow_html=True,
        )
        col1, col2, col3 = st.columns(3)
        with col1:
            provider = st.selectbox("Provider", list(satellite_options.keys()))
        with col2:
            satellite = st.selectbox("Satellite", satellite_options.get(provider, []))
        with col3:
            product_type = st.selectbox(
                "Product Type", product_types_options.get(satellite, [])
            )
    with st.container(border=True):
        # Geographic Area
        tiles_gdf = sat_tiles.get(satellite)
        drawing_map = create_drawing_map(
            center_lat=12.193479,
            center_lng=123.326770,
            zoom=5,
            tiles_gdf=tiles_gdf,
        )
        st.markdown(
            f'<div class="section-title">{geometry_icon_svg} Geographic Area</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="section-subtitle">Define the area of interest using GeoJSON or WKT format</div>',
            unsafe_allow_html=True,
        )
        # Display the map and capture interactions
        map_data = st_folium(
            drawing_map,
            key="drawing_map",
            width="100%",
            height=500,
            returned_objects=["all_drawings"],
        )
        # Process and display polygon data
        if map_data["all_drawings"] is not None and len(map_data["all_drawings"]) > 0:
            # Extract polygons from the drawing data
            current_polygons = []
            for feature in map_data["all_drawings"]:
                if feature["geometry"]["type"] in ["Polygon", "Rectangle"]:
                    coordinates = feature["geometry"]["coordinates"][
                        0
                    ]  # Get outer ring
                    current_polygons.append(
                        {
                            "type": feature["geometry"]["type"],
                            "coordinates": coordinates,
                            "properties": feature.get("properties", {}),
                        }
                    )

            # Create Shapely polygons and get WKT strings
            wkt_polygons = []
            for poly_info in current_polygons:
                try:
                    polygon = Polygon(poly_info["coordinates"])
                    wkt_polygons.append(polygon.wkt)
                except ValueError as e:
                    wkt_polygons.append(f"# Error creating polygon: {e}")

            # Update session state
            st.session_state.polygons = current_polygons
            st.session_state.polygons_wkt = wkt_polygons

            # Find intersecting tiles
            tiles_gdf = sat_tiles.get(satellite)
            intersecting_tiles = []
            if tiles_gdf is not None:
                for poly_info in current_polygons:
                    try:
                        polygon = Polygon(poly_info["coordinates"])
                        intersects = tiles_gdf[tiles_gdf.intersects(polygon)]
                        intersecting_tiles.extend(intersects["Name"].tolist())
                    except ValueError:
                        pass
            st.session_state.intersecting_tiles = list(set(intersecting_tiles))

            # Add intersecting tiles layer to the map
            if intersecting_tiles:
                intersects_gdf = tiles_gdf[tiles_gdf["Name"].isin(intersecting_tiles)]
                folium.GeoJson(
                    intersects_gdf,
                    name="Intersecting Tiles",
                    style_function=lambda x: {
                        "color": "red",
                        "weight": 3,
                        "fillColor": "red",
                        "fillOpacity": 0.3,
                    },
                ).add_to(drawing_map)
                # Update the map display with intersecting tiles
                st_folium(
                    drawing_map,
                    key="drawing_map",
                    width="100%",
                    height=500,
                    returned_objects=["all_drawings"],
                )

            # Display WKT data in text area
            if wkt_polygons:
                geometries = st.text_area(
                    "Polygons in WKT or GeoJSON",
                    value="\n".join(wkt_polygons),
                    height=100,
                    key="polygon_data",
                )
            else:
                geometries = st.text_area(
                    "Polygons in WKT or GeoJSON",
                    value="No polygons drawn yet. Start drawing on the map!",
                    height=100,
                    key="empty_polygon_data",
                )
        else:
            geometries = st.text_area(
                "Polygons in WKT or GeoJSON",
                value="No polygons drawn yet. Start drawing on the map!",
                height=100,
                key="no_polygon_data",
            )

    with st.container(border=True):
        # Time Range
        st.markdown(
            f'<div class="section-title">{calendar_icon_svg} Time Range</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="section-subtitle">Specify the date range for satellite imagery</div>',
            unsafe_allow_html=True,
        )
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")
    with st.container(border=False, horizontal_alignment="center"):
        # Download Button
        if st.button("Download Products"):
            if not geometries:
                st.error("Please provide a valid geometry.")
            elif not start_date or not end_date:
                st.error("Please specify both start and end dates.")
            else:
                # save the geometry in file depending on the kind wkt or geojson
                if geometries.strip().startswith("{"):
                    # GeoJSON format
                    with open("example_aoi.geojson", "w") as geojson_file:
                        geojson_file.write(geometries)
                else:
                    # WKT format
                    with open("example_aoi.wkt", "w") as wkt_file:
                        wkt_file.write(geometries)
                # empty nohup.out file
                open("nohup.out", "w").close()
                # call the cli script with the appropriate arguments
                os.system(
                    f"nohup python cli.py --provider {provider.lower()} --collection {satellite.split(' ')[0]} --product-type {product_type} --start-date {start_date} --end-date {end_date} &"
                )
                # Show logs live like tail -f
                show_live_logs()

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
    # show the content of config.yaml
    with open("config.yaml", "r") as config_file:
        config_content = config_file.read()
    st.code(config_content, language="yaml")
