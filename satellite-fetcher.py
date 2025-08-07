import streamlit as st
from streamlit_card import card
from loguru import logger
from providers import Copernicus
from utilities import ConfigLoader, GeometryHandler

# ---------- PAGE CONFIG ----------
st.set_page_config(page_title="Satellite Imagery Downloader", layout="wide")
# Here you would call the function to download products based on the selected options
configuration = ConfigLoader(config_file_path="config.yaml")
logger.info("Configuration loaded successfully.")

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


# ---------- CSS STYLES ----------
st.markdown("""
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
""", unsafe_allow_html=True)

# ---------- VARIABLES ----------
# Provider → Satellite mapping
satellite_options = {
    "Copernicus": ["SENTINEL-1", "SENTINEL-2", "SENTINEL-3", "SENTINEL-5P"],
    "USGS": ["LANDSAT-8", "LANDSAT-9", "MODIS"],
    "Others...": ["CustomSat A", "CustomSat B"]
}
# Product types for each satellite
# This can be extended based on actual product types available for each satellite
product_types_options = {
    "SENTINEL-1": ["SLC", "GRD", "GRDCOG", "OCN"],
    "SENTINEL-2": ["S2MSI1C", "S2MSI2A"],
    "SENTINEL-3": ["S3OL1EFR", "S3OL1ERR", "S3SL1RBT", "S3OL2WFR", "S3OL2WRR", "S3OL2LFR", "S3OL2LRR", "S3SL2LST", "S3SL2FRP", "S3SR2LAN", "S3SY2SYN", "S3SY2VGP", "S3SY2VG1", "S3SY2V10", "S3SY2AOD"],
    "SENTINEL-5P": ["L2__NO2___", "L2__CH4___", "L2__CO____", "L2__O3____", "L2__SO2___", "L2__HCHO__"],
    "LANDSAT-8": ["LANDSAT-8"],
    "LANDSAT-9": ["LANDSAT-9"],
    "MODIS": ["MODIS"]
}

# ---------- TABS ----------
tabs = st.tabs(["Configuration", "Results", "Settings"])

with tabs[0]:
    with st.container(border=True):
        # Provider & Satellite Selection
        st.markdown(
            f'<div class="section-title">{satellite_icon_svg} Provider & Satellite Selection</div>',
            unsafe_allow_html=True
        )
        st.markdown('<div class="section-subtitle">Choose your satellite data provider and specific satellite</div>', unsafe_allow_html=True)
        col1, col2 , col3 = st.columns(3)
        with col1:
            provider = st.selectbox("Provider", list(satellite_options.keys()))
        with col2:
            satellite = st.selectbox("Satellite", satellite_options.get(provider, []))
        with col3:
            product_type = st.selectbox("Product Type", product_types_options.get(satellite, []))
    with st.container(border=True):
        # Geographic Area
        st.markdown(f'<div class="section-title">{geometry_icon_svg} Geographic Area</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Define the area of interest using GeoJSON or WKT format</div>', unsafe_allow_html=True)
        geometry = st.text_area("Geometry (GeoJSON or WKT)", placeholder="Enter GeoJSON polygon or WKT string…")
    with st.container(border=True):
        # Time Range
        st.markdown(f'<div class="section-title">{calendar_icon_svg} Time Range</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-subtitle">Specify the date range for satellite imagery</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")
    with st.container(border=False, horizontal_alignment="center"):
        # Download Button
        if st.button("Download Products"):
            if not geometry:
                st.error("Please provide a valid geometry.")
            elif not start_date or not end_date:
                st.error("Please specify both start and end dates.")
            else:
                # Load area of interest geometry
                geometry_handler = GeometryHandler(file_path="example_aoi.wkt")  
                logger.info(f"Geometry loaded: {geometry_handler.geometry}")
                # Initialize Copernicus provider
                copernicus_provider = Copernicus(config_loader=configuration)
                # Example 1: Search for Sentinel-2 products over Rome, Italy
                logger.info(f"Searching for {satellite} products globally...")
                products = copernicus_provider.search_products(
                    collection=satellite,
                    product_type=product_type,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    aoi=geometry_handler.geometry  # Area of interest geometry
                )
                if products:
                    logger.info(f"Found {len(products)} products")
                    st.success(f"Found {len(products)} products. You can now download them.")
                    # Here you would call the function to download products based on the selected options
                    # copernicus_provider.download_products_concurrent(product_ids=products)
                else:
                    st.warning("No products found for the specified criteria.")

with tabs[1]:
    st.write("Results will be displayed here.")

with tabs[2]:
    st.write("Settings go here.")
