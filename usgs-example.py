#!/usr/bin/env python3
from providers import USGS
from loguru import logger
from utilities import ConfigLoader, GeometryHandler

def main():
    """Example usage"""
    # Load configuration
    configuration = ConfigLoader(config_file_path="config.yaml")    
    logger.info("Configuration loaded successfully.")
    # Load area of interest geometry
    geometry_handler = GeometryHandler(file_path="example_aoi.wkt")  
    logger.info(f"Geometry loaded: {geometry_handler.geometry}")
    # Initialize USGS provider
    usgs_provider = USGS(config_loader=configuration)
    products = usgs_provider.search_products(
        collection="LANDSAT_8_C1",
        product_type=None,
        start_date="2024-06-01",
        end_date="2024-06-30",
        aoi=geometry_handler.geometry  # Area of interest geometry
    )
    logger.info(f"Found {len(products)} products matching the search criteria.")

    # # Download all products individually
    # if products:
    #     logger.info("Downloading all products individually...")
    #     usgs_provider.download_products_concurrent(product_ids=products)

    # logger.info("Search and download completed successfully!")

if __name__ == "__main__":
    main()