#!/usr/bin/env python3
from providers import Copernicus
from loguru import logger
from utilities import ConfigLoader, GeometryHandler

def main():
    """Example usage"""
    # Initialize logger
    logger.add("satellite_fetcher.log", rotation="10 MB", level="INFO")
    # Load configuration
    configuration = ConfigLoader(config_file_path="config.yaml")    
    logger.info("Configuration loaded successfully.")
    # Load area of interest geometry
    geometry_handler = GeometryHandler(file_path="example_aoi.wkt")  
    logger.info(f"Geometry loaded: {geometry_handler.geometry}")
    # Initialize Copernicus provider
    copernicus_provider = Copernicus(config_loader=configuration)
    # Example 1: Search for Sentinel-2 products over Rome, Italy
    logger.info("Searching for Sentinel-2 products globally...")
    products = copernicus_provider.search_products(
        collection="SENTINEL-2",
        product_type='S2MSI2A',
        start_date="2025-07-01",
        end_date="2025-07-31",
        aoi=geometry_handler.geometry  # Area of interest geometry
    )

    # Download all products individually
    # if products:
    #     logger.info("Downloading all products individually...")
    #     copernicus_provider.download_products_concurrent(product_ids=products)

    logger.info("Search and download completed successfully!")

if __name__ == "__main__":
    main()