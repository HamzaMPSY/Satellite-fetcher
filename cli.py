#!/usr/bin/env python3
from providers import Copernicus, USGS
from loguru import logger
from config.config_loader import ConfigLoader

def main():
    """Example usage"""
    # Initialize logger
    logger.add("satellite_fetcher.log", rotation="10 MB", level="INFO")
    # Load configuration
    configuration = ConfigLoader(config_file_path="config.yaml")    
    logger.info("Configuration loaded successfully.")
    # Example: Print a configuration variable
    
    # Initialize Copernicus provider
    copernicus_provider = Copernicus(config_loader=configuration)
    # Example 1: Search for Sentinel-2 products over Rome, Italy
    logger.info("Searching for Sentinel-2 products globally...")
    products = copernicus_provider.search_products(
        collection="SENTINEL-2",
        start_date="2024-01-01",
        end_date="2024-01-31",
        bbox=[12.0, 41.0, 13.0, 42.0],  # Bounding box for Rome
        cloud_cover_max=10
    )

    # Download all products individually
    if products:
        logger.info("Downloading all products individually...")
        copernicus_provider.download_products_concurrent(product_ids=products)

    logger.info("Search and download completed successfully!")

if __name__ == "__main__":
    main()