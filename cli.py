#!/usr/bin/env python3
"""
Satellite Product Search and Download CLI

Provides a command-line interface to search and download Earth observation satellite products
from Copernicus and USGS data providers, using configuration and AOI (area of interest) inputs.
All operations and errors are logged to both terminal and file.
"""

import argparse
from loguru import logger
from providers import Copernicus, USGS
from utilities import ConfigLoader, GeometryHandler

def main():
    """
    Main entry point for the satellite product fetcher CLI.

    Parses command-line arguments, loads configuration and AOI geometry, determines the requested
    provider, performs product search and downloads results. All steps are logged to file and console.
    """
    # Set up argument parser for all required input parameters
    parser = argparse.ArgumentParser(description="Satellite Product Search and Download CLI")
    parser.add_argument("--provider", type=str, required=True, choices=["copernicus", "usgs"], help="Data provider (copernicus or usgs)")
    parser.add_argument("--satellite", type=str, required=True, help="Satellite mission or collection name")
    parser.add_argument("--product-type", type=str, required=False, help="Type of product to search for")
    parser.add_argument("--start-date", type=str, required=True, help="Start date for search (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="End date for search (YYYY-MM-DD)")
    parser.add_argument("--aoi_file", type=str, default="example_aoi.wkt", help="Path to AOI file (in WKT format)")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to configuration YAML file")

    args = parser.parse_args()

    # Initialize persistent file logger for the CLI session
    logger.add("satellite_fetcher.log", rotation="10 MB", level="INFO")

    # Load configuration file for provider credentials and endpoints
    configuration = ConfigLoader(config_file_path=args.config)
    logger.info("Configuration loaded successfully.")

    # Load area of interest from WKT file and log geometry info
    geometry_handler = GeometryHandler(file_path=args.aoi_file)
    logger.info(f"Geometry loaded: {geometry_handler.geometry}")

    # Map string provider names to their implementations
    provider_map = {
        "copernicus": Copernicus,
        "usgs": USGS,
    }
    # Select provider based on input argument
    provider_cls = provider_map.get(args.provider.lower())
    if not provider_cls:
        logger.error(f"Unknown provider: {args.provider}. Exiting.")
        exit(1)

    # Initialize the selected provider with loaded configuration
    provider_instance = provider_cls(config_loader=configuration)
    logger.info(f"Initialized provider: {args.provider}")

    logger.info(f"Searching for products with provider: {args.provider}, collection: {args.satellite}, product_type: {args.product_type}, dates: {args.start_date} to {args.end_date}")

    # Execute the search for available products matching the filters
    products = provider_instance.search_products(
        collection=args.satellite,
        product_type=args.product_type,
        start_date=args.start_date,
        end_date=args.end_date,
        aoi=geometry_handler.geometry
    )

    # Download each product one by one if any were found
    if products:
        logger.info(f"Found {len(products)} products. Downloading all products individually...")
        provider_instance.download_products(product_ids=products)
    else:
        logger.info("No products found for the given options.")

    logger.info("Search and download completed successfully!")

if __name__ == "__main__":
    main()
