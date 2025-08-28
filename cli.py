#!/usr/bin/env python3
"""
Satellite Product Search and Download CLI

Provides a command-line interface to search and download Earth observation satellite products
from Copernicus and USGS data providers, using configuration and AOI (area of interest) inputs.
All operations and errors are logged to both terminal and file.
"""

import argparse
import os
from loguru import logger
from providers import Copernicus, Usgs, OpenTopography, Cds
from utilities import ConfigLoader, GeometryHandler, OCIFSManager
from hashlib import md5

def main():
    """
    Main entry point for the satellite product fetcher CLI.

    Parses command-line arguments, loads configuration and AOI geometry, determines the requested
    provider, performs product search and downloads results. All steps are logged to file and console.
    """
    # Set up argument parser for all required input parameters
    parser = argparse.ArgumentParser(description="Satellite Product Search and Download CLI")
    parser.add_argument("--provider", type=str, required=True, choices=["copernicus", "usgs", "opentopography", "cds"], help="Data provider (copernicus , usgs or open_topography)")
    parser.add_argument("--collection", type=str, required=True, help="collection name")
    parser.add_argument("--product-type", type=str, required=False, help="Type of product to search for")
    parser.add_argument("--tile-id", type=str, required=False, help="ID of the tile to search for")
    parser.add_argument("--start-date", type=str, required=False, help="Start date for search (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=False, help="End date for search (YYYY-MM-DD)")
    parser.add_argument("--aoi_file", type=str, default="example_aoi.wkt", help="Path to AOI file (in WKT format)")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to configuration YAML file")
    parser.add_argument("--log-type", type=str, default="all", choices=["all", "tqdm"], help="Log type: 'all' to show all logs, 'tqdm' to show only tqdm progress bars")
    parser.add_argument("--destination", type=str, default="local", choices=["local", "oci"], help="Type of destination (local or OCI)")
    parser.add_argument("--bucket", type=str, default="mosaic", help="OCI bucket name")
    parser.add_argument("--profile", type=str, default="DEFAULT", help="OCI profile to use")

    args = parser.parse_args()

    if args.log_type == "tqdm":
        logger.remove(0)

    # Load configuration file for provider credentials and endpoints
    configuration = ConfigLoader(config_file_path=args.config)
    logger.info("Configuration loaded successfully.")

    # Load area of interest from WKT file and log geometry info
    geometry_handler = GeometryHandler(file_path=args.aoi_file)
    logger.info(f"Geometry loaded: {len(geometry_handler.geometries)} geometries")

    # Map string provider names to their implementations
    provider_map = {
        "copernicus": Copernicus,
        "usgs": Usgs,
        "opentopography": OpenTopography,
        "cds": Cds
    }
    # Select provider based on input argument
    provider_cls = provider_map.get(args.provider.lower())
    if not provider_cls:
        logger.error(f"Unknown provider: {args.provider}. Exiting.")
        exit(1)

    # check if destination is OCI
    if args.destination == "oci":
        ocifs = OCIFSManager(bucket=args.bucket, profile=args.profile)
        logger.info(f"Initialized OCIFS manager with profile: {args.profile}")

    # Initialize the selected provider with loaded configuration
    provider_instance = provider_cls(config_loader=configuration, ocifs_manager=ocifs)
    logger.info(f"Initialized provider: {args.provider}")

    logger.info(f"Searching for products with provider: {args.provider}, collection: {args.collection}, product_type: {args.product_type}, dates: {args.start_date} to {args.end_date}")

    # Execute the search for available products matching the filters
    for geom in geometry_handler.geometries:
        products = provider_instance.search_products(
            collection=args.collection,
            product_type=args.product_type,
            start_date=args.start_date,
            end_date=args.end_date,
            aoi=geom,
            tile_id=args.tile_id
        )

        # Download each product one by one if any were found
        if products:
            logger.info(f"Found {len(products)} products. Downloading all products individually...")
            provider_instance.download_products(product_ids=products, output_dir=os.path.join("downloads", md5(geom.wkt.encode()).hexdigest(), args.start_date.replace('/', '') + '_' + args.end_date.replace('/', '') ,args.provider , args.collection, args.product_type))
        else:
            logger.info("No products found for the given options.")

    logger.info("Search and download completed successfully!")

if __name__ == "__main__":
    main()
