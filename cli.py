#!/usr/bin/env python3
"""
Satellite Product Search and Download CLI
=======================================

This command line utility wraps the provider interfaces defined under
``providers/`` to search for and download Earth observation data.  It
parses user supplied parameters such as provider, collection name,
product type, date range and area of interest (AOI) and orchestrates
the search and download operations.  Results are logged to both
terminal and a file (``nohup.out`` when run via the Streamlit UI).

The implementation here mirrors the original script provided with a
few clarifications and minor fixes:

* The argument descriptions have been tightened and corrected.
* Cleaned up unused imports and consistent logging messages.
* When a tile identifier is provided with the Copernicus provider the
  AOI will be ignored, matching the documented behaviour.

All of the heavy lifting is still delegated to the underlying provider
classes which encapsulate authentication, product search and
downloading.
"""

import argparse
import os
from hashlib import md5

from loguru import logger

from providers import Cds, Copernicus, GoogleEarthEngine, Modis, OpenTopography, Usgs
from utilities import ConfigLoader, GeometryHandler, OCIFSManager


def main() -> None:
    """Entry point for the satellite product fetcher CLI."""
    parser = argparse.ArgumentParser(
        description="Satellite Product Search and Download CLI"
    )
    parser.add_argument(
        "--provider",
        type=str,
        required=True,
        choices=["copernicus", "usgs", "opentopography", "cds", "modis", "google_earth_engine"],
        help="Data provider: 'copernicus', 'usgs', 'opentopography', 'cds', 'modis' or 'google_earth_engine'",
    )
    parser.add_argument(
        "--collection",
        type=str,
        required=True,
        help="Collection identifier for the chosen provider",
    )
    parser.add_argument(
        "--product-type",
        type=str,
        required=False,
        help="Optional product type to filter results",
    )
    parser.add_argument(
        "--tile-id",
        type=str,
        required=False,
        help="ID of the tile to search for (Copernicus only)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=False,
        help="Start date for search (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=False,
        help="End date for search (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--aoi_file",
        type=str,
        default="example_aoi.wkt",
        help="Path to AOI file (WKT or GeoJSON)",
    )
    parser.add_argument(
        "--crop-aoi",
        action="store_true",
        help="After download, crop each product to the AOI boundary",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration YAML file with credentials and endpoints",
    )
    parser.add_argument(
        "--log-type",
        type=str,
        default="all",
        choices=["all", "tqdm"],
        help="Log type: 'all' to show all logs, 'tqdm' to show only tqdm progress bars",
    )
    parser.add_argument(
        "--destination",
        type=str,
        default="local",
        choices=["local", "oci"],
        help="Download destination: 'local' or 'oci' (Oracle Cloud Infrastructure)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="mosaic",
        help="OCI bucket name (when destination is oci)",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="DEFAULT",
        help="OCI profile to use (when destination is oci)",
    )
    args = parser.parse_args()

    # Adjust log verbosity
    if args.log_type == "tqdm":
        logger.remove(0)

    # Load configuration
    configuration = ConfigLoader(config_file_path=args.config)
    logger.info("Configuration loaded successfully.")

    # Read geometry
    geometry_handler = GeometryHandler(file_path=args.aoi_file)
    logger.info(f"Geometry loaded: {len(geometry_handler.geometries)} geometries")

    # Provider lookup
    provider_map = {
        "copernicus": Copernicus,
        "usgs": Usgs,
        "opentopography": OpenTopography,
        "cds": Cds,
        "modis": Modis,
        "google_earth_engine": GoogleEarthEngine,
    }
    provider_cls = provider_map.get(args.provider.lower())
    if not provider_cls:
        logger.error(f"Unknown provider: {args.provider}. Exiting.")
        return

    # Destination handling
    ocifs = None
    if args.destination == "oci":
        ocifs = OCIFSManager(bucket=args.bucket, profile=args.profile)
        logger.info(f"Initialized OCIFS manager with profile: {args.profile}")

    # Instantiate provider
    provider_instance = provider_cls(config_loader=configuration, ocifs_manager=ocifs)
    logger.info(f"Initialized provider: {args.provider}")
    logger.info(
        f"Searching for products with provider: {args.provider}, collection: {args.collection}, product_type: {args.product_type}, dates: {args.start_date} to {args.end_date}"
    )

    # For Copernicus, allow direct tile search which bypasses AOI
    if args.tile_id and args.provider.lower() == "copernicus":
        logger.info(
            f"Tile ID provided ({args.tile_id}), ignoring AOI for Copernicus search."
        )
        products = provider_instance.search_products(
            collection=args.collection,
            product_type=args.product_type,
            start_date=args.start_date,
            end_date=args.end_date,
            tile_id=args.tile_id,
        )
        if products:
            logger.info(f"Found {len(products)} products. Downloading all products individually...")
            start_clean = args.start_date.replace("/", "") if args.start_date else None
            end_clean = args.end_date.replace("/", "") if args.end_date else None
            date_segment = (
                "_".join([p for p in [start_clean, end_clean] if p])
                if (start_clean or end_clean)
                else None
            )
            path_parts = [
                "downloads",
                md5(args.tile_id.encode()).hexdigest(),
                date_segment,
                args.provider,
                args.collection,
                args.product_type,
            ]
            path_parts = [str(p) for p in path_parts if p]
            output_dir = os.path.join(*path_parts)
            provider_instance.download_products(product_ids=products, output_dir=output_dir)
        else:
            logger.info("No products found for the given options.")
        return

    # Otherwise perform an AOI based search (or simple search if AOI missing)
    if not geometry_handler.geometries:
        # No geometry provided; call search once with a null AOI
        products = provider_instance.search_products(
            collection=args.collection,
            product_type=args.product_type,
            start_date=args.start_date,
            end_date=args.end_date,
            aoi=None,
            tile_id=args.tile_id,
        )
        if products:
            logger.info(f"Found {len(products)} products. Downloading all products individually...")
            start_clean = args.start_date.replace("/", "") if args.start_date else None
            end_clean = args.end_date.replace("/", "") if args.end_date else None
            date_segment = (
                "_".join([p for p in [start_clean, end_clean] if p])
                if (start_clean or end_clean)
                else None
            )
            path_parts = [
                "downloads",
                "no_aoi",
                date_segment,
                args.provider,
                args.collection,
                args.product_type,
            ]
            path_parts = [str(p) for p in path_parts if p]
            output_dir = os.path.join(*path_parts)
            provider_instance.download_products(product_ids=products, output_dir=output_dir)
        else:
            logger.info("No products found for the given options.")
        return

    # Iterate over AOI geometries
    for geom in geometry_handler.geometries:
        products = provider_instance.search_products(
            collection=args.collection,
            product_type=args.product_type,
            start_date=args.start_date,
            end_date=args.end_date,
            aoi=geom,
            tile_id=args.tile_id,
        )
        if products:
            logger.info(f"Found {len(products)} products. Downloading all products individually...")
            start_clean = args.start_date.replace("/", "") if args.start_date else None
            end_clean = args.end_date.replace("/", "") if args.end_date else None
            date_segment = (
                "_".join([p for p in [start_clean, end_clean] if p])
                if (start_clean or end_clean)
                else None
            )
            path_parts = [
                "downloads",
                md5(geom.wkt.encode()).hexdigest(),
                date_segment,
                args.provider,
                args.collection,
                args.product_type,
            ]
            path_parts = [str(p) for p in path_parts if p]
            output_dir = os.path.join(*path_parts)
            provider_instance.download_products(product_ids=products, output_dir=output_dir)
            if args.crop_aoi:
                logger.info("Cropping AOI...")
                geometry_handler.crop_aoi(folder_path=output_dir, provider=args.provider, aoi=geom)
        else:
            logger.info("No products found for the given options.")

    logger.info("Search and download completed successfully!")


if __name__ == "__main__":
    main()