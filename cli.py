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

FIX: Added provider name normalization so that both "googleearthengine"
and "google_earth_engine" are accepted (the Streamlit UI sends the
former via .lower() on "GoogleEarthEngine").

FIX: Added explicit error handling around provider instantiation and
search to surface meaningful errors in nohup.out instead of silent
failures.

FIX: Added sys.exit(1) on fatal errors so that the Streamlit UI
can detect process exit and show the error to the user.
"""

import argparse
import os
import sys
from hashlib import md5

from loguru import logger

from providers import Cds, Copernicus, GoogleEarthEngine, Modis, OpenTopography, Usgs
from utilities import ConfigLoader, GeometryHandler, OCIFSManager


# ── FIX: Normalization map for provider names ────────────────────────
# The Streamlit UI builds the --provider value using PROVIDER_CLI_MAP which
# maps e.g. "GoogleEarthEngine" → "google_earth_engine".  However, if a
# user runs the CLI manually they might type "googleearthengine" (naive
# .lower()).  This dict normalizes all known variants to canonical keys.
_PROVIDER_ALIASES = {
    "copernicus": "copernicus",
    "usgs": "usgs",
    "opentopography": "opentopography",
    "cds": "cds",
    "modis": "modis",
    "google_earth_engine": "google_earth_engine",
    # Common aliases / typos
    "googleearthengine": "google_earth_engine",
    "gee": "google_earth_engine",
    "open_topography": "opentopography",
}


def _normalize_provider(raw: str) -> str:
    """Return the canonical provider key, or the original string if unknown."""
    return _PROVIDER_ALIASES.get(raw.lower().strip(), raw.lower().strip())


def main() -> None:
    """Entry point for the satellite product fetcher CLI."""
    parser = argparse.ArgumentParser(
        description="Satellite Product Search and Download CLI"
    )
    parser.add_argument(
        "--provider",
        type=str,
        required=True,
        # FIX: Accept any string here; we normalize below instead of
        # relying on argparse choices (which would reject "googleearthengine").
        help=(
            "Data provider: 'copernicus', 'usgs', 'opentopography', "
            "'cds', 'modis' or 'google_earth_engine'"
        ),
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

    # ── FIX: Normalize provider name ─────────────────────────────────
    args.provider = _normalize_provider(args.provider)

    # Adjust log verbosity
    if args.log_type == "tqdm":
        logger.remove(0)

    # Load configuration
    try:
        configuration = ConfigLoader(config_file_path=args.config)
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load configuration from '{args.config}': {e}")
        sys.exit(1)

    # Read geometry
    try:
        geometry_handler = GeometryHandler(file_path=args.aoi_file)
        logger.info(f"Geometry loaded: {len(geometry_handler.geometries)} geometries")
    except Exception as e:
        logger.error(f"Failed to load geometry from '{args.aoi_file}': {e}")
        sys.exit(1)

    # Provider lookup
    provider_map = {
        "copernicus": Copernicus,
        "usgs": Usgs,
        "opentopography": OpenTopography,
        "cds": Cds,
        "modis": Modis,
        "google_earth_engine": GoogleEarthEngine,
    }
    provider_cls = provider_map.get(args.provider)
    if not provider_cls:
        logger.error(
            f"Unknown provider: '{args.provider}'. "
            f"Valid providers: {', '.join(sorted(provider_map.keys()))}. Exiting."
        )
        sys.exit(1)

    # Destination handling
    ocifs = None
    if args.destination == "oci":
        try:
            ocifs = OCIFSManager(bucket=args.bucket, profile=args.profile)
            logger.info(f"Initialized OCIFS manager with profile: {args.profile}")
        except Exception as e:
            logger.error(f"Failed to initialize OCIFS manager: {e}")
            sys.exit(1)

    # Instantiate provider
    try:
        provider_instance = provider_cls(config_loader=configuration, ocifs_manager=ocifs)
        logger.info(f"Initialized provider: {args.provider}")
    except Exception as e:
        logger.error(f"Failed to initialize provider '{args.provider}': {e}")
        sys.exit(1)

    logger.info(
        f"Searching for products with provider: {args.provider}, "
        f"collection: {args.collection}, product_type: {args.product_type}, "
        f"dates: {args.start_date} to {args.end_date}"
    )

    # For Copernicus, allow direct tile search which bypasses AOI
    if args.tile_id and args.provider == "copernicus":
        logger.info(
            f"Tile ID provided ({args.tile_id}), ignoring AOI for Copernicus search."
        )
        try:
            products = provider_instance.search_products(
                collection=args.collection,
                product_type=args.product_type,
                start_date=args.start_date,
                end_date=args.end_date,
                tile_id=args.tile_id,
            )
        except Exception as e:
            logger.error(f"Search failed for tile_id={args.tile_id}: {e}")
            sys.exit(1)

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
            try:
                provider_instance.download_products(product_ids=products, output_dir=output_dir)
            except Exception as e:
                logger.error(f"Download failed: {e}")
                sys.exit(1)
        else:
            logger.info("No products found for the given options.")
        return

    # Otherwise perform an AOI based search (or simple search if AOI missing)
    if not geometry_handler.geometries:
        logger.warning("No geometries found in AOI file — searching without AOI constraint.")
        try:
            products = provider_instance.search_products(
                collection=args.collection,
                product_type=args.product_type,
                start_date=args.start_date,
                end_date=args.end_date,
                aoi=None,
                tile_id=args.tile_id,
            )
        except Exception as e:
            logger.error(f"Search failed (no AOI): {e}")
            sys.exit(1)

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
            try:
                provider_instance.download_products(product_ids=products, output_dir=output_dir)
            except Exception as e:
                logger.error(f"Download failed: {e}")
                sys.exit(1)
        else:
            logger.info("No products found for the given options.")
        return

    # Iterate over AOI geometries
    total_downloaded = 0
    for idx, geom in enumerate(geometry_handler.geometries):
        logger.info(
            f"Processing geometry {idx + 1}/{len(geometry_handler.geometries)}"
        )
        try:
            products = provider_instance.search_products(
                collection=args.collection,
                product_type=args.product_type,
                start_date=args.start_date,
                end_date=args.end_date,
                aoi=geom,
                tile_id=args.tile_id,
            )
        except Exception as e:
            logger.error(f"Search failed for geometry {idx + 1}: {e}")
            continue

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
            try:
                provider_instance.download_products(product_ids=products, output_dir=output_dir)
                total_downloaded += len(products)
            except Exception as e:
                logger.error(f"Download failed for geometry {idx + 1}: {e}")
                continue

            if args.crop_aoi:
                logger.info("Cropping AOI...")
                try:
                    geometry_handler.crop_aoi(folder_path=output_dir, provider=args.provider, aoi=geom)
                except Exception as e:
                    logger.error(f"Crop failed for geometry {idx + 1}: {e}")
        else:
            logger.info(f"No products found for geometry {idx + 1}.")

    logger.info("Search and download completed successfully!")


if __name__ == "__main__":
    main()