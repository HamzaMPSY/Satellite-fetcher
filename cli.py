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
from shapely.ops import unary_union

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
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help="Maximum number of concurrent file downloads",
    )
    parser.add_argument(
        "--parallel-days",
        type=int,
        default=1,
        help="Number of days to process in parallel (Copernicus only)",
    )
    parser.add_argument(
        "--concurrent-per-day",
        type=int,
        default=2,
        help="Concurrent downloads per day worker (Copernicus only)",
    )
    args = parser.parse_args()

    # ── FIX: Normalize provider name ─────────────────────────────────
    args.provider = _normalize_provider(args.provider)
    args.parallel_days = max(1, int(args.parallel_days or 1))
    args.concurrent_per_day = max(1, int(args.concurrent_per_day or 1))
    if args.max_concurrent is not None:
        args.max_concurrent = max(1, int(args.max_concurrent))

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
        provider_kwargs = {
            "config_loader": configuration,
            "ocifs_manager": ocifs,
        }
        if args.max_concurrent is not None and args.provider in {"copernicus", "usgs"}:
            provider_kwargs["max_concurrent"] = args.max_concurrent
        provider_instance = provider_cls(**provider_kwargs)
        logger.info(f"Initialized provider: {args.provider}")
    except Exception as e:
        logger.error(f"Failed to initialize provider '{args.provider}': {e}")
        sys.exit(1)

    logger.info(
        f"Searching for products with provider: {args.provider}, "
        f"collection: {args.collection}, product_type: {args.product_type}, "
        f"dates: {args.start_date} to {args.end_date}"
    )

    def _run_copernicus_parallel_days(
        output_dir: str,
        aoi=None,
        tile_id=None,
    ) -> int:
        """Run Copernicus downloads by day in parallel workers."""
        if not hasattr(provider_instance, "download_date_range"):
            raise RuntimeError("Provider does not support day-parallel download mode")
        if not args.start_date or not args.end_date:
            raise ValueError("parallel-days requires both --start-date and --end-date")

        logger.info(
            "Running Copernicus day-parallel mode: "
            f"workers={args.parallel_days}, concurrent_per_day={args.concurrent_per_day}, "
            f"estimated_total_concurrency={args.parallel_days * args.concurrent_per_day}"
        )
        return int(
            provider_instance.download_date_range(
                collection=args.collection,
                product_type=args.product_type,
                start_date=args.start_date,
                end_date=args.end_date,
                aoi=aoi,
                tile_id=tile_id,
                workers=args.parallel_days,
                concurrent_per_worker=args.concurrent_per_day,
                output_dir=output_dir,
                config_path=args.config,
            )
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
            requested_count = len(products)
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

            if args.provider == "copernicus" and args.parallel_days > 1:
                try:
                    downloaded_count = _run_copernicus_parallel_days(
                        output_dir=output_dir,
                        aoi=None,
                        tile_id=args.tile_id,
                    )
                except Exception as e:
                    logger.error(f"Day-parallel download failed: {e}")
                    sys.exit(1)
                if downloaded_count <= 0:
                    logger.error(
                        "Download failed: 0 products succeeded in day-parallel mode."
                    )
                    sys.exit(2)
                logger.info(
                    f"Search and download completed: {downloaded_count} product(s) downloaded "
                    f"in day-parallel mode."
                )
                return

            try:
                downloaded_paths = provider_instance.download_products(
                    product_ids=products, output_dir=output_dir
                )
            except Exception as e:
                logger.error(f"Download failed: {e}")
                sys.exit(1)
            succeeded_count = len(downloaded_paths or [])
            if succeeded_count == 0:
                logger.error(
                    f"Download failed: 0/{requested_count} succeeded. "
                    "Most common cause is provider rate limiting (HTTP 429)."
                )
                sys.exit(2)
            if succeeded_count < requested_count:
                logger.warning(
                    f"Download partially successful: {succeeded_count}/{requested_count} succeeded."
                )
            logger.info(
                f"Search and download completed: {succeeded_count}/{requested_count} succeeded."
            )
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
            requested_count = len(products)
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

            if args.provider == "copernicus" and args.parallel_days > 1:
                try:
                    downloaded_count = _run_copernicus_parallel_days(
                        output_dir=output_dir,
                        aoi=None,
                        tile_id=args.tile_id,
                    )
                except Exception as e:
                    logger.error(f"Day-parallel download failed: {e}")
                    sys.exit(1)
                if downloaded_count <= 0:
                    logger.error(
                        "Download failed: 0 products succeeded in day-parallel mode."
                    )
                    sys.exit(2)
                logger.info(
                    f"Search and download completed: {downloaded_count} product(s) downloaded "
                    f"in day-parallel mode."
                )
                return

            try:
                downloaded_paths = provider_instance.download_products(
                    product_ids=products, output_dir=output_dir
                )
            except Exception as e:
                logger.error(f"Download failed: {e}")
                sys.exit(1)
            succeeded_count = len(downloaded_paths or [])
            if succeeded_count == 0:
                logger.error(
                    f"Download failed: 0/{requested_count} succeeded. "
                    "Most common cause is provider rate limiting (HTTP 429)."
                )
                sys.exit(2)
            if succeeded_count < requested_count:
                logger.warning(
                    f"Download partially successful: {succeeded_count}/{requested_count} succeeded."
                )
            logger.info(
                f"Search and download completed: {succeeded_count}/{requested_count} succeeded."
            )
        else:
            logger.info("No products found for the given options.")
        return

    if args.provider == "copernicus" and args.parallel_days > 1:
        start_clean = args.start_date.replace("/", "") if args.start_date else None
        end_clean = args.end_date.replace("/", "") if args.end_date else None
        date_segment = (
            "_".join([p for p in [start_clean, end_clean] if p])
            if (start_clean or end_clean)
            else None
        )
        geom_signature = "|".join(g.wkt for g in geometry_handler.geometries)
        geom_hash = md5(geom_signature.encode()).hexdigest() if geom_signature else "no_aoi"
        path_parts = [
            "downloads",
            geom_hash,
            date_segment,
            args.provider,
            args.collection,
            args.product_type,
        ]
        path_parts = [str(p) for p in path_parts if p]
        output_dir = os.path.join(*path_parts)

        try:
            union_geom = unary_union(geometry_handler.geometries)
        except Exception as e:
            logger.error(f"Failed to build union AOI for day-parallel mode: {e}")
            sys.exit(1)

        try:
            downloaded_count = _run_copernicus_parallel_days(
                output_dir=output_dir,
                aoi=union_geom,
                tile_id=args.tile_id,
            )
        except Exception as e:
            logger.error(f"Day-parallel download failed: {e}")
            sys.exit(1)

        if downloaded_count <= 0:
            logger.error(
                "Download failed: 0 products succeeded in day-parallel mode."
            )
            sys.exit(2)

        if args.crop_aoi:
            logger.info("Cropping AOI...")
            try:
                geometry_handler.crop_aoi(folder_path=output_dir, provider=args.provider, aoi=union_geom)
            except Exception as e:
                logger.error(f"Crop failed for aggregated AOI: {e}")

        logger.info(
            f"Search and download completed: {downloaded_count} product(s) downloaded "
            f"in day-parallel mode."
        )
        return

    # Iterate over AOI geometries, aggregate unique product IDs once.
    all_products = []
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
            logger.info(f"Found {len(products)} products for geometry {idx + 1}.")
            all_products.extend(products)
        else:
            logger.info(f"No products found for geometry {idx + 1}.")

    unique_products = list(dict.fromkeys(all_products))
    succeeded_count = 0
    if unique_products:
        logger.info(
            f"Aggregated {len(unique_products)} unique products from "
            f"{len(all_products)} geometry-level hits. Starting single download batch."
        )

        start_clean = args.start_date.replace("/", "") if args.start_date else None
        end_clean = args.end_date.replace("/", "") if args.end_date else None
        date_segment = (
            "_".join([p for p in [start_clean, end_clean] if p])
            if (start_clean or end_clean)
            else None
        )
        geom_signature = "|".join(g.wkt for g in geometry_handler.geometries)
        geom_hash = md5(geom_signature.encode()).hexdigest() if geom_signature else "no_aoi"
        path_parts = [
            "downloads",
            geom_hash,
            date_segment,
            args.provider,
            args.collection,
            args.product_type,
        ]
        path_parts = [str(p) for p in path_parts if p]
        output_dir = os.path.join(*path_parts)

        try:
            downloaded_paths = provider_instance.download_products(
                product_ids=unique_products, output_dir=output_dir
            )
        except Exception as e:
            logger.error(f"Download failed for aggregated geometry batch: {e}")
            sys.exit(1)
        succeeded_count = len(downloaded_paths or [])
        if succeeded_count == 0:
            logger.error(
                f"Download failed for aggregated geometry batch: 0/{len(unique_products)} succeeded. "
                "Most common cause is provider rate limiting (HTTP 429)."
            )
            sys.exit(2)
        if succeeded_count < len(unique_products):
            logger.warning(
                f"Download partially successful: {succeeded_count}/{len(unique_products)} succeeded."
            )

        if args.crop_aoi:
            logger.info("Cropping AOI...")
            try:
                union_geom = unary_union(geometry_handler.geometries)
                geometry_handler.crop_aoi(folder_path=output_dir, provider=args.provider, aoi=union_geom)
            except Exception as e:
                logger.error(f"Crop failed for aggregated AOI: {e}")
    else:
        logger.info("No products found for the provided AOI geometries.")

    if unique_products:
        logger.info(
            f"Search and download completed: {succeeded_count}/{len(unique_products)} succeeded."
        )
    else:
        logger.info("Search completed with no downloadable products.")


if __name__ == "__main__":
    main()
