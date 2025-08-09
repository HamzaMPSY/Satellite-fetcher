#!/usr/bin/env python3
import asyncio
from loguru import logger
from providers import Copernicus
from utilities import ConfigLoader, GeometryHandler


async def main():
    logger.add("satellite_fetcher.log", rotation="10 MB", level="INFO")

    configuration = ConfigLoader(config_file_path="config.yaml")
    logger.info("Configuration loaded successfully.")

    geometry_handler = GeometryHandler("example_aoi.wkt")

    copernicus_provider = Copernicus(config_loader=configuration)
    await copernicus_provider.get_access_token()

    for idx, geom in enumerate(geometry_handler.get_all_geometries(), start=1):
        logger.info(f"Processing AOI {idx}")
        products = await copernicus_provider.search_products(
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date="2024-01-01",
            end_date="2024-12-31",
            aoi=geom,
            cloud_cover_max=10.0
        )
        if not products:
            logger.warning(f"No products found for AOI {idx}.")
            continue
        logger.info(f"Found {len(products)} products for AOI {idx}.")
        logger.info(f"Downloading products for AOI {idx}...")

        await copernicus_provider.download_products_concurrent(
            products,
            aoi_geometry=geom,
            output_dir="downloads",
            crop=True  # enable cropping -- TODO : make this dynamic,based on the AOI (sub-tile level / full tile / more than one tile)
        )

    logger.info("Search and download completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
