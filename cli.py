#!/usr/bin/env python3
from copernicus_downloader import CopernicusDownloader
from loguru import logger

def main():
    """Example usage"""

    try:
        # Initialize downloader
        downloader = CopernicusDownloader()

        # Example 1: Search for Sentinel-2 products over Rome, Italy
        logger.info("Searching for Sentinel-2 products globally...")
        products = downloader.search_products(
            collection="SENTINEL-2",
            start_date="2024-01-01",
            end_date="2024-01-31",
            cloud_cover_max=10
        )

        # Download all products individually
        if products:
            logger.info("Downloading all products individually...")
            for product in products:
                product_id = product['Id']
                try:
                    filepath = downloader.download_product(product_id)
                    logger.info(f"Downloaded: {filepath}")
                except Exception as e:
                    logger.error(f"Failed to download {product_id}: {e}")

        logger.info("Search and download completed successfully!")

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("\nMake sure you have:")
        logger.error("1. Created a .env file with CDSE_USERNAME and CDSE_PASSWORD")
        logger.error("2. Registered at https://dataspace.copernicus.eu/")
        logger.error("3. Installed required packages: pip install requests python-dotenv")

if __name__ == "__main__":
    main()
