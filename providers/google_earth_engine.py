import os
from typing import Dict, List
from datetime import datetime

import ee
import requests
from loguru import logger
from shapely.geometry import Polygon

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager


class GoogleEarthEngine(ProviderBase):
    """
    Provider for interacting with Google Earth Engine (GEE).

    This class provides authentication, search, and download functionality for the GEE
    catalogue. It handles GEE authentication and image export.

    Attributes:
        project_id (str): GEE Project ID.
        service_account_json (str): Path to service account JSON key (optional).
    """

    def __init__(self, config_loader: ConfigLoader, ocifs_manager=None):
        """
        Initialize GEE provider from the given config loader.

        Args:
            config_loader (ConfigLoader): Loads configuration for credentials.
            ocifs_manager: Ignored for now, but kept for signature compatibility.
        """
        self.project_id = config_loader.get_var(
            "providers.google_earth_engine.credentials.project_id"
        )
        self.service_account_json = config_loader.get_var(
            "providers.google_earth_engine.credentials.service_account_json"
        )

        if not self.project_id:
            logger.warning(
                "Project ID not found in config. GEE might fail if not using default credentials with a set project."
            )

        self.get_access_token()
        self.download_manager = DownloadManager(
            config_loader=config_loader, ocifs_manager=ocifs_manager
        )

    def get_access_token(self) -> str:
        """
        Authenticate with Google Earth Engine.
        """
        try:
            if self.service_account_json and os.path.exists(self.service_account_json):
                logger.info(
                    f"Authenticating with service account: {self.service_account_json}"
                )
                credentials = ee.ServiceAccountCredentials(
                    self.project_id, self.service_account_json
                )
                ee.Initialize(credentials=credentials, project=self.project_id)
            else:
                logger.info("Authenticating with default credentials.")
                ee.Initialize(project=self.project_id)
            
            logger.info("Successfully initialized Google Earth Engine.")
            return "authenticated"  # GEE doesn't use a token string in the same way
        except Exception as e:
            logger.error(f"Failed to initialize Google Earth Engine: {e}")
            raise

    def search_products(
        self,
        collection: str,
        product_type: str = None,  # Not strictly used in GEE filter usually, but kept for interface
        start_date: str = None,
        end_date: str = None,
        aoi: Polygon = None,
        tile_id: str = None,
    ) -> List[Dict]:
        """
        Search for images in a GEE collection.

        Args:
            collection (str): GEE collection ID (e.g., "COPERNICUS/S2_SR").
            product_type (str): Ignored for GEE.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).
            aoi (Polygon): Area of interest.
            tile_id (str): Ignored for now.

        Returns:
            List[Dict]: List of image IDs.
        """
        if not start_date:
            start_date = "2020-01-01"
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Searching GEE collection '{collection}' from {start_date} to {end_date}."
        )

        try:
            ee_collection = ee.ImageCollection(collection).filterDate(
                start_date, end_date
            )

            if aoi:
                # Convert Shapely Polygon to GEE Geometry
                coords = list(aoi.exterior.coords)
                # GEE expects a list of lists of coords [[x, y], [x, y], ...]
                # Shapely returns tuples, so convert them.
                ee_geometry = ee.Geometry.Polygon([list(c) for c in coords])
                ee_collection = ee_collection.filterBounds(ee_geometry)

            # Limit results to avoid overwhelming
            count = ee_collection.size().getInfo()
            logger.info(f"Found {count} images in GEE collection.")
            
            # Get the list of images. 
            # WARNING: getInfo() can be slow for large collections.
            # We'll take the top 50 for now to be safe, or maybe the user wants all?
            # The interface expects a list of IDs.
            
            images_list = ee_collection.limit(100).toList(100).getInfo()
            
            product_ids = [img["id"] for img in images_list]
            return product_ids

        except Exception as e:
            logger.error(f"GEE search failed: {e}")
            raise

    def download_products(self, product_ids: List[str], output_dir: str) -> List[str]:
        """
        Download images from GEE using DownloadManager.

        Args:
            product_ids (List[str]): List of GEE image IDs.
            output_dir (str): Output directory.

        Returns:
            List[str]: List of downloaded file paths.
        """
        logger.info(
            f"Starting download for {len(product_ids)} GEE products to directory '{output_dir}'."
        )
        
        product_dict = {
            "urls": [],
            "file_names": [],
            "headers": {}, # GEE download URLs are signed, no extra auth headers needed
        }

        for image_id in product_ids:
            try:
                logger.info(f"Preparing download URL for GEE image: {image_id}")
                image = ee.Image(image_id)
                
                # Use default scale/region logic as before
                # We'll use a default scale of 100m to avoid hitting limits too easily
                scale = 100 
                
                url = image.getDownloadURL({
                    'scale': scale,
                    'crs': 'EPSG:4326',
                    'filePerBand': False,
                    'format': 'GEO_TIFF'
                })
                
                filename = image_id.replace("/", "_") + ".zip"
                product_dict["urls"].append(url)
                product_dict["file_names"].append(filename)

            except Exception as e:
                logger.error(f"Failed to get download URL for {image_id}: {e}")
                continue

        if not product_dict["urls"]:
            logger.warning("No valid download URLs generated.")
            return []

        logger.info(
            f"Triggering DownloadManager for {len(product_dict['urls'])} product(s)."
        )
        return self.download_manager.download_products(product_dict, output_dir)
