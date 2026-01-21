import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import cdsapi
from loguru import logger
from shapely.geometry import Polygon
from tqdm import tqdm

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, OCIFSManager


class Cds(ProviderBase):
    """
    Provider for interacting with the Copernicus Climate Data Store (CDS) API for climate data access.
    """

    def __init__(self, config_loader: ConfigLoader, ocifs_manager: OCIFSManager):
        """
        Initialize the CDS Provider using configuration values from the provided ConfigLoader.
        """
        self.service_url = config_loader.get_var("providers.cds.base_urls.service_url")
        self.api_key = config_loader.get_var("providers.cds.credentials.api_key")
        self.datasets = config_loader.get_var("providers.cds.datasets")
        self.variables = config_loader.get_var("providers.cds.variables")
        self.ocifs_manager = ocifs_manager
        logger.info("Initializing CDS Provider.")
        self.client = cdsapi.Client(url=self.service_url, key=self.api_key)

    def get_access_token(self) -> str:
        """
        Authenticates with the CDS API and stores the resulting API key for future requests.
        """
        return self.api_key

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: Polygon,
        tile_id: str = None,
    ) -> List[Dict]:
        """Search and download products in CDS API using cdsapi library"""
        # mapping the keys with actual api values
        collection = self.datasets[collection]
        variables = [self.variables[var] for var in product_type.split(",")]

        logger.info(
            f"Searching products in collection {collection} with product_type={variables} for {start_date} to {end_date}."
        )
        products = []
        # Convert strings to datetime objects
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        minx, miny, maxx, maxy = aoi.bounds
        west, east = sorted([minx, maxx])
        south, north = sorted([miny, maxy])
        area = [north, west, south, east]
        # Loop over each day
        current = start
        while current <= end:
            request = {
                "date": [current.strftime("%Y-%m-%d")],
                "time": ["12:00"],
                "data_format": "netcdf_zip",
                "variable": variables,
                "area": area,
            }
            products.append(
                {
                    "result": self.client.retrieve(collection, request),
                    "file_name": f'CAMS_{current.strftime("%Y-%m-%d")}.nc',
                }
            )
            current += timedelta(days=1)
        logger.info(f"Found {len(products)} products")
        return products

    def download_products(
        self, product_ids: List, output_dir: str = "downloads"
    ) -> List[str]:
        """Use the cdsapi itself to download the products"""
        logger.info(
            f"Starting download for {len(product_ids)} products to directory '{output_dir}'."
        )
        output_dir = output_dir.replace(",", "_")
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        for product in tqdm(product_ids, desc="Downloading Cams:"):
            zip_path = os.path.join(output_dir, product["file_name"])
            product["result"].download(
                target=os.path.join(output_dir, product["file_name"])
            )
            tmp_dir = tempfile.mkdtemp()  # create a temporary directory
            # Step 1: Unzip to temp folder
            with zipfile.ZipFile(
                os.path.join(output_dir, product["file_name"]), "r"
            ) as zip_ref:
                zip_ref.extractall(tmp_dir)
            # Step 2: Find the extracted file (there should be only one)
            extracted_files = os.listdir(tmp_dir)
            if len(extracted_files) != 1:
                raise ValueError(
                    f"Expected one file in {zip_path}, found: {extracted_files}"
                )
            tmp_file_path = os.path.join(tmp_dir, extracted_files[0])
            # Step 3: Delete the original zip file
            os.remove(zip_path)
            # Step 4: Rename/move the extracted file to the same name as the zip (without .zip)
            shutil.move(tmp_file_path, zip_path)
            # Clean up temp dir
            os.rmdir(tmp_dir)
        logger.info("Successfully Downloaded All Products")
