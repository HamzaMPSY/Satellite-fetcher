"""
Copernicus Data Space Ecosystem Downloader Library

Provides a CopernicusDownloader class for authenticating, searching, and downloading satellite imagery.
"""

import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class CopernicusDownloader:
    """Main class for downloading images from Copernicus Data Space Ecosystem"""

    def __init__(self):
        self.base_url = "https://catalogue.dataspace.copernicus.eu"
        self.token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        self.download_url = "https://zipper.dataspace.copernicus.eu"

        self.username = os.getenv('CDSE_USERNAME')
        self.password = os.getenv('CDSE_PASSWORD')
        self.totp = os.getenv('CDSE_TOTP')  # Optional 2FA token

        if not self.username or not self.password:
            raise ValueError("Please set CDSE_USERNAME and CDSE_PASSWORD in your .env file")

        self.access_token = None
        self.session = requests.Session()

        logger.add("copernicus_downloader.log", rotation="10 MB")

    def get_access_token(self) -> str:
        """Get OAuth2 access token from Copernicus Identity Service"""

        data = {
            'client_id': 'cdse-public',
            'username': self.username,
            'password': self.password,
            'grant_type': 'password'
        }

        if self.totp:
            data['totp'] = self.totp

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        try:
            response = requests.post(self.token_url, data=data, headers=headers)
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data['access_token']

            logger.info("Successfully obtained access token")
            return self.access_token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get access token: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise

    def search_products(self, 
                       collection: str = "SENTINEL-2",
                       start_date: str = None,
                       end_date: str = None,
                       bbox: List[float] = None,
                       cloud_cover_max: int = 20,
                       limit: int = 10) -> List[Dict]:
        """
        Search for products in the Copernicus catalogue
        """
        if not self.access_token:
            self.get_access_token()

        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        query_params = {
            '$filter': f"Collection/Name eq '{collection}' "
                      f"and ContentDate/Start gt {start_date}T00:00:00.000Z "
                      f"and ContentDate/Start lt {end_date}T23:59:59.999Z"
        }

        if collection in ["SENTINEL-2"] and cloud_cover_max is not None:
            query_params['$filter'] += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_cover_max})"

        if bbox:
            west, south, east, north = bbox
            query_params['$filter'] += f" and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(({west} {south},{east} {south},{east} {north},{west} {north},{west} {south}))')"

        query_params['$top'] = limit
        query_params['$orderby'] = "ContentDate/Start desc"

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        try:
            url = f"{self.base_url}/odata/v1/Products"
            response = self.session.get(url, params=query_params, headers=headers)
            response.raise_for_status()

            data = response.json()
            products = data.get('value', [])

            logger.info(f"Found {len(products)} products")
            return products

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            raise

    def download_products_concurrent(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products sequentially.
        """
        if not self.access_token:
            self.get_access_token()
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        results = []
        for pid in product_ids:
            try:
                results.append(self.download_product(pid, output_dir))
            except Exception as e:
                logger.error(f"Download failed: {e}")
        return results

    def download_product(self, product_id: str, output_dir: str = "downloads") -> str:
        """
        Download a product by ID
        """
        if not self.access_token:
            self.get_access_token()

        Path(output_dir).mkdir(parents=True, exist_ok=True)

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        product_url = f"{self.base_url}/odata/v1/Products({product_id})"

        try:
            response = self.session.get(product_url, headers=headers)
            response.raise_for_status()
            product_info = response.json()

            product_name = product_info['Name']
            logger.info(f"Downloading: {product_name}")

            download_url = f"{self.download_url}/odata/v1/Products({product_id})/$value"

            with self.session.get(download_url, headers=headers, stream=True) as r:
                r.raise_for_status()

                filename = product_name + ".zip"
                if 'content-disposition' in r.headers:
                    cd = r.headers['content-disposition']
                    if 'filename=' in cd:
                        filename = cd.split('filename=')[1].strip('"')

                filepath = Path(output_dir) / filename

                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0

                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                print(f"\rProgress: {progress:.1f}%", end='', flush=True)

                print()
                logger.info(f"Download completed: {filepath}")
                return str(filepath)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Download failed: {e}")
            raise
