import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from loguru import logger
from dotenv import load_dotenv
from providers.provider_base import ProviderBase
from download_manager.download_manager import DownloadManager

class USGS(ProviderBase):
    """Main class for downloading images from USGS Landsat"""

    def __init__(self):
        self.base_url = "https://m2m.cr.usgs.gov"
        self.token_url = "https://m2m.cr.usgs.gov/api/login"
        self.download_url = "https://m2m.cr.usgs.gov"

        self.username = os.getenv('USGS_USERNAME')
        self.password = os.getenv('USGS_PASSWORD')

        if not self.username or not self.password:
            raise ValueError("Please set USGS_USERNAME and USGS_PASSWORD in your .env file")

        self.access_token = None
        self.session = requests.Session()
        self.download_manager = DownloadManager(self.base_url, self.download_url, self.access_token)

        logger.add("usgs_landsat_downloader.log", rotation="10 MB")

    def get_access_token(self) -> str:
        """Get OAuth2 access token from USGS Identity Service"""

        data = {
            'username': self.username,
            'password': self.password,
        }

        headers = {
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(self.token_url, json=data, headers=headers)
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
                        collection: str = "LANDSAT_8_C1",
                        start_date: str = None,
                        end_date: str = None,
                        bbox: List[float] = None,
                        cloud_cover_max: int = 20,
                        limit: int = 10) -> List[Dict]:
        """
        Search for products in the USGS Landsat catalogue
        """
        if not self.access_token:
            self.get_access_token()

        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        query_params = {
            'datasetName': collection,
            'temporal': f"{start_date},{end_date}",
            'bbox': f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
        }

        if cloud_cover_max is not None:
            query_params['maxCloudCover'] = cloud_cover_max

        query_params['maxResults'] = limit

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        try:
            url = f"{self.base_url}/api/scene-search"
            response = self.session.get(url, params=query_params, headers=headers)
            response.raise_for_status()

            data = response.json()
            products = data.get('results', [])

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
        return self.download_manager.download_products_concurrent(product_ids, output_dir)
