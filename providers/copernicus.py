import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger
from config.config_loader import ConfigLoader
from providers.provider_base import ProviderBase
from download_manager import DownloadManager

class Copernicus(ProviderBase):
    """Main class for downloading images from Copernicus Data Space Ecosystem"""

    def __init__(self, config_loader: ConfigLoader ):
        self.base_url = config_loader.get_var("providers.copernicus.base_urls.base_url")
        self.token_url = config_loader.get_var("providers.copernicus.base_urls.token_url")
        self.download_url = config_loader.get_var("providers.copernicus.base_urls.download_url")

        self.username = config_loader.get_var("providers.copernicus.credentials.cdse_username")
        self.password = config_loader.get_var("providers.copernicus.credentials.cdse_password")

        if not self.username or not self.password:
            raise ValueError("Please set cdse_username and cdse_password in your config.yaml file")

        self.access_token = self.get_access_token()
        self.download_manager = DownloadManager(self.base_url, self.download_url, self.access_token)
        self.session = requests.Session()


    def get_access_token(self) -> str:
        """Get OAuth2 access token from Copernicus Identity Service"""

        data = {
            'client_id': 'cdse-public',
            'username': self.username,
            'password': self.password,
            'grant_type': 'password'
        }

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
            # get only the product IDs
            products = [product['Id'] for product in products]
            logger.info(f"Found {len(products)} products")
            return products

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            raise

    def download_products_concurrent(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products concurrently.
        """
        return self.download_manager.download_products_concurrent(product_ids, output_dir)
