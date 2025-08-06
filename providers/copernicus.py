import requests
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger
from utilities import ConfigLoader, DownloadManager
from providers.provider_base import ProviderBase
from shapely.geometry import Polygon


class Copernicus(ProviderBase):
    """Main class for downloading images from Copernicus Data Space Ecosystem"""

    def __init__(self, config_loader: ConfigLoader):
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
                        product_type: str = "S2MSI2A",
                        start_date: str = None,
                        end_date: str = None,
                        aoi: Polygon = None) -> List[Dict]:
        """
        Search for products in the Copernicus catalogue
        """

        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        query_params = {
            "$filter": (
                f"Collection/Name eq '{collection}' "
                f"and ContentDate/Start gt '{start_date}T00:00:00Z' "
                f"and ContentDate/Start lt '{end_date}T23:59:59Z'"
            )
        }

        if product_type:
            query_params["$filter"] += (
                f" and Attributes/OData.CSC.StringAttribute/any("
                f"att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )

        if aoi:
            # Get coordinates as a WKT-like string without 'POLYGON' prefix
            coords_str = ", ".join([f"{x} {y}" for x, y in aoi.exterior.coords])

            # Append to your query filter
            query_params["$filter"] += (
                f" and OData.CSC.Intersects(area=geography'SRID=4326;"
                f"POLYGON(({coords_str}))')"
            )

        query_params["$orderby"] = "ContentDate/Start desc"


        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        logger.info(f"Searching for products in collection '{collection}' from {start_date} to {end_date}")
        logger.info(f"Query parameters: {query_params}")

        try:
            url = f"{self.base_url}/odata/v1/Products"
            response = self.session.get(url, params=query_params, headers=headers)
            response.raise_for_status()

            data = response.json()
            products = data.get('value', [])
            # get only the product IDs
            
            logger.info(f"Found {len(products)} products")
            logger.debug(f"Products: {products}")
            return [product['Id'] for product in products]

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            raise

    def download_products_concurrent(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products concurrently.
        """
        return self.download_manager.download_products_concurrent(product_ids, output_dir)
