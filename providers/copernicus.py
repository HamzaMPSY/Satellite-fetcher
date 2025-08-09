import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict
from loguru import logger
from shapely.geometry import Polygon
from utilities import ConfigLoader, DownloadManager


class Copernicus:
    """Main class for downloading images from Copernicus Data Space Ecosystem (async version)"""

    def __init__(self, config_loader: ConfigLoader):
        self.base_url = config_loader.get_var("providers.copernicus.base_urls.base_url")
        self.token_url = config_loader.get_var("providers.copernicus.base_urls.token_url")
        self.download_url = config_loader.get_var("providers.copernicus.base_urls.download_url")

        self.username = config_loader.get_var("providers.copernicus.credentials.cdse_username")
        self.password = config_loader.get_var("providers.copernicus.credentials.cdse_password")

        if not self.username or not self.password:
            raise ValueError("Please set cdse_username and cdse_password in your config.yaml file")

        self.access_token = None
        self.download_manager = None
        self.session = None

    async def get_access_token(self) -> str:
        data = {
            'client_id': 'cdse-public',
            'username': self.username,
            'password': self.password,
            'grant_type': 'password'
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.token_url, data=data, headers=headers) as response:
                response.raise_for_status()
                token_data = await response.json()
                self.access_token = token_data['access_token']
                logger.info("Successfully obtained access token")
                return self.access_token

    async def search_products(
        self,
        collection: str = "SENTINEL-2",
        product_type: str = "S2MSI2A",
        start_date: str = None,
        end_date: str = None,
        aoi: Polygon = None,
        cloud_cover_max: float = 20.0
    ) -> List[Dict]:

        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        filter_str = (
            f"Collection/Name eq '{collection}' "
            f"and ContentDate/Start gt '{start_date}T00:00:00Z' "
            f"and ContentDate/Start lt '{end_date}T23:59:59Z'"
        )

        if product_type:
            filter_str += (
                f" and Attributes/OData.CSC.StringAttribute/any("
                f"att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )

        if aoi:
            coords_str = ", ".join([f"{x} {y}" for x, y in aoi.exterior.coords])
            filter_str += (
                f" and OData.CSC.Intersects(area=geography'SRID=4326;"
                f"POLYGON(({coords_str}))')"
            )
        if cloud_cover_max is not None:
            filter_str += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_cover_max})"

        query_params = {
            "$filter": filter_str,
            "$orderby": "ContentDate/Start desc",
            "$top": "1000"
        }

        headers = {'Authorization': f'Bearer {self.access_token}'}

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/odata/v1/Products"
            async with session.get(url, params=query_params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                products = data.get('value', [])
                logger.info(f"Found {len(products)} products")
                return [product['Id'] for product in products]

    async def download_products_concurrent(self, product_ids: List[str], aoi_geometry=None, output_dir: str = "downloads", crop: bool = False):
        if not self.access_token:
            await self.get_access_token()
        if not self.download_manager:
            self.download_manager = DownloadManager(self.base_url, self.download_url, self.access_token)
        results = await self.download_manager.download_products_concurrent(product_ids, aoi_geometry, output_dir, crop)
        return results
