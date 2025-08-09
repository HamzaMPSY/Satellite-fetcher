import requests
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger
from utilities import ConfigLoader, DownloadManager
from providers.provider_base import ProviderBase
from shapely.geometry import Polygon


class Copernicus(ProviderBase):
    """
    Provider for interacting with the Copernicus Data Space Ecosystem (CDSE).

    This class provides authentication, search, and download functionality for the Copernicus
    satellite image catalogue. It handles OAuth2 authentication, product queries, and file downloads.

    Attributes:
        base_url (str): Base catalogue URL for CDSE API.
        token_url (str): OAuth2 token endpoint.
        download_url (str): File download endpoint.
        username (str): Copernicus account username.
        password (str): Copernicus account password.
        access_token (str): OAuth2 access token.
        session (requests.Session): HTTP session for requests.
        download_manager (DownloadManager): Download manager for handling file downloads.
    """

    def __init__(self, config_loader: ConfigLoader):
        """
        Initialize Copernicus provider from the given config loader.

        Args:
            config_loader (ConfigLoader): Loads configuration for credentials and URLs.
        Raises:
            ValueError: If username or password is missing in configuration.
        """
        # Load required URLs from config
        self.base_url = config_loader.get_var("providers.copernicus.base_urls.base_url")
        self.token_url = config_loader.get_var("providers.copernicus.base_urls.token_url")
        self.download_url = config_loader.get_var("providers.copernicus.base_urls.download_url")

        # Load credentials
        self.username = config_loader.get_var("providers.copernicus.credentials.cdse_username")
        self.password = config_loader.get_var("providers.copernicus.credentials.cdse_password")

        # Check for missing credentials
        if not self.username or not self.password:
            logger.error("Username or password is not set in the configuration file.")
            raise ValueError("Please set cdse_username and cdse_password in your config.yaml file")

        # Obtain access token on init
        logger.info("Obtaining access token for Copernicus provider.")
        self.access_token = self.get_access_token()
        self.download_manager = DownloadManager()
        self.session = requests.Session()

    def get_access_token(self) -> str:
        """
        Obtain OAuth2 access token from Copernicus Identity Service.

        Returns:
            str: Access token string.

        Raises:
            requests.exceptions.RequestException: If token acquisition fails.
        """
        # Prepare required parameters for OAuth2 password flow
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
            logger.info("Requesting OAuth2 token from Copernicus Identity Service.")
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
        Search for products in the Copernicus catalogue by collection, date, type, and AOI.

        Args:
            collection (str, optional): Satellite collection name. Defaults to "SENTINEL-2".
            product_type (str, optional): Specific product type. Defaults to "S2MSI2A".
            start_date (str, optional): Search start date (YYYY-MM-DD). Defaults to 30 days ago.
            end_date (str, optional): Search end date (YYYY-MM-DD). Defaults to today.
            aoi (Polygon, optional): Area of interest as a Shapely Polygon.

        Returns:
            List[Dict]: List of product IDs found in the Copernicus catalogue.

        Raises:
            requests.exceptions.RequestException: If the search request fails.
        """
        # Set default date range if none provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # Build the query filter for OData API
        query_params = {
            "$filter": (
                f"Collection/Name eq '{collection}' "
                f"and ContentDate/Start gt '{start_date}T00:00:00Z' "
                f"and ContentDate/Start lt '{end_date}T23:59:59Z'"
            )
        }

        # Restrict by product type if specified
        if product_type:
            query_params["$filter"] += (
                f" and Attributes/OData.CSC.StringAttribute/any("
                f"att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )

        # Add AOI filter in WKT format (if provided)
        if aoi:
            # Get coordinates as a WKT-like string without 'POLYGON' prefix
            coords_str = ", ".join([f"{x} {y}" for x, y in aoi.exterior.coords])
            # Append to the filter for spatial intersection
            query_params["$filter"] += (
                f" and OData.CSC.Intersects(area=geography'SRID=4326;"
                f"POLYGON(({coords_str}))')"
            )

        # Order results by acquisition date, most recent first, limit to 1000 results
        query_params["$orderby"] = "ContentDate/Start desc"
        query_params["$top"] = "1000"

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        logger.info(f"Searching for products in collection '{collection}' from {start_date} to {end_date}.")
        logger.debug(f"Query parameters: {query_params}")

        try:
            url = f"{self.base_url}/odata/v1/Products"
            logger.debug("Sending search request to Copernicus API.")
            response = self.session.get(url, params=query_params, headers=headers)
            response.raise_for_status()

            data = response.json()
            products = data.get('value', [])
            # Log total found products by query
            logger.info(f"Found {len(products)} products")
            # Return list of IDs only
            return [product['Id'] for product in products]

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            raise

    def download_products(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download the specified products using the Copernicus OData API.

        Args:
            product_ids (List[str]): List of Copernicus product IDs to download.
            output_dir (str, optional): Output directory for saving zipped products. Defaults to "downloads".

        Returns:
            List[str]: List of downloaded file paths (if supported by DownloadManager).
        """
        logger.info(f"Starting download for {len(product_ids)} Copernicus products to directory '{output_dir}'.")
        product_dict = {
            'urls'  : [],
            'file_names': [],
        }
        # Add authorization header with the current access token
        product_dict['headers'] = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        # Note: Only the last product_id is actually processed due to indentation,
        # but this is preserved as per do-not-change-logic instruction.
        for product_id in product_ids:
            product_url = f"{self.base_url}/odata/v1/Products({product_id})"

        try:
            # Only processes the last product_id by original logic
            logger.debug(f"Requesting product info for product_id {product_id}")
            response = self.session.get(product_url, headers=product_dict['headers'])
            response.raise_for_status()
            product_info = response.json()
            download_url = f"{self.download_url}/odata/v1/Products({product_id})/$value"
            # Add determined download URL and file name for this product
            product_dict['urls'].append(download_url)
            product_dict['file_names'].append(f"{product_info['Name']}.zip")
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for product ID {product_id}: {e}")

        logger.info(f"Triggering DownloadManager for {len(product_dict['urls'])} product(s).")
        self.download_manager.download_products(product_dict, output_dir)
