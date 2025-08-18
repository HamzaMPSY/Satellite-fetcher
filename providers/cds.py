from providers import ProviderBase
from utilities import ConfigLoader, DownloadManager
from loguru import logger
import requests
import json
from shapely.geometry import Polygon
from datetime import datetime, timedelta
from typing import List, Dict
import base64
import cdsapi 
class Cds(ProviderBase):
    
    """
    Provider for interacting with the Copernicus Climate Data Store (CDS) API for climate data access.
    """

    def __init__(self, config_loader: ConfigLoader):
        """
        Initialize the CDS Provider using configuration values from the provided ConfigLoader.
        """
        self.service_url = config_loader.get_var("providers.cds.base_urls.service_url")
        self.api_key = config_loader.get_var("providers.cds.credentials.api_key")
        self.user_id = config_loader.get_var("providers.cds.credentials.user_id")
        self.session = requests.Session()
        logger.info("Initializing CDS Provider.")
        self.download_manager = DownloadManager(config_loader=config_loader)
        self.access_token = self.get_access_token()

    def get_access_token(self) -> str:
        """
        Authenticates with the CDS API and stores the resulting API key for future requests.
        """
        token = base64.b64encode(f"{self.user_id}:{self.api_key}".encode()).decode()
        return token
    

    def search_products(self,
                        collection: str,
                        product_type: str,
                        start_date: str,
                        end_date: str,
                        aoi: Polygon) -> List[Dict]:
        """
        """
        logger.info(f"Searching products in collection {collection} with product_type={product_type} for {start_date} to {end_date}.")
        
        # Construct the search payload
        payload = {
            'variable': ['radiative_forcing_of_carbon_dioxide'], 
            'forcing_type': 'instantaneous', 
            'band': ['long_wave'], 
            'sky_type': ['all_sky'], 
            'level': ['surface'], 
            'version': ['2'], 
            'year': ['2018'], 
            'month': ['06']
        }

        products = self._send_request(f"{self.service_url}/{collection}/execution", payload)

        return products

    def download_products(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        """
        logger.info(f"Starting download for {len(product_ids)} products to directory '{output_dir}'.")

    def _send_request(self, url:str , payload: dict):
        """
        Send a POST request to the given cds URL with JSON data and specified (optional) API key.

        Args:
            url (str): Endpoint URL.
            payload (dict): Data to be sent in the request body.

        Returns:
            The 'data' field from the JSON response if successful.

        Raises:
            Exception: If the HTTP request fails, or API returns an error.
        """

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'ecmwf-datastores-client/0.4.0',
            'PRIVATE-TOKEN': f'{self.api_key}'
        }
        

        logger.debug(f"Sending POST request to {url} with data={payload} and headers={headers}")
        resp = self.session.post(url, json.dumps(payload), headers=headers)
        if resp.status_code != 200:
            logger.error(f"HTTP {resp.status_code} error from {url}: {resp.text}")
            raise Exception(f"HTTP {resp.status_code} error from {url}: {resp.text}")
        try:
            output = resp.json()
        except Exception as e:
            logger.error(f"Error parsing JSON response from {url}: {e}")
            raise Exception(f"Error parsing JSON response from {url}: {e}")

        if output.get('errorCode') is not None:
            logger.error(f"API Error {output['errorCode']}: {output.get('errorMessage')}")
            raise Exception(f"API Error {output['errorCode']}: {output.get('errorMessage')}")
        return output['data']
        
