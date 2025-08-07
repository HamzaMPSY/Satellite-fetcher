import requests
import threading
import json
import os
from datetime import datetime
from typing import List, Dict

from shapely import Polygon
from .provider_base import ProviderBase
from utilities import ConfigLoader

class UsgsProvider(ProviderBase):
    """
    USGS Provider for satellite image search and download,
    with configuration via ConfigLoader.
    """

    def __init__(self, config_loader: ConfigLoader):
        self.service_url = config_loader.get_var("providers.usgs.base_urls.service_url")
        self.username = config_loader.get_var("providers.usgs.credentials.username")
        self.token = config_loader.get_var("providers.usgs.credentials.token")
        self.maxthreads = int(config_loader.get_var("providers.usgs.maxthreads") or 5)
        self.api_key = None
        self.download_sema = threading.Semaphore(value=self.maxthreads)
        self.session = requests.Session()

    def get_access_token(self) -> str:
        payload = {'username': self.username, 'token': self.token}
        resp = self._send_request(self.service_url + "login-token", payload)
        self.api_key = resp  # The API key becomes the token for subsequent requests
        return self.api_key

    def search_products(self,
                        collection: str,
                        product_type: str,
                        start_date: str,
                        end_date: str,
                        aoi: Polygon) -> List[Dict]:
        spatial_filter = {
            'filterType': "mbr",
            'lowerLeft': {'latitude': aoi.bounds[1], 'longitude': aoi.bounds[0]},
            'upperRight': {'latitude': aoi.bounds[3], 'longitude': aoi.bounds[2]},
        }
        temporal_filter = {'start': start_date, 'end': end_date}
        payload = {
            'datasetName': collection,
            'spatialFilter': spatial_filter,
            'temporalFilter': temporal_filter
        }
        datasets = self._send_request(self.service_url + "dataset-search", payload, self.api_key)
        products = []
        for ds in datasets:
            if ds['datasetAlias'] != collection:
                continue
            acquisition_filter = {"end": end_date, "start": start_date}
            scene_payload = {
                'datasetName': ds['datasetAlias'],
                'startingNumber': 1,
                'sceneFilter': {
                    'spatialFilter': spatial_filter,
                    'acquisitionFilter': acquisition_filter,
                }
            }
            scenes = self._send_request(self.service_url + "scene-search", scene_payload, self.api_key)
            if scenes.get('recordsReturned', 0) > 0:
                for result in scenes['results']:
                    products.append(result)
        return products

    def download_products_concurrent(self, product_ids: List[str], output_dir: str) -> List[str]:
        pass

    def _send_request(self, url, data, api_key=None):
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['X-Auth-Token'] = api_key
        resp = self.session.post(url, json.dumps(data), headers=headers)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code} error from {url}: {resp.text}")
        try:
            output = resp.json()
        except Exception as e:
            raise Exception(f"Error parsing JSON response from {url}: {e}")
        if output.get('errorCode') is not None:
            raise Exception(f"API Error {output['errorCode']}: {output.get('errorMessage')}")
        return output['data']
