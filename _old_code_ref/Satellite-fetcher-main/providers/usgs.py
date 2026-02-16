import concurrent.futures
import json
import os
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse

import requests

# Using loguru for enhanced logging throughout this provider.
from loguru import logger
from shapely import Polygon

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager, OCIFSManager


class Usgs(ProviderBase):
    """
    Provider for interacting with the USGS M2M API for satellite image search and download.

    This class provides methods to authenticate with USGS, perform scene searches based on geographic
    polygons, and download the matching products for further processing.

    Attributes:
        service_url (str): The base URL for the USGS M2M API endpoints.
        username (str): USGS account username.
        token (str): USGS account token.
        api_key (str): API key obtained from USGS on login.
        session (requests.Session): HTTP session for all requests.
        download_manager (DownloadManager): Download manager instance for product downloads.
    """

    def __init__(self, config_loader: ConfigLoader, ocifs_manager: OCIFSManager = None):
        """
        Initialize the USGS Provider using configuration values from the provided ConfigLoader.

        Args:
            config_loader (ConfigLoader): Loads configuration variables required for authentication and requests.
        """
        self.service_url = config_loader.get_var("providers.usgs.base_urls.service_url")
        self.username = config_loader.get_var("providers.usgs.credentials.username")
        self.token = config_loader.get_var("providers.usgs.credentials.token")
        self.api_key = None
        self.session = requests.Session()
        logger.info("Initializing USGS Provider and obtaining API token.")
        self.get_access_token()
        self.download_manager = DownloadManager(
            config_loader=config_loader, ocifs_manager=ocifs_manager
        )
        self.config_loader = config_loader

    def get_access_token(self) -> str:
        """
        Authenticates with the USGS API and stores the resulting API key for future requests.
        """
        payload = {"username": self.username, "token": self.token}
        logger.info("Requesting USGS API token using provided credentials.")
        resp = self._send_request(self.service_url + "login-token", payload)
        self.api_key = resp  # The API key becomes the token for subsequent requests
        logger.info("Received and stored API token from USGS API.")

    def _aoi_to_geojson(self, aoi: Polygon) -> dict:
        """
        Convert a Shapely Polygon object to a GeoJSON dictionary compatible with USGS M2M API.

        Args:
            aoi (Polygon): Shapely Polygon object representing the area of interest.

        Returns:
            dict: GeoJSON-style dictionary of the polygon.
        """
        coords = list(aoi.exterior.coords)
        # Ensure polygon is closed for GeoJSON, as USGS expects this format
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        return {"type": "Polygon", "coordinates": [[list(coord) for coord in coords]]}

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str = None,
        end_date: str = None,
        aoi: Polygon = None,
        tile_id: str = None,
    ) -> List[Dict]:
        """
        Search for satellite scenes in a specific collection, product type, and date range within the provided AOI.

        Args:
            collection (str): Dataset name (e.g. 'landsat_ot_c2_l2' for certain Landsat missions).
            product_type (str): Desired processing level or product type (e.g., 'L1TP').
            start_date (str): Acquisition start date (formatted as YYYY-MM-DD).
            end_date (str): Acquisition end date (formatted as YYYY-MM-DD).
            aoi (Polygon): Shapely Polygon object for spatial filtering.

        Returns:
            List[Dict]: List of entity IDs of scenes available for bulk download.
        """
        logger.info(
            f"Searching products in collection {collection} with product_type={product_type} for {start_date} to {end_date}."
        )
        self.dataset = collection

        # Build spatial filter using AOI
        spatial_filter = {"filterType": "geojson", "geoJson": self._aoi_to_geojson(aoi)}
        # Build acquisition (temporal) filter
        scene_filter = {
            "spatialFilter": spatial_filter,
            "acquisitionFilter": {"start": start_date, "end": end_date},
        }
        scene_payload = {
            "datasetName": collection,
            "sceneFilter": scene_filter,
            "maxResults": 1000,  # Adjust this as necessary for your use-case
        }

        # Send the search request to the USGS API.
        scenes = self._send_request(
            os.path.join(self.service_url, "scene-search"), scene_payload, self.api_key
        )
        logger.info(
            f"Found {scenes.get('totalHits', 0)} scenes matching the dataset '{collection}'."
        )

        products = []
        # Only collect entityIds that have a downloadable 'bulk' option
        if scenes.get("recordsReturned", 0) > 0 and "results" in scenes:
            for result in scenes["results"]:
                # Only add scene if a bulk download option exists
                if (
                    result["options"]["bulk"] == True
                    and product_type[1:] in result["displayId"]
                ):
                    for option in result["metadata"]:
                        if option.get("fieldName") == "Satellite" and option.get(
                            "value"
                        ) == int(product_type[0]):
                            products.append(result["entityId"])
                            break

        logger.info(f"Returning {len(products)} downloadable product entity IDs.")
        return products

    def download_products(
        self, product_ids: List[str], output_dir: str = "downloads"
    ) -> List[str]:
        """
        Download all products (scenes) given a list of entity IDs for the set dataset.

        Args:
            product_ids (List[str]): List of USGS entity IDs (products to download).
            output_dir (str, optional): Directory path to save downloaded files. Defaults to "downloads".

        Returns:
            List[str]: List of file paths for the downloaded products (if supported by the download manager).
        """
        logger.info(
            f"Starting download for {len(product_ids)} products to directory '{output_dir}'."
        )
        # STEP 1: Retrieve available download options for entity IDs
        payload = {"datasetName": self.dataset, "entityIds": ",".join(product_ids)}
        logger.debug(
            f"Requesting download options for dataset '{self.dataset}' and product_ids: {product_ids}"
        )
        options = self._send_request(
            self.service_url + "download-options", payload, self.api_key
        )

        # Extract list of available options
        downloads = []
        if isinstance(options, dict):
            option_list = options.get("options", [])
        else:
            option_list = options
        for opt in option_list:
            if (
                opt.get("available")
                and opt.get("entityId")
                and opt.get("id")
                and "Bundle" in opt.get("productName")
            ):
                downloads.append({"entityId": opt["entityId"], "productId": opt["id"]})

        if not downloads:
            logger.error("No available downloads for the selected products.")
            return []

        # STEP 2: Make a batch download request for all available products
        label = datetime.now().strftime("dl_%Y%m%d_%H%M%S")
        req_payload = {"downloads": downloads, "label": label}
        logger.info(f"Submitting download request for {len(downloads)} products.")
        req_results = self._send_request(
            self.service_url + "download-request", req_payload, self.api_key
        )
        final_downloads = (
            req_results.get("availableDownloads", [])
            if isinstance(req_results, dict)
            else []
        )
        logger.info(f"Found {len(final_downloads)} available downloads after polling.")

        product_dict = {
            "urls": [],
            "file_names": [],
        }
        # Add headers for authentication (if needed by DownloadManager)
        product_dict["headers"] = {}
        # Add token refresh callback for 401 handling
        product_dict["refresh_token_callback"] = self.get_access_token

        product_dict["urls"] = [download["url"] for download in final_downloads]

        def get_filename(download):
            with requests.get(download["url"], stream=True) as r:
                # First try content-disposition header
                if "Content-Disposition" in r.headers:
                    cd = r.headers["Content-Disposition"]
                    # e.g. 'attachment; filename="file.zip"'
                    filename = cd.split("filename=")[-1].strip('"')
                else:
                    # fallback: get from URL path
                    filename = urlparse(download["url"]).path.split("/")[-1]
            return filename

        max_concurrent = self.config_loader.get_var("download_manager.max_concurrent")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent
        ) as executor:
            product_dict["file_names"] = list(
                executor.map(get_filename, final_downloads)
            )

        logger.info(
            f"Initiating download for {len(product_dict['urls'])} files using DownloadManager."
        )
        self.download_manager.download_products(product_dict, output_dir)
        logger.info("All downloads triggered; check output directory for results.")

    def _send_request(self, url, data, api_key=None):
        """
        Send a POST request to the given USGS URL with JSON data and specified (optional) API key.

        Args:
            url (str): Endpoint URL.
            data (dict): Data to be sent in the request body.
            api_key (str, optional): API key for authentication.

        Returns:
            The 'data' field from the JSON response if successful.

        Raises:
            Exception: If the HTTP request fails, or API returns an error.
        """
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-Auth-Token"] = api_key

        logger.debug(
            f"Sending POST request to {url} with data={data} and headers={headers}"
        )
        resp = self.session.post(url, json.dumps(data), headers=headers)
        if resp.status_code != 200:
            logger.error(f"HTTP {resp.status_code} error from {url}: {resp.text}")
            raise Exception(f"HTTP {resp.status_code} error from {url}: {resp.text}")

        try:
            output = resp.json()
        except Exception as e:
            logger.error(f"Error parsing JSON response from {url}: {e}")
            raise Exception(f"Error parsing JSON response from {url}: {e}")

        if output.get("errorCode") is not None:
            logger.error(
                f"API Error {output['errorCode']}: {output.get('errorMessage')}"
            )
            raise Exception(
                f"API Error {output['errorCode']}: {output.get('errorMessage')}"
            )
        return output["data"]
