from shapely.geometry import Polygon
from typing import List, Dict
from providers import ProviderBase
from utilities import ConfigLoader, DownloadManager
import requests
from urllib.parse import urlencode, urlparse, parse_qs
# Using loguru for enhanced logging throughout this provider.
from loguru import logger
from hashlib import md5

class OpenTopography(ProviderBase):
    """
    Provider for accessing and downloading topographic data from the OpenTopography API.

    This class enables searching for Digital Elevation Models (DEMs) using an area of interest (AOI)
    and downloading DEM products via the OpenTopography service. It handles query construction, interaction
    with the OpenTopography API, and integrates with the DownloadManager for retrieving datasets.

    Attributes:
        service_url (str): The base URL for the OpenTopography API endpoints.
        api_key (str): API key for authenticating with the OpenTopography API.
        session (requests.Session): Persistent HTTP session for making requests.
        download_manager (DownloadManager): Handles downloading and saving of data products.
    """

    def __init__(self, config_loader: ConfigLoader):
        """
        Initialize the OpenTopography provider with configuration values.

        Loads API credentials and service endpoints from the provided ConfigLoader, and sets up the DownloadManager
        and HTTP session for subsequent operations.

        Args:
            config_loader (ConfigLoader): Loads required configuration variables for provider operation.
        """
        self.service_url = config_loader.get_var("providers.openTopography.base_urls.service_url")
        self.api_key = config_loader.get_var("providers.openTopography.credentials.api_key")
        logger.info("Initializing OpenTopography Provider and obtaining API token.")
        self.download_manager = DownloadManager(config_loader=config_loader)
        self.session = requests.Session()


    def get_access_token(self) -> str:
        """
        Placeholder for compatibility with other providers. 
        OpenTopography does not require explicit access token retrieval; API key is sufficient.
        """
        pass  # OpenTopography does not require a token-based authentication like other providers.


    def search_products(self,
                        collection: str,
                        aoi: Polygon,
                        product_type: str=None,
                        start_date: str=None,
                        end_date: str=None
                        ) -> List[Dict]:
        """
        Search for DEM products from OpenTopography within a specified AOI and collection.

        Constructs a query URL for the OpenTopography API using input parameters and area bounds,
        then formats the downloadable DEM product links for use with the download manager.

        Args:
            collection (str): DEM dataset or type identifier accepted by OpenTopography ('demtype').
            aoi (Polygon): Area of interest as a Shapely Polygon used for spatial querying.
            product_type (str, optional): Processing level or DEM variant (not always required).
            start_date (str, optional): Start date for product acquisition (format: YYYY-MM-DD).
            end_date (str, optional): End date for product acquisition (format: YYYY-MM-DD).

        Returns:
            List[Dict]: List containing query result information, such as constructed URLs for download.
        """
        logger.info(f"Searching products in collection {collection} with product_type={product_type} for {start_date} to {end_date}.")

        # Prepare the search payload with the collection, product type, and date range.
        payload = {
            "demtype": collection,
            "south": aoi.bounds[1],
            "north": aoi.bounds[3],
            "west": aoi.bounds[0],
            "east": aoi.bounds[2],
            "outputFormat": "GTiff",
            "API_Key": self.api_key
        }

        # Send the search request to the USGS API.
        results = self._create_url(self.service_url, payload)
        return [results]

    def download_products(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download DEM products using URLs or identifiers.

        Prepares filenames, organizes download tasks, and delegates the actual download of DEM files
        to the DownloadManager. Optionally specifies an output directory.

        Args:
            product_ids (List[str]): List of downloadable product URLs or identifiers.
            output_dir (str, optional): Directory where downloaded files are saved. Defaults to "downloads".

        Returns:
            List[str]: List of output file paths, if tracked by the DownloadManager.
        """
        logger.info(f"Downloading {len(product_ids)} products to {output_dir}.")

        product_dict = {
            'urls': [],
            'file_names': [],
        }
        # Add headers for authentication (if needed by DownloadManager)
        product_dict['headers'] = {}

        for product in product_ids:
            # Extract Landsat product ID from query string in download URL
            parsed_url = urlparse(product)
            query_params = parse_qs(parsed_url.query)
            # Determine file name based on landsat_product_id in URL; fallback to "unknown.tif"
            file_name = query_params.get("demtype", [None])[0] + md5(product.encode()).hexdigest() + ".tif"
            product_dict['urls'].append(product)
            product_dict['file_names'].append(file_name)

        logger.info(f"Initiating download for {len(product_dict['urls'])} files using DownloadManager.")
        self.download_manager.download_products(product_dict, output_dir)
        logger.info("All downloads triggered; check output directory for results.")

    def _create_url(self, url: str, data: dict):
        """
        Build a fully-encoded request URL for the OpenTopography API.

        Args:
            url (str): The OpenTopography base API endpoint.
            data (dict): Query parameters as key-value pairs.

        Returns:
            str: Complete URL with query string suitable for OpenTopography API requests.
        """
        return f"{url}?{urlencode(data)}"
