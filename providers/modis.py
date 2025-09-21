from typing import Dict, List

import requests

# Using loguru for enhanced logging throughout this provider.
from loguru import logger
from shapely.geometry import Polygon
from shapely.geometry.polygon import orient

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager
from utilities.ocifs_manager import OCIFSManager


class Modis(ProviderBase):
    """
    Provider for accessing and downloading Modis data from the MODIS API.

    This class enables searching for MODIS products using an area of interest (AOI)
    and downloading product data via the MODIS service. It handles query construction, interaction
    with the MODIS API, and integrates with the DownloadManager for retrieving datasets.

    Attributes:
        service_url (str): The base URL for the MODIS API endpoints.
        api_key (str): API key for authenticating with the MODIS API.
        session (requests.Session): Persistent HTTP session for making requests.
        download_manager (DownloadManager): Handles downloading and saving of data products.
    """

    def __init__(self, config_loader: ConfigLoader, ocifs_manager: OCIFSManager):
        """
        Initialize the Modis provider with configuration values.

        Loads API credentials and service endpoints from the provided ConfigLoader, and sets up the DownloadManager
        and HTTP session for subsequent operations.

        Args:
            config_loader (ConfigLoader): Loads required configuration variables for provider operation.
        """
        self.service_url = config_loader.get_var(
            "providers.modis.base_urls.service_url"
        )
        self.api_key = config_loader.get_var("providers.modis.credentials.token")

        logger.info("Initializing MODIS Provider and obtaining API token.")
        self.download_manager = DownloadManager(
            config_loader=config_loader, ocifs_manager=ocifs_manager
        )
        self.session = requests.Session()

    def get_access_token(self) -> str:
        """
        Placeholder for compatibility with other providers.
        MODIS does not require explicit access token retrieval; API key is sufficient.
        """
        pass  # MODIS does not require a token-based authentication like other providers.

    def search_products(
        self,
        collection: str,
        aoi: Polygon,
        product_type: str = None,
        start_date: str = None,
        end_date: str = None,
        tile_id: str = None,
    ) -> List[Dict]:
        """
        Search for MODIS products from the MODIS API within a specified AOI and collection.

        Constructs a query URL for the MODIS API using input parameters and area bounds,
        then formats the downloadable MODIS product links for use with the download manager.

        Args:
            collection (str): MODIS dataset or type identifier accepted by MODIS.
            aoi (Polygon): Area of interest as a Shapely Polygon used for spatial querying.
            product_type (str, optional): Processing level or MODIS variant (not always required).
            start_date (str, optional): Start date for product acquisition (format: YYYY-MM-DD).
            end_date (str, optional): End date for product acquisition (format: YYYY-MM-DD).

        Returns:
            List[Dict]: List containing query result information, such as constructed URLs for download.
        """

        logger.debug(f"Getting the intersecting Modis tiles for the aoi: {aoi.bounds}")
        # Prepare the search payload with the collection, product type, and date range.
        query_params = {
            "short_name": collection,
            "version": product_type,
            "temporal": f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",  # date range
            "polygon": ",".join(
                [
                    f"{lon},{lat}"
                    for lon, lat in list(orient(aoi, sign=1.0).exterior.coords)
                ]
            ),
            "page_size": 50,
        }

        headers = {"Authorization": f"Bearer {self.api_key}"}

        logger.info(
            f"Searching for products in collection '{collection}' from {start_date} to {end_date}."
        )
        logger.debug(f"Query parameters: {query_params}")

        try:
            logger.debug("Sending search request to EarthData API.")
            response = self.session.get(
                self.service_url, params=query_params, headers=headers
            )
            response.raise_for_status()

            data = response.json()
            granules = data["feed"]["entry"]
            products = []
            # Print product IDs and download URLs
            for g in granules:
                for link in g["links"]:
                    if (
                        "href" in link
                        and "data#" in link.get("rel", "")
                        and link["href"].endswith(".hdf")
                    ):
                        products.append(link["href"])

            # Log total found products by query
            logger.info(f"Found {len(products)} products matching the criteria.")
            # Return list of IDs only
            return products

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            raise

    def download_products(
        self, product_ids: List[str], output_dir: str = "downloads"
    ) -> List[str]:
        """
        Download Modis products using URLs or identifiers.

        Prepares filenames, organizes download tasks, and delegates the actual download of Modis files
        to the DownloadManager. Optionally specifies an output directory.

        Args:
            product_ids (List[str]): List of downloadable product URLs or identifiers.
            output_dir (str, optional): Directory where downloaded files are saved. Defaults to "downloads".

        Returns:
            List[str]: List of output file paths, if tracked by the DownloadManager.
        """
        logger.info(
            f"Starting download for {len(product_ids)} Modis products to directory '{output_dir}'."
        )
        product_dict = {
            "urls": [],
            "file_names": [],
        }
        # Add authorization header with the current access token
        product_dict["headers"] = {"Authorization": f"Bearer {self.api_key}"}

        for url in product_ids:
            file_name = url.split("/")[-1]
            product_dict["urls"].append(url)
            product_dict["file_names"].append(file_name)

        return self.download_manager.download_products(
            product_ids=product_dict, output_dir=output_dir
        )
