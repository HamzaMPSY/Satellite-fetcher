from abc import ABC, abstractmethod
from typing import Dict, List

from shapely import Polygon


class ProviderBase(ABC):
    """
    Abstract base class for all satellite imagery data providers.

    This interface defines the methods required for authentication, searching,
    and downloading satellite data products. All concrete providers
    must implement these methods.

    Methods:
        get_access_token: Authenticate with external provider and retrieve access token.
        search_products: Search for available satellite products by spatial/temporal filters.
        download_products: Download multiple products matching product IDs to output directory.
    """

    @abstractmethod
    def get_access_token(self) -> str:
        """
        Authenticate with the external provider to retrieve an OAuth2 access token.

        Returns:
            str: Provider API authentication token.

        Raises:
            Exception: If authentication fails.
        """
        pass

    @abstractmethod
    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: Polygon,
        tile_id: str = None,
    ) -> List[Dict]:
        """
        Search for products in the provider's catalogue.

        Args:
            collection (str): Dataset or mission name (e.g., "landsat_ot_c2_l2").
            product_type (str): Product type or processing level.
            start_date (str): Acquisition start date in "YYYY-MM-DD" format.
            end_date (str): Acquisition end date in "YYYY-MM-DD" format.
            aoi (Polygon): Area of interest as a shapely Polygon (WGS84 coords).

        Returns:
            List[Dict]: List of product/catalog entries matching the query.

        Raises:
            Exception: If the search fails or is unsupported.
        """
        pass

    @abstractmethod
    def download_products(self, product_ids: List[str], output_dir: str) -> List[str]:
        """
        Download the given product IDs to the specified output directory.
        The implementation should support downloading multiple files concurrently (if possible).

        Args:
            product_ids (List[str]): List of product/entity IDs to download.
            output_dir (str): Path to store all downloaded files.

        Returns:
            List[str]: List of output file paths upon successful download.

        Raises:
            Exception: If download fails for any product.
        """
        pass
