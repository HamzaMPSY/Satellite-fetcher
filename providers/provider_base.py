from abc import ABC, abstractmethod
from typing import List, Dict

from shapely import Polygon

class ProviderBase(ABC):
    """
    Abstract base class for satellite imagery providers.
    Defines common methods that all providers must implement.
    """

    @abstractmethod
    def get_access_token(self) -> str:
        """Get OAuth2 access token from the provider's identity service."""
        pass

    @abstractmethod
    def search_products(self,
                        collection: str,
                        product_type: str,
                        start_date: str,
                        end_date: str,
                        aoi: Polygon) -> List[Dict]:
        """Search for products in the provider's catalogue."""
        pass

    @abstractmethod
    def download_products_concurrent(self, product_ids: List[str], output_dir: str) -> List[str]:
        """Download multiple products concurrently."""
        pass
