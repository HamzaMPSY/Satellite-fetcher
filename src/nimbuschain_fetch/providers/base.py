from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from shapely.geometry.base import BaseGeometry


class ProviderBase(ABC):
    @abstractmethod
    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        raise NotImplementedError


ProviderFactory = Any
