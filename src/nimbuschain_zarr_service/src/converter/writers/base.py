from abc import ABC, abstractmethod

import xarray as xr


class BaseWriter(ABC):
    @abstractmethod
    def write(self, data: xr.Dataset, path: str) -> None:
        pass
