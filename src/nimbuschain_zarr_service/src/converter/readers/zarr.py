from collections.abc import MutableMapping
from typing import Union

import xarray as xr

from .base import BaseReader


class ZarrReader(BaseReader):
    """Reads Zarr stores (local or cloud) into xarray Dataset."""

    def read(
        self,
        path: Union[str, MutableMapping],
        chunks: str = "auto",
    ) -> xr.Dataset:
        return xr.open_zarr(path, chunks=chunks, consolidated=True)
