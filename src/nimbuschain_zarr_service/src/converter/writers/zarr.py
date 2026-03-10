from collections.abc import MutableMapping
from typing import Optional, Union

import xarray as xr

from .base import BaseWriter


class ZarrWriter(BaseWriter):
    """Writes xarray Dataset to Zarr format (local or cloud)."""

    def __init__(self, chunks: Optional[dict] = None):

        if chunks is None:
            chunks = {"time": 1, "y": 512, "x": 512}
        self.chunks = chunks

    def write(
        self,
        data: xr.Dataset,
        path: Union[str, MutableMapping],
        mode: str = "w",
    ) -> None:
        # Clear encoding chunks to avoid conflicts with new chunking
        for var in data.variables:
            data[var].encoding.pop("chunks", None)

        data = data.chunk(self.chunks)
        data.to_zarr(path, mode=mode, consolidated=True)
