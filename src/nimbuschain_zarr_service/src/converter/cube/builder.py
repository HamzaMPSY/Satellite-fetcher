from typing import List, Optional

import xarray as xr


class CubeBuilder:
    """Stacks and appends satellite datasets into data cubes."""

    @staticmethod
    def stack(
        datasets: List[xr.Dataset],
        chunks: Optional[dict] = None,
    ) -> xr.Dataset:
        if chunks is None:
            chunks = {"time": 1, "y": 512, "x": 512}

        cube = xr.concat(datasets, dim="time")
        return cube.chunk(chunks)

    @staticmethod
    def append(existing_zarr: str, new_data: xr.Dataset) -> None:
        new_data.to_zarr(existing_zarr, append_dim="time")
