from __future__ import annotations

import xarray as xr


class CubeBuilder:
    """Utility to stack datasets along time and optionally append to existing Zarr."""

    def __init__(self, *, chunks: dict[str, int] | None = None):
        self.chunks = chunks or {"time": 1, "y": 512, "x": 512}

    def stack(self, datasets: list[xr.Dataset]) -> xr.Dataset:
        combined = xr.concat(datasets, dim="time")
        return combined.chunk(self.chunks)

    def append(self, existing_zarr: str, new_data: xr.Dataset) -> None:
        new_data.chunk(self.chunks).to_zarr(existing_zarr, append_dim="time", consolidated=True)
