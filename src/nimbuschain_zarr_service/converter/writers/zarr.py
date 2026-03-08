from __future__ import annotations

import xarray as xr


class ZarrWriter:
    def __init__(self, *, chunks: dict[str, int] | None = None):
        self.chunks = chunks or {"time": 1, "y": 512, "x": 512}

    def _clear_chunk_encodings(self, ds: xr.Dataset) -> xr.Dataset:
        for var in ds.variables:
            enc = ds[var].encoding
            if "chunks" in enc:
                enc.pop("chunks", None)
        return ds

    def write(self, data: xr.Dataset, path: str, *, mode: str = "w") -> None:
        cleaned = self._clear_chunk_encodings(data)
        cleaned = cleaned.chunk(self.chunks)
        cleaned.to_zarr(path, mode=mode, consolidated=True)
