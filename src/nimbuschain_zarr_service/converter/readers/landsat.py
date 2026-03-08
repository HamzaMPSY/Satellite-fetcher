from __future__ import annotations

from pathlib import Path
from typing import Any
import re

import rioxarray  # type: ignore
import xarray as xr

from nimbuschain_zarr_service.converter.config import CollectionConfig
from nimbuschain_zarr_service.converter.fs import RemoteFS, is_remote_path, iter_matching_files
from nimbuschain_zarr_service.converter.utils import ensure_time


class LandsatReader:
    """Reads Landsat products (local or remote) into an xarray.Dataset with variable `bands`."""

    def __init__(
        self,
        collection_cfg: CollectionConfig,
        *,
        prefer_ref: list[str] | None = None,
        chunks: dict[str, int] | None = None,
        prefetch: bool = False,
    ) -> None:
        self.cfg = collection_cfg
        self.prefer_ref = prefer_ref or collection_cfg.reference_band_order or ["red", "B04"]
        self.chunks = chunks or {"x": 512, "y": 512}
        self.prefetch = prefetch

    def read(self, uri: str) -> xr.Dataset:
        if is_remote_path(uri):
            return self._read_remote(uri)
        return self._read_local(Path(uri))

    def _discover_local(self, root: Path) -> dict[str, Path]:
        exts = self.cfg.extensions or {".tif", ".tiff"}
        candidates = list(iter_matching_files(root, extensions=exts))
        return self._match_bands(candidates)

    def _discover_remote(self, rfs: RemoteFS) -> dict[str, str]:
        exts = self.cfg.extensions or {".tif", ".tiff"}
        files = rfs.list_files(extensions=exts)
        return self._match_bands(files)

    def _match_bands(self, files: list[Any]) -> dict[str, Any]:
        band_map: dict[str, Any] = {}
        for band in self.cfg.bands:
            for pattern in band.patterns:
                regex = re.compile(pattern, re.IGNORECASE)
                found = next((f for f in files if regex.search(str(f))), None)
                if found is not None:
                    band_map[band.name] = found
                    break
        return band_map

    def _choose_reference(self, band_map: dict[str, Any]) -> str:
        for candidate in self.prefer_ref:
            if candidate in band_map:
                return candidate
        if band_map:
            return next(iter(band_map.keys()))
        raise ValueError("No bands found to choose reference from.")

    def _open_band(self, path: Path, *, ref_da: xr.DataArray | None, categorical: bool) -> xr.DataArray:
        da = rioxarray.open_rasterio(path, chunks=self.chunks).squeeze("band", drop=True)
        if ref_da is None:
            return da
        if da.rio.crs != ref_da.rio.crs or da.rio.shape != ref_da.rio.shape or da.rio.transform() != ref_da.rio.transform():
            resampling = "nearest" if categorical else "bilinear"
            return da.rio.reproject_match(ref_da, resampling=resampling)
        return da

    def _read_local(self, root: Path) -> xr.Dataset:
        band_map = self._discover_local(root)
        if not band_map:
            raise FileNotFoundError("No Landsat bands matched local product")
        ref_name = self._choose_reference(band_map)
        ref_da = self._open_band(band_map[ref_name], ref_da=None, categorical=False)
        band_arrays = []
        band_names = []
        for band in self.cfg.bands:
            path = band_map.get(band.name)
            if path is None:
                continue
            arr = self._open_band(path, ref_da=ref_da, categorical=band.categorical)
            band_arrays.append(arr)
            band_names.append(band.name)

        stacked = xr.concat(band_arrays, dim="band")
        stacked = stacked.assign_coords(band=band_names)
        time_val = ensure_time(None)  # Could be improved with MTL parse
        stacked = stacked.expand_dims(time=[time_val])
        ds = xr.Dataset({"bands": stacked})
        ds["bands"].rio.write_crs(ref_da.rio.crs, inplace=True)
        ds["bands"].rio.write_transform(ref_da.rio.transform(), inplace=True)
        ds.attrs["product_id"] = root.name
        ds.attrs["original_path"] = str(root)
        return ds

    def _read_remote(self, uri: str) -> xr.Dataset:
        rfs = RemoteFS(uri)
        try:
            band_map = self._discover_remote(rfs)
            if not band_map:
                raise FileNotFoundError("No Landsat bands matched remote product")
            if self.prefetch:
                localized = {name: rfs.download(path) for name, path in band_map.items()}
            else:
                localized = {name: Path(path) if isinstance(path, str) else path for name, path in band_map.items()}
            ref_name = self._choose_reference(localized)
            ref_da = self._open_band(localized[ref_name], ref_da=None, categorical=False)
            band_arrays = []
            band_names = []
            for band in self.cfg.bands:
                path = localized.get(band.name)
                if path is None:
                    continue
                arr = self._open_band(path, ref_da=ref_da, categorical=band.categorical)
                band_arrays.append(arr)
                band_names.append(band.name)

            stacked = xr.concat(band_arrays, dim="band")
            stacked = stacked.assign_coords(band=band_names)
            time_val = ensure_time(None)
            stacked = stacked.expand_dims(time=[time_val])
            ds = xr.Dataset({"bands": stacked})
            ds["bands"].rio.write_crs(ref_da.rio.crs, inplace=True)
            ds["bands"].rio.write_transform(ref_da.rio.transform(), inplace=True)
            ds.attrs["product_id"] = uri
            ds.attrs["original_path"] = uri
            return ds
        finally:
            rfs.cleanup()
