import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import rioxarray
import xarray as xr
from loguru import logger
from rasterio.enums import Resampling

from converter.utilities import ConfigLoader

from .base import BaseReader, RemoteFS


class Sentinel2Reader(BaseReader):
    """Reads Sentinel-2 L1C/L2A products into xarray Dataset.

    Works with both local paths and OCI bucket paths. For OCI reading,
    pass an OCIStore to the constructor.
    """

    def __init__(self, config_file_path: str, oci_store=None):
        super().__init__()
        self.config_loader = ConfigLoader(config_file_path)
        self.oci_store = oci_store
        self._remote_fs = RemoteFS(oci_store) if oci_store else None

    def read(self, path: str, chunks: Optional[dict] = None) -> xr.Dataset:
        if chunks is None:
            chunks = {"x": 512, "y": 512}

        is_remote = self._remote_fs is not None
        product_name = path.rstrip("/").split("/")[-1] if is_remote else Path(path).name

        is_l2a = "MSIL2A" in product_name
        is_l1c = "MSIL1C" in product_name

        if not (is_l2a or is_l1c):
            raise ValueError(
                "The provided file is not a valid Sentinel-2 L1C or L2A product."
            )
        if is_l2a:
            bands_map = self.config_loader.get_var("S2.L2A.bands_map")
            ext = self.config_loader.get_var("S2.L2A.ext")
            subdir_filter = self.config_loader.get_var("S2.L2A.subdir_filter")
            cat_bands = self.config_loader.get_var("S2.L2A.categorical_bands")
        elif is_l1c:
            bands_map = self.config_loader.get_var("S2.L1C.bands_map")
            ext = self.config_loader.get_var("S2.L1C.ext")
            subdir_filter = self.config_loader.get_var("S2.L1C.subdir_filter")
            cat_bands = self.config_loader.get_var("S2.L1C.categorical_bands")

        if subdir_filter == "empty":
            subdir_filter = None

        # --- File discovery: local vs OCI ---
        if is_remote:
            logger.info(f"Discovering bands for {product_name} in OCI...")
            files = self._find_bands_remote(bands_map, path, ext, self._remote_fs)
            logger.info(f"Found {len(files)} matching band files in OCI")
        else:
            path = Path(path)
            img_data_dirs = [
                d
                for d in path.rglob("*")
                if d.is_dir() and (subdir_filter is None or subdir_filter in d.name)
            ]

            if not img_data_dirs:
                raise FileNotFoundError(f"No IMG_DATA directory found in {path}")

            img_dir = img_data_dirs[0]
            files = self._find_bands(bands_map, img_dir, ext)

        all_bands = [b for sublist in bands_map.values() for b in sublist]
        regexes = [re.compile(p) for p in all_bands]

        sorted_files = sorted(
            files,
            key=lambda s: next(
                (i for i, r in enumerate(regexes) if r.search(str(s).split("/")[-1])),
                len(regexes),
            ),
        )

        if not sorted_files:
            raise FileNotFoundError("No band files found in the specified directory.")

        # --- Optimize: Download all bands in parallel if remote ---
        if is_remote and self._remote_fs:
            files_to_download = [
                str(f) for f in sorted_files
            ]  # sorted_files are strings in remote mode
            self._remote_fs.download_batch(files_to_download)

        # --- Reference band ---
        try:
            ref_file = next(f for f in sorted_files if "B04" in str(f).split("/")[-1])
        except StopIteration:
            ref_file = sorted_files[0]

        ref_da = self._open_rasterio(ref_file, chunks=chunks, remote_fs=self._remote_fs)
        ref_shape = (ref_da.sizes["y"], ref_da.sizes["x"])
        ref_crs = ref_da.rio.crs
        ref_transform = ref_da.rio.transform()

        # --- Read all bands ---
        band_arrays = {}
        for f in sorted_files:
            fname = str(f).split("/")[-1]
            match_idx = next(
                (i for i, r in enumerate(regexes) if r.search(fname)), None
            )
            if match_idx is None:
                continue

            band_name = all_bands[match_idx]

            da = self._open_rasterio(f, chunks=chunks, remote_fs=self._remote_fs)

            if da.sizes["y"] != ref_shape[0] or da.sizes["x"] != ref_shape[1]:
                resampling = (
                    Resampling.nearest
                    if band_name in cat_bands
                    else Resampling.bilinear
                )
                da = da.rio.reproject(
                    ref_crs,
                    shape=ref_shape,
                    transform=ref_transform,
                    resampling=resampling,
                )

            da = da.squeeze("band", drop=True)
            band_arrays[band_name] = da

        # --- Stack and build Dataset ---
        final_bands = []
        final_names = []
        target_order = all_bands

        for bname in target_order:
            if bname in band_arrays:
                final_bands.append(band_arrays[bname])
                final_names.append(bname)

        if not final_bands:
            raise ValueError("No matching bands found for the target order")

        stacked = xr.concat(final_bands, dim="band")
        stacked = stacked.assign_coords(band=final_names)

        date_str = product_name.split("_")[2]
        try:
            timestamp = datetime.strptime(date_str, "%Y%m%dT%H%M%S")
        except ValueError:
            timestamp = datetime.now()

        stacked = stacked.expand_dims(time=[np.datetime64(timestamp)])

        ds = xr.Dataset(
            {"bands": stacked},
            attrs={
                "product_id": product_name,
                "original_path": str(path),
            },
        )

        ds = ds.rio.write_crs(ref_crs)
        ds = ds.rio.write_transform(ref_transform)

        # Clean up temp files if we were reading from OCI
        if self._remote_fs:
            self._remote_fs.cleanup()

        return ds
