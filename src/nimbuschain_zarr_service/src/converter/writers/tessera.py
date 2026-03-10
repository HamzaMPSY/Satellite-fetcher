from pathlib import Path
from typing import List, Optional

import numpy as np
import xarray as xr
from PIL import Image

from .base import BaseWriter


class TesseraWriter(BaseWriter):

    def __init__(
        self,
        tile_size: int = 500,
        s2_bands: Optional[List[str]] = None,
        overwrite: bool = True,
    ):
        self.tile_size = tile_size
        self.overwrite = overwrite
        self.s2_bands = s2_bands or [
            "B02",
            "B03",
            "B04",
            "B05",
            "B06",
            "B07",
            "B08",
            "B8A",
            "B11",
            "B12",
        ]

    def write(
        self,
        data: xr.Dataset,
        path: str,
        sar_ascending: Optional[xr.Dataset] = None,
        sar_descending: Optional[xr.Dataset] = None,
    ) -> None:
        output_root = Path(path) / "retiled_d_pixel"
        output_root.mkdir(parents=True, exist_ok=True)

        if not all(b in data.band.values for b in self.s2_bands):
            missing = [b for b in self.s2_bands if b not in data.band.values]
            raise ValueError(f"Dataset missing required Sentinel-2 bands: {missing}")

        ds_s2 = data.sel(band=self.s2_bands)

        invalid_vals = [0, 1, 2, 3, 8, 9]

        if "SCL" in data.band.values:
            scl = data["bands"].sel(band="SCL")
            mask = ~scl.isin(invalid_vals)
        else:
            da_first = (
                ds_s2["bands"] if "bands" in ds_s2 else ds_s2[list(ds_s2.data_vars)[0]]
            )
            mask = ~np.isnan(da_first.isel(band=0))

        height = data.sizes["y"]
        width = data.sizes["x"]

        sar_asc_np, sar_asc_doys = self._extract_sar_arrays(sar_ascending)
        sar_desc_np, sar_desc_doys = self._extract_sar_arrays(sar_descending)

        for y in range(0, height, self.tile_size):
            for x in range(0, width, self.tile_size):
                y_max = min(y + self.tile_size, height)
                x_max = min(x + self.tile_size, width)
                curr_h = y_max - y
                curr_w = x_max - x

                chip_dir_name = f"{y}_{x}_{y_max}_{x_max}"
                chip_path = output_root / chip_dir_name

                if chip_path.exists() and not self.overwrite:
                    continue
                chip_path.mkdir(parents=True, exist_ok=True)

                chip_ds = ds_s2.isel(y=slice(y, y_max), x=slice(x, x_max))

                if "bands" in chip_ds:
                    da = chip_ds["bands"]
                else:
                    da = chip_ds[list(chip_ds.data_vars)[0]]

                da = da.transpose("time", "y", "x", "band")
                bands_np = np.nan_to_num(da.values, nan=0.0).astype(np.float32)
                np.save(chip_path / "bands.npy", bands_np)

                import pandas as pd

                times = data.coords["time"].values
                doys = pd.to_datetime(times).dayofyear.to_numpy().astype(np.int32)
                np.save(chip_path / "doys.npy", doys)

                chip_mask = mask.isel(y=slice(y, y_max), x=slice(x, x_max))
                if hasattr(chip_mask, "compute"):
                    chip_mask = chip_mask.compute()
                mask_vals = (
                    chip_mask.values if hasattr(chip_mask, "values") else chip_mask
                )
                if callable(mask_vals):
                    mask_vals = np.array(chip_mask)
                np.save(chip_path / "masks.npy", np.asarray(mask_vals).astype(np.uint8))

                roi_da = ~np.isnan(da.isel(time=0, band=0))
                roi_np = roi_da.values.astype(np.uint8)
                Image.fromarray(roi_np * 255).save(chip_path / "roi.tiff")

                self._write_sar_chip(
                    chip_path,
                    "sar_ascending",
                    sar_asc_np,
                    sar_asc_doys,
                    y,
                    y_max,
                    x,
                    x_max,
                    curr_h,
                    curr_w,
                )
                self._write_sar_chip(
                    chip_path,
                    "sar_descending",
                    sar_desc_np,
                    sar_desc_doys,
                    y,
                    y_max,
                    x,
                    x_max,
                    curr_h,
                    curr_w,
                )

    def _extract_sar_arrays(self, sar_ds: Optional[xr.Dataset]) -> tuple:
        if sar_ds is None:
            return None, None

        import pandas as pd

        da = sar_ds["bands"] if "bands" in sar_ds else sar_ds[list(sar_ds.data_vars)[0]]
        da = da.transpose("time", "y", "x", "band")
        sar_np = np.nan_to_num(da.values, nan=0.0).astype(np.float32)
        doys = (
            pd.to_datetime(sar_ds.coords["time"].values)
            .dayofyear.to_numpy()
            .astype(np.int32)
        )
        return sar_np, doys

    def _write_sar_chip(
        self,
        chip_path: Path,
        prefix: str,
        sar_np: Optional[np.ndarray],
        sar_doys: Optional[np.ndarray],
        y: int,
        y_max: int,
        x: int,
        x_max: int,
        curr_h: int,
        curr_w: int,
    ) -> None:
        if sar_np is not None and sar_doys is not None:
            chip_sar = sar_np[:, y:y_max, x:x_max, :]
            np.save(chip_path / f"{prefix}.npy", chip_sar)
            np.save(chip_path / f"{prefix}_doy.npy", sar_doys)
        else:
            empty_sar = np.zeros((0, curr_h, curr_w, 2), dtype=np.float32)
            empty_doy = np.zeros((0,), dtype=np.int32)
            np.save(chip_path / f"{prefix}.npy", empty_sar)
            np.save(chip_path / f"{prefix}_doy.npy", empty_doy)
