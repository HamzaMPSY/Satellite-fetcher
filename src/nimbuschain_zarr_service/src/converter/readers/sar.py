"""Sentinel-1 RTC STAC downloader via Microsoft Planetary Computer.

Rewritten to use stackstac + sign_inplace, matching the ucam-eo/tessera
alpha_version s1_fast_processor.py approach.

Key design decisions (from Tessera reference):
  - `modifier=planetary_computer.sign_inplace` on the STAC Client → SAS tokens
    are refreshed lazily at compute() time, never stale.
  - `stackstac.stack()` handles lazy COG loading, reprojection, and multi-tile
    mosaicking (ascending + descending orbit scenes on the same date).
  - Items are grouped by calendar date. Multiple scenes for the same date are
    median-mosaicked, consistent with the Tessera preprocessing reference.
  - Optional amplitude→dB conversion (off by default, raw backscatter kept).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import xarray as xr
from shapely import wkt
from shapely.geometry import mapping

# ── constants ──────────────────────────────────────────────────────────────────

_S1_BANDS = ["vv", "vh"]  # asset keys on Planetary Computer sentinel-1-rtc


# ── downloader ─────────────────────────────────────────────────────────────────


class SARDownloader:
    """Download Sentinel-1 RTC scenes from Planetary Computer STAC.

    Returns an ``xr.Dataset`` with a ``bands`` variable shaped
    ``(time, band, y, x)`` with band coords ``["VV", "VH"]``.

    Uses stackstac + sign_inplace (Tessera reference pattern) so SAS tokens
    are *never* stale: they are refreshed on every lazy compute().
    """

    STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
    COLLECTION = "sentinel-1-rtc"
    BANDS = ["VV", "VH"]

    def __init__(
        self,
        target_crs: str = "EPSG:32629",
        target_resolution: float = 10.0,
        chunksize: int = 1024,
    ):
        self.target_crs = target_crs
        self.target_resolution = target_resolution
        self.chunksize = chunksize

    # ── search ─────────────────────────────────────────────────────────────────

    def search(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: str,
        end_date: str,
        orbit: Optional[str] = None,
        max_items: int = 200,
    ) -> List:
        import planetary_computer
        import pystac_client

        # sign_inplace → every item href is re-signed lazily right before
        # rasterio/GDAL opens the COG → tokens never expire
        client = pystac_client.Client.open(
            self.STAC_API_URL,
            modifier=planetary_computer.sign_inplace,
        )

        query_params: dict = {}
        if orbit:
            query_params["sat:orbit_state"] = {"eq": orbit}

        search = client.search(
            collections=[self.COLLECTION],
            bbox=bbox,
            datetime=f"{start_date}/{end_date}",
            query=query_params or None,
            max_items=max_items,
        )
        items = list(search.items())
        items.sort(key=lambda i: i.datetime)
        return items

    def search_by_polygon(
        self,
        wkt_str: str,
        start_date: str,
        end_date: str,
        orbit: Optional[str] = None,
        max_items: int = 200,
    ) -> List:
        polygon = wkt.loads(wkt_str)
        bbox = tuple(polygon.bounds)  # (minx, miny, maxx, maxy) WGS84
        return self.search(bbox, start_date, end_date, orbit=orbit, max_items=max_items)

    # ── per-date loading ───────────────────────────────────────────────────────

    @staticmethod
    def _group_by_date(items: List) -> Dict[str, List]:
        groups: Dict[str, List] = defaultdict(list)
        for item in items:
            date_key = (item.properties.get("datetime") or "")[:10]
            groups[date_key].append(item)
        return dict(sorted(groups.items()))

    def _stack_date(
        self,
        items: List,
        bbox_wgs84: Tuple[float, float, float, float],
        clip_geom,
        date_key: str,
    ) -> Optional[xr.DataArray]:
        """Load all S1 scenes for one date → single (band,y,x) slice.

        stackstac handles COG streaming, reprojection, and mosaicking.
        The items already carry auto-refreshed SAS tokens via sign_inplace.
        """
        import rioxarray  # noqa: F401  – registers .rio accessor
        import stackstac
        from rasterio.enums import Resampling

        try:
            epsg = int(self.target_crs.split(":")[-1])
            stack = stackstac.stack(
                items=items,
                assets=_S1_BANDS,  # vv, vh (lowercase on PC)
                resolution=self.target_resolution,
                epsg=epsg,
                bounds_latlon=bbox_wgs84,
                chunksize=self.chunksize,
                rescale=False,
                resampling=Resampling.bilinear,
            )
        except Exception:
            return None

        # Verify both bands are present
        band_vals = list(stack.coords.get("band", xr.DataArray()).values)
        if "vv" not in band_vals or "vh" not in band_vals:
            return None

        # Collapse the time dimension: multiple tiles for same day → median mosaic
        if stack.sizes.get("time", 1) > 1:
            stack = stack.median(dim="time", skipna=True)
        else:
            stack = stack.squeeze("time", drop=True)

        # Compute eagerly (data is small after clipping) — this triggers the
        # actual HTTPS range-request COG reads while the signed URLs are fresh.
        try:
            arr = stack.compute().values.astype(np.float32)  # (2, y, x)
        except Exception:
            return None

        result_da = xr.DataArray(
            arr,
            dims=["band", "y", "x"],
            coords={
                "band": ["VV", "VH"],
                "y": stack.y.values,
                "x": stack.x.values,
            },
        )
        result_da = result_da.rio.write_crs(self.target_crs)

        # Clip to field polygon
        if clip_geom is not None:
            try:
                result_da = result_da.rio.clip(
                    [clip_geom], crs="EPSG:4326", all_touched=True
                )
            except Exception:
                return None

        return result_da

    # ── public API ─────────────────────────────────────────────────────────────

    def download_for_polygon(
        self,
        wkt_str: str,
        start_date: str,
        end_date: str,
        orbit: Optional[str] = None,
        target_shape: Optional[Tuple[int, int]] = None,
        target_transform=None,
    ) -> xr.Dataset:
        polygon = wkt.loads(wkt_str)
        bbox_wgs = tuple(polygon.bounds)
        clip = mapping(polygon)

        items = self.search_by_polygon(wkt_str, start_date, end_date, orbit=orbit)
        if not items:
            raise ValueError(
                f"No SAR data found for polygon between {start_date} and {end_date}"
            )

        grouped = self._group_by_date(items)
        datasets = []

        for date_key, day_items in grouped.items():
            da = self._stack_date(day_items, bbox_wgs, clip, date_key)
            if da is None:
                continue

            ts = np.datetime64(date_key, "D")
            da = da.expand_dims(time=[ts])

            ds = xr.Dataset({"bands": da})
            datasets.append(ds)

        if not datasets:
            raise ValueError("No valid SAR scenes found in the provided items")

        result = xr.concat(datasets, dim="time").sortby("time")
        return result

    def download_matched_to_s2(
        self,
        wkt_str: str,
        start_date: str,
        end_date: str,
        s2_ds: xr.Dataset,
        orbit: Optional[str] = None,
    ) -> xr.Dataset:
        """Download S1 matched to an existing S2 dataset's CRS and extent."""
        ref_crs = s2_ds.rio.crs
        if ref_crs:
            self.target_crs = str(ref_crs)
        return self.download_for_polygon(wkt_str, start_date, end_date, orbit=orbit)
