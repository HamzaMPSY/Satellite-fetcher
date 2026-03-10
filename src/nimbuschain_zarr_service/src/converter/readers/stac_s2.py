"""Sentinel-2 L2A STAC downloader via Microsoft Planetary Computer.

Rewritten to use stackstac + sign_inplace, matching the ucam-eo/tessera
alpha_version s2_fast_processor.py approach.

Key design decisions (from Tessera reference):
  - `modifier=planetary_computer.sign_inplace` on the STAC Client → tokens are
    refreshed lazily at compute() time, never stale.
  - `stackstac.stack()` handles lazy COG loading, reprojection, clipping and
    chunking in one shot.  Much faster than loading band-by-band with rioxarray.
  - Items are grouped by calendar date so two Sentinel-2 tiles covering the
    same orbit day are mosaicked into one time slice.
  - SCL-based cloud masking: invalid pixels (cloud, shadow, no-data) are set to
    NaN so the ZarrWriter can store float16/float32 with proper nodata.

Downloads the 10 bands Tessera expects:
  B02, B03, B04, B05, B06, B07, B08, B8A, B11, B12
"""

from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import xarray as xr
from shapely import wkt
from shapely.geometry import mapping

# ── band configuration ─────────────────────────────────────────────────────────

TESSERA_S2_BANDS = [
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

# MPC asset-key → stackstac band name
MPC_ASSET_KEYS = {
    "B02": "B02",
    "B03": "B03",
    "B04": "B04",
    "B05": "B05",
    "B06": "B06",
    "B07": "B07",
    "B08": "B08",
    "B8A": "B8A",
    "B11": "B11",
    "B12": "B12",
}

# SCL values that are valid (no cloud, no shadow, no no-data)
# 4=vegetation, 5=bare soil, 6=water, 7=unclassified, 10=thin cirrus, 11=snow
_SCL_VALID = {4, 5, 6, 7, 10, 11}

# Baseline offset correction (Sentinel-2 processing baseline ≥ 04.00 adds 1000)
_BASELINE_CUTOFF = datetime.datetime(2022, 1, 25)
_BASELINE_OFFSET = 1000


# ── downloader ─────────────────────────────────────────────────────────────────


class Sentinel2STACDownloader:
    """Download Sentinel-2 L2A scenes from Planetary Computer STAC.

    Returns an ``xr.Dataset`` with a ``bands`` variable shaped
    ``(time, band, y, x)`` — the same schema produced by ``Sentinel2Reader``
    and expected throughout the satkit / tessera-crop pipeline.

    Uses stackstac + sign_inplace (the Tessera reference implementation pattern)
    so SAS tokens are *never* stale: they are refreshed on every lazy compute().
    """

    STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
    COLLECTION = "sentinel-2-l2a"

    def __init__(
        self,
        target_crs: str = "EPSG:32629",
        target_resolution: float = 10.0,
        bands: Optional[List[str]] = None,
        apply_scl_mask: bool = True,
        chunksize: int = 1024,
    ):
        self.target_crs = target_crs
        self.target_resolution = target_resolution
        self.bands = bands or TESSERA_S2_BANDS
        self.apply_scl_mask = apply_scl_mask
        self.chunksize = chunksize

    # ── search ─────────────────────────────────────────────────────────────────

    def search(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: str,
        end_date: str,
        max_cloud_cover: Optional[float] = None,
        max_items: int = 200,
    ) -> List:
        import planetary_computer
        import pystac_client

        # sign_inplace → every item href is re-signed lazily right before
        # rasterio/GDAL opens the COG, guaranteeing tokens are always fresh.
        client = pystac_client.Client.open(
            self.STAC_API_URL,
            modifier=planetary_computer.sign_inplace,
        )

        query = {}
        if max_cloud_cover is not None:
            query["eo:cloud_cover"] = {"lt": max_cloud_cover}

        search = client.search(
            collections=[self.COLLECTION],
            bbox=bbox,
            datetime=f"{start_date}/{end_date}",
            query=query or None,
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
        max_cloud_cover: Optional[float] = None,
        max_items: int = 200,
    ) -> List:
        polygon = wkt.loads(wkt_str)
        bbox = polygon.bounds  # (minx, miny, maxx, maxy)  WGS84
        return self.search(bbox, start_date, end_date, max_cloud_cover, max_items)

    # ── per-date loading ───────────────────────────────────────────────────────

    @staticmethod
    def _group_by_date(items: List) -> Dict[str, List]:
        """Group STAC items by calendar date (YYYY-MM-DD)."""
        groups: Dict[str, List] = defaultdict(list)
        for item in items:
            date_key = item.properties.get("datetime", "")[:10]
            groups[date_key].append(item)
        return dict(sorted(groups.items()))

    def _stack_date(
        self,
        items: List,
        bbox_wgs84: Tuple[float, float, float, float],
        clip_geom,
        date_key: str,
    ) -> Optional[xr.DataArray]:
        """Load all scenes for one date with stackstac → single (band,y,x) slice.

        stackstac handles:
          - COG streaming with proper range requests
          - reprojection to target_crs
          - multi-tile mosaicking (several tiles for same date)

        The items were searched via a client with sign_inplace so their assets
        already carry valid, auto-refreshed SAS tokens.
        """
        import stackstac
        from rasterio.enums import Resampling

        assets_to_load = list(self.bands)
        if self.apply_scl_mask and "SCL" not in assets_to_load:
            assets_to_load = assets_to_load + ["SCL"]

        try:
            epsg = int(self.target_crs.split(":")[-1])
            stack = stackstac.stack(
                items=items,
                assets=assets_to_load,
                resolution=self.target_resolution,
                epsg=epsg,
                bounds_latlon=bbox_wgs84,  # WGS84 bbox → stackstac reprojects
                chunksize=self.chunksize,
                rescale=False,
                resampling=Resampling.bilinear,
            )
        except Exception:
            return None

        # stackstac returns (time, band, y, x).  Collapse time by median mosaic.
        # Use first axis for mosaicking when multiple tiles cover the same date.
        if stack.sizes.get("time", 1) > 1:
            # median is robust to cloud edges at tile borders
            stack = stack.median(dim="time", skipna=True)
        else:
            stack = stack.squeeze("time", drop=True)

        # ── SCL cloud masking ──────────────────────────────────────────────────
        if self.apply_scl_mask and "SCL" in stack.coords.get("band", []):
            scl = stack.sel(band="SCL").values.astype(np.float32)
            valid_mask = np.isin(scl, list(_SCL_VALID))  # True where cloud-free
            # Apply mask to spectral bands only
            band_da = stack.sel(band=self.bands)
            band_np = band_da.values.astype(np.float32)  # (n_bands, y, x)
            band_np[:, ~valid_mask] = np.nan
        else:
            band_da = stack.sel(
                band=[
                    b
                    for b in self.bands
                    if b in stack.coords.get("band", stack.band.values.tolist())
                ]
            )
            band_np = band_da.values.astype(np.float32)

        # ── baseline offset correction ─────────────────────────────────────────
        # Scenes processed with baseline ≥ 04.00 (after 2022-01-25) have +1000
        try:
            date_dt = datetime.datetime.strptime(date_key, "%Y-%m-%d")
            if date_dt > _BASELINE_CUTOFF:
                valid = ~np.isnan(band_np) & (band_np >= _BASELINE_OFFSET)
                band_np[valid] -= _BASELINE_OFFSET
        except ValueError:
            pass

        # ── clip to field polygon ──────────────────────────────────────────────
        import rioxarray  # noqa: F401

        result_da = xr.DataArray(
            band_np,
            dims=["band", "y", "x"],
            coords={
                "band": self.bands,
                "y": band_da.y.values,
                "x": band_da.x.values,
            },
        )
        result_da = result_da.rio.write_crs(self.target_crs)

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
        max_cloud_cover: Optional[float] = None,
        max_items: int = 200,
        progress: bool = True,
    ) -> xr.Dataset:
        """Search + download all matching scenes for a WKT polygon.

        Returns an ``xr.Dataset`` with variable ``bands`` shaped (time, band, y, x).
        """
        polygon = wkt.loads(wkt_str)
        bbox_wgs = tuple(polygon.bounds)  # (minx, miny, maxx, maxy)
        clip = mapping(polygon)

        items = self.search_by_polygon(
            wkt_str, start_date, end_date, max_cloud_cover, max_items
        )
        if not items:
            raise ValueError(f"No S2 scenes found between {start_date} and {end_date}")

        grouped = self._group_by_date(items)
        datasets = []

        for i, (date_key, day_items) in enumerate(grouped.items()):
            if progress:
                cloud_vals = [
                    it.properties.get("eo:cloud_cover", "?") for it in day_items
                ]
                print(f"  [{i+1}/{len(grouped)}] {date_key}  cloud={cloud_vals}")

            da = self._stack_date(day_items, bbox_wgs, clip, date_key)
            if da is None:
                continue

            ts = np.datetime64(date_key, "D")
            da = da.expand_dims(time=[ts])

            ds = xr.Dataset({"bands": da})
            datasets.append(ds)

        if not datasets:
            raise ValueError(
                "All S2 scenes failed to load (bad geometry or all cloudy)"
            )

        result = xr.concat(datasets, dim="time").sortby("time")
        return result
