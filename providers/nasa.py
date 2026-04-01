import asyncio
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List
from urllib.parse import urlparse

import aiohttp
import requests
from loguru import logger
from shapely.geometry import Polygon

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager, OCIFSManager


class Nasa(ProviderBase):
    """
    Provider for interacting with the NASA Earthdata CMR API to download HLS data.
    """

    def __init__(self, config_loader: ConfigLoader, ocifs_manager: OCIFSManager = None):
        # Default to JSON endpoint for easy parsing
        self.cmr_url = config_loader.get_var("providers.nasa.base_urls.cmr_url") or "https://cmr.earthdata.nasa.gov/search/granules.json"
        
        self.username = config_loader.get_var("providers.nasa.credentials.earthdata_username")
        self.password = config_loader.get_var("providers.nasa.credentials.earthdata_password")

        if not self.username or not self.password:
            logger.error("Username or password is not set in the configuration file.")
            raise ValueError("Please set earthdata_username and earthdata_password in config.yaml")

        self.max_retries = config_loader.get_var("download_manager.max_retries") or 3
        self.initial_delay = config_loader.get_var("download_manager.initial_delay") or 2
        self.backoff_factor = config_loader.get_var("download_manager.backoff_factor") or 2

        logger.info("Obtaining access token for NASA Earthdata provider.")
        self.access_token = self.get_access_token()
        self.download_manager = DownloadManager(config_loader=config_loader, ocifs_manager=ocifs_manager)
        self.session = requests.Session()

    def get_access_token(self) -> str:
        """
        Obtain access token from Earthdata Login using Basic Auth.
        """
        token_url = "https://urs.earthdata.nasa.gov/api/users/token"
        try:
            logger.info("Requesting EDL token from Earthdata Login.")
            response = requests.post(token_url, auth=(self.username, self.password))
            
            if response.status_code in (400, 403):
                # Limit reached or already exists, attempt to fetch existing token
                logger.info("Token request failed (likely limit reached), fetching existing tokens...")
                tokens_resp = requests.get("https://urs.earthdata.nasa.gov/api/users/tokens", auth=(self.username, self.password))
                if tokens_resp.status_code == 200:
                    tokens = tokens_resp.json()
                    if tokens and isinstance(tokens, list) and len(tokens) > 0:
                        self.access_token = tokens[0].get("access_token")
                        logger.info("Found existing Earthdata Login token.")
                        return self.access_token
            
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            logger.info("Successfully obtained EDL access token.")
            return self.access_token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get access token from Earthdata Login: {e}")
            if hasattr(e.response, "text"):
                logger.error(f"Response: {e.response.text}")
            raise

    @staticmethod
    def _normalize_tile_id(tile_id: str = None) -> str:
        if not tile_id:
            return None
        normalized = tile_id.strip().upper()
        if normalized.startswith("T"):
            normalized = normalized[1:]
        return normalized

    @staticmethod
    def _extract_granule_id(item: Dict) -> str:
        return (
            item.get("title")
            or item.get("producer_granule_id")
            or item.get("granule-ur")
            or item.get("id")
        )

    @staticmethod
    def _matches_tile_id(granule_id: str, tile_id: str = None) -> bool:
        normalized_tile = Nasa._normalize_tile_id(tile_id)
        if not normalized_tile or not granule_id:
            return True
        return re.search(rf"\.T{re.escape(normalized_tile)}\.", granule_id.upper()) is not None

    @staticmethod
    def _extract_file_name(url: str) -> str:
        return os.path.basename(urlparse(url).path)

    @staticmethod
    def _is_downloadable_asset(url: str) -> bool:
        return Nasa._extract_file_name(url).lower().endswith(".tif")

    @staticmethod
    def _build_relative_path(granule_id: str, url: str) -> str:
        file_name = Nasa._extract_file_name(url)
        if granule_id and file_name:
            return os.path.join(granule_id, file_name)
        return file_name or granule_id

    def search_products(
        self,
        collection: str = "HLS",
        product_type: str = "HLSS30.v2.0",
        start_date: str = None,
        end_date: str = None,
        aoi: Polygon = None,
        tile_id: str = None,
    ) -> List[Dict]:
        """
        Search for products via NASA CMR API.
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Extract version if present in the format like 'HLSS30.v2.0'
        if ".v" in product_type:
            parts = product_type.split(".v")
            short_name = parts[0]
            version = parts[1]
        else:
            short_name = product_type
            version = None

        query_params = {
            "short_name": short_name,
            "temporal": f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
            "page_size": 2000,
        }
        if version:
            query_params["version"] = version

        if aoi:
            minx, miny, maxx, maxy = aoi.bounds
            query_params["bounding_box"] = f"{minx},{miny},{maxx},{maxy}"

        if tile_id:
            normalized_tile = self._normalize_tile_id(tile_id)
            if normalized_tile:
                query_params["attribute[]"] = f"string,MGRS_TILE_ID,{normalized_tile}"

        logger.info(f"Searching NASA CMR with params: {query_params}")

        try:
            response = self.session.get(self.cmr_url, params=query_params)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("feed", {}).get("entry", [])
            logger.info(f"Found {len(items)} matching granules.")

            results = []
            seen_urls = set()
            for item in items:
                granule_id = self._extract_granule_id(item)
                if not self._matches_tile_id(granule_id, tile_id):
                    continue

                links = item.get("links", [])
                for link in links:
                    download_url = link.get("href")
                    if not download_url or download_url in seen_urls:
                        continue

                    # Keep only raster assets so each granule folder contains the HLS band TIFFs.
                    if (
                        link.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/data#"
                        and self._is_downloadable_asset(download_url)
                    ):
                        seen_urls.add(download_url)
                        results.append(
                            {
                                "url": download_url,
                                "title": granule_id,
                                "granule_id": granule_id,
                                "relative_path": self._build_relative_path(granule_id, download_url),
                            }
                        )

            logger.info(f"Extracted download links for {len(results)} granules.")
            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed: {e}")
            if hasattr(e.response, "text"):
                logger.error(f"Response: {e.response.text}")
            raise

    def download_products(self, product_ids: List[Dict], output_dir: str = "downloads") -> List[str]:
        """
        Fetch download URLs returned by CMR and delegate to DownloadManager.
        """
        logger.info(f"Preparing to download {len(product_ids)} NASA products to '{output_dir}'.")
        from typing import Any
        product_dict: Dict[str, Any] = {
            "urls": [],
            "file_names": []
        }

        # EDL Bearer Token header for authenticated downloads
        product_dict["headers"] = {"Authorization": f"Bearer {self.access_token}"}
        product_dict["refresh_token_callback"] = self.get_access_token

        for item in product_ids:
            if isinstance(item, dict) and "url" in item:
                url = item["url"]
                granule_id = item.get("granule_id") or item.get("title")
                file_path = item.get("relative_path") or self._build_relative_path(granule_id, url)

                product_dict["urls"].append(url)
                product_dict["file_names"].append(file_path)

        logger.info(f"Triggering DownloadManager for {len(product_dict['urls'])} NASA product(s).")
        return self.download_manager.download_products(product_dict, output_dir)

    def compute_products(self, product_ids: List[Dict], aoi: Polygon, equation: str, output_dir: str, index_name: str = "computed"):
        import os
        import re
        import xarray as xr
        import rioxarray
        import geopandas as gpd
        from collections import defaultdict
        
        logger.info(f"Preparing to compute '{equation}' on {len(product_ids)} bands, output: '{output_dir}'.")
        os.makedirs(output_dir, exist_ok=True)

        # Parse required bands from the equation like "(B08 - B04) / (B08 + B04)"
        required_bands = set(re.findall(r'[A-Za-z0-9_]+', equation))
        required_bands = {b for b in required_bands if not b.isdigit()}
        logger.info(f"Parsed required bands from equation: {required_bands}")

        # Ensure Earthdata Login headers are passed to GDAL
        os.environ["GDAL_HTTP_HEADERS"] = f"Authorization: Bearer {self.access_token}"
        os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
        
        # Group products by granule (title)
        granules = defaultdict(list)
        for item in product_ids:
            if isinstance(item, dict) and "url" in item:
                granule_id = item.get("granule_id") or item.get("title")
                if granule_id:
                    granules[granule_id].append(item["url"])

        for title, urls in granules.items():
            logger.info(f"Processing granule {title}")
            band_arrays = {}
            for band in required_bands:
                band_url = next((u for u in urls if u.endswith(f".{band}.tif")), None)
                if not band_url:
                    logger.warning(f"Could not find band {band} for granule {title}. Skipping...")
                    break
                
                # VSICURL prefix allows GDAL to read http streams
                vsi_url = f"/vsicurl/{band_url}" if band_url.startswith("http") else band_url
                
                try:
                    logger.debug(f"Streaming {band} from {vsi_url}")
                    da = rioxarray.open_rasterio(vsi_url)
                    
                    if aoi:
                        # Reproject AOI
                        gdf = gpd.GeoDataFrame(index=[0], crs="epsg:4326", geometry=[aoi])
                        gdf = gdf.to_crs(da.rio.crs)
                        logger.debug(f"Clipping {band} to AOI")
                        da = da.rio.clip(gdf.geometry.values, gdf.crs, drop=True, from_disk=True)
                    
                    # Force load in memory before applying equation to optimize array operations
                    da.load()
                    band_arrays[band] = da
                except Exception as e:
                    logger.error(f"Failed to stream/clip {band}: {e}")
                    break
            
            if len(band_arrays) == len(required_bands):
                try:
                    logger.info(f"Evaluating '{equation}' on {title}")
                    # Create safe local environment for equation evaluation (xarray objects only)
                    eval_locals = {band: da for band, da in band_arrays.items()}
                    result = eval(equation, {"__builtins__": None}, eval_locals)
                    
                    out_path = os.path.join(output_dir, f"{title}.{index_name}.tif")
                    logger.info(f"Saving computed result to {out_path}")
                    # Write to new GeoTIFF
                    result.rio.to_raster(out_path)
                except Exception as e:
                    logger.error(f"Error computing or saving equation for {title}: {e}")
            else:
                logger.warning(f"Missing required bands for {title}, skipping computation.")
