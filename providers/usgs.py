import os
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse, unquote

import requests
from loguru import logger
from shapely.geometry import Polygon

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager, OCIFSManager


class Usgs(ProviderBase):
    def __init__(self, config_loader: ConfigLoader, ocifs_manager: OCIFSManager = None):
        self.service_url = config_loader.get_var("providers.usgs.base_urls.service_url")
        if not self.service_url.endswith("/"):
            self.service_url += "/"

        self.username = config_loader.get_var("providers.usgs.credentials.username")
        self.token = config_loader.get_var("providers.usgs.credentials.token")

        self.api_key = None
        self.session = requests.Session()
        self.dataset = None

        logger.info("Initializing USGS Provider and obtaining API token.")
        self.get_access_token()

        self.download_manager = DownloadManager(config_loader=config_loader, ocifs_manager=ocifs_manager)
        self.config_loader = config_loader

    def get_access_token(self) -> str:
        payload = {"username": self.username, "token": self.token}
        self.api_key = self._send_request(self.service_url + "login-token", payload)
        return self.api_key

    def _aoi_to_geojson(self, aoi: Polygon) -> dict:
        coords = list(aoi.exterior.coords)
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        return {"type": "Polygon", "coordinates": [[list(c) for c in coords]]}

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aoi: Optional[Polygon] = None,
        tile_id: str = None,
    ) -> List[Dict]:
        if not aoi:
            raise ValueError("USGS search requires an AOI polygon.")
        if not start_date or not end_date:
            raise ValueError("USGS search requires start_date and end_date.")

        self.dataset = collection

        spatial_filter = {"filterType": "geojson", "geoJson": self._aoi_to_geojson(aoi)}
        scene_filter = {
            "spatialFilter": spatial_filter,
            "acquisitionFilter": {"start": start_date, "end": end_date},
        }
        scene_payload = {"datasetName": collection, "sceneFilter": scene_filter, "maxResults": 1000}

        scenes = self._send_request(self.service_url + "scene-search", scene_payload, self.api_key)
        total = scenes.get("totalHits", 0) if isinstance(scenes, dict) else 0
        logger.info(f"Found {total} scenes in dataset '{collection}'.")

        products = []
        results = scenes.get("results", []) if isinstance(scenes, dict) else []
        if not results:
            return products

        sat_digit = None
        suffix = None
        if product_type:
            try:
                sat_digit = int(product_type[0])
                suffix = product_type[1:]
            except Exception:
                suffix = product_type

        for r in results:
            try:
                if not r.get("options", {}).get("bulk", False):
                    continue

                display_id = r.get("displayId", "")
                if suffix and suffix not in display_id:
                    continue

                if sat_digit is not None:
                    ok_sat = False
                    for m in r.get("metadata", []):
                        if m.get("fieldName") == "Satellite":
                            try:
                                if int(m.get("value")) == sat_digit:
                                    ok_sat = True
                                    break
                            except Exception:
                                pass
                    if not ok_sat:
                        continue

                products.append(r["entityId"])
            except Exception:
                continue

        logger.info(f"Returning {len(products)} downloadable entity IDs.")
        return products

    def download_products(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        if not self.dataset:
            raise ValueError("USGS dataset not set. Call search_products first.")
        if not product_ids:
            return []

        payload = {"datasetName": self.dataset, "entityIds": ",".join(product_ids)}
        options = self._send_request(self.service_url + "download-options", payload, self.api_key)

        option_list = options.get("options", []) if isinstance(options, dict) else options
        downloads = []
        for opt in option_list:
            if opt.get("available") and opt.get("entityId") and opt.get("id") and "Bundle" in (opt.get("productName") or ""):
                downloads.append({"entityId": opt["entityId"], "productId": opt["id"]})

        if not downloads:
            logger.error("No available Bundle downloads.")
            return []

        label = datetime.now().strftime("dl_%Y%m%d_%H%M%S")
        req_payload = {"downloads": downloads, "label": label}
        req_results = self._send_request(self.service_url + "download-request", req_payload, self.api_key)

        final_downloads = req_results.get("availableDownloads", []) if isinstance(req_results, dict) else []
        if not final_downloads:
            logger.error("No availableDownloads returned.")
            return []

        urls, names = [], []
        for i, d in enumerate(final_downloads):
            url = d.get("url")
            if not url:
                continue
            urls.append(url)

            path = unquote(urlparse(url).path)
            base = os.path.basename(path)
            if base and "." in base:
                fname = base
            else:
                fname = f"usgs_{self.dataset}_{label}_{i}.zip"
            names.append(fname)

        product_dict = {
            "urls": urls,
            "file_names": names,
            "headers": {},
            "refresh_token_callback": self.get_access_token,
        }

        logger.info(f"Downloading {len(urls)} files via DownloadManager.")
        return self.download_manager.download_products(product_dict, output_dir)

    def _send_request(self, url, data, api_key=None):
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-Auth-Token"] = api_key

        resp = self.session.post(url, json=data, headers=headers, timeout=60)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code} from {url}: {resp.text}")

        output = resp.json()
        if output.get("errorCode") is not None:
            raise Exception(f"API Error {output['errorCode']}: {output.get('errorMessage')}")

        return output["data"]
