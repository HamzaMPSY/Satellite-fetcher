from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import requests
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.providers.base import ProviderBase
from nimbuschain_fetch.settings import Settings


class UsgsProvider(ProviderBase):
    def __init__(self, settings: Settings, download_manager: DownloadManager):
        self.settings = settings
        self.download_manager = download_manager
        self.service_url = settings.nimbus_usgs_service_url.rstrip("/") + "/"
        self.username = settings.nimbus_usgs_username
        self.token = settings.nimbus_usgs_token
        self.session = requests.Session()
        self.api_key: str | None = None
        self.dataset: str | None = None

        if not self.username or not self.token:
            raise ValueError("USGS credentials are missing in environment variables.")

        self.get_access_token()

    def get_access_token(self) -> str:
        payload = {"username": self.username, "token": self.token}
        self.api_key = self._send_request("login-token", payload)
        return self.api_key

    def _send_request(self, endpoint: str, data: dict[str, Any]) -> Any:
        url = f"{self.service_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-Auth-Token"] = self.api_key

        response = self.session.post(url, json=data, headers=headers, timeout=60)
        response.raise_for_status()
        payload = response.json()
        if payload.get("errorCode"):
            raise RuntimeError(f"USGS API error {payload['errorCode']}: {payload.get('errorMessage')}")
        return payload.get("data")

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None = None,
    ) -> list[str]:
        if aoi is None:
            raise ValueError("USGS search requires an AOI polygon.")

        self.dataset = collection
        scene_payload = {
            "datasetName": collection,
            "sceneFilter": {
                "spatialFilter": {
                    "filterType": "geojson",
                    "geoJson": mapping(aoi),
                },
                "acquisitionFilter": {
                    "start": start_date,
                    "end": end_date,
                },
            },
            "maxResults": 1000,
        }
        data = self._send_request("scene-search", scene_payload)
        scenes = data.get("results", []) if isinstance(data, dict) else []

        product_ids: list[str] = []
        for scene in scenes:
            if not isinstance(scene, dict):
                continue
            entity_id = scene.get("entityId")
            if not entity_id:
                continue
            if product_type and product_type not in str(scene.get("displayId", "")):
                continue
            product_ids.append(str(entity_id))

        return product_ids

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        if not product_ids:
            return []
        if not self.dataset:
            raise RuntimeError("USGS dataset is not set. Call search_products first.")

        options_payload = {"datasetName": self.dataset, "entityIds": ",".join(product_ids)}
        options = self._send_request("download-options", options_payload)

        downloads: list[dict[str, Any]] = []
        opt_list = options.get("options", []) if isinstance(options, dict) else options
        for item in opt_list:
            if not isinstance(item, dict):
                continue
            if not item.get("available"):
                continue
            if "Bundle" not in str(item.get("productName", "")):
                continue
            if item.get("entityId") and item.get("id"):
                downloads.append({"entityId": item["entityId"], "productId": item["id"]})

        if not downloads:
            return []

        label = datetime.utcnow().strftime("dl_%Y%m%d_%H%M%S")
        request_payload = {"downloads": downloads, "label": label}
        request_result = self._send_request("download-request", request_payload)

        available = (
            request_result.get("availableDownloads", []) if isinstance(request_result, dict) else []
        )
        urls: list[str] = []
        file_names: list[str] = []
        for idx, item in enumerate(available):
            url = item.get("url")
            if not url:
                continue
            urls.append(str(url))

            path_name = Path(unquote(urlparse(url).path)).name
            if path_name and "." in path_name:
                file_names.append(path_name)
            else:
                file_names.append(f"usgs_{self.dataset}_{idx}.zip")

        payload = {
            "headers": {},
            "urls": urls,
            "file_names": file_names,
            "refresh_token_callback": self.get_access_token,
        }
        return self.download_manager.download_products(payload, output_dir=output_dir)
