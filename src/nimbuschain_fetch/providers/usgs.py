from __future__ import annotations

from datetime import datetime
from pathlib import Path
import time
from typing import Any
from urllib.parse import unquote, urlparse

import requests
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.providers.base import ProviderBase
from nimbuschain_fetch.settings import Settings
try:
    from nimbuschain_fetch.usgs_product_type import usgs_product_type_matches
except ModuleNotFoundError:
    import re

    def _normalize_usgs_product_type_from_display_id(display_id: str) -> str:
        parts = [part.strip().upper() for part in str(display_id or "").split("_") if part.strip()]
        if len(parts) < 2:
            return ""
        platform = parts[0]
        product_code = parts[1]
        digits = re.findall(r"\d", platform)
        if not digits or not product_code.startswith("L"):
            return ""
        return f"{digits[-1]}{product_code}"

    def usgs_product_type_matches(display_id: str, product_type: str) -> bool:
        requested = str(product_type or "").strip().upper()
        if not requested:
            return True
        display = str(display_id or "").strip().upper()
        if not display:
            return False
        if requested in display:
            return True
        return requested == _normalize_usgs_product_type_from_display_id(display)


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
        self.scene_names: dict[str, str] = {}

        if not self.username or not self.token:
            raise ValueError("USGS credentials are missing in environment variables.")

        self.get_access_token()

    def get_access_token(self) -> str:
        payload = {"username": self.username, "token": self.token}
        self.api_key = self._send_request("login-token", payload)
        return self.api_key

    def _send_request(self, endpoint: str, data: dict[str, Any]) -> Any:
        url = f"{self.service_url}{endpoint}"
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["X-Auth-Token"] = self.api_key

            try:
                response = self.session.post(url, json=data, headers=headers, timeout=60)
            except requests.RequestException as exc:
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"USGS request failed on {endpoint} after {max_attempts} attempts: {exc}"
                    ) from exc
                time.sleep(min(8.0, 1.5 * attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt < max_attempts:
                    time.sleep(min(10.0, 2.0 * attempt))
                    continue
                body = (response.text or "").strip()[:500]
                raise RuntimeError(
                    f"USGS HTTP {response.status_code} on {endpoint} after {max_attempts} attempts. "
                    f"Response: {body}"
                )

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                body = (response.text or "").strip()[:500]
                raise RuntimeError(
                    f"USGS HTTP {response.status_code} on {endpoint}. Response: {body}"
                ) from exc

            payload = response.json()
            if payload.get("errorCode"):
                error_code = str(payload.get("errorCode"))
                # Token can expire between calls: refresh once and retry transparently.
                if (
                    endpoint != "login-token"
                    and error_code.upper() in {"AUTH_UNAUTHORIZED", "AUTH_INVALID", "AUTH_EXPIRED"}
                    and attempt < max_attempts
                ):
                    self.get_access_token()
                    time.sleep(0.5)
                    continue
                raise RuntimeError(f"USGS API error {error_code}: {payload.get('errorMessage')}")
            return payload.get("data")

        raise RuntimeError(f"USGS request failed on {endpoint}: retry budget exhausted")

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
            "maxResults": 250,
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
            display_id = str(scene.get("displayId", "")).strip()
            if not usgs_product_type_matches(display_id, product_type):
                continue
            entity_id_str = str(entity_id)
            self.scene_names[entity_id_str] = display_id or entity_id_str
            product_ids.append(entity_id_str)

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

            entity_id = str(item.get("entityId") or "").strip()
            preferred_name = self.scene_names.get(entity_id) or f"usgs_{self.dataset}_{idx}"

            path_name = Path(unquote(urlparse(url).path)).name.strip()
            suffixes = "".join(Path(path_name).suffixes) if path_name else ""
            if suffixes and "." not in Path(preferred_name).name:
                file_names.append(f"{preferred_name}{suffixes}")
            elif path_name and "." in path_name:
                file_names.append(path_name)
            else:
                file_names.append(preferred_name)

        payload = {
            "headers": {},
            "urls": urls,
            "file_names": file_names,
            "refresh_token_callback": self.get_access_token,
        }
        return self.download_manager.download_products(payload, output_dir=output_dir)
