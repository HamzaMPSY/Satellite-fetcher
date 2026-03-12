from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

import requests
from requests import RequestException
from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.providers.base import ProviderBase
from nimbuschain_fetch.settings import Settings


class CopernicusProvider(ProviderBase):
    def __init__(self, settings: Settings, download_manager: DownloadManager):
        self.settings = settings
        self.download_manager = download_manager
        self.base_url = settings.nimbus_copernicus_base_url.rstrip("/")
        self.token_url = settings.nimbus_copernicus_token_url
        self.download_url = settings.nimbus_copernicus_download_url.rstrip("/")
        self.username = settings.nimbus_copernicus_username
        self.password = settings.nimbus_copernicus_password
        self.session = requests.Session()
        self._access_token: str | None = None

        if not self.username or not self.password:
            raise ValueError("Copernicus credentials are missing in environment variables.")

    def _retry_after_seconds(self, response: requests.Response) -> float | None:
        raw_value = response.headers.get("Retry-After")
        if not raw_value:
            return None
        try:
            return max(0.0, float(raw_value))
        except Exception:
            return None

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: int = 60,
        retries: int = 4,
        allow_auth_refresh: bool = True,
    ) -> requests.Response:
        merged_headers = dict(headers or {})
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=merged_headers,
                    params=params,
                    timeout=timeout,
                )
            except RequestException as exc:
                last_error = exc
                if attempt >= retries:
                    raise
                time.sleep(min(10.0, 1.5 * attempt))
                continue

            if response.status_code == 401 and allow_auth_refresh and attempt < retries:
                self._access_token = None
                merged_headers = dict(headers or {})
                merged_headers.update(self._auth_header())
                time.sleep(0.5)
                continue

            if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                retry_after = self._retry_after_seconds(response)
                time.sleep(retry_after if retry_after is not None else min(12.0, 2.0 * attempt))
                continue

            response.raise_for_status()
            return response

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Copernicus request failed: {method} {url}")

    def get_access_token(self) -> str:
        payload = {
            "client_id": "cdse-public",
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=payload, headers=headers, timeout=40)
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("Copernicus token endpoint did not return access_token.")
        self._access_token = token
        return token

    def _auth_header(self) -> dict[str, str]:
        token = self._access_token or self.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def _build_filter(
        self,
        *,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None,
    ) -> str:
        query = (
            f"Collection/Name eq '{collection}' "
            f"and ContentDate/Start gt '{start_date}T00:00:00Z' "
            f"and ContentDate/Start lt '{end_date}T23:59:59Z'"
        )

        if product_type:
            query += (
                " and Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )

        if tile_id:
            query += (
                " and Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'tileId' and "
                f"att/OData.CSC.StringAttribute/Value eq '{tile_id}')"
            )

        if aoi is not None:
            query += f" and OData.CSC.Intersects(area=geography'SRID=4326;{aoi.wkt}')"

        return query

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None = None,
    ) -> list[str]:
        if not start_date:
            start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.utcnow().strftime("%Y-%m-%d")

        params = {
            "$filter": self._build_filter(
                collection=collection,
                product_type=product_type,
                start_date=start_date,
                end_date=end_date,
                aoi=aoi,
                tile_id=tile_id,
            ),
            "$orderby": "ContentDate/Start desc",
            "$top": "1000",
        }

        url = f"{self.base_url}/odata/v1/Products"
        try:
            response = self._request(
                "GET",
                url,
                params=params,
                headers=self._auth_header(),
                timeout=60,
            )
        except RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code == 503:
                raise RuntimeError(
                    "Copernicus catalogue search is temporarily unavailable (HTTP 503). "
                    "Retry in a few seconds."
                ) from exc
            raise RuntimeError("Copernicus catalogue search failed.") from exc
        payload = response.json()
        values: list[dict[str, Any]] = payload.get("value", [])
        return [str(item.get("Id")) for item in values if item.get("Id")]

    def _fetch_product_name(self, product_id: str) -> str:
        try:
            url = f"{self.base_url}/odata/v1/Products({product_id})"
            resp = self._request("GET", url, headers=self._auth_header(), timeout=60)
            name = resp.json().get("Name")
            if name:
                return f"{name}.zip"
        except RequestException:
            pass
        return f"{product_id}.zip"

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        if not product_ids:
            return []
        urls: list[str] = []
        file_names: list[str] = []

        for product_id in product_ids:
            urls.append(f"{self.download_url}/odata/v1/Products({product_id})/$value")
            file_names.append(self._fetch_product_name(product_id))

        payload = {
            "headers": self._auth_header(),
            "urls": urls,
            "file_names": file_names,
            "refresh_token_callback": self.get_access_token,
        }
        return self.download_manager.download_products(payload, output_dir=output_dir)
