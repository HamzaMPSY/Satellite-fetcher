from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests
from requests import RequestException
from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.providers.base import ProviderBase
from nimbuschain_fetch.settings import Settings


class CopernicusProvider(ProviderBase):
    def __init__(
        self,
        settings: Settings,
        download_manager: DownloadManager,
        *,
        username: str | None = None,
        password: str | None = None,
        account_label: str | None = None,
        download_strategy: str = "default",
    ):
        self.settings = settings
        self.download_manager = download_manager
        self.base_url = settings.nimbus_copernicus_base_url.rstrip("/")
        self.token_url = settings.nimbus_copernicus_token_url
        self.download_url = settings.nimbus_copernicus_download_url.rstrip("/")
        self.username = str(username if username is not None else settings.nimbus_copernicus_username or "").strip()
        self.password = str(password if password is not None else settings.nimbus_copernicus_password or "").strip()
        self.account_label = str(account_label or "primary").strip() or "primary"
        self.download_strategy = str(download_strategy or "default").strip().lower() or "default"
        self.session = requests.Session()
        self._access_token: str | None = None
        self.last_download_metadata: dict[str, Any] = {}

        if not self.username or not self.password:
            raise ValueError("Copernicus credentials are missing in environment variables.")

    def _account_pool_requested(self) -> bool:
        return self.download_strategy == "copernicus_account_pool"

    def _account_pool_accounts(self) -> list[dict[str, str]]:
        return list(self.settings.copernicus_account_pool_accounts)

    def _account_pool_concurrency(self) -> int:
        return max(1, int(self.settings.nimbus_copernicus_account_pool_concurrency or 4))

    def _selected_account_pool(self, product_count: int) -> list[dict[str, str]]:
        accounts = self._account_pool_accounts()
        if not accounts:
            return []
        needed = max(1, min(len(accounts), max(1, int(product_count))))
        return accounts[: min(len(accounts), needed)]

    def _build_download_manager_for_account(self) -> DownloadManager:
        per_account_concurrency = self._account_pool_concurrency()
        return DownloadManager(
            max_concurrent=per_account_concurrency,
            max_retries=self.download_manager.max_retries,
            initial_delay=self.download_manager.initial_delay,
            backoff_factor=self.download_manager.backoff_factor,
            max_retry_delay=self.download_manager.max_retry_delay,
            connect_timeout=self.download_manager.connect_timeout,
            read_timeout=self.download_manager.read_timeout,
            chunk_size=self.download_manager.chunk_size,
            max_connections=self.download_manager.max_connections,
            max_connections_per_host=per_account_concurrency,
            enable_resume=self.download_manager.enable_resume,
            min_resume_size=self.download_manager.min_resume_size,
            gateway_timeout_retries=self.download_manager.gateway_timeout_retries,
            gateway_timeout_floor_delay=self.download_manager.gateway_timeout_floor_delay,
            progress_callback=self.download_manager.progress_callback,
            cancel_checker=self.download_manager.cancel_checker,
            retry_callback=self.download_manager.retry_callback,
        )

    @staticmethod
    def _distribute_products(
        product_ids: list[str],
        *,
        account_count: int,
    ) -> list[list[str]]:
        batches: list[list[str]] = [[] for _ in range(max(1, account_count))]
        for index, product_id in enumerate(product_ids):
            batches[index % len(batches)].append(product_id)
        return [batch for batch in batches if batch]

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
        retries: int = 6,
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
                time.sleep(self._backoff_delay(response.status_code, attempt, retry_after))
                continue

            response.raise_for_status()
            return response

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Copernicus request failed: {method} {url}")

    @staticmethod
    def _backoff_delay(status_code: int, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            base_wait = max(0.0, float(retry_after))
        else:
            base_wait = 0.0
        if int(status_code) == 429:
            return min(max(base_wait, 2.0), 300.0)
        if int(status_code) == 504:
            return min(max(base_wait, 8.0 * attempt), 180.0)
        if int(status_code) in {500, 502, 503}:
            return min(max(base_wait, 3.0 * attempt), 90.0)
        return min(max(base_wait, 2.0 * attempt), 60.0)

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
            if status_code in {503, 504}:
                raise RuntimeError(
                    f"Copernicus catalogue search is temporarily unavailable (HTTP {status_code}). "
                    "Retry in a few seconds."
                ) from exc
            raise RuntimeError("Copernicus catalogue search failed.") from exc
        payload = response.json()
        values: list[dict[str, Any]] = payload.get("value", [])
        return [str(item.get("Id")) for item in values if item.get("Id")]

    def _fetch_product_name(self, product_id: str) -> str:
        try:
            url = f"{self.base_url}/odata/v1/Products({product_id})"
            resp = self._request("GET", url, headers=self._auth_header(), timeout=60, retries=6)
            name = resp.json().get("Name")
            if name:
                return f"{name}.zip"
        except RequestException:
            pass
        return f"{product_id}.zip"

    def _download_products_single_account(self, product_ids: list[str], output_dir: str) -> list[str]:
        if not product_ids:
            return []
        urls: list[str] = []
        file_names: list[str] = []
        contexts: list[dict[str, Any]] = []

        for product_id in product_ids:
            resolved_name = self._fetch_product_name(product_id)
            urls.append(f"{self.download_url}/odata/v1/Products({product_id})/$value")
            file_names.append(resolved_name)
            contexts.append(
                {
                    "account_label": self.account_label,
                    "product_id": str(product_id),
                    "file_name": resolved_name,
                    "provider": "copernicus",
                }
            )

        payload = {
            "headers": self._auth_header(),
            "urls": urls,
            "file_names": file_names,
            "contexts": contexts,
            "refresh_token_callback": self.get_access_token,
        }
        return self.download_manager.download_products(payload, output_dir=output_dir)

    def _download_products_account_batch(
        self,
        *,
        account: dict[str, str],
        assigned_product_ids: list[str],
        output_dir: str,
    ) -> list[tuple[str, str]]:
        account_provider = CopernicusProvider(
            self.settings,
            self._build_download_manager_for_account(),
            username=account["username"],
            password=account["password"],
            account_label=account.get("label") or "pool",
            download_strategy="default",
        )
        paths = account_provider._download_products_single_account(assigned_product_ids, output_dir)
        return list(zip(assigned_product_ids, paths))

    def plan_download_metadata(self, product_count: int) -> dict[str, Any]:
        base = {
            "download_strategy": "copernicus_account_pool" if self._account_pool_requested() else "default",
            "account_pool_requested": self._account_pool_requested(),
            "account_pool_configured": self.settings.copernicus_account_pool_available,
            "account_pool_size": int(self.settings.copernicus_account_pool_size),
            "account_pool_per_account_concurrency": self._account_pool_concurrency(),
        }
        if not self._account_pool_requested():
            return base
        selected_accounts = self._selected_account_pool(product_count)
        assignment_summary: list[dict[str, Any]] = []
        if selected_accounts:
            synthetic_ids = [f"planned-{index}" for index in range(max(0, int(product_count)))]
            batches = self._distribute_products(synthetic_ids, account_count=len(selected_accounts))
            assignment_summary = [
                {
                    "account_label": str(account.get("label") or "pool").strip() or "pool",
                    "product_count": len(batch),
                }
                for account, batch in zip(selected_accounts, batches)
            ]
        base.update(
            account_pool_selected_accounts=len(selected_accounts),
            account_pool_per_account_concurrency=self._account_pool_concurrency(),
            account_pool_assignments=assignment_summary,
        )
        if len(self._account_pool_accounts()) <= 1:
            base["account_pool_fallback_reason"] = "insufficient_accounts"
        return base

    def _download_products_with_account_pool(self, product_ids: list[str], output_dir: str) -> list[str]:
        selected_accounts = self._selected_account_pool(len(product_ids))
        if len(selected_accounts) <= 1:
            self.last_download_metadata = {
                "download_strategy": "copernicus_account_pool",
                "account_pool_requested": True,
                "account_pool_configured": self.settings.copernicus_account_pool_available,
                "account_pool_size": int(self.settings.copernicus_account_pool_size),
                "account_pool_selected_accounts": len(selected_accounts),
                "account_pool_per_account_concurrency": self._account_pool_concurrency(),
            }
            if len(self._account_pool_accounts()) <= 1:
                self.last_download_metadata["account_pool_fallback_reason"] = "insufficient_accounts"
            return self._download_products_single_account(product_ids, output_dir)

        batches = self._distribute_products(product_ids, account_count=len(selected_accounts))
        ordered_paths: dict[str, str] = {}
        failures: list[str] = []
        assignment_summary: list[dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=len(batches)) as executor:
            future_map = {
                executor.submit(
                    self._download_products_account_batch,
                    account=account,
                    assigned_product_ids=batch,
                    output_dir=output_dir,
                ): (account, batch)
                for account, batch in zip(selected_accounts, batches)
                if batch
            }
            for future in as_completed(future_map):
                account, batch = future_map[future]
                label = str(account.get("label") or "pool").strip() or "pool"
                assignment_summary.append(
                    {
                        "account_label": label,
                        "product_count": len(batch),
                    }
                )
                try:
                    for product_id, path in future.result():
                        ordered_paths[str(product_id)] = str(path)
                except Exception as exc:
                    failures.append(f"{label}: {exc}")

        paths = [ordered_paths[product_id] for product_id in product_ids if product_id in ordered_paths]
        self.last_download_metadata = {
            "download_strategy": "copernicus_account_pool",
            "account_pool_requested": True,
            "account_pool_configured": self.settings.copernicus_account_pool_available,
            "account_pool_size": int(self.settings.copernicus_account_pool_size),
            "account_pool_selected_accounts": len(selected_accounts),
            "account_pool_per_account_concurrency": self._account_pool_concurrency(),
            "account_pool_assignments": assignment_summary,
            "account_pool_failures": failures,
        }
        if not paths and failures:
            detail = " | ".join(failures[:3])
            if len(failures) > 3:
                detail += f" | +{len(failures) - 3} more"
            raise RuntimeError(f"Copernicus account pool failed for all accounts. Causes: {detail}")
        return paths

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        self.last_download_metadata = {
            "download_strategy": "default",
            "account_pool_requested": self._account_pool_requested(),
            "account_pool_configured": self.settings.copernicus_account_pool_available,
            "account_pool_size": int(self.settings.copernicus_account_pool_size),
            "account_pool_per_account_concurrency": self._account_pool_concurrency(),
        }
        if self._account_pool_requested():
            return self._download_products_with_account_pool(product_ids, output_dir)
        return self._download_products_single_account(product_ids, output_dir)
