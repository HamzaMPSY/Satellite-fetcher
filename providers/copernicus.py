import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import requests
from requests.exceptions import RequestException, Timeout as RequestsTimeout
from loguru import logger
from shapely.geometry import Polygon

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager, OCIFSManager


def _date_list(start: str, end: str) -> List[Tuple[str, str]]:
    """Generate one-day (start, end) intervals between two dates (inclusive)."""
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    ranges = []
    curr = start_dt
    while curr <= end_dt:
        day_start = curr.strftime("%Y-%m-%d")
        day_end = (curr + timedelta(days=1)).strftime("%Y-%m-%d")
        ranges.append((day_start, day_end))
        curr += timedelta(days=1)
    return ranges


def _download_one_day(args) -> int:
    """
    Worker function for download_date_range().  Instantiates a new Copernicus
    instance, effectue la recherche pour un jour et télécharge les produits.
    """
    (
        config_path,
        collection,
        product_type,
        day_start,
        day_end,
        aoi,
        tile_id,
        max_concurrent,
        output_dir,
    ) = args
    config_loader = ConfigLoader(config_file_path=config_path)
    provider = Copernicus(config_loader=config_loader, max_concurrent=max_concurrent)
    product_ids = provider.search_products(
        collection=collection,
        product_type=product_type,
        start_date=day_start,
        end_date=day_end,
        aoi=aoi,
        tile_id=tile_id,
    )
    if not product_ids:
        return 0
    downloaded = provider.download_products(product_ids, output_dir=output_dir)
    return len(downloaded or [])


class Copernicus(ProviderBase):
    """
    Accès aux produits de la Copernicus Data Space.  Supporte la recherche et
    le téléchargement en mode concurrent.  Respecte la limite de quatre
    connexions simultanées pour les utilisateurs généraux:contentReference[oaicite:1]{index=1}.
    """

    def __init__(
        self,
        config_loader: ConfigLoader,
        ocifs_manager: OCIFSManager = None,
        max_concurrent: Optional[int] = None,
    ):
        # URL de base, URL du token et URL de téléchargement
        self.base_url = config_loader.get_var("providers.copernicus.base_urls.base_url")
        self.token_url = config_loader.get_var("providers.copernicus.base_urls.token_url")
        self.download_url = config_loader.get_var("providers.copernicus.base_urls.download_url")

        # Identifiants
        self.username = config_loader.get_var("providers.copernicus.credentials.cdse_username")
        self.password = config_loader.get_var("providers.copernicus.credentials.cdse_password")
        if not self.username or not self.password:
            raise ValueError("Missing Copernicus credentials in config.yaml")

        # Gestionnaire de téléchargement
        self.download_manager = DownloadManager(config_loader=config_loader, ocifs_manager=ocifs_manager)

        # Si max_concurrent est fourni, on écrase la configuration par défaut
        if max_concurrent:
            self.download_manager.max_concurrent = max_concurrent

        # Copernicus throughput tuning:
        # default to 2 concurrent downloads (faster than strict sequential),
        # but cap at 4 to stay within typical service limits.
        cfg_conc = config_loader.get_var(
            "providers.copernicus.download.max_concurrent", 2
        )
        cfg_per_host = config_loader.get_var(
            "providers.copernicus.download.max_connections_per_host", cfg_conc
        )
        try:
            eff_conc = int(max_concurrent) if max_concurrent else int(cfg_conc)
        except Exception:
            eff_conc = 2
        eff_conc = max(1, min(eff_conc, 4))

        try:
            eff_per_host = int(cfg_per_host)
        except Exception:
            eff_per_host = eff_conc
        eff_per_host = max(1, min(eff_per_host, eff_conc, 4))

        self.download_manager.max_concurrent = eff_conc
        self.download_manager.max_connections_per_host = eff_per_host

        # Paramètres de retry du DownloadManager utilisés pour fetch_product_infos()
        self.max_retries = config_loader.get_var("download_manager.max_retries", 5)
        self.initial_delay = config_loader.get_var("download_manager.initial_delay", 2)
        self.backoff_factor = config_loader.get_var("download_manager.backoff_factor", 1.5)

        # HTTP robustness for auth/search calls (outside aiohttp DownloadManager).
        self.http_connect_timeout = float(
            config_loader.get_var("providers.copernicus.http.connect_timeout", 15)
        )
        self.http_read_timeout = float(
            config_loader.get_var("providers.copernicus.http.read_timeout", 60)
        )
        self.auth_max_retries = int(
            config_loader.get_var("providers.copernicus.http.auth_max_retries", 4)
        )
        self.auth_initial_delay = float(
            config_loader.get_var("providers.copernicus.http.auth_initial_delay", 2)
        )
        self.auth_backoff_factor = float(
            config_loader.get_var("providers.copernicus.http.auth_backoff_factor", 1.8)
        )

        # Création de la session HTTP et obtention du token
        self.session = requests.Session()
        logger.info("Obtaining access token for Copernicus provider.")
        self.access_token = self.get_access_token()

    def get_access_token(self) -> str:
        """Authenticate against the Copernicus API and return the access token."""
        data = {
            "client_id": "cdse-public",
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        delay = self.auth_initial_delay
        last_error: Optional[Exception] = None

        for attempt in range(1, self.auth_max_retries + 1):
            try:
                response = requests.post(
                    self.token_url,
                    data=data,
                    headers=headers,
                    timeout=(self.http_connect_timeout, self.http_read_timeout),
                )
                response.raise_for_status()
                token_data = response.json()
                token = token_data.get("access_token")
                if not token:
                    raise ValueError("No access_token field in token response")
                self.access_token = token
                return self.access_token
            except (RequestsTimeout, RequestException, ValueError) as e:
                last_error = e
                if attempt < self.auth_max_retries:
                    logger.warning(
                        f"Copernicus auth failed (attempt {attempt}/{self.auth_max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay = min(delay * self.auth_backoff_factor, 60)
                else:
                    break

        raise ConnectionError(
            "Unable to reach Copernicus identity service after "
            f"{self.auth_max_retries} attempts "
            f"(connect_timeout={self.http_connect_timeout}s, read_timeout={self.http_read_timeout}s). "
            f"Last error: {last_error}"
        ) from last_error

    def _build_query_params(
        self,
        collection: str,
        product_type: Optional[str],
        start_date: str,
        end_date: str,
        aoi: Optional[Polygon],
        tile_id: Optional[str],
        top: int = 1000,
        with_count: bool = False,
    ) -> Dict[str, str]:
        query_params = {
            "$filter": (
                f"Collection/Name eq '{collection}' "
                f"and ContentDate/Start gt '{start_date}T00:00:00Z' "
                f"and ContentDate/Start lt '{end_date}T23:59:59Z'"
            ),
            "$orderby": "ContentDate/Start desc",
            "$top": str(max(1, int(top))),
        }
        if with_count:
            query_params["$count"] = "true"

        if product_type:
            query_params["$filter"] += (
                " and Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )

        if tile_id:
            query_params["$filter"] += (
                " and Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'tileId' and "
                f"att/OData.CSC.StringAttribute/Value eq '{tile_id}')"
            )

        if aoi:
            query_params["$filter"] += (
                " and OData.CSC.Intersects(area=geography'SRID=4326;"
                f"{aoi.wkt}')"
            )
        return query_params

    def _request_products(self, query_params: Dict[str, str]) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.base_url}/odata/v1/Products"
        try:
            response = self.session.get(
                url,
                params=query_params,
                headers=headers,
                timeout=(self.http_connect_timeout, self.http_read_timeout),
            )
            response.raise_for_status()
            return response.json()
        except RequestsTimeout as e:
            raise TimeoutError(
                "Copernicus catalogue request timed out "
                f"(connect_timeout={self.http_connect_timeout}s, read_timeout={self.http_read_timeout}s)."
            ) from e
        except RequestException as e:
            raise ConnectionError(f"Copernicus catalogue request failed: {e}") from e

    @staticmethod
    def _extract_tile_id(product: Dict[str, Any]) -> Optional[str]:
        attrs = product.get("Attributes") or []
        if not isinstance(attrs, list):
            return None
        for a in attrs:
            if not isinstance(a, dict):
                continue
            if str(a.get("Name", "")).lower() == "tileid":
                val = a.get("Value")
                return str(val) if val is not None else None
        return None

    def search_products_detailed(
        self,
        collection: str = "SENTINEL-2",
        product_type: str = "S2MSI2A",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aoi: Optional[Polygon] = None,
        tile_id: Optional[str] = None,
        top: int = 250,
    ) -> Dict[str, Any]:
        """Return product details for preview UI (name, date, size, tile)."""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        query_params = self._build_query_params(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi=aoi,
            tile_id=tile_id,
            top=top,
            with_count=True,
        )
        data = self._request_products(query_params)
        products = data.get("value", []) if isinstance(data, dict) else []
        total = data.get("@odata.count", len(products)) if isinstance(data, dict) else len(products)

        items: List[Dict[str, Any]] = []
        for p in products:
            if not isinstance(p, dict):
                continue
            size_raw = p.get("ContentLength", 0)
            try:
                size_bytes = int(size_raw)
            except Exception:
                size_bytes = 0
            items.append(
                {
                    "id": p.get("Id"),
                    "name": p.get("Name"),
                    "sensing_time": p.get("ContentDate", {}).get("Start")
                    if isinstance(p.get("ContentDate"), dict)
                    else None,
                    "size_bytes": size_bytes,
                    "size_mb": round(size_bytes / (1024 * 1024), 1) if size_bytes else None,
                    "tile_id": self._extract_tile_id(p),
                }
            )
        return {"total": int(total) if str(total).isdigit() else len(items), "items": items}

    def search_products(
        self,
        collection: str = "SENTINEL-2",
        product_type: str = "S2MSI2A",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aoi: Optional[Polygon] = None,
        tile_id: Optional[str] = None,
    ) -> List[str]:
        """
        Requête OData pour obtenir les identifiants de produits.  Si les
        paramètres date ou aoi sont omis, l’intervalle couvre les 30 derniers
        jours.
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        detail = self.search_products_detailed(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi=aoi,
            tile_id=tile_id,
            top=1000,
        )
        products = detail.get("items", []) if isinstance(detail, dict) else []
        logger.info(f"Found {len(products)} products between {start_date} and {end_date}")
        return [p["id"] for p in products if isinstance(p, dict) and p.get("id")]

    def download_products(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products concurrently.  Uses fetch_product_infos() to
        retrieve download URLs in parallel, puis remet la main au
        DownloadManager.
        """
        logger.info(f"Starting download for {len(product_ids)} Copernicus products to '{output_dir}'.")
        product_dict: Dict[str, List] = {
            "urls": [],
            "file_names": [],
            "headers": {"Authorization": f"Bearer {self.access_token}"},
            "refresh_token_callback": self.get_access_token,
        }

        # Ajuster le parallélisme pour la collecte des métadonnées (maximum 10)
        max_info_conc = min(10, max(2, int(self.download_manager.max_concurrent) * 2))

        product_infos = asyncio.run(
            self.fetch_product_infos(
                product_ids=product_ids,
                base_url=self.base_url,
                download_url=self.download_url,
                headers=product_dict["headers"],
                max_concurrent=max_info_conc,
            )
        )

        for info in product_infos:
            if info:
                product_dict["urls"].append(info["download_url"])
                product_dict["file_names"].append(info["file_name"])

        logger.info(f"Triggering DownloadManager for {len(product_dict['urls'])} product(s).")
        return self.download_manager.download_products(product_dict, output_dir)

    def download_date_range(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: Optional[Polygon] = None,
        tile_id: Optional[str] = None,
        workers: int = 1,
        concurrent_per_worker: int = 2,
        output_dir: str = "downloads",
        config_path: str = "config.yaml",
    ) -> int:
        """
        Découpe une période en jours et télécharge les produits de chaque jour
        avec un parallélisme contrôlé.
        """
        workers = max(1, int(workers))
        concurrent_per_worker = max(1, int(concurrent_per_worker))
        total_conc = workers * concurrent_per_worker
        if total_conc > 4:
            logger.warning(
                f"High total concurrency requested for Copernicus: {total_conc} "
                f"(workers={workers} × concurrent_per_worker={concurrent_per_worker}). "
                "May trigger more HTTP 429 responses."
            )
        day_ranges = _date_list(start_date, end_date)
        args = [
            (
                config_path,
                collection,
                product_type,
                day_start,
                day_end,
                aoi,
                tile_id,
                concurrent_per_worker,
                output_dir,
            )
            for (day_start, day_end) in day_ranges
        ]
        downloaded = 0
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_download_one_day, arg) for arg in args]
            for fut in as_completed(futures):
                downloaded += fut.result()
        logger.info(f"Downloaded {downloaded} products across {len(day_ranges)} day(s).")
        return downloaded

    async def fetch_product_infos(
        self,
        product_ids: List[str],
        base_url: str,
        download_url: str,
        headers: Dict[str, str],
        max_concurrent: int = 10,
    ) -> List[Dict]:
        """
        Concurrent retrieval of product metadata via OData.  Limits the number
        of simultaneous requests using aiohttp semaphores.
        """
        sem = asyncio.Semaphore(max_concurrent)
        timeout = aiohttp.ClientTimeout(total=60, connect=20, sock_read=30)

        async def fetch_one(session: aiohttp.ClientSession, product_id: str):
            url = f"{base_url}/odata/v1/Products({product_id})"
            delay = self.initial_delay
            for attempt in range(1, self.max_retries + 1):
                async with sem:
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 429:
                                retry_after = resp.headers.get("Retry-After")
                                wait = int(retry_after) if retry_after else delay
                                logger.warning(f"429 for {product_id}, wait {wait}s (attempt {attempt}/{self.max_retries})")
                                await asyncio.sleep(wait)
                                delay = min(delay * self.backoff_factor, 60)
                                continue
                            if 500 <= resp.status < 600:
                                logger.warning(f"HTTP {resp.status} for {product_id}, retry in {delay}s")
                                await asyncio.sleep(delay)
                                delay = min(delay * self.backoff_factor, 60)
                                continue
                            resp.raise_for_status()
                            product_info = await resp.json()
                            return {
                                "download_url": f"{download_url}/odata/v1/Products({product_id})/$value",
                                "file_name": f"{product_info['Name']}.zip",
                            }
                    except aiohttp.ClientError as e:
                        logger.warning(f"Client error for {product_id}: {e} (attempt {attempt}/{self.max_retries})")
                        await asyncio.sleep(delay)
                        delay = min(delay * self.backoff_factor, 60)
            logger.error(f"Failed product info for {product_id} after {self.max_retries} attempts")
            return None

        async with aiohttp.ClientSession(headers=headers, timeout=timeout, trust_env=True) as session:
            tasks = [fetch_one(session, pid) for pid in product_ids]
            return await asyncio.gather(*tasks, return_exceptions=False)
