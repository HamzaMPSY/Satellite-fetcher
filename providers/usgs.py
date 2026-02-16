import concurrent.futures
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote

import requests
from loguru import logger
from shapely.geometry import Polygon, mapping

from providers.provider_base import ProviderBase
from utilities import ConfigLoader, DownloadManager, OCIFSManager


class Usgs(ProviderBase):
    def __init__(
        self,
        config_loader: ConfigLoader,
        ocifs_manager: OCIFSManager = None,
        max_concurrent: Optional[int] = None,
    ):
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
        if max_concurrent:
            try:
                mc = max(1, int(max_concurrent))
                self.download_manager.max_concurrent = mc
                self.download_manager.max_connections_per_host = max(
                    1, min(mc, int(self.download_manager.max_connections_per_host))
                )
            except Exception:
                pass
        self.config_loader = config_loader

    @staticmethod
    def _filename_from_content_disposition(content_disposition: Optional[str]) -> Optional[str]:
        """Extract a filename from Content-Disposition header."""
        if not content_disposition:
            return None

        # RFC 5987 form: filename*=UTF-8''encoded_name
        m_star = re.search(r"filename\*\s*=\s*([^;]+)", content_disposition, flags=re.IGNORECASE)
        if m_star:
            raw = m_star.group(1).strip().strip('"')
            if "''" in raw:
                raw = raw.split("''", 1)[1]
            decoded = unquote(raw)
            if decoded:
                return os.path.basename(decoded)

        # Legacy form: filename="name.ext" or filename=name.ext
        m_plain = re.search(r"filename\s*=\s*\"?([^\";]+)\"?", content_disposition, flags=re.IGNORECASE)
        if m_plain:
            value = m_plain.group(1).strip()
            if value:
                return os.path.basename(value)
        return None

    def _resolve_download_filename(self, url: str, label: str, index: int) -> str:
        """
        Resolve USGS filename robustly:
        1) Content-Disposition from HEAD/GET
        2) URL path basename
        3) deterministic fallback
        """
        fallback = f"usgs_{self.dataset}_{label}_{index}.zip"
        if not url:
            return fallback

        # Prefer server-declared filename
        try:
            h = self.session.head(url, allow_redirects=True, timeout=30)
            cd = h.headers.get("Content-Disposition")
            fname = self._filename_from_content_disposition(cd)
            if fname:
                return fname
        except Exception:
            pass

        # Some endpoints only expose Content-Disposition on GET
        try:
            g = self.session.get(url, stream=True, allow_redirects=True, timeout=30)
            cd = g.headers.get("Content-Disposition")
            fname = self._filename_from_content_disposition(cd)
            g.close()
            if fname:
                return fname
        except Exception:
            pass

        path = unquote(urlparse(url).path)
        base = os.path.basename(path)
        if base and "." in base:
            return base
        return fallback

    @staticmethod
    def _dedupe_names(names: List[str]) -> List[str]:
        """Make filenames unique to avoid accidental overwrite."""
        seen: Dict[str, int] = {}
        out: List[str] = []
        for n in names:
            if n not in seen:
                seen[n] = 1
                out.append(n)
                continue
            count = seen[n]
            seen[n] = count + 1
            stem, ext = os.path.splitext(n)
            out.append(f"{stem}_{count}{ext}")
        return out

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Best-effort integer parsing for provider payload values."""
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value).strip().replace(",", "")
        if not s:
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    @staticmethod
    def _metadata_value(metadata: Any, field_names: List[str]) -> Optional[str]:
        """Extract a metadata value by fieldName (case-insensitive)."""
        if not isinstance(metadata, list):
            return None
        wanted = {str(n).strip().lower() for n in field_names}
        for item in metadata:
            if not isinstance(item, dict):
                continue
            key = str(item.get("fieldName", "")).strip().lower()
            if key in wanted:
                val = item.get("value")
                if val is not None and str(val).strip():
                    return str(val).strip()
        return None

    def _scene_tile_id(self, scene: Dict[str, Any]) -> Optional[str]:
        """Build WRS path/row tile id when available."""
        metadata = scene.get("metadata", [])
        path_raw = self._metadata_value(metadata, ["Path", "WRS Path"])
        row_raw = self._metadata_value(metadata, ["Row", "WRS Row"])
        path_i = self._safe_int(path_raw)
        row_i = self._safe_int(row_raw)
        if path_i is not None and row_i is not None:
            return f"{path_i:03d}{row_i:03d}"
        if path_raw and row_raw:
            return f"{path_raw}/{row_raw}"
        return None

    def _scene_sensing_time(self, scene: Dict[str, Any]) -> Optional[str]:
        """Extract best available acquisition timestamp from USGS scene payload."""
        direct_candidates = [
            scene.get("acquisitionDate"),
            scene.get("displayDate"),
        ]
        temporal = scene.get("temporalCoverage")
        if isinstance(temporal, dict):
            direct_candidates.extend(
                [temporal.get("startDate"), temporal.get("endDate")]
            )
        for c in direct_candidates:
            if c is not None and str(c).strip():
                return str(c).strip()

        metadata = scene.get("metadata", [])
        return self._metadata_value(
            metadata,
            [
                "Date Acquired",
                "Acquisition Date",
                "AcquisitionDate",
                "Sensing Time",
                "Scene Center Time",
            ],
        )

    def _scene_matches_product(
        self, scene: Dict[str, Any], product_type: Optional[str]
    ) -> bool:
        """Apply the same product filtering rule used in search_products()."""
        if not scene.get("options", {}).get("bulk", False):
            return False
        if not product_type:
            return True

        sat_digit = None
        suffix = None
        try:
            sat_digit = int(str(product_type)[0])
            suffix = str(product_type)[1:]
        except Exception:
            suffix = str(product_type)

        display_id = str(scene.get("displayId", ""))
        if suffix and suffix not in display_id:
            return False

        if sat_digit is not None:
            ok_sat = False
            metadata = scene.get("metadata", [])
            if isinstance(metadata, list):
                for m in metadata:
                    if not isinstance(m, dict):
                        continue
                    if m.get("fieldName") == "Satellite":
                        try:
                            if int(m.get("value")) == sat_digit:
                                ok_sat = True
                                break
                        except Exception:
                            pass
            if not ok_sat:
                return False
        return True

    def _collect_filtered_scenes(
        self,
        collection: str,
        product_type: Optional[str],
        start_date: str,
        end_date: str,
        aoi: Polygon,
        max_results: int,
    ) -> tuple[List[Dict[str, Any]], int]:
        """Search USGS scenes and return product-filtered records."""
        self.dataset = collection
        spatial_filter = {
            "filterType": "geojson",
            "geoJson": self._aoi_to_geojson(aoi),
        }
        scene_filter = {
            "spatialFilter": spatial_filter,
            "acquisitionFilter": {"start": start_date, "end": end_date},
        }
        scene_payload = {
            "datasetName": collection,
            "sceneFilter": scene_filter,
            "maxResults": max(1, int(max_results)),
        }

        scenes = self._send_request(
            self.service_url + "scene-search", scene_payload, self.api_key
        )
        total_hits = scenes.get("totalHits", 0) if isinstance(scenes, dict) else 0
        logger.info(f"Found {total_hits} scenes in dataset '{collection}'.")

        results = scenes.get("results", []) if isinstance(scenes, dict) else []
        filtered: List[Dict[str, Any]] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            if self._scene_matches_product(r, product_type):
                filtered.append(r)
        return filtered, self._safe_int(total_hits) or len(filtered)

    def _fetch_bundle_sizes(
        self, collection: str, entity_ids: List[str]
    ) -> Dict[str, int]:
        """Fetch USGS bundle size per entity when available."""
        if not entity_ids:
            return {}
        payload = {"datasetName": collection, "entityIds": ",".join(entity_ids)}
        try:
            options = self._send_request(
                self.service_url + "download-options", payload, self.api_key
            )
        except Exception as e:
            logger.debug(f"USGS preview size lookup failed: {e}")
            return {}

        option_list = options.get("options", []) if isinstance(options, dict) else options
        sizes: Dict[str, int] = {}
        for opt in option_list:
            if not isinstance(opt, dict):
                continue
            if not opt.get("available"):
                continue
            product_name = str(opt.get("productName") or "")
            if "Bundle" not in product_name:
                continue
            entity_id = str(opt.get("entityId") or "").strip()
            if not entity_id:
                continue
            size_bytes = None
            for k in (
                "filesize",
                "fileSize",
                "size",
                "downloadSize",
                "sizeBytes",
                "bytes",
            ):
                size_bytes = self._safe_int(opt.get(k))
                if size_bytes is not None and size_bytes > 0:
                    break
            if size_bytes is None or size_bytes <= 0:
                continue
            prev = sizes.get(entity_id, 0)
            if size_bytes > prev:
                sizes[entity_id] = size_bytes
        return sizes

    def get_access_token(self) -> str:
        payload = {"username": self.username, "token": self.token}
        self.api_key = self._send_request(self.service_url + "login-token", payload)
        return self.api_key

    def _aoi_to_geojson(self, aoi: Polygon) -> dict:
        """
        Convert AOI geometry to GeoJSON accepted by USGS.
        Supports Polygon and MultiPolygon.
        """
        if aoi is None or getattr(aoi, "is_empty", True):
            raise ValueError("USGS AOI is empty.")

        gj = mapping(aoi)
        gtype = gj.get("type")
        coords = gj.get("coordinates")
        if gtype in ("Polygon", "MultiPolygon") and coords:
            return {"type": gtype, "coordinates": coords}

        # Fallback for unsupported geometry types: use bounding polygon.
        env = aoi.envelope
        ecoords = list(env.exterior.coords)
        if ecoords and ecoords[0] != ecoords[-1]:
            ecoords.append(ecoords[0])
        return {"type": "Polygon", "coordinates": [[list(c) for c in ecoords]]}

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aoi: Optional[Polygon] = None,
        tile_id: str = None,
    ) -> List[str]:
        if not aoi:
            raise ValueError("USGS search requires an AOI polygon.")
        if not start_date or not end_date:
            raise ValueError("USGS search requires start_date and end_date.")

        filtered, _ = self._collect_filtered_scenes(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi=aoi,
            max_results=1000,
        )
        products: List[str] = []
        for r in filtered:
            eid = r.get("entityId")
            if eid:
                products.append(eid)

        logger.info(f"Returning {len(products)} downloadable entity IDs.")
        return products

    def search_products_detailed(
        self,
        collection: str,
        product_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aoi: Optional[Polygon] = None,
        tile_id: str = None,
        max_items: int = 50,
    ) -> Dict[str, Any]:
        """
        Return detailed USGS preview items for UI:
        id, name, sensing_time, tile_id, size_bytes/size_mb.
        """
        if not aoi:
            raise ValueError("USGS preview requires an AOI polygon.")
        if not start_date or not end_date:
            raise ValueError("USGS preview requires start_date and end_date.")

        fetch_limit = max(200, max_items * 4)
        filtered, _ = self._collect_filtered_scenes(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi=aoi,
            max_results=fetch_limit,
        )

        items: List[Dict[str, Any]] = []
        for scene in filtered[: max(1, int(max_items))]:
            sid = scene.get("entityId")
            sid_s = str(sid) if sid is not None else ""
            item: Dict[str, Any] = {
                "id": sid_s,
                "name": str(scene.get("displayId") or sid_s or "USGS product"),
                "sensing_time": self._scene_sensing_time(scene),
                "tile_id": self._scene_tile_id(scene),
                "size_bytes": None,
                "size_mb": None,
            }
            items.append(item)

        size_map = self._fetch_bundle_sizes(
            collection=collection,
            entity_ids=[it["id"] for it in items if it.get("id")],
        )
        for it in items:
            sid = str(it.get("id") or "")
            sb = size_map.get(sid)
            if sb and sb > 0:
                it["size_bytes"] = sb
                it["size_mb"] = round(sb / (1024 * 1024), 1)

        return {"total": len(filtered), "items": items}

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

        urls = []
        for d in final_downloads:
            url = d.get("url")
            if not url:
                continue
            urls.append(url)

        if not urls:
            logger.error("USGS returned downloads without valid URLs.")
            return []

        max_workers_cfg = self.config_loader.get_var("download_manager.max_concurrent", 4)
        try:
            max_workers = max(2, min(int(max_workers_cfg), 8))
        except Exception:
            max_workers = 4

        indexed_urls = list(enumerate(urls))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            names = list(
                executor.map(
                    lambda p: self._resolve_download_filename(p[1], label, p[0]),
                    indexed_urls,
                )
            )
        names = self._dedupe_names(names)

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
