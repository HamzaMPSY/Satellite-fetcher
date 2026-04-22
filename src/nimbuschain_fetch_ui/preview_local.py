from __future__ import annotations

import os
import time
from typing import Any

import requests
from shapely.geometry import mapping

from nimbuschain_fetch.copernicus_query import build_copernicus_filter as _shared_build_copernicus_filter

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

from nimbuschain_fetch_ui.aoi_utils import parse_aoi_text
from nimbuschain_fetch.preview import preview_products_from_env


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _coerce_size_mb(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        size_bytes = float(value)
    except Exception:
        return None
    return round(size_bytes / (1024.0 * 1024.0), 2)


def _copernicus_attr(attrs: list[dict[str, Any]], name: str) -> str:
    target = name.strip().lower()
    for attr in attrs:
        attr_name = str(attr.get("Name") or attr.get("name") or "").strip().lower()
        if attr_name != target:
            continue
        value = attr.get("Value")
        if value is None:
            value = attr.get("value")
        return str(value or "").strip()
    return ""


def build_copernicus_filter(
    *,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    tile_id: str | None = None,
) -> str:
    geom = parse_aoi_text(aoi_wkt)
    if geom is None or getattr(geom, "is_empty", True):
        raise ValueError("Copernicus preview requires a valid AOI polygon.")
    return _shared_build_copernicus_filter(
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi=geom,
        tile_id=tile_id,
    )


def _copernicus_error_message(status_code: int, detail: str) -> str:
    text = str(detail or "").strip().lower()
    if "invalidparameter" in text or "inappropriate content detected" in text or int(status_code) == 400:
        return "Copernicus rejected the search query."
    if int(status_code) in {401, 403}:
        return "Copernicus credentials are invalid or rejected."
    if int(status_code) in {429, 500, 502, 503, 504}:
        return "Copernicus is temporarily unavailable."
    return "Copernicus preview failed because of a technical error."


def parse_copernicus_products(payload: dict[str, Any], *, max_items: int) -> dict[str, Any]:
    values = payload.get("value", []) if isinstance(payload, dict) else []
    total = payload.get("@odata.count") if isinstance(payload, dict) else None
    try:
        total_int = int(total) if total is not None else len(values)
    except Exception:
        total_int = len(values)

    parsed: list[dict[str, Any]] = []
    for item in list(values)[:max_items]:
        attrs = item.get("Attributes", []) or []
        content_date = item.get("ContentDate", {}) or {}
        parsed.append(
            {
                "id": str(item.get("Id") or ""),
                "name": str(item.get("Name") or item.get("Id") or "product"),
                "tile_id": _copernicus_attr(attrs, "tileId") or "-",
                "sensing_time": str(content_date.get("Start") or "-"),
                "size_mb": _coerce_size_mb(item.get("ContentLength")),
                "product_type": _copernicus_attr(attrs, "productType") or "",
            }
        )

    return {"items": parsed, "total": total_int, "error": ""}


def _copernicus_preview(
    *,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    max_items: int,
    tile_id: str | None,
) -> dict[str, Any]:
    base_url = _env("NIMBUS_COPERNICUS_BASE_URL", "https://catalogue.dataspace.copernicus.eu")
    token_url = _env(
        "NIMBUS_COPERNICUS_TOKEN_URL",
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
    )
    username = _env("NIMBUS_COPERNICUS_USERNAME")
    password = _env("NIMBUS_COPERNICUS_PASSWORD")

    if not username or not password:
        return {
            "items": [],
            "total": 0,
            "error": "Copernicus preview unavailable: missing NIMBUS_COPERNICUS_USERNAME or NIMBUS_COPERNICUS_PASSWORD in UI container.",
        }

    token_payload = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    token_response = requests.post(
        token_url,
        data=token_payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=40,
    )
    if not token_response.ok:
        return {
            "items": [],
            "total": 0,
            "error": f"Copernicus preview authentication failed ({token_response.status_code}).",
        }

    access_token = token_response.json().get("access_token")
    if not access_token:
        return {
            "items": [],
            "total": 0,
            "error": "Copernicus preview authentication failed: access_token missing in token response.",
        }

    try:
        query = build_copernicus_filter(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi_wkt=aoi_wkt,
            tile_id=tile_id,
        )
    except ValueError:
        return {
            "items": [],
            "total": 0,
            "error": "A valid AOI polygon is required for the Copernicus preview.",
        }
    params = {
        "$filter": query,
        "$orderby": "ContentDate/Start desc",
        "$top": str(max(50, max_items * 3)),
        "$count": "true",
    }
    products_response = None
    for attempt in range(1, 5):
        products_response = requests.get(
            f"{base_url.rstrip('/')}/odata/v1/Products",
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=60,
        )
        if products_response.status_code not in {429, 500, 502, 503, 504}:
            break
        if attempt < 4:
            retry_after = products_response.headers.get("Retry-After")
            try:
                delay = max(0.0, float(retry_after)) if retry_after else min(10.0, 2.0 * attempt)
            except Exception:
                delay = min(10.0, 2.0 * attempt)
            time.sleep(delay)
    assert products_response is not None
    if not products_response.ok:
        detail = f"Copernicus preview search failed (HTTP {products_response.status_code})."
        body = " ".join(str(products_response.text or "").strip().split())
        if body:
            detail += f" Response: {body[:500]}{'...' if len(body) > 500 else ''}"
        return {
            "items": [],
            "total": 0,
            "error": _copernicus_error_message(products_response.status_code, detail),
        }

    payload = products_response.json()
    return parse_copernicus_products(payload, max_items=max_items)


def parse_usgs_scenes(payload: dict[str, Any], *, max_items: int, product_type: str) -> dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    scenes = data.get("results", []) if isinstance(data, dict) else []

    filtered: list[dict[str, Any]] = []
    for scene in scenes:
        if not isinstance(scene, dict):
            continue
        display_id = str(scene.get("displayId") or "")
        if not usgs_product_type_matches(display_id, product_type):
            continue
        temporal = scene.get("temporalCoverage", {}) if isinstance(scene.get("temporalCoverage"), dict) else {}
        filtered.append(
            {
                "id": str(scene.get("entityId") or ""),
                "name": display_id or str(scene.get("entityId") or "scene"),
                "tile_id": str(scene.get("entityId") or "-"),
                "sensing_time": str(temporal.get("startDate") or scene.get("acquisitionDate") or "-"),
                "size_mb": None,
            }
        )

    total = len(filtered)
    return {"items": filtered[:max_items], "total": total, "error": ""}


def _usgs_request(
    *,
    service_url: str,
    endpoint: str,
    payload: dict[str, Any],
    auth_token: str | None = None,
) -> dict[str, Any]:
    max_attempts = 4
    url = f"{service_url.rstrip('/')}/{endpoint}"
    for attempt in range(1, max_attempts + 1):
        headers = {"Content-Type": "application/json"}
        if auth_token:
            headers["X-Auth-Token"] = auth_token

        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=60,
            )
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
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(
                f"USGS HTTP {response.status_code} on {endpoint} after {max_attempts} attempts. "
                f"Response: {body_text}"
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(
                f"USGS HTTP {response.status_code} on {endpoint}. Response: {body_text}"
            ) from exc

        body = response.json()
        if body.get("errorCode"):
            raise RuntimeError(f"USGS API error {body['errorCode']}: {body.get('errorMessage')}")
        return body

    raise RuntimeError(f"USGS request failed on {endpoint}: retry budget exhausted")


def _usgs_preview(
    *,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    max_items: int,
) -> dict[str, Any]:
    service_url = _env("NIMBUS_USGS_SERVICE_URL", "https://m2m.cr.usgs.gov/api/api/json/stable/")
    username = _env("NIMBUS_USGS_USERNAME")
    token = _env("NIMBUS_USGS_TOKEN")

    if not username or not token:
        return {
            "items": [],
            "total": 0,
            "error": "USGS preview unavailable: missing NIMBUS_USGS_USERNAME or NIMBUS_USGS_TOKEN in UI container.",
        }

    geom = parse_aoi_text(aoi_wkt)
    if geom is None or getattr(geom, "is_empty", True):
        return {"items": [], "total": 0, "error": "USGS preview requires a valid AOI polygon."}

    try:
        login_payload = {"username": username, "token": token}
        login_body = _usgs_request(service_url=service_url, endpoint="login-token", payload=login_payload)
        api_key = login_body.get("data")
        if not api_key:
            return {"items": [], "total": 0, "error": "USGS preview authentication failed: api key is empty."}

        search_payload = {
            "datasetName": collection,
            "sceneFilter": {
                "spatialFilter": {"filterType": "geojson", "geoJson": mapping(geom)},
                "acquisitionFilter": {"start": start_date, "end": end_date},
            },
            "maxResults": max(100, max_items * 5),
        }
        search_body = _usgs_request(
            service_url=service_url,
            endpoint="scene-search",
            payload=search_payload,
            auth_token=str(api_key),
        )
        return parse_usgs_scenes(search_body, max_items=max_items, product_type=product_type)
    except Exception as exc:
        return {"items": [], "total": 0, "error": f"USGS preview failed: {exc}"}


def preview_products_local(
    *,
    provider: str,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi_wkt: str,
    max_items: int = 50,
    tile_ids: list[str] | None = None,
) -> dict[str, Any]:
    return preview_products_from_env(
        provider=provider,
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi_wkt=aoi_wkt,
        max_items=max_items,
        tile_ids=tile_ids,
    )
