from __future__ import annotations

import json
import os
import time
from typing import Any

import requests
import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from nimbuschain_fetch.usgs_product_type import usgs_product_type_matches


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _preview_error(message: str, *, error_kind: str, detail: str | None = None) -> dict[str, Any]:
    return {
        "items": [],
        "total": 0,
        "error": message,
        "error_kind": error_kind,
        "error_detail": detail or "",
    }


def _classify_preview_error(message: str) -> str:
    text = str(message or "").strip().lower()
    if any(token in text for token in ["auth_invalid", "auth unauthorized", "auth_unauthorized", "credential verification failed", "authentication failed", "access_token missing", "api key is empty"]):
        return "credentials_invalid"
    if any(token in text for token in ["temporarily unavailable", "retry in a few seconds", "http 429", "http 500", "http 502", "http 503", "http 504", "request failed on", "retry budget exhausted"]):
        return "provider_unavailable"
    return "technical"


def _safe_union(geoms: list[BaseGeometry]) -> BaseGeometry | None:
    items = [geom for geom in geoms if geom is not None and not getattr(geom, "is_empty", True)]
    if not items:
        return None
    try:
        union_all = getattr(shapely, "union_all", None)
        if callable(union_all):
            return union_all(items)
    except Exception:
        pass
    try:
        return unary_union(items)
    except Exception:
        pass
    merged = items[0]
    for geom in items[1:]:
        try:
            merged = merged.union(geom)
        except Exception:
            continue
    return merged


def parse_aoi_text(text: str) -> BaseGeometry | None:
    if not text or not text.strip():
        return None
    raw = text.strip()
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
        except Exception:
            return None
        try:
            obj_type = str(obj.get("type", "")).strip()
            if obj_type == "Feature":
                geometry = obj.get("geometry")
                return shape(geometry) if geometry else None
            if obj_type == "FeatureCollection":
                geoms: list[BaseGeometry] = []
                for feature in obj.get("features", []) or []:
                    geometry = feature.get("geometry") if isinstance(feature, dict) else None
                    if geometry:
                        geoms.append(shape(geometry))
                return _safe_union(geoms)
            return shape(obj)
        except Exception:
            return None
    try:
        return shapely_wkt.loads(raw)
    except Exception:
        return None


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
    if aoi_wkt:
        query += f" and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}')"
    return query


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
    return {"items": parsed, "total": total_int, "error": "", "error_kind": "", "error_detail": ""}


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
        return _preview_error(
            "Copernicus credentials are missing in the backend runtime.",
            error_kind="credentials_invalid",
            detail="Missing NIMBUS_COPERNICUS_USERNAME or NIMBUS_COPERNICUS_PASSWORD.",
        )
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
        return _preview_error(
            "Copernicus credentials are invalid or rejected.",
            error_kind="credentials_invalid" if token_response.status_code in {400, 401, 403} else "provider_unavailable",
            detail=f"Copernicus preview authentication failed ({token_response.status_code}).",
        )
    access_token = token_response.json().get("access_token")
    if not access_token:
        return _preview_error(
            "Copernicus credentials are invalid or rejected.",
            error_kind="credentials_invalid",
            detail="Copernicus preview authentication failed: access_token missing in token response.",
        )
    query = build_copernicus_filter(
        collection=collection,
        product_type=product_type,
        start_date=start_date,
        end_date=end_date,
        aoi_wkt=aoi_wkt,
        tile_id=tile_id,
    )
    params = {"$filter": query, "$orderby": "ContentDate/Start desc", "$top": str(max(50, max_items * 3)), "$count": "true"}
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
        return _preview_error(
            "Copernicus is temporarily unavailable.",
            error_kind="provider_unavailable",
            detail=(
                "Copernicus preview search is temporarily unavailable "
                f"(HTTP {products_response.status_code}). Retry in a few seconds."
            ),
        )
    return parse_copernicus_products(products_response.json(), max_items=max_items)


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
    return {"items": filtered[:max_items], "total": total, "error": "", "error_kind": "", "error_detail": ""}


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
            response = requests.post(url, json=payload, headers=headers, timeout=60)
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                raise RuntimeError(f"USGS request failed on {endpoint} after {max_attempts} attempts: {exc}") from exc
            time.sleep(min(8.0, 1.5 * attempt))
            continue
        if response.status_code in {429, 500, 502, 503, 504}:
            if attempt < max_attempts:
                time.sleep(min(10.0, 2.0 * attempt))
                continue
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(
                f"USGS HTTP {response.status_code} on {endpoint} after {max_attempts} attempts. Response: {body_text}"
            )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(f"USGS HTTP {response.status_code} on {endpoint}. Response: {body_text}") from exc
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
        return _preview_error(
            "USGS credentials are missing in the backend runtime.",
            error_kind="credentials_invalid",
            detail="Missing NIMBUS_USGS_USERNAME or NIMBUS_USGS_TOKEN.",
        )
    geom = parse_aoi_text(aoi_wkt)
    if geom is None or getattr(geom, "is_empty", True):
        return _preview_error(
            "A valid AOI polygon is required for the USGS preview.",
            error_kind="technical",
            detail="USGS preview requires a valid AOI polygon.",
        )
    try:
        login_body = _usgs_request(
            service_url=service_url,
            endpoint="login-token",
            payload={"username": username, "token": token},
        )
        api_key = login_body.get("data")
        if not api_key:
            return _preview_error(
                "USGS credentials are invalid or rejected.",
                error_kind="credentials_invalid",
                detail="USGS preview authentication failed: api key is empty.",
            )
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
        detail = f"USGS preview failed: {exc}"
        error_kind = _classify_preview_error(detail)
        if error_kind == "credentials_invalid":
            message = "USGS credentials are invalid or rejected."
        elif error_kind == "provider_unavailable":
            message = "USGS is temporarily unavailable."
        else:
            message = "USGS preview failed because of a technical error."
        return _preview_error(message, error_kind=error_kind, detail=detail)


def preview_products_from_env(
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
    provider_value = provider.strip().lower()
    safe_max_items = max(1, min(200, int(max_items or 50)))
    if not aoi_wkt or not aoi_wkt.strip():
        return _preview_error(
            "Preview unavailable: AOI is empty.",
            error_kind="technical",
            detail="Preview unavailable: AOI is empty.",
        )
    tile_id = None
    if tile_ids:
        cleaned = [str(tile).strip() for tile in tile_ids if str(tile).strip()]
        if len(cleaned) == 1:
            tile_id = cleaned[0]
    if provider_value == "copernicus":
        try:
            return _copernicus_preview(
                collection=collection,
                product_type=product_type,
                start_date=start_date,
                end_date=end_date,
                aoi_wkt=aoi_wkt,
                max_items=safe_max_items,
                tile_id=tile_id,
            )
        except Exception as exc:
            detail = f"Copernicus preview failed: {exc}"
            error_kind = _classify_preview_error(detail)
            if error_kind == "credentials_invalid":
                message = "Copernicus credentials are invalid or rejected."
            elif error_kind == "provider_unavailable":
                message = "Copernicus is temporarily unavailable."
            else:
                message = "Copernicus preview failed because of a technical error."
            return _preview_error(message, error_kind=error_kind, detail=detail)
    if provider_value == "usgs":
        return _usgs_preview(
            collection=collection,
            product_type=product_type,
            start_date=start_date,
            end_date=end_date,
            aoi_wkt=aoi_wkt,
            max_items=safe_max_items,
        )
    return _preview_error(
        f"Preview unsupported for provider '{provider}'.",
        error_kind="technical",
        detail=f"Preview unsupported for provider '{provider}'.",
    )
