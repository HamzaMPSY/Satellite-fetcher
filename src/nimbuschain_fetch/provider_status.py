from __future__ import annotations

from datetime import datetime, timezone
import threading
import time
from typing import Any

import requests

from nimbuschain_fetch.settings import Settings


_CACHE_LOCK = threading.Lock()
_STATUS_CACHE: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}
_DEFAULT_STATUS_TTL_SECONDS = 45.0


def clear_provider_status_cache() -> None:
    with _CACHE_LOCK:
        _STATUS_CACHE.clear()


def classify_provider_error(message: str) -> str:
    text = str(message or "").strip().lower()
    if any(
        token in text
        for token in [
            "temporarily unavailable",
            "retry in a few seconds",
            "http 429",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
            "request failed on",
            "retry budget exhausted",
        ]
    ):
        return "provider_unavailable"
    if any(
        token in text
        for token in [
            "auth_invalid",
            "auth unauthorized",
            "auth_unauthorized",
            "credential verification failed",
            "authentication failed",
            "access_token missing",
            "api key is empty",
        ]
    ):
        return "credentials_invalid"
    return "technical"


def get_provider_statuses(
    settings: Settings,
    *,
    provider: str | None = None,
    force_refresh: bool = False,
    ttl_seconds: float = _DEFAULT_STATUS_TTL_SECONDS,
) -> list[dict[str, Any]]:
    if provider:
        names = [str(provider).strip().lower()]
    else:
        names = ["copernicus", "usgs"]
    return [
        get_provider_status(
            settings,
            provider_name,
            force_refresh=force_refresh,
            ttl_seconds=ttl_seconds,
        )
        for provider_name in names
    ]


def get_provider_status(
    settings: Settings,
    provider: str,
    *,
    force_refresh: bool = False,
    ttl_seconds: float = _DEFAULT_STATUS_TTL_SECONDS,
) -> dict[str, Any]:
    normalized = str(provider or "").strip().lower()
    if normalized not in {"copernicus", "usgs"}:
        return {
            "provider": normalized or "unknown",
            "configured": False,
            "auth_valid": False,
            "error_kind": "technical",
            "message": f"Unsupported provider '{provider}'.",
            "detail": f"Unsupported provider '{provider}'.",
            "last_checked_at": datetime.now(timezone.utc).isoformat(),
            "credential_source": "runtime_env",
        }

    cache_key = _status_cache_key(settings, normalized)
    if not force_refresh:
        with _CACHE_LOCK:
            cached = _STATUS_CACHE.get(cache_key)
        if cached is not None:
            cached_at, payload = cached
            if (time.time() - cached_at) <= max(1.0, float(ttl_seconds)):
                return dict(payload)

    if normalized == "usgs":
        payload = _build_usgs_status(settings)
    else:
        payload = _build_copernicus_status(settings)

    with _CACHE_LOCK:
        _STATUS_CACHE[cache_key] = (time.time(), dict(payload))
    return payload


def _status_cache_key(settings: Settings, provider: str) -> tuple[str, ...]:
    if provider == "usgs":
        return (
            provider,
            settings.nimbus_usgs_service_url,
            settings.nimbus_usgs_username or "",
            settings.nimbus_usgs_token or "",
        )
    return (
        provider,
        settings.nimbus_copernicus_token_url,
        settings.nimbus_copernicus_username or "",
        settings.nimbus_copernicus_password or "",
    )


def _build_usgs_status(settings: Settings) -> dict[str, Any]:
    username = settings.nimbus_usgs_username
    token = settings.nimbus_usgs_token
    payload = _base_status(
        provider="usgs",
        username_present=bool(username),
        token_present=bool(token),
    )
    if not username or not token:
        payload.update(
            configured=False,
            auth_valid=False,
            error_kind="credentials_missing",
            message="USGS credentials are missing in the backend runtime.",
            detail="Missing NIMBUS_USGS_USERNAME or NIMBUS_USGS_TOKEN.",
        )
        return payload

    payload["configured"] = True
    service_url = settings.nimbus_usgs_service_url.rstrip("/") + "/"
    try:
        body = _post_json_with_retries(
            f"{service_url}login-token",
            payload={"username": username, "token": token},
        )
        if body.get("errorCode"):
            detail = f"USGS API error {body['errorCode']}: {body.get('errorMessage')}"
            error_kind = classify_provider_error(detail)
            payload.update(
                auth_valid=False,
                error_kind=error_kind,
                message=_provider_error_message("USGS", error_kind),
                detail=detail,
            )
            return payload

        api_key = body.get("data")
        if not api_key:
            payload.update(
                auth_valid=False,
                error_kind="credentials_invalid",
                message="USGS credentials are invalid or rejected.",
                detail="USGS runtime authentication succeeded without returning an API key.",
            )
            return payload
    except Exception as exc:
        detail = f"USGS runtime authentication failed: {exc}"
        error_kind = classify_provider_error(detail)
        payload.update(
            auth_valid=False,
            error_kind=error_kind,
            message=_provider_error_message("USGS", error_kind),
            detail=detail,
        )
        return payload

    payload.update(
        auth_valid=True,
        error_kind="",
        message="USGS runtime credentials are valid.",
        detail="",
    )
    return payload


def _build_copernicus_status(settings: Settings) -> dict[str, Any]:
    username = settings.nimbus_copernicus_username
    password = settings.nimbus_copernicus_password
    payload = _base_status(
        provider="copernicus",
        username_present=bool(username),
        password_present=bool(password),
    )
    payload.update(
        account_pool_configured=settings.copernicus_account_pool_available,
        account_pool_size=int(settings.copernicus_account_pool_size),
        account_pool_concurrency=int(settings.nimbus_copernicus_account_pool_concurrency),
    )
    if not username or not password:
        payload.update(
            configured=False,
            auth_valid=False,
            error_kind="credentials_missing",
            message="Copernicus credentials are missing in the backend runtime.",
            detail="Missing NIMBUS_COPERNICUS_USERNAME or NIMBUS_COPERNICUS_PASSWORD.",
        )
        return payload

    payload["configured"] = True
    try:
        response = requests.post(
            settings.nimbus_copernicus_token_url,
            data={
                "client_id": "cdse-public",
                "username": username,
                "password": password,
                "grant_type": "password",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=40,
        )
        if not response.ok:
            error_kind = "credentials_invalid" if response.status_code in {400, 401, 403} else "provider_unavailable"
            payload.update(
                auth_valid=False,
                error_kind=error_kind,
                message=_provider_error_message("Copernicus", error_kind),
                detail=f"Copernicus runtime authentication failed ({response.status_code}).",
            )
            return payload
        token_body = response.json()
        if not token_body.get("access_token"):
            payload.update(
                auth_valid=False,
                error_kind="credentials_invalid",
                message="Copernicus credentials are invalid or rejected.",
                detail="Copernicus runtime authentication failed: access_token missing in token response.",
            )
            return payload
    except Exception as exc:
        detail = f"Copernicus runtime authentication failed: {exc}"
        error_kind = classify_provider_error(detail)
        payload.update(
            auth_valid=False,
            error_kind=error_kind,
            message=_provider_error_message("Copernicus", error_kind),
            detail=detail,
        )
        return payload

    payload.update(
        auth_valid=True,
        error_kind="",
        message="Copernicus runtime credentials are valid.",
        detail="",
    )
    return payload


def _provider_error_message(provider_label: str, error_kind: str) -> str:
    if error_kind == "credentials_invalid":
        return f"{provider_label} credentials are invalid or rejected."
    if error_kind == "credentials_missing":
        return f"{provider_label} credentials are missing in the backend runtime."
    if error_kind == "provider_unavailable":
        return f"{provider_label} is temporarily unavailable."
    return f"{provider_label} runtime authentication failed because of a technical error."


def _base_status(
    *,
    provider: str,
    username_present: bool = False,
    token_present: bool = False,
    password_present: bool = False,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "configured": False,
        "auth_valid": False,
        "error_kind": "",
        "message": "",
        "detail": "",
        "last_checked_at": datetime.now(timezone.utc).isoformat(),
        "credential_source": "runtime_env",
        "username_present": username_present,
        "token_present": token_present,
        "password_present": password_present,
    }


def _post_json_with_retries(url: str, *, payload: dict[str, Any], timeout: int = 40) -> dict[str, Any]:
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                raise RuntimeError(f"request failed on login-token after {max_attempts} attempts: {exc}") from exc
            time.sleep(min(8.0, 1.5 * attempt))
            continue

        if response.status_code in {429, 500, 502, 503, 504}:
            if attempt < max_attempts:
                time.sleep(min(10.0, 2.0 * attempt))
                continue
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(
                f"HTTP {response.status_code} on login-token after {max_attempts} attempts. Response: {body_text}"
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body_text = (response.text or "").strip()[:500]
            raise RuntimeError(f"HTTP {response.status_code} on login-token. Response: {body_text}") from exc

        return response.json()

    raise RuntimeError("request failed on login-token: retry budget exhausted")
