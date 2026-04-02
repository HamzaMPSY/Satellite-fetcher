from __future__ import annotations

from typing import Any


def select_provider_status(snapshot: Any, provider: str) -> dict[str, Any] | None:
    target = str(provider or "").strip().lower()
    if not target or not isinstance(snapshot, dict):
        return None
    items = snapshot.get("providers")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("provider") or "").strip().lower() == target:
            return item
    return None


def provider_actions_disabled(provider: str, snapshot: Any) -> bool:
    if str(provider or "").strip().lower() != "usgs":
        return False
    item = select_provider_status(snapshot, "usgs")
    if item is None:
        return False
    if item.get("configured") is False:
        return True
    return item.get("auth_valid") is False


def provider_action_guidance(provider: str, snapshot: Any) -> str:
    normalized = str(provider or "").strip().lower()
    item = select_provider_status(snapshot, normalized)
    if item is None:
        return ""
    error_kind = str(item.get("error_kind") or "").strip().lower()
    if error_kind in {"credentials_missing", "credentials_invalid"}:
        if normalized == "copernicus":
            return (
                "Update NIMBUS_COPERNICUS_USERNAME / NIMBUS_COPERNICUS_PASSWORD in the runtime environment "
                "and restart the API/worker services."
            )
        return "Update NIMBUS_USGS_TOKEN in the runtime environment and restart the API/worker services."
    if error_kind == "provider_unavailable":
        return "USGS is temporarily unavailable. Retry after the provider recovers."
    return ""


def provider_preview_error_payload(provider: str, snapshot: Any) -> dict[str, str]:
    if not provider_actions_disabled(provider, snapshot):
        return {"error": "", "error_kind": "", "error_detail": ""}
    item = select_provider_status(snapshot, provider) or {}
    return {
        "error": str(item.get("message") or "Provider runtime authentication is blocking preview."),
        "error_kind": str(item.get("error_kind") or "technical"),
        "error_detail": str(item.get("detail") or item.get("message") or ""),
    }


def provider_auth_state_label(item: dict[str, Any] | None) -> str:
    if not isinstance(item, dict):
        return "-"
    if item.get("configured") is False:
        return "missing"
    if item.get("auth_valid") is True:
        return "valid"
    if item.get("auth_valid") is False:
        return str(item.get("error_kind") or "invalid").replace("_", " ")
    return "unknown"
