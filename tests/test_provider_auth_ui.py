from __future__ import annotations

from nimbuschain_fetch_ui.provider_auth import (
    provider_action_guidance,
    provider_actions_disabled,
    provider_preview_error_payload,
    select_provider_status,
)


def test_invalid_usgs_auth_disables_download_and_preview_actions() -> None:
    snapshot = {
        "providers": [
            {
                "provider": "usgs",
                "configured": True,
                "auth_valid": False,
                "error_kind": "credentials_invalid",
                "message": "USGS credentials are invalid or rejected.",
                "detail": "USGS API error AUTH_INVALID: User credential verification failed",
            }
        ]
    }

    assert select_provider_status(snapshot, "usgs") is not None
    assert provider_actions_disabled("usgs", snapshot) is True
    assert "NIMBUS_USGS_TOKEN" in provider_action_guidance("usgs", snapshot)
    blocked = provider_preview_error_payload("usgs", snapshot)
    assert blocked["error_kind"] == "credentials_invalid"
    assert "AUTH_INVALID" in blocked["error_detail"]


def test_valid_usgs_auth_keeps_actions_enabled() -> None:
    snapshot = {
        "providers": [
            {
                "provider": "usgs",
                "configured": True,
                "auth_valid": True,
                "error_kind": "",
                "message": "USGS runtime credentials are valid.",
                "detail": "",
            }
        ]
    }

    assert provider_actions_disabled("usgs", snapshot) is False
    assert provider_action_guidance("usgs", snapshot) == ""
    assert provider_preview_error_payload("usgs", snapshot) == {
        "error": "",
        "error_kind": "",
        "error_detail": "",
    }
