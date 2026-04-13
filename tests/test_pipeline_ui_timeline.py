from __future__ import annotations

from nimbuschain_fetch_ui.app import _mask_types_from_payload


def test_mask_types_are_read_from_request_payload_for_pipeline_timeline() -> None:
    payload = {
        "request": {
            "mask_types": ["water", "cloud"],
        },
        "metadata": {},
        "pipeline_metadata": {},
        "conversion_metadata": {},
    }

    assert _mask_types_from_payload(payload) == ("water", "cloud")
