from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.api.preview import router as preview_router


def test_preview_endpoint_returns_backend_payload(monkeypatch):
    app = FastAPI()
    app.include_router(preview_router)

    def _fake_preview_products_from_env(**kwargs):
        assert kwargs["provider"] == "USGS"
        assert kwargs["collection"] == "landsat_ot_c2_l1"
        return {
            "items": [
                {
                    "id": "entity-1",
                    "name": "LC09_L1TP_190026_20260301_20260301_02_T1",
                    "tile_id": "entity-1",
                    "sensing_time": "2026-03-01",
                    "size_mb": None,
                }
            ],
            "total": 1,
            "error": "",
            "error_kind": "",
            "error_detail": "",
        }

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.preview.preview_products_from_env",
        _fake_preview_products_from_env,
    )

    with TestClient(app) as client:
        response = client.post(
            "/v1/preview",
            json={
                "provider": "USGS",
                "collection": "landsat_ot_c2_l1",
                "product_type": "L1TP",
                "start_date": "2026-03-01",
                "end_date": "2026-03-15",
                "aoi_wkt": "POLYGON((0 0,1 0,1 1,0 1,0 0))",
                "max_items": 25,
                "tile_ids": [],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["error"] == ""
    assert body["error_kind"] == ""
    assert body["items"][0]["id"] == "entity-1"


def test_settings_strip_usgs_credentials():
    settings = Settings(
        NIMBUS_USGS_SERVICE_URL=" https://m2m.cr.usgs.gov/api/api/json/stable/ ",
        NIMBUS_USGS_USERNAME=" user@example.com ",
        NIMBUS_USGS_TOKEN=" token-value ",
    )
    assert settings.nimbus_usgs_service_url == "https://m2m.cr.usgs.gov/api/api/json/stable/"
    assert settings.nimbus_usgs_username == "user@example.com"
    assert settings.nimbus_usgs_token == "token-value"
