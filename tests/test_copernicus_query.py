from __future__ import annotations

import requests
from shapely import wkt as shapely_wkt

from nimbuschain_fetch.copernicus_query import build_copernicus_filter
from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.preview import preview_products_from_env
from nimbuschain_fetch.providers.copernicus import CopernicusProvider
from nimbuschain_fetch.settings import Settings


def _settings(tmp_path) -> Settings:
    return Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_COPERNICUS_USERNAME="user@example.com",
        NIMBUS_COPERNICUS_PASSWORD="secret",
    )


def test_build_copernicus_filter_escapes_literals_and_rounds_geometry() -> None:
    geom = shapely_wkt.loads(
        "POLYGON((1.123456789 50.987654321,1.123456789 51.123456789,2.987654321 51.123456789,2.987654321 50.987654321,1.123456789 50.987654321))"
    )

    query = build_copernicus_filter(
        collection="SENTINEL-2",
        product_type="S2MSI2A'OOPS",
        start_date="2026-03-01",
        end_date="2026-03-15",
        aoi=geom,
        tile_id="T31TCJ'X",
    )

    assert "S2MSI2A''OOPS" in query
    assert "T31TCJ''X" in query
    assert "1.123457 50.987654" in query
    assert "{type" not in query


def test_preview_copernicus_normalizes_geojson_before_request(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class _TokenResponse:
        status_code = 200
        ok = True
        text = ""

        def json(self):
            return {"access_token": "token-value"}

    class _ProductsResponse:
        status_code = 200
        ok = True
        text = ""

        def json(self):
            return {"value": [], "@odata.count": 0}

    def _fake_get(*args, **kwargs):
        captured["filter"] = kwargs["params"]["$filter"]
        return _ProductsResponse()

    monkeypatch.setenv("NIMBUS_COPERNICUS_USERNAME", "user@example.com")
    monkeypatch.setenv("NIMBUS_COPERNICUS_PASSWORD", "secret")
    monkeypatch.setattr("nimbuschain_fetch.preview.requests.post", lambda *args, **kwargs: _TokenResponse())
    monkeypatch.setattr("nimbuschain_fetch.preview.requests.get", _fake_get)

    payload = preview_products_from_env(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date="2026-03-01",
        end_date="2026-03-15",
        aoi_wkt='{"type":"Polygon","coordinates":[[[1.95,50.60],[1.95,51.23],[2.62,51.23],[2.62,50.60],[1.95,50.60]]]}',
    )

    assert payload["error"] == ""
    assert "POLYGON" in captured["filter"]
    assert "{" not in captured["filter"]


def test_preview_copernicus_surfaces_invalid_parameter_detail(monkeypatch) -> None:
    class _TokenResponse:
        status_code = 200
        ok = True
        text = ""

        def json(self):
            return {"access_token": "token-value"}

    class _ProductsResponse:
        status_code = 400
        ok = False
        text = '{"code":"InvalidParameter","message":"Inappropriate content detected!!!"}'

        def json(self):
            return {"code": "InvalidParameter", "message": "Inappropriate content detected!!!"}

    monkeypatch.setenv("NIMBUS_COPERNICUS_USERNAME", "user@example.com")
    monkeypatch.setenv("NIMBUS_COPERNICUS_PASSWORD", "secret")
    monkeypatch.setattr("nimbuschain_fetch.preview.requests.post", lambda *args, **kwargs: _TokenResponse())
    monkeypatch.setattr("nimbuschain_fetch.preview.requests.get", lambda *args, **kwargs: _ProductsResponse())

    payload = preview_products_from_env(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date="2026-03-01",
        end_date="2026-03-15",
        aoi_wkt="POLYGON((1.95 50.60,1.95 51.23,2.62 51.23,2.62 50.60,1.95 50.60))",
    )

    assert payload["error_kind"] == "technical"
    assert payload["error"] == "Copernicus rejected the search query."
    assert "InvalidParameter" in payload["error_detail"]
    assert "HTTP 400" in payload["error_detail"]


def test_copernicus_provider_surfaces_invalid_parameter_detail(monkeypatch, tmp_path) -> None:
    provider = CopernicusProvider(settings=_settings(tmp_path), download_manager=DownloadManager())

    class _Response:
        status_code = 400
        text = '{"code":"InvalidParameter","message":"Inappropriate content detected!!!"}'

    def _raise_request(*args, **kwargs):
        raise requests.HTTPError("boom", response=_Response())

    monkeypatch.setattr(provider, "_auth_header", lambda: {"Authorization": "Bearer token-value"})
    monkeypatch.setattr(provider, "_request", _raise_request)

    geom = shapely_wkt.loads("POLYGON((1.95 50.60,1.95 51.23,2.62 51.23,2.62 50.60,1.95 50.60))")

    try:
        provider.search_products(
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date="2026-03-01",
            end_date="2026-03-15",
            aoi=geom,
        )
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected RuntimeError")

    assert "Copernicus rejected the catalogue search query" in message
    assert "InvalidParameter" in message
