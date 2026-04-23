from __future__ import annotations

from types import SimpleNamespace

import pytest

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.providers.usgs import UsgsProvider
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch.usgs_product_type import canonicalize_usgs_product_type


class _DummyDownloadManager:
    def download_products(self, payload, output_dir):
        return [output_dir, payload]


class _FakeResponse:
    def __init__(self, *, status_code=200, json_body=None, text=""):
        self.status_code = status_code
        self._json_body = json_body if json_body is not None else {"data": None, "errorCode": None}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_body


def _make_settings() -> Settings:
    return Settings(
        NIMBUS_USGS_SERVICE_URL="https://m2m.cr.usgs.gov/api/api/json/stable/",
        NIMBUS_USGS_USERNAME="user@example.com",
        NIMBUS_USGS_TOKEN="token-value",
    )


def test_download_options_uses_entity_id_list(monkeypatch):
    monkeypatch.setattr(UsgsProvider, "get_access_token", lambda self: "api-key")

    provider = UsgsProvider(settings=_make_settings(), download_manager=_DummyDownloadManager())
    provider.dataset = "landsat_ot_c2_l1"
    provider.scene_names = {"abc": "LC09_TEST"}

    captured = {}

    def _fake_send_request(self, endpoint, data):
        captured["endpoint"] = endpoint
        captured["data"] = data
        return {"options": []}

    monkeypatch.setattr(UsgsProvider, "_send_request", _fake_send_request)
    result = provider.download_products(["abc", "def"], output_dir="/tmp/out")

    assert captured["endpoint"] == "download-options"
    assert captured["data"]["datasetName"] == "landsat_ot_c2_l1"
    assert captured["data"]["entityIds"] == ["abc", "def"]
    assert result == []


def test_download_products_fails_early_without_machine_access(monkeypatch):
    monkeypatch.setattr(UsgsProvider, "get_access_token", lambda self: "api-key")
    provider = UsgsProvider(settings=_make_settings(), download_manager=_DummyDownloadManager())
    provider.dataset = "landsat_ot_c2_l1"
    provider.permissions = {"user"}

    with pytest.raises(RuntimeError) as excinfo:
        provider.download_products(["abc"], output_dir="/tmp/out")

    text = str(excinfo.value)
    assert "lacks MACHINE download access" in text
    assert "Current permissions: ['user']" in text


def test_download_options_403_includes_permission_hint(monkeypatch):
    monkeypatch.setattr(UsgsProvider, "get_access_token", lambda self: "api-key")
    provider = UsgsProvider(settings=_make_settings(), download_manager=_DummyDownloadManager())
    provider.api_key = "api-key"
    provider.permissions = {"user"}

    calls = {"count": 0}

    def _fake_post(url, json, headers, timeout):
        calls["count"] += 1
        if calls["count"] < 4:
            return _FakeResponse(status_code=403, text="<!DOCTYPE html><html>forbidden</html>")
        return _FakeResponse(status_code=403, text="<!DOCTYPE html><html>forbidden</html>")

    provider.session = SimpleNamespace(post=_fake_post)

    with pytest.raises(RuntimeError) as excinfo:
        provider._send_request("download-options", {"datasetName": "landsat_ot_c2_l1", "entityIds": ["abc"]})

    text = str(excinfo.value)
    assert "USGS HTTP 403 on download-options" in text
    assert "MACHINE download access" in text


def test_canonicalize_usgs_product_type_strips_satellite_digit() -> None:
    assert canonicalize_usgs_product_type("9L1TP") == "L1TP"
    assert canonicalize_usgs_product_type("8L2SP") == "L2SP"
    assert canonicalize_usgs_product_type("L1TP") == "L1TP"
    assert NimbusFetcher._normalize_product_type_for_zarr("9L1TP") == "L1TP"
