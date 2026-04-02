from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.provider_status import clear_provider_status_cache
from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.api.jobs import router as jobs_router
from nimbuschain_fetch_service.api.providers import router as providers_router


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, json_body: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._json_body = json_body or {}
        self.text = text
        self.ok = 200 <= status_code < 300

    def json(self) -> dict:
        return dict(self._json_body)

    def raise_for_status(self) -> None:
        if self.ok:
            return
        import requests

        raise requests.HTTPError(f"HTTP {self.status_code}")


class _DummyFetcher:
    def __init__(self) -> None:
        self.submit_calls = 0

    async def submit_job(self, request):
        self.submit_calls += 1
        return "job-created"

    async def submit_batch(self, request):
        self.submit_calls += 1
        return ["job-created"]


def _usgs_settings() -> Settings:
    return Settings(
        NIMBUS_USGS_SERVICE_URL="https://m2m.cr.usgs.gov/api/api/json/stable/",
        NIMBUS_USGS_USERNAME="user@example.com",
        NIMBUS_USGS_TOKEN="token-value",
    )


def test_provider_status_endpoint_reports_missing_usgs_credentials() -> None:
    clear_provider_status_cache()
    app = FastAPI()
    app.state.settings = Settings(NIMBUS_USGS_USERNAME="", NIMBUS_USGS_TOKEN="")
    app.include_router(providers_router)

    with TestClient(app) as client:
        response = client.get("/v1/providers/status", params={"provider": "usgs", "force_refresh": "true"})

    assert response.status_code == 200
    body = response.json()
    item = body["providers"][0]
    assert item["provider"] == "usgs"
    assert item["configured"] is False
    assert item["auth_valid"] is False
    assert item["error_kind"] == "credentials_missing"


def test_provider_status_endpoint_reports_auth_invalid(monkeypatch) -> None:
    clear_provider_status_cache()
    app = FastAPI()
    app.state.settings = _usgs_settings()
    app.include_router(providers_router)

    monkeypatch.setattr(
        "nimbuschain_fetch.provider_status.requests.post",
        lambda *args, **kwargs: _FakeResponse(
            json_body={"errorCode": "AUTH_INVALID", "errorMessage": "User credential verification failed"}
        ),
    )

    with TestClient(app) as client:
        response = client.get("/v1/providers/status", params={"provider": "usgs", "force_refresh": "true"})

    assert response.status_code == 200
    item = response.json()["providers"][0]
    assert item["configured"] is True
    assert item["auth_valid"] is False
    assert item["error_kind"] == "credentials_invalid"
    assert "AUTH_INVALID" in item["detail"]


def test_provider_status_endpoint_reports_provider_unavailable(monkeypatch) -> None:
    clear_provider_status_cache()
    app = FastAPI()
    app.state.settings = _usgs_settings()
    app.include_router(providers_router)

    monkeypatch.setattr(
        "nimbuschain_fetch.provider_status.requests.post",
        lambda *args, **kwargs: _FakeResponse(status_code=503, text="upstream down"),
    )
    monkeypatch.setattr("nimbuschain_fetch.provider_status.time.sleep", lambda _value: None)

    with TestClient(app) as client:
        response = client.get("/v1/providers/status", params={"provider": "usgs", "force_refresh": "true"})

    assert response.status_code == 200
    item = response.json()["providers"][0]
    assert item["configured"] is True
    assert item["auth_valid"] is False
    assert item["error_kind"] == "provider_unavailable"
    assert "HTTP 503" in item["detail"]


def test_usgs_job_creation_is_rejected_when_provider_auth_is_invalid(monkeypatch) -> None:
    clear_provider_status_cache()
    app = FastAPI()
    fetcher = _DummyFetcher()
    app.state.fetcher = fetcher
    app.state.settings = _usgs_settings()
    app.include_router(jobs_router)

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.jobs.get_provider_status",
        lambda *args, **kwargs: {
            "provider": "usgs",
            "configured": True,
            "auth_valid": False,
            "error_kind": "credentials_invalid",
            "message": "USGS credentials are invalid or rejected.",
            "detail": "USGS API error AUTH_INVALID: User credential verification failed",
        },
    )

    with TestClient(app) as client:
        response = client.post(
            "/v1/jobs",
            json={
                "job_type": "search_download",
                "provider": "usgs",
                "collection": "landsat_ot_c2_l1",
                "product_type": "L1TP",
                "start_date": "2026-03-01",
                "end_date": "2026-03-15",
                "aoi": {"wkt": "POLYGON((0 0,1 0,1 1,0 1,0 0))"},
            },
        )

    assert response.status_code == 400
    assert "NIMBUS_USGS_TOKEN" in response.text
    assert fetcher.submit_calls == 0
