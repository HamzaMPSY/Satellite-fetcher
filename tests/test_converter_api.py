from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
import requests

from nimbuschain_fetch.settings import Settings
from nimbuschain_fetch_service.api.converter import router as converter_router


class _FakeZarrServiceClient:
    def __init__(self, *, service_url: str):
        self.service_url = service_url

    def close(self) -> None:
        return None

    def health(self) -> tuple[int, dict[str, object]]:
        return 200, {"status": "ok", "service": "zarr-converter-service", "source_url": self.service_url}

    def readiness(self) -> tuple[int, dict[str, object]]:
        return 503, {"status": "not_ready", "service": "zarr-converter-service"}

    def schema(self) -> tuple[int, dict[str, object]]:
        return 200, {"status": "ok", "service": "zarr-converter-service", "zarr_model": {"version": "v1"}}


class _FakeMaskServiceClient:
    def __init__(self, *, service_url: str):
        self.service_url = service_url

    def close(self) -> None:
        return None

    def health(self) -> dict[str, object]:
        return {"status": "ok", "service": "mask-service", "source_url": self.service_url}


def test_converter_routes_proxy_zarr_service_contract(monkeypatch) -> None:
    app = FastAPI()
    app.state.settings = Settings(NIMBUS_ZARR_SERVICE_URL="http://zarr-service.internal:8010")
    app.include_router(converter_router)

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.converter.ZarrServiceClient",
        _FakeZarrServiceClient,
    )

    with TestClient(app) as client:
        health_response = client.get("/v1/converter/health")
        readiness_response = client.get("/v1/converter/readiness")
        schema_response = client.get("/v1/converter/schema")

    assert health_response.status_code == 200
    assert health_response.json()["source_url"] == "http://zarr-service.internal:8010"

    assert readiness_response.status_code == 503
    assert readiness_response.json()["status"] == "not_ready"

    assert schema_response.status_code == 200
    assert schema_response.json()["zarr_model"]["version"] == "v1"


def test_converter_health_returns_503_when_zarr_service_is_unreachable(monkeypatch) -> None:
    app = FastAPI()
    app.state.settings = Settings(NIMBUS_ZARR_SERVICE_URL="http://127.0.0.1:8010")
    app.include_router(converter_router)

    class _UnavailableZarrServiceClient:
        def __init__(self, *, service_url: str):
            self.service_url = service_url

        def close(self) -> None:
            return None

        def health(self) -> tuple[int, dict[str, object]]:
            raise requests.ConnectionError("connection refused")

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.converter.ZarrServiceClient",
        _UnavailableZarrServiceClient,
    )

    with TestClient(app) as client:
        response = client.get("/v1/converter/health")

    assert response.status_code == 503
    assert "Zarr service health request failed" in response.json()["detail"]


def test_converter_health_uses_launch_mode_zarr_default_when_url_is_missing(monkeypatch) -> None:
    app = FastAPI()
    app.state.settings = Settings(NIMBUS_ZARR_SERVICE_URL="")
    app.include_router(converter_router)

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.converter.ZarrServiceClient",
        _FakeZarrServiceClient,
    )

    with TestClient(app) as client:
        response = client.get("/v1/converter/health")

    assert response.status_code == 200
    assert response.json()["source_url"] == "http://127.0.0.1:8010"


def test_mask_health_uses_mps_host_url_over_stale_mask_url(monkeypatch) -> None:
    app = FastAPI()
    app.state.settings = Settings(
        NIMBUS_PIPELINE_LAUNCH_MODE="mps",
        NIMBUS_HOST_MPS_MASK_URL="http://host.containers.internal:18021",
        NIMBUS_MASK_SERVICE_URL="http://nimbus-mask:8020",
    )
    app.include_router(converter_router)

    monkeypatch.setattr(
        "nimbuschain_fetch_service.api.converter.MaskServiceClient",
        _FakeMaskServiceClient,
    )

    with TestClient(app) as client:
        response = client.get("/v1/mask/health")

    assert response.status_code == 200
    assert response.json()["source_url"] == "http://host.containers.internal:18021"


def test_settings_strip_zarr_service_url() -> None:
    settings = Settings(NIMBUS_ZARR_SERVICE_URL=" http://127.0.0.1:8010/ ")
    assert settings.nimbus_zarr_service_url == "http://127.0.0.1:8010/"
