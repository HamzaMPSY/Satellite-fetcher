from __future__ import annotations

import asyncio

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings
import pytest


def _sqlite_settings(tmp_path, **extra) -> Settings:
    values = {
        "NIMBUS_DB_BACKEND": "sqlite",
        "NIMBUS_DB_PATH": str(tmp_path / "nimbus.db"),
        "NIMBUS_DATA_DIR": str(tmp_path / "downloads"),
    }
    values.update(extra)
    return Settings(**values)


def test_fetcher_uses_remote_zarr_client_when_service_url_is_configured(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["service_url"] = service_url

        def convert(self, **kwargs):
            captured["kwargs"] = dict(kwargs)
            return (
                "remote-output.zarr",
                "optical",
                {"status": "written"},
                {"band_names": ["B02"], "dimensions": ["time", "band", "y", "x"], "shape": [1, 1, 1, 1]},
            )

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.ZarrServiceClient", _FakeZarrClient)

    fetcher = NimbusFetcher(
        settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010")
    )

    converted = fetcher._convert_single_raw_output(
        job_id="job-123",
        provider_name="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        raw_uri="/tmp/raw.zip",
        scene_id="SCENE-1",
        output_uri="/tmp/out.zarr",
    )

    assert converted["zarr_uri"] == "remote-output.zarr"
    assert captured["service_url"] == "http://nimbus-zarr:8010"
    kwargs = dict(captured["kwargs"])
    assert kwargs["job_id"] == "job-123"
    assert kwargs["pipeline_id"] == "job-123"
    assert kwargs["provider"] == "copernicus"
    assert kwargs["collection"] == "SENTINEL-2"
    assert kwargs["scene_id"] == "SCENE-1"
    assert kwargs["raw_uri"] == "/tmp/raw.zip"
    assert kwargs["output_uri"] == "/tmp/out.zarr"
    assert kwargs["product_type"] == "S2MSI2A"
    assert kwargs["trace_id"]


def test_fetcher_converter_requires_zarr_service_url(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL=""))

    with pytest.raises(RuntimeError, match="Zarr service URL is not configured."):
        fetcher._converter()


def test_fetcher_stop_closes_remote_zarr_client(tmp_path) -> None:
    closed = {"value": False}

    class _ClosableClient:
        def close(self) -> None:
            closed["value"] = True

    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL=""))
    fetcher._zarr_converter = _ClosableClient()
    fetcher._started = True

    asyncio.run(fetcher.stop())

    assert closed["value"] is True
    assert fetcher._zarr_converter is None


def test_fetcher_masker_requires_mask_service_url(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_MASK_SERVICE_URL=""))

    with pytest.raises(RuntimeError, match="Mask service URL is not configured."):
        fetcher._masker()


def test_fetcher_inspects_zarr_dataset_via_remote_client(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["service_url"] = service_url

        def inspect_dataset(self, *, zarr_uri: str) -> dict[str, object]:
            captured["zarr_uri"] = zarr_uri
            return {
                "dimensions": ["time", "band", "y", "x"],
                "shape": [1, 2, 3, 4],
                "band_names": ["B02", "B03"],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.ZarrServiceClient", _FakeZarrClient)

    fetcher = NimbusFetcher(
        settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010")
    )

    dataset_summary = fetcher._inspect_zarr_dataset("/tmp/example.zarr")

    assert captured["service_url"] == "http://nimbus-zarr:8010"
    assert captured["zarr_uri"] == "/tmp/example.zarr"
    assert dataset_summary["shape"] == [1, 2, 3, 4]
