from __future__ import annotations

from pathlib import Path

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import DownloadProductsRequest, ProviderName
from nimbuschain_fetch.settings import Settings


class _FakeProvider:
    def __init__(self) -> None:
        self.dataset = None

    def download_products(self, product_ids, output_dir):
        return [str(Path(output_dir) / "product.tar")]


def test_usgs_provider_job_uses_conservative_legacy_like_download_profile(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_PROVIDER_LIMITS="copernicus=2,usgs=4",
    )
    fetcher = NimbusFetcher(settings=settings)

    captured: dict[str, object] = {}

    class _FakeDownloadManager:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.DownloadManager", _FakeDownloadManager)
    monkeypatch.setattr(fetcher, "_build_provider", lambda provider_name, download_manager: _FakeProvider())
    monkeypatch.setattr(fetcher, "_update_pipeline", lambda *args, **kwargs: None)

    request = DownloadProductsRequest(
        job_type="download_products",
        provider=ProviderName.usgs,
        collection="landsat_ot_c2_l1",
        product_ids=["scene-1"],
    )

    result = fetcher._run_provider_job(
        "job-1",
        request,
        tmp_path,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda: False,
    )

    assert result["paths"] == [str(tmp_path / "product.tar")]
    assert captured["max_concurrent"] == 2
    assert captured["initial_delay"] == 2.0
    assert captured["backoff_factor"] == 1.5
    assert captured["connect_timeout"] == 30.0
    assert captured["chunk_size"] == 128 * 1024
    assert captured["max_connections"] == 50
    assert captured["max_connections_per_host"] == 2
