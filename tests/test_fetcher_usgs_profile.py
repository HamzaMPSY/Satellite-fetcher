from __future__ import annotations

import asyncio
import json
from pathlib import Path

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import DownloadProductsRequest, ProviderName
from nimbuschain_fetch.ports import ProviderCapabilities
from nimbuschain_fetch.providers.copernicus import CopernicusProvider
from nimbuschain_fetch.settings import Settings


class _FakeProvider:
    def __init__(self) -> None:
        self.dataset = None

    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities()

    def configure_job(self, *, collection=None, product_type=None, download_strategy="default") -> None:
        _ = (product_type, download_strategy)
        self.dataset = collection

    def plan_download_metadata(self, product_count: int) -> dict[str, object]:
        _ = product_count
        return {}

    def download_metadata(self) -> dict[str, object]:
        return {}

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
    monkeypatch.setattr(fetcher, "_build_provider", lambda provider_name, download_manager, **kwargs: _FakeProvider())
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


def test_copernicus_provider_job_uses_legacy_download_profile_from_old_zip(
    monkeypatch,
    tmp_path: Path,
) -> None:
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
    monkeypatch.setattr(fetcher, "_build_provider", lambda provider_name, download_manager, **kwargs: _FakeProvider())
    monkeypatch.setattr(fetcher, "_update_pipeline", lambda *args, **kwargs: None)

    request = DownloadProductsRequest(
        job_type="download_products",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
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
    assert captured["max_retries"] == 5
    assert captured["initial_delay"] == 2.0
    assert captured["backoff_factor"] == 1.5
    assert captured["connect_timeout"] == 30.0
    assert captured["chunk_size"] == 128 * 1024
    assert captured["max_connections"] == 50
    assert captured["max_connections_per_host"] == 2


def test_download_manager_429_backoff_matches_legacy_profile() -> None:
    manager = DownloadManager(initial_delay=2.0, max_retry_delay=120.0)

    assert manager._retry_delay_for_http_status(
        status=429,
        attempt=1,
        current_delay=2.0,
        retry_after=None,
    ) == 2.0
    assert manager._retry_delay_for_http_status(
        status=429,
        attempt=1,
        current_delay=2.0,
        retry_after=45.0,
    ) == 45.0


def test_download_manager_avoids_deadlock_inside_running_event_loop(
    monkeypatch,
    tmp_path: Path,
) -> None:
    manager = DownloadManager()

    async def _fake_download_all(product_ids: dict, output_dir: Path) -> list[str]:
        return [str(output_dir / "scene.zip")]

    monkeypatch.setattr(manager, "_download_all", _fake_download_all)
    monkeypatch.setattr(
        "nimbuschain_fetch.download.download_manager.asyncio.run_coroutine_threadsafe",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("download_products should not use run_coroutine_threadsafe here")
        ),
    )

    async def _invoke() -> list[str]:
        return manager.download_products(
            {
                "urls": ["https://example.test/scene.zip"],
                "file_names": ["scene.zip"],
            },
            str(tmp_path),
        )

    assert asyncio.run(_invoke()) == [str(tmp_path / "scene.zip")]


def test_settings_parse_copernicus_account_pool(tmp_path: Path) -> None:
    pool_file = tmp_path / "copernicus_accounts.json"
    pool_file.write_text(
        json.dumps(
            {
                "accounts": [
                    {"label": "secondary-1", "username": "user-2@example.com", "password": "pw-2"},
                    {"label": "secondary-2", "username": "user-3@example.com", "password": "pw-3"},
                ]
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        NIMBUS_COPERNICUS_USERNAME="user-1@example.com",
        NIMBUS_COPERNICUS_PASSWORD="pw-1",
        NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE=str(pool_file),
    )

    assert settings.copernicus_account_pool_size == 3
    assert settings.copernicus_account_pool_available is True
    assert [item["label"] for item in settings.copernicus_account_pool_accounts] == [
        "primary",
        "secondary-1",
        "secondary-2",
    ]


def test_copernicus_account_pool_selects_required_account_count(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_COPERNICUS_USERNAME="user-1@example.com",
        NIMBUS_COPERNICUS_PASSWORD="pw-1",
        NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON=json.dumps(
            [
                {"label": "secondary-1", "username": "user-2@example.com", "password": "pw-2"},
                {"label": "secondary-2", "username": "user-3@example.com", "password": "pw-3"},
            ]
        ),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE=str(tmp_path / "copernicus-pool.inline.json"),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY="4",
    )
    provider = CopernicusProvider(settings=settings, download_manager=DownloadManager(), download_strategy="copernicus_account_pool")

    captured: list[tuple[str, list[str]]] = []

    def _fake_account_batch(self, *, account, assigned_product_ids, output_dir):
        captured.append((str(account.get("label") or ""), list(assigned_product_ids)))
        return [(product_id, str(Path(output_dir) / f"{product_id}.zip")) for product_id in assigned_product_ids]

    monkeypatch.setattr(CopernicusProvider, "_download_products_account_batch", _fake_account_batch)

    product_ids = [f"scene-{index}" for index in range(1, 10)]
    paths = provider.download_products(product_ids, str(tmp_path))

    assert len(paths) == len(product_ids)
    assert provider.last_download_metadata["account_pool_selected_accounts"] == 3
    assert provider.last_download_metadata["account_pool_per_account_concurrency"] == 4
    assert sorted(len(batch) for _, batch in captured) == [3, 3, 3]


def test_copernicus_account_pool_plan_metadata_includes_assignment_counts(tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_COPERNICUS_USERNAME="user-1@example.com",
        NIMBUS_COPERNICUS_PASSWORD="pw-1",
        NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON=json.dumps(
            [
                {"label": "secondary-1", "username": "user-2@example.com", "password": "pw-2"},
                {"label": "secondary-2", "username": "user-3@example.com", "password": "pw-3"},
            ]
        ),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE=str(tmp_path / "copernicus-pool.inline.json"),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY="4",
    )
    provider = CopernicusProvider(
        settings=settings,
        download_manager=DownloadManager(),
        download_strategy="copernicus_account_pool",
    )

    metadata = provider.plan_download_metadata(10)

    assert metadata["account_pool_selected_accounts"] == 3
    assert metadata["account_pool_assignments"] == [
        {"account_label": "primary", "product_count": 4},
        {"account_label": "secondary-1", "product_count": 3},
        {"account_label": "secondary-2", "product_count": 3},
    ]


def test_copernicus_account_pool_plan_metadata_uses_all_available_accounts_before_reuse(tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_COPERNICUS_USERNAME="user-1@example.com",
        NIMBUS_COPERNICUS_PASSWORD="pw-1",
        NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON=json.dumps(
            [
                {"label": "secondary-1", "username": "user-2@example.com", "password": "pw-2"},
                {"label": "secondary-2", "username": "user-3@example.com", "password": "pw-3"},
                {"label": "secondary-3", "username": "user-4@example.com", "password": "pw-4"},
            ]
        ),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE=str(tmp_path / "copernicus-pool.inline.json"),
        NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY="4",
    )
    provider = CopernicusProvider(
        settings=settings,
        download_manager=DownloadManager(),
        download_strategy="copernicus_account_pool",
    )

    metadata = provider.plan_download_metadata(5)

    assert metadata["account_pool_selected_accounts"] == 4
    assert metadata["account_pool_assignments"] == [
        {"account_label": "primary", "product_count": 2},
        {"account_label": "secondary-1", "product_count": 1},
        {"account_label": "secondary-2", "product_count": 1},
        {"account_label": "secondary-3", "product_count": 1},
    ]


def test_build_download_telemetry_groups_progress_by_account() -> None:
    telemetry = NimbusFetcher._build_download_telemetry(
        pipeline_metadata={
            "download_strategy": "copernicus_account_pool",
            "products_found": 12,
            "products_downloaded": 6,
            "account_pool_selected_accounts": 3,
            "account_pool_size": 3,
            "account_pool_per_account_concurrency": 4,
            "download_started_at": "2026-04-15T11:35:20+00:00",
            "download_finished_at": "2026-04-15T11:39:05+00:00",
            "download_window_seconds": 225.0,
            "account_pool_assignments": [
                {"account_label": "primary", "product_count": 4},
                {"account_label": "secondary-1", "product_count": 4},
                {"account_label": "secondary-2", "product_count": 4},
            ],
        },
        file_progress={
            "scene-1": {
                "account_label": "primary",
                "file_name": "scene-1.zip",
                "downloaded": 100,
                "total": 100,
                "completed": True,
                "last_update_mono": 1.0,
            },
            "scene-2": {
                "account_label": "secondary-1",
                "file_name": "scene-2.zip",
                "downloaded": 50,
                "total": 100,
                "completed": False,
                "last_update_mono": 2.0,
            },
        },
        bytes_downloaded=150,
        bytes_total=200,
        progress_pct=75.0,
        speed_bps=25.0,
        retry_state={
            "secondary-1": {
                "retry_count": 2,
                "last_reason": "http_429",
                "status": "rate_limited",
                "last_retry_at": "2026-04-13T09:00:00+00:00",
            }
        },
        phase="running",
        last_file="scene-2.zip",
    )

    assert telemetry["selected_accounts"] == 3
    assert telemetry["progress_pct"] == 75.0
    assert telemetry["rate_limited_accounts"] == 1
    assert telemetry["files_completed"] == 1
    assert telemetry["duration_seconds"] == 225.0
    assert telemetry["started_at"] == "2026-04-15T11:35:20+00:00"
    assert telemetry["accounts"][0]["account_label"] == "primary"
    assert telemetry["accounts"][0]["progress_pct"] == 100.0
    assert telemetry["accounts"][1]["account_label"] == "secondary-1"
    assert telemetry["accounts"][1]["rate_limited"] is True
    assert telemetry["accounts"][1]["current_file"] == "scene-2.zip"


def test_fetcher_propagates_copernicus_account_pool_strategy(monkeypatch, tmp_path: Path) -> None:
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

    class _FakeCopernicusProvider:
        def __init__(self) -> None:
            self.download_strategy = "default"
            self.last_download_metadata = {
                "download_strategy": "copernicus_account_pool",
                "account_pool_selected_accounts": 2,
                "account_pool_per_account_concurrency": 4,
            }

        def capabilities(self) -> ProviderCapabilities:
            return ProviderCapabilities()

        def configure_job(self, *, collection=None, product_type=None, download_strategy="default") -> None:
            _ = (collection, product_type)
            self.download_strategy = str(download_strategy or "default")

        def plan_download_metadata(self, product_count: int) -> dict[str, object]:
            return {
                "account_pool_selected_accounts": 2,
                "account_pool_per_account_concurrency": 4,
            }

        def download_metadata(self) -> dict[str, object]:
            return dict(self.last_download_metadata)

        def download_products(self, product_ids, output_dir):
            return [str(Path(output_dir) / "product.tar")]

    fake_provider = _FakeCopernicusProvider()

    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.DownloadManager", _FakeDownloadManager)
    monkeypatch.setattr(fetcher, "_build_provider", lambda provider_name, download_manager, **kwargs: fake_provider)
    monkeypatch.setattr(fetcher, "_update_pipeline", lambda *args, **kwargs: None)

    request = DownloadProductsRequest(
        job_type="download_products",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_ids=["scene-1"],
        download_strategy="copernicus_account_pool",
    )

    result = fetcher._run_provider_job(
        "job-1",
        request,
        tmp_path,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda: False,
    )

    assert result["metadata"]["download_strategy"] == "copernicus_account_pool"
    assert result["metadata"]["account_pool_selected_accounts"] == 2
    assert fake_provider.download_strategy == "copernicus_account_pool"
