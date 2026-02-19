from __future__ import annotations

import time
from pathlib import Path

import pytest
from anyio.from_thread import BlockingPortal, start_blocking_portal

from nimbuschain_fetch.download.download_manager import DownloadCancelled
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings


class FakeProvider:
    def __init__(self, settings, download_manager):
        self.settings = settings
        self.download_manager = download_manager
        self.dataset = None

    def search_products(self, collection, product_type, start_date, end_date, aoi, tile_id=None):
        _ = (collection, product_type, start_date, end_date, aoi, tile_id)
        return ["FAKE_1", "FAKE_2"]

    def download_products(self, product_ids, output_dir):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        paths = []
        for idx, product_id in enumerate(product_ids):
            file_path = out / f"{product_id}_{idx}.bin"
            payload = b"x" * 1024
            file_path.write_bytes(payload)
            if self.download_manager.progress_callback:
                self.download_manager.progress_callback(
                    file_path.name,
                    len(payload),
                    len(payload),
                    len(payload),
                )
            paths.append(str(file_path))
        return paths


class SlowCancelableProvider(FakeProvider):
    def download_products(self, product_ids, output_dir):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        paths = []
        for product_id in product_ids:
            file_path = out / f"{product_id}.bin"
            downloaded = 0
            total = 200 * 1024
            for _ in range(200):
                if self.download_manager.cancel_checker and self.download_manager.cancel_checker():
                    raise DownloadCancelled("cancelled in provider")
                chunk = b"y" * 1024
                with file_path.open("ab") as handle:
                    handle.write(chunk)
                downloaded += len(chunk)
                if self.download_manager.progress_callback:
                    self.download_manager.progress_callback(
                        file_path.name,
                        len(chunk),
                        downloaded,
                        total,
                    )
                time.sleep(0.01)
            paths.append(str(file_path))
        return paths


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    return Settings(
        nimbus_db_backend="sqlite",
        nimbus_db_path=tmp_path / "nimbus.db",
        nimbus_data_dir=tmp_path / "downloads",
        nimbus_runtime_role="all",
        nimbus_max_jobs=2,
        nimbus_provider_limits="copernicus=2,usgs=2",
        nimbus_api_key=None,
    )


@pytest.fixture
def fetcher(test_settings: Settings):
    engine = NimbusFetcher(
        settings=test_settings,
        provider_registry={"copernicus": FakeProvider, "usgs": FakeProvider},
    )
    with start_blocking_portal() as portal:
        portal.call(engine.start)
        try:
            yield _EngineHarness(engine=engine, portal=portal)
        finally:
            portal.call(engine.stop)


class _EngineHarness:
    def __init__(self, *, engine: NimbusFetcher, portal: BlockingPortal):
        self._engine = engine
        self._portal = portal

    def submit_job(self, request) -> str:
        return str(self._portal.call(self._engine.submit_job, request))

    def cancel_job(self, job_id: str) -> bool:
        return bool(self._portal.call(self._engine.cancel_job, job_id))

    def get_job(self, job_id: str):
        return self._engine.get_job(job_id)

    def get_result(self, job_id: str):
        return self._engine.get_result(job_id)

    def list_jobs(
        self,
        *,
        state: str | None,
        provider: str | None,
        date_from,
        date_to,
        page: int,
        page_size: int,
    ):
        return self._engine.list_jobs(
            state=state,
            provider=provider,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
        )


def wait_for_terminal(fetcher: NimbusFetcher, job_id: str, timeout: float = 12.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = fetcher.get_job(job_id)
        if status.state.value in {"succeeded", "failed", "cancelled"}:
            return status
        time.sleep(0.05)
    raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
