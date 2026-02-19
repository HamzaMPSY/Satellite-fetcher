from __future__ import annotations

from anyio.from_thread import start_blocking_portal

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import SearchDownloadRequest
from nimbuschain_fetch.settings import Settings

from tests.conftest import SlowCancelableProvider, wait_for_terminal


def _search_request(output_dir: str | None = None) -> SearchDownloadRequest:
    return SearchDownloadRequest.model_validate(
        {
            "job_type": "search_download",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "aoi": {"wkt": "POLYGON((0 0,0 1,1 1,1 0,0 0))"},
            "output_dir": output_dir,
        }
    )


def test_submit_job_and_result(fetcher) -> None:
    job_id = fetcher.submit_job(_search_request())
    status = wait_for_terminal(fetcher, job_id)
    assert status.state.value == "succeeded"
    assert status.bytes_downloaded > 0

    result = fetcher.get_result(job_id)
    assert len(result.paths) >= 2
    assert any(path.endswith("manifest.json") for path in result.paths)
    assert result.manifest_entry["job_id"] == job_id


def test_list_jobs_filter(fetcher) -> None:
    job_id = fetcher.submit_job(_search_request(output_dir="jobs/a"))
    _ = wait_for_terminal(fetcher, job_id)

    listing = fetcher.list_jobs(
        state="succeeded",
        provider="copernicus",
        date_from=None,
        date_to=None,
        page=1,
        page_size=20,
    )
    assert listing.total >= 1
    assert any(item.job_id == job_id for item in listing.items)


def test_cancel_job_marks_cancelled(tmp_path) -> None:
    settings = Settings(
        nimbus_db_backend="sqlite",
        nimbus_db_path=tmp_path / "cancel.db",
        nimbus_data_dir=tmp_path / "downloads",
        nimbus_runtime_role="all",
        nimbus_max_jobs=1,
        nimbus_provider_limits="copernicus=1,usgs=1",
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"copernicus": SlowCancelableProvider, "usgs": SlowCancelableProvider},
    )

    with start_blocking_portal() as portal:
        portal.call(fetcher.start)
        try:
            job_id = str(portal.call(fetcher.submit_job, _search_request()))
            portal.call(fetcher.cancel_job, job_id)
            status = wait_for_terminal(fetcher, job_id, timeout=20)
            assert status.state.value in {"cancelled", "failed"}
        finally:
            portal.call(fetcher.stop)
