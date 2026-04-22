from __future__ import annotations

import asyncio
from pathlib import Path

from nimbuschain_fetch import cli
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import ProviderName, SearchDownloadRequest
from nimbuschain_fetch.settings import Settings


def test_fetch_cli_builds_download_only_search_request(tmp_path: Path) -> None:
    aoi_file = tmp_path / "aoi.wkt"
    aoi_file.write_text("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", encoding="utf-8")

    args = cli.build_parser().parse_args(
        [
            "--provider",
            "usgs",
            "--collection",
            "landsat_ot_c2_l1",
            "--product-type",
            "L1TP",
            "--start-date",
            "2026-04-14",
            "--end-date",
            "2026-04-21",
            "--aoi_file",
            str(aoi_file),
            "--download-only",
        ]
    )

    request = cli._build_request(args)

    assert request["job_type"] == "search_download"
    assert request["download_only"] is True
    assert request["cube_mode"] == "none"
    assert request["mask_types"] == []


def test_execute_job_stops_after_raw_download_when_download_only(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
    )
    fetcher = NimbusFetcher(settings=settings)
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.usgs,
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        start_date="2026-04-14",
        end_date="2026-04-21",
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        download_only=True,
    )
    job_id = "download-only-job"

    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )

    def _fake_run_provider_job(job_id, request, output_dir, progress_callback, retry_callback, is_cancelled):
        raw_path = Path(output_dir) / "scene.tar"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(b"raw-scene")
        return {
            "paths": [str(raw_path)],
            "metadata": {
                "job_type": request.job_type,
                "provider": request.provider.value,
                "collection": request.collection,
                "product_type": request.product_type,
                "products_found": 1,
                "products_downloaded": 1,
                "output_dir": str(output_dir),
            },
        }

    monkeypatch.setattr(fetcher, "_run_provider_job", _fake_run_provider_job)

    def _unexpected_convert(**kwargs):
        raise AssertionError("download_only job should not convert to Zarr")

    monkeypatch.setattr(fetcher, "_convert_raw_outputs", _unexpected_convert)

    asyncio.run(fetcher._execute_job(job_id, lambda: False))

    status = fetcher.get_job(job_id)
    result = fetcher.get_result(job_id)

    assert status.state.value == "succeeded"
    assert status.pipeline_state.value == "downloaded"
    assert result.raw_outputs
    assert not result.zarr_outputs
    assert result.pipeline_metadata["download_only"] is True
