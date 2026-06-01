from __future__ import annotations

import asyncio
from datetime import date

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import JobConvertRequest, JobStatusResponse, ProviderName, SearchDownloadRequest
from nimbuschain_fetch.settings import Settings
from nimbuschain_shared.clients.zarr import ZarrServiceClient
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


def test_fetcher_converter_uses_launch_mode_zarr_default(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL=""))

    converter = fetcher._converter()
    try:
        assert converter.service_url == "http://127.0.0.1:8010"
    finally:
        converter.close()


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


def test_fetcher_masker_uses_mps_host_url_over_stale_mask_url(tmp_path) -> None:
    fetcher = NimbusFetcher(
        settings=_sqlite_settings(
            tmp_path,
            NIMBUS_PIPELINE_LAUNCH_MODE="mps",
            NIMBUS_HOST_MPS_MASK_URL="http://host.containers.internal:18021",
            NIMBUS_MASK_SERVICE_URL="http://nimbus-mask:8020",
        )
    )

    masker = fetcher._masker()
    try:
        assert masker.service_url == "http://host.containers.internal:18021"
    finally:
        masker.close()


def test_mps_launch_mode_uses_host_sen2like_url_over_container_url(tmp_path) -> None:
    settings = _sqlite_settings(
        tmp_path,
        NIMBUS_PIPELINE_LAUNCH_MODE="mps",
        NIMBUS_HOST_MPS_SEN2LIKE_URL="http://host.containers.internal:18031",
        NIMBUS_SEN2LIKE_SERVICE_URL="http://nimbus-sen2like:8030",
    )

    assert settings.effective_sen2like_service_url == "http://host.containers.internal:18031"


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


def test_zarr_client_serializes_cube_dates_before_http_json() -> None:
    captured: dict[str, object] = {}

    class _FakeResponse:
        status_code = 200

        def json(self) -> dict[str, object]:
            return {"cube_summary": {"status": "written", "cube_outputs": ["/tmp/cube.zarr"], "items": []}}

    class _FakeSession:
        def post(self, url: str, *, json, timeout):
            captured["url"] = url
            captured["json"] = dict(json)
            captured["timeout"] = timeout
            return _FakeResponse()

        def close(self) -> None:
            return None

    client = ZarrServiceClient(service_url="http://nimbus-zarr:8010")
    client._session = _FakeSession()  # type: ignore[assignment]

    summary = client.build_grouped_cubes(
        job_id="job-123",
        pipeline_id="job-123",
        trace_id="trace-123",
        source_zarr_uris=["/tmp/source.zarr"],
        output_dir="/tmp/cubes",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        stage_label="after_mask",
    )

    assert captured["url"] == "http://nimbus-zarr:8010/cubes/grouped/build"
    payload = dict(captured["json"])
    assert payload["start_date"] == "2026-04-01"
    assert payload["end_date"] == "2026-04-02"
    assert summary["status"] == "written"


def test_failed_zarr_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-zarr-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="zarr_failed",
        pipeline_step="zarr_failed",
        raw_outputs=["/tmp/raw-scene.zip"],
        errors=["Zarr conversion failed."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_zarr"
    assert status.resume_label == "Resume Pipeline"
    assert "Downloaded raw outputs are already available" in str(status.resume_reason)


def test_failed_sen2like_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(
        settings=_sqlite_settings(
            tmp_path,
            NIMBUS_SEN2LIKE_SERVICE_URL="http://nimbus-sen2like:8030",
            NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010",
        )
    )
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.usgs,
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-sen2like-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="sen2like_failed",
        pipeline_step="sen2like_failed",
        raw_outputs=["/tmp/LC08_FAKE.tar"],
        errors=["Sen2Like was killed."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_sen2like"
    assert "Sen2Like can be retried" in str(status.resume_reason)


def test_resume_job_retries_failed_zarr_step(monkeypatch, tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-zarr-retry"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="zarr_failed",
        pipeline_step="zarr_failed",
        raw_outputs=["/tmp/raw-scene.zip"],
        errors=["Zarr conversion failed."],
    )

    captured: dict[str, object] = {}

    def _fake_convert_existing_job(target_job_id: str, request_payload, *, continue_pipeline: bool = False) -> JobStatusResponse:
        captured["job_id"] = target_job_id
        captured["request"] = request_payload
        captured["continue_pipeline"] = continue_pipeline
        return JobStatusResponse(
            job_id=target_job_id,
            job_type="search_download",
            job_kind="fetch",
            service_name="fetch_service",
            source_job_id=None,
            state="succeeded",
            pipeline_state="zarr_written",
            pipeline_step="zarr_written",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
        )

    monkeypatch.setattr(fetcher, "convert_existing_job", _fake_convert_existing_job)

    response = fetcher.resume_job(job_id)

    assert captured["job_id"] == job_id
    assert captured["continue_pipeline"] is True
    assert response.resume_action == "resume_pipeline_from_zarr"
    assert response.resumed_job_id == job_id
    assert response.spawned_new_job is False
    assert response.job.pipeline_state == "zarr_written"


def test_resume_job_routes_failed_sen2like_step(monkeypatch, tmp_path) -> None:
    fetcher = NimbusFetcher(
        settings=_sqlite_settings(
            tmp_path,
            NIMBUS_SEN2LIKE_SERVICE_URL="http://nimbus-sen2like:8030",
            NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010",
        )
    )
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.usgs,
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-sen2like-route"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="sen2like_failed",
        pipeline_step="sen2like_failed",
        raw_outputs=["/tmp/LC08_FAKE.tar"],
        errors=["Sen2Like was killed."],
    )
    captured: dict[str, object] = {}

    def _fake_resume_pipeline_from_sen2like_failure(*, job_id: str, row) -> JobStatusResponse:
        captured["job_id"] = job_id
        captured["row"] = row
        return JobStatusResponse(
            job_id=job_id,
            job_type="search_download",
            job_kind="fetch",
            service_name="fetch_service",
            source_job_id=None,
            state="succeeded",
            pipeline_state="zarr_written",
            pipeline_step="zarr_written",
            provider=ProviderName.usgs,
            collection="landsat_ot_c2_l1",
        )

    monkeypatch.setattr(
        fetcher,
        "_resume_pipeline_from_sen2like_failure",
        _fake_resume_pipeline_from_sen2like_failure,
    )

    response = fetcher.resume_job(job_id)

    assert captured["job_id"] == job_id
    assert response.resume_action == "resume_pipeline_from_sen2like"
    assert response.resumed_job_id == job_id
    assert response.spawned_new_job is False
    assert response.job.pipeline_state == "zarr_written"


def test_manual_conversion_marks_job_failed_when_resumed_zarr_step_errors(monkeypatch, tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-zarr-error-state"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="zarr_failed",
        pipeline_step="zarr_failed",
        raw_outputs=["/tmp/raw-scene.zip"],
        errors=["Initial Zarr conversion failed."],
    )

    monkeypatch.setattr(fetcher, "_convert_raw_outputs", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        fetcher.convert_existing_job(job_id, JobConvertRequest())

    row = fetcher.store.get_job(job_id) or {}
    result = fetcher.store.get_result(job_id) or {}
    assert row["state"] == "failed"
    assert row["pipeline_state"] == "zarr_failed"
    assert row["pipeline_step"] == "zarr_failed"
    assert row["errors"] == ["boom"]
    assert result["conversion_metadata"]["status"] == "failed"
    assert result["conversion_metadata"]["error"] == "boom"


def test_manual_conversion_marks_job_cancelled_when_resume_is_cancelled(monkeypatch, tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-zarr-cancel-state"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="zarr_failed",
        pipeline_step="zarr_failed",
        raw_outputs=["/tmp/raw-scene.zip"],
        errors=["Initial Zarr conversion failed."],
    )

    monkeypatch.setattr(
        fetcher,
        "_convert_raw_outputs",
        lambda *args, **kwargs: (_ for _ in ()).throw(fetcher.job_cancelled_error_cls("cancelled")),
    )

    status = fetcher.convert_existing_job(job_id, JobConvertRequest())

    row = fetcher.store.get_job(job_id) or {}
    assert status.state == "cancelled"
    assert row["state"] == "cancelled"
    assert row["pipeline_state"] == "cancelled"
    assert row["pipeline_step"] == "cancelled"


def test_failed_mask_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        mask_types=["water"],
    )
    job_id = "resume-mask-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="failed",
        pipeline_step="water_failed",
        pipeline_metadata={"mask_status": "failed", "mask_types": ["water"]},
        zarr_outputs=["/tmp/example.zarr"],
        errors=["Water mask failed."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_mask"
    assert "interrupted mask step" in str(status.resume_reason)


def test_failed_cube_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        cube_mode="before_mask",
    )
    job_id = "resume-cube-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="cube_failed",
        pipeline_step="cube_failed",
        zarr_outputs=["/tmp/example.zarr"],
        errors=["Cube build failed."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_cube"


def test_failed_cube_building_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        cube_mode="after_mask",
        mask_types=["water"],
    )
    job_id = "resume-cube-building-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="cube_building",
        pipeline_step="cube_building",
        zarr_outputs=["/tmp/example.zarr"],
        pipeline_metadata={"cube_mode": "after_mask", "mask_types": ["water"]},
        errors=["Cube build interrupted."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_cube"
    assert "interrupted cube step" in str(status.resume_reason)


def test_failed_zarr_converting_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
    )
    job_id = "resume-zarr-converting-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="zarr_converting",
        pipeline_step="zarr_converting",
        raw_outputs=["/tmp/raw-scene.zip"],
        errors=["Zarr conversion interrupted."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_zarr"
    assert "interrupted Zarr step" in str(status.resume_reason)


def test_failed_mask_runtime_job_exposes_resume_metadata(tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        mask_types=["water", "cloud"],
    )
    job_id = "resume-mask-runtime-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="running_water_inference",
        pipeline_step="running_water_inference",
        pipeline_metadata={"mask_types": ["water", "cloud"]},
        zarr_outputs=["/tmp/example.zarr"],
        errors=["Mask execution interrupted."],
    )

    status = fetcher.get_job(job_id)

    assert status.can_resume is True
    assert status.resume_action == "resume_pipeline_from_mask"
    assert "interrupted mask step" in str(status.resume_reason)


def test_resume_job_routes_failed_mask_step(monkeypatch, tmp_path) -> None:
    fetcher = NimbusFetcher(settings=_sqlite_settings(tmp_path, NIMBUS_ZARR_SERVICE_URL="http://nimbus-zarr:8010"))
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        aoi={"wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"},
        mask_types=["water"],
    )
    job_id = "resume-mask-route"
    fetcher.store.create_job(
        job_id=job_id,
        job_type=request.job_type,
        provider=request.provider.value,
        collection=request.collection,
        request_payload=request.model_dump(mode="json"),
    )
    fetcher.store.update_job(
        job_id,
        state="failed",
        pipeline_state="failed",
        pipeline_step="water_failed",
        pipeline_metadata={"mask_status": "failed", "mask_types": ["water"]},
        zarr_outputs=["/tmp/example.zarr"],
        errors=["Water mask failed."],
    )

    captured: dict[str, object] = {}

    def _fake_resume_pipeline_from_mask_failure(*, job_id: str, row) -> JobStatusResponse:
        captured["job_id"] = job_id
        captured["row"] = row
        return JobStatusResponse(
            job_id=job_id,
            job_type="search_download",
            job_kind="fetch",
            service_name="fetch_service",
            source_job_id=None,
            state="succeeded",
            pipeline_state="masked_zarr_written",
            pipeline_step="masked_zarr_written",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
        )

    monkeypatch.setattr(fetcher, "_resume_pipeline_from_mask_failure", _fake_resume_pipeline_from_mask_failure)

    response = fetcher.resume_job(job_id)

    assert captured["job_id"] == job_id
    assert response.resume_action == "resume_pipeline_from_mask"
    assert response.job.pipeline_state == "masked_zarr_written"
