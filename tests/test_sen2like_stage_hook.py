from __future__ import annotations

from nimbuschain_fetch.pipeline import PipelineContext, PipelineOrchestrator, StageStatus
from nimbuschain_fetch.pipeline.defaults import PipelineOptions, build_default_pipeline_stages
from nimbuschain_fetch.pipeline.sen2like import is_landsat_context, is_landsat_selection
import nimbuschain_fetch.pipeline.sen2like as sen2like_module


def test_landsat_context_detection_requires_usgs() -> None:
    landsat = PipelineContext(
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
    )
    sentinel = PipelineContext(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )

    assert is_landsat_context(landsat) is True
    assert is_landsat_context(sentinel) is False
    assert is_landsat_selection(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    ) is False


def test_default_pipeline_excludes_sen2like_for_sentinel() -> None:
    options = PipelineOptions(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    orchestrator = PipelineOrchestrator(build_default_pipeline_stages(options))

    assert orchestrator.plan() == ["fetch", "zarr"]
    assert "sen2like" not in orchestrator.stages


def test_sen2like_stage_skips_landsat_when_service_url_missing() -> None:
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert [result.name for result in results] == ["fetch", "sen2like"]
    assert results[-1].status == StageStatus.skipped
    assert results[-1].metadata["reason"] == "sen2like_service_url_missing"


def test_sen2like_stage_skips_when_input_path_is_missing() -> None:
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
        sen2like_service_url="http://nimbus-sen2like:8030",
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert results[-1].status == StageStatus.skipped
    assert results[-1].metadata["reason"] == "sen2like_input_missing"
    assert results[-1].metadata["service_url"] == "http://nimbus-sen2like:8030"


def test_sen2like_stage_calls_configured_service(monkeypatch) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, *, service_url: str):
            self.service_url = service_url

        def normalize(self, **kwargs):
            calls.append({"service_url": self.service_url, **kwargs})
            return {
                "status": "succeeded",
                "outputs": [
                    {
                        "output_dir": "/tmp/sen2like/LC08_SCENE",
                        "normalized_uri": "/tmp/sen2like/LC08_SCENE/T31UDQ_L2F",
                    }
                ],
                "duration_seconds": 1.25,
            }

        def close(self) -> None:
            calls.append({"closed": True})

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeClient)
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
        sen2like_service_url="http://nimbus-sen2like:8030",
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
        payload={"raw_uri": "/data/downloads/raw/LC08_SCENE", "sen2like_workers": 8},
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert results[-1].status == StageStatus.succeeded
    assert results[-1].metadata["service_url_configured"] is True
    assert results[-1].metadata["service_url"] == "http://nimbus-sen2like:8030"
    assert results[-1].outputs == ["/tmp/sen2like/LC08_SCENE/T31UDQ_L2F"]
    assert calls[0]["products"] == ["/data/downloads/raw/LC08_SCENE"]
    assert calls[0]["workers"] == 8
    assert calls[-1] == {"closed": True}


def test_sen2like_stage_normalizes_all_raw_uris(monkeypatch) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, *, service_url: str):
            self.service_url = service_url

        def normalize(self, **kwargs):
            calls.append({"service_url": self.service_url, **kwargs})
            return {
                "status": "succeeded",
                "outputs": [
                    {"normalized_uri": "/tmp/sen2like/LC08_A/T31UDQ_L2F"},
                    {"normalized_uri": "/tmp/sen2like/LC08_B/T31UDQ_L2F"},
                ],
            }

        def close(self) -> None:
            pass

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeClient)
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        sen2like_service_url="http://nimbus-sen2like:8030",
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        payload={
            "raw_uri": "/data/downloads/raw/LC08_A",
            "raw_uris": [
                "/data/downloads/raw/LC08_A",
                "/data/downloads/raw/LC08_B",
            ],
        },
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert results[-1].status == StageStatus.succeeded
    assert calls[0]["products"] == [
        "/data/downloads/raw/LC08_A",
        "/data/downloads/raw/LC08_B",
    ]
    assert calls[0]["workers"] == 4
    assert results[-1].outputs == [
        "/tmp/sen2like/LC08_A/T31UDQ_L2F",
        "/tmp/sen2like/LC08_B/T31UDQ_L2F",
    ]
    assert results[-1].metadata["landsat_inputs"] == calls[0]["products"]


def test_sen2like_stage_fails_when_service_fails_by_default(monkeypatch) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, *, service_url: str):
            self.service_url = service_url

        def normalize(self, **kwargs):
            calls.append({"service_url": self.service_url, **kwargs})
            raise RuntimeError("sen2like container was killed")

        def close(self) -> None:
            calls.append({"closed": True})

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeClient)
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        sen2like_service_url="http://nimbus-sen2like:8030",
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        payload={
            "raw_uris": [
                "/data/downloads/raw/LC08_A",
                "/data/downloads/raw/LC08_B",
            ],
        },
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert results[-1].status == StageStatus.failed
    assert results[-1].outputs == []
    assert results[-1].metadata["fallback_to_raw"] is False
    assert results[-1].metadata["fallback_allowed"] is False
    assert results[-1].metadata["failure_reason"] == "sen2like_service_failed"
    assert results[-1].metadata["error_type"] == "RuntimeError"
    assert context.get("zarr_inputs") is None
    assert calls[-1] == {"closed": True}


def test_sen2like_stage_can_opt_into_raw_fallback_for_degraded_tests(monkeypatch) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, *, service_url: str):
            self.service_url = service_url

        def normalize(self, **kwargs):
            calls.append({"service_url": self.service_url, **kwargs})
            raise RuntimeError("sen2like container was killed")

        def close(self) -> None:
            calls.append({"closed": True})

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeClient)
    options = PipelineOptions(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        sen2like_service_url="http://nimbus-sen2like:8030",
        allow_sen2like_raw_fallback=True,
    )
    context = PipelineContext(
        job_id="job-landsat",
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        payload={
            "raw_uris": [
                "/data/downloads/raw/LC08_A",
                "/data/downloads/raw/LC08_B",
            ],
        },
    )

    results = PipelineOrchestrator(build_default_pipeline_stages(options)).run(
        context,
        target_stage="sen2like",
    )

    assert results[-1].status == StageStatus.succeeded
    assert results[-1].outputs == [
        "/data/downloads/raw/LC08_A",
        "/data/downloads/raw/LC08_B",
    ]
    assert results[-1].metadata["fallback_to_raw"] is True
    assert results[-1].metadata["fallback_reason"] == "sen2like_service_failed"
    assert results[-1].metadata["fallback_error_type"] == "RuntimeError"
    assert context.get("zarr_inputs") == results[-1].outputs
    assert calls[-1] == {"closed": True}
