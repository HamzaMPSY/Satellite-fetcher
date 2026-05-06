from __future__ import annotations

from nimbuschain_fetch.pipeline import PipelineContext, PipelineOrchestrator, StageStatus
from nimbuschain_fetch.pipeline.defaults import PipelineOptions, build_default_pipeline_stages
from nimbuschain_fetch.pipeline.sen2like import is_landsat_context


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


def test_sen2like_stage_records_configured_service_url() -> None:
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

    assert results[-1].status == StageStatus.succeeded
    assert results[-1].metadata["service_url_configured"] is True
    assert results[-1].metadata["service_url"] == "http://nimbus-sen2like:8030"
