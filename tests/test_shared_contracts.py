from __future__ import annotations

from nimbuschain_mask_service.contracts import MaskApplyRequest as LegacyMaskApplyRequest
from nimbuschain_mask_service import runtime as legacy_runtime
from nimbuschain_shared.contracts import (
    ConvertRequest,
    ConvertResponse,
    MaskApplyRequest,
    Sen2LikeNormalizeRequest,
    Sen2LikeNormalizeResponse,
)
from nimbuschain_shared.runtime import normalize_device_name, resolve_inference_device


def test_mask_contract_is_reexported_from_shared_library() -> None:
    assert LegacyMaskApplyRequest is MaskApplyRequest

    request = MaskApplyRequest.model_validate(
        {
            "source_zarr_uri": "/tmp/source.zarr",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "scene_id": "SCENE-1",
            "mask_types": ["water", "cloud"],
        }
    )

    assert request.source_zarr_uri == "/tmp/source.zarr"
    assert request.mask_types == ["water", "cloud"]


def test_zarr_contracts_are_available_from_shared_library() -> None:
    request = ConvertRequest(
        job_id="job-1",
        pipeline_id="pipe-1",
        trace_id="trace-1",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="SCENE-1",
        raw_uri="/tmp/raw.zip",
        raw_format="auto",
        output_uri="/tmp/output.zarr",
    )
    response = ConvertResponse(
        job_id=request.job_id,
        pipeline_id=request.pipeline_id,
        status="written",
        stage="zarr_converting",
        service="zarr-converter-service",
        message="ok",
        accepted_at="2026-04-24T00:00:00+00:00",
        zarr_uri="/tmp/output.zarr",
        data_family="optical",
        band_names=["B02"],
        dimensions=["time", "band", "y", "x"],
        normalization_summary={"ok": True},
    )

    assert request.provider == "copernicus"
    assert response.zarr_uri == "/tmp/output.zarr"


def test_sen2like_contracts_are_available_from_shared_library() -> None:
    request = Sen2LikeNormalizeRequest(
        job_id="job-1",
        pipeline_id="pipe-1",
        trace_id="trace-1",
        products=[" /tmp/LC08_SCENE.tar "],
        working_dir="/tmp/sen2like",
        workers=4,
        steps=["packaging"],
    )
    response = Sen2LikeNormalizeResponse(
        status="succeeded",
        job_id=request.job_id,
        pipeline_id=request.pipeline_id,
        trace_id=request.trace_id,
        products=request.product_inputs(),
        working_dir="/tmp/sen2like",
        outputs=[
            {
                "product": "/tmp/LC08_SCENE.tar",
                "output_dir": "/tmp/sen2like/LC08_SCENE",
                "normalized_uri": "/tmp/sen2like/LC08_SCENE/SAFE/S2L_SCENE.SAFE",
                "exists": True,
            }
        ],
        duration_seconds=1.25,
        return_code=0,
        command=["python", "Pipeline.py"],
    )

    assert request.product_inputs() == ["/tmp/LC08_SCENE.tar"]
    assert response.outputs[0].normalized_uri == "/tmp/sen2like/LC08_SCENE/SAFE/S2L_SCENE.SAFE"


def test_runtime_helpers_are_reexported_from_shared_library(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_TEST_DEVICE", raising=False)

    assert legacy_runtime.normalize_device_name("gpu") == normalize_device_name("gpu") == "cuda"
    assert (
        legacy_runtime.resolve_inference_device(explicit="cpu", env_var="NIMBUS_TEST_DEVICE")
        == resolve_inference_device(explicit="cpu", env_var="NIMBUS_TEST_DEVICE")
        == "cpu"
    )
