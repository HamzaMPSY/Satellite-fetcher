from __future__ import annotations

import pytest

from nimbuschain_mask_service.client import MaskServiceClient


def test_mask_client_requires_service_url() -> None:
    with pytest.raises(ValueError, match="service_url is required for MaskServiceClient"):
        MaskServiceClient(service_url="")


def test_mask_client_posts_apply_request_to_remote_service() -> None:
    captured: dict[str, object] = {}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"status": "written", "masked_zarr_uri": "/tmp/output.zarr"}

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            captured["url"] = url
            captured["json"] = dict(json)
            captured["timeout"] = timeout
            captured["params"] = params
            return _FakeResponse()

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client._session = _FakeSession()
    try:
        result = client.apply_masks_to_zarr(
            zarr_uri="/tmp/source.zarr",
            provider="copernicus",
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            scene_id="S2A_SCENE",
            acquisition_datetime="2026-04-01T10:00:00Z",
            dataset_summary={"shape": [1, 12, 4, 4]},
            mask_types=["cloud"],
            job_id="job-123",
        )
    finally:
        client.close()

    assert result["status"] == "written"
    assert captured["url"] == "http://nimbus-mask:8020/apply"
    assert captured["timeout"] == (30, None)
    assert captured["params"] == {"job_id": "job-123"}
    assert dict(captured["json"])["source_zarr_uri"] == "/tmp/source.zarr"
