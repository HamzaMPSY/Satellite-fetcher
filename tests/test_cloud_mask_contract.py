from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("zarr")

import zarr

from nimbuschain_fetch_service.api.converter import router as converter_router
import nimbuschain_mask_service.service as mask_service_module
from nimbuschain_mask_service.inference import run_cloud_inference
from nimbuschain_mask_service.service import MaskService


def _write_cloud_shadow_scene(root: Path) -> Path:
    source = root / "cloud_shadow_source.zarr"
    group = zarr.open_group(str(source), mode="w", zarr_format=2)
    band_names = [
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B09",
        "B11",
        "B12",
    ]
    height = 12
    width = 12
    imagery = group.create_array(
        "imagery",
        shape=(1, len(band_names), height, width),
        chunks=(1, len(band_names), height, width),
        dtype=np.uint16,
        overwrite=True,
    )
    imagery[:] = 0

    cloud = np.zeros((height, width), dtype=np.uint16)
    cloud[2:6, 2:6] = 10000
    shadow = np.zeros((height, width), dtype=np.uint16)
    shadow[7:10, 7:10] = 300
    background = np.full((height, width), 1500, dtype=np.uint16)
    background_nir = np.full((height, width), 8000, dtype=np.uint16)

    idx = {name: band_names.index(name) for name in band_names}
    for band_name in ("B02", "B03", "B04"):
        imagery[0, idx[band_name], :, :] = background
        imagery[0, idx[band_name], 2:6, 2:6] = 10000
        imagery[0, idx[band_name], 7:10, 7:10] = 250

    imagery[0, idx["B08"], :, :] = background_nir
    imagery[0, idx["B08"], 2:6, 2:6] = 300
    imagery[0, idx["B08"], 7:10, 7:10] = 100

    imagery[0, idx["B11"], :, :] = 2200
    imagery[0, idx["B12"], :, :] = 1800
    imagery[0, idx["B11"], 2:6, 2:6] = 1200
    imagery[0, idx["B12"], 2:6, 2:6] = 900
    imagery[0, idx["B11"], 7:10, 7:10] = 400
    imagery[0, idx["B12"], 7:10, 7:10] = 300

    imagery[0, idx["B01"], :, :] = 0
    imagery[0, idx["B05"], :, :] = 1800
    imagery[0, idx["B06"], :, :] = 1800
    imagery[0, idx["B07"], :, :] = 1800
    imagery[0, idx["B8A"], :, :] = 1800
    imagery[0, idx["B09"], :, :] = 1800

    group.create_array(
        "band",
        shape=(len(band_names),),
        chunks=(len(band_names),),
        dtype="U3",
        overwrite=True,
    )
    group["band"][:] = np.array(band_names, dtype="U3")
    group.attrs.update(
        {
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "scene_id": "S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
            "source_uri": "/downloads/raw/cloud_shadow_source.SAFE.zip",
            "product_type": "S2MSI2A",
            "band_names": band_names,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, len(band_names), height, width],
            "data_family": "optical",
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        }
    )
    zarr.consolidate_metadata(str(source))
    return source


def test_omnicloudmask_branch_preserves_shadow_class_pixels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = _write_cloud_shadow_scene(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"

    def _fake_predict_from_array(array, **_kwargs):
        class_map = np.zeros((array.shape[1], array.shape[2]), dtype=np.uint8)
        class_map[2:6, 2:6] = 1
        class_map[7:10, 7:10] = 3
        return class_map

    fake_module = types.SimpleNamespace(predict_from_array=_fake_predict_from_array)
    monkeypatch.setitem(sys.modules, "omnicloudmask", fake_module)

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-cloud-omnicloudmask",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="omnicloudmask",
        threshold=0.45,
    )

    assert result["status"] == "written"
    masked = zarr.open_group(str(output), mode="r", use_consolidated=False)
    cloud_mask = np.asarray(masked["masks"]["cloud"][0, :, :])
    cloud_prob = np.asarray(masked["masks"]["cloud_probability"][0, :, :])
    assert np.all(cloud_mask[2:6, 2:6] == 1)
    assert np.all(cloud_mask[7:10, 7:10] == 1)
    assert float(cloud_prob[7:10, 7:10].mean()) > 0.0
    assert result["inference"]["includes_shadows"] is True
    assert result["inference"]["shadow_fraction"] > 0.0
    assert masked.attrs["cloud_mask_status"] == "written"


def test_heuristic_cloud_mask_should_capture_shadow_pixels_adjacent_to_cloud(tmp_path: Path) -> None:
    source = _write_cloud_shadow_scene(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-cloud-heuristic",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="heuristic",
        threshold=0.45,
    )

    assert result["status"] == "written"
    masked = zarr.open_group(str(output), mode="r", use_consolidated=False)
    cloud_mask = np.asarray(masked["masks"]["cloud"][0, :, :])
    assert cloud_mask[2:6, 2:6].mean() > 0.5
    assert cloud_mask[7:10, 7:10].mean() > 0.5


def test_mask_cloud_route_transports_backend_threshold_and_overwrite(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class RecordingFetcher:
        def apply_cloud_mask_existing_job(self, job_id: str, request):
            captured["job_id"] = job_id
            captured["request"] = request.model_dump(mode="python") if hasattr(request, "model_dump") else request
            return {
                "job_id": f"{job_id}-mask-cloud",
                "source_job_id": job_id,
                "source_zarr_uri": "/tmp/source.zarr",
                "masked_zarr_uri": "/tmp/masked.zarr",
                "mask_types": ["cloud"],
                "water_mask": {},
                "cloud_mask": {
                    "status": "written",
                    "backend": "omnicloudmask",
                    "threshold": 0.62,
                    "classes": {"0": "clear", "1": "cloud", "2": "thin cloud", "3": "cloud shadow"},
                },
                "masked_zarr_outputs": ["/tmp/masked.zarr"],
                "watermask_outputs": [],
                "cloudmask_outputs": ["/tmp/cloud_mask.tif", "/tmp/cloud_mask_status.json"],
                "job": {
                    "job_id": f"{job_id}-mask-cloud",
                    "job_kind": "mask",
                    "service_name": "mask_service",
                    "source_job_id": job_id,
                    "state": "succeeded",
                    "pipeline_state": "masked_zarr_written",
                    "provider": "copernicus",
                    "collection": "SENTINEL-2",
                },
            }

    app = FastAPI()
    app.state.fetcher = RecordingFetcher()
    app.include_router(converter_router)

    with TestClient(app) as client:
        response = client.post(
            "/v1/jobs/job-123/mask-cloud",
            json={
                "zarr_uri": "/tmp/source.zarr",
                "backend": "omnicloudmask",
                "threshold": 0.62,
                "overwrite": True,
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["mask_types"] == ["cloud"]
    assert body["cloud_mask"]["backend"] == "omnicloudmask"
    assert body["cloud_mask"]["threshold"] == 0.62
    assert body["job"]["job_kind"] == "mask"
    assert captured["job_id"] == "job-123"
    assert captured["request"]["backend"] == "omnicloudmask"


def test_mask_route_transports_water_backend_fields() -> None:
    captured: dict[str, object] = {}

    class RecordingFetcher:
        def apply_mask_existing_job(self, job_id: str, request):
            captured["job_id"] = job_id
            captured["request"] = request.model_dump(mode="python") if hasattr(request, "model_dump") else request
            return {
                "job_id": f"{job_id}-mask",
                "source_job_id": job_id,
                "source_zarr_uri": "/tmp/source.zarr",
                "masked_zarr_uri": None,
                "mask_types": ["water", "cloud"],
                "water_mask": {},
                "cloud_mask": {},
                "masked_zarr_outputs": [],
                "watermask_outputs": [],
                "cloudmask_outputs": [],
                "job": {
                    "job_id": f"{job_id}-mask",
                    "job_kind": "mask",
                    "service_name": "mask_service",
                    "source_job_id": job_id,
                    "state": "queued",
                    "pipeline_state": "queued",
                    "provider": "copernicus",
                    "collection": "SENTINEL-2",
                },
            }

    app = FastAPI()
    app.state.fetcher = RecordingFetcher()
    app.include_router(converter_router)

    with TestClient(app) as client:
        response = client.post(
            "/v1/jobs/job-321/mask",
            json={
                "zarr_uri": "/tmp/source.zarr",
                "mask_types": ["water", "cloud"],
                "backend": "heuristic",
                "water_backend": "omniwatermask",
                "include_shadows": True,
            },
        )

    assert response.status_code == 200
    assert captured["job_id"] == "job-321"
    assert captured["request"]["water_backend"] == "omniwatermask"
    assert captured["request"]["mask_types"] == ["water", "cloud"]
    assert captured["request"]["overwrite"] is True


def test_tiled_omnicloudmask_reads_rgbnir_without_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_cloud_shadow_scene(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"
    captured: dict[str, object] = {}

    def _recording_read_required_channels_window(
        root,
        *,
        band_names,
        required_bands,
        scale_hint,
        row_start,
        row_stop,
        col_start,
        col_stop,
        normalize,
        time_index=0,
    ):
        captured["required_bands"] = tuple(required_bands)
        captured["normalize"] = bool(normalize)
        height = row_stop - row_start
        width = col_stop - col_start
        channels = {
            band_name: np.full((height, width), 1200.0, dtype=np.float32)
            for band_name in required_bands
        }
        return channels, []

    def _fake_run_cloud_inference(**kwargs):
        channels = dict(kwargs["channels"])
        captured["channel_keys"] = tuple(channels.keys())
        sample = next(iter(channels.values()))
        mask = np.ones(sample.shape, dtype=np.uint8)
        probability = np.ones(sample.shape, dtype=np.float32)
        return types.SimpleNamespace(
            mask=mask,
            probability=probability,
            summary={
                "cloud_fraction": 1.0,
                "cloud_only_fraction": 1.0,
                "shadow_fraction": 0.0,
                "includes_shadows": True,
            },
        )

    monkeypatch.setattr(mask_service_module, "read_required_channels_window", _recording_read_required_channels_window)
    monkeypatch.setattr(mask_service_module, "run_cloud_inference", _fake_run_cloud_inference)
    monkeypatch.setattr(mask_service_module, "_cloud_tile_size", lambda: 1024)

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-cloud-rgbnir",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="omnicloudmask",
        threshold=0.45,
    )

    assert result["status"] == "written"
    assert captured["required_bands"] == ("B04", "B03", "B08")
    assert captured["channel_keys"] == ("B04", "B03", "B08")
    assert captured["normalize"] is False


def test_omnicloudmask_class_map_is_not_suppressed_by_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeModule:
        @staticmethod
        def predict_from_array(_array, **kwargs):
            if kwargs.get("export_confidence"):
                confidence = np.zeros((4, 4, 4), dtype=np.float32)
                confidence[1, :, :] = 0.10
                confidence[2, :, :] = 0.08
                confidence[3, 1:3, 1:3] = 0.12
                return confidence
            class_map = np.zeros((4, 4), dtype=np.uint8)
            class_map[:, :] = 1
            class_map[1:3, 1:3] = 3
            return class_map

    monkeypatch.setitem(sys.modules, "omnicloudmask", _FakeModule)

    sensor = mask_service_module.resolve_sensor_mask_spec(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    channels = {
        "B04": np.full((4, 4), 5000.0, dtype=np.float32),
        "B03": np.full((4, 4), 4500.0, dtype=np.float32),
        "B08": np.full((4, 4), 3500.0, dtype=np.float32),
    }

    with_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.95,
        backend="omnicloudmask",
        include_shadows=True,
    )
    without_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.95,
        backend="omnicloudmask",
        include_shadows=False,
    )

    assert int(with_shadows.mask[0, 0]) == 1
    assert int(with_shadows.mask[1, 1]) == 1
    assert int(without_shadows.mask[1, 1]) == 0
    assert with_shadows.summary["mask_source"] == "class_map"


def test_omnicloudmask_calls_model_once_when_confidence_export_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class _FakeModule:
        @staticmethod
        def predict_from_array(_array, **kwargs):
            calls.append(dict(kwargs))
            class_map = np.zeros((4, 4), dtype=np.uint8)
            class_map[:, :] = 1
            return class_map

    monkeypatch.delenv("NIMBUS_CLOUDMASK_EXPORT_CONFIDENCE", raising=False)
    monkeypatch.setitem(sys.modules, "omnicloudmask", _FakeModule)

    sensor = mask_service_module.resolve_sensor_mask_spec(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    channels = {
        "B04": np.full((4, 4), 5000.0, dtype=np.float32),
        "B03": np.full((4, 4), 4500.0, dtype=np.float32),
        "B08": np.full((4, 4), 3500.0, dtype=np.float32),
    }

    result = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="omnicloudmask",
        include_shadows=True,
    )

    assert len(calls) == 1
    assert "export_confidence" not in calls[0]
    assert result.summary["probability_source"] == "class_map"
