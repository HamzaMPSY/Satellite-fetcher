from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("zarr")

import zarr

from nimbuschain_mask_service.inference import CloudMaskResult, run_cloud_inference
from nimbuschain_mask_service.sensor_mapping import resolve_sensor_mask_spec
from nimbuschain_mask_service.service import MaskService


def _write_sentinel_scene(root: Path) -> Path:
    source = root / "sentinel_scene.zarr"
    group = zarr.open_group(str(source), mode="w", zarr_format=2)
    band_names = ["B02", "B03", "B04", "B08", "B11", "B12"]
    imagery = group.create_array(
        "imagery",
        shape=(1, len(band_names), 4, 4),
        chunks=(1, len(band_names), 4, 4),
        dtype=np.uint16,
        overwrite=True,
    )
    for band_name in band_names:
        imagery[0, band_names.index(band_name), :, :] = 1000
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
            "source_uri": "/downloads/raw/sentinel_scene.SAFE.zip",
            "product_type": "S2MSI2A",
            "band_names": band_names,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, len(band_names), 4, 4],
            "data_family": "optical",
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        }
    )
    zarr.consolidate_metadata(str(source))
    return source


def _write_landsat_l1_scene(root: Path) -> Path:
    source = root / "landsat_l1_scene.zarr"
    group = zarr.open_group(str(source), mode="w", zarr_format=2)
    band_names = ["B2", "B3", "B4", "B5", "B6", "B7"]
    imagery = group.create_array(
        "imagery",
        shape=(1, len(band_names), 4, 4),
        chunks=(1, len(band_names), 4, 4),
        dtype=np.uint16,
        overwrite=True,
    )
    for band_name in band_names:
        imagery[0, band_names.index(band_name), :, :] = 20000
    group.create_array(
        "band",
        shape=(len(band_names),),
        chunks=(len(band_names),),
        dtype="U2",
        overwrite=True,
    )
    group["band"][:] = np.array(band_names, dtype="U2")
    group.attrs.update(
        {
            "provider": "usgs",
            "collection": "landsat_ot_c2_l1",
            "scene_id": "LC09_L1TP_199023_20260325_20260326_02_T1",
            "source_uri": "/downloads/raw/landsat_l1_scene.tar",
            "product_type": "L1TP",
            "band_names": band_names,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, len(band_names), 4, 4],
            "data_family": "optical",
            "crs": "EPSG:32631",
            "transform": [30.0, 0.0, 399960.0, 0.0, -30.0, 5300040.0],
            "radiometric_metadata": {
                "sun_elevation": 45.0,
                "bands": {
                    f"B{index}": {
                        "mult": 2.0e-5,
                        "add": -0.1,
                        "apply_sun_elevation": True,
                    }
                    for index in range(1, 10)
                },
            },
        }
    )
    zarr.consolidate_metadata(str(source))
    return source


def test_omnicloudmask_uses_class_map_and_does_not_suppress_shadow_pixels_by_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeModule:
        @staticmethod
        def predict_from_array(_array, **kwargs):
            if kwargs.get("export_confidence"):
                confidence = np.zeros((4, 4, 4), dtype=np.float32)
                confidence[1, 0:2, 0:2] = 0.95
                confidence[2, 2:4, 0:2] = 0.80
                confidence[3, 1:3, 2:4] = 0.10
                return confidence
            classes = np.zeros((4, 4), dtype=np.uint8)
            classes[0:2, 0:2] = 1
            classes[2:4, 0:2] = 2
            classes[1:3, 2:4] = 3
            return classes

    monkeypatch.setitem(sys.modules, "omnicloudmask", _FakeModule)
    sensor = resolve_sensor_mask_spec(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    channels = {
        "B04": np.full((4, 4), 0.9, dtype=np.float32),
        "B03": np.full((4, 4), 0.8, dtype=np.float32),
        "B08": np.full((4, 4), 0.2, dtype=np.float32),
    }

    with_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.99,
        backend="omnicloudmask",
        include_shadows=True,
    )
    without_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.99,
        backend="omnicloudmask",
        include_shadows=False,
    )

    assert with_shadows.summary["mask_source"] == "class_map"
    assert with_shadows.summary["probability_source"] == "class_map"
    assert with_shadows.summary["confidence_available"] is False
    assert with_shadows.summary["requested_threshold"] == pytest.approx(0.99)
    assert int(with_shadows.mask[0, 0]) == 1
    assert int(with_shadows.mask[2, 0]) == 1
    assert int(with_shadows.mask[1, 3]) == 1
    assert int(without_shadows.mask[1, 3]) == 0
    assert float(with_shadows.probability[1, 3]) == 1.0
    assert float(without_shadows.probability[1, 3]) == 0.0


def test_omnicloudmask_service_reads_rgbnir_only_without_heuristic_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_sentinel_scene(tmp_path)
    output = tmp_path / "masked.zarr"
    captured: dict[str, object] = {}

    def _fake_read_required_channels_window(
        root,
        *,
        band_names,
        required_bands,
        scale_hint,
        row_start,
        row_stop,
        col_start,
        col_stop,
        normalize=True,
        time_index=0,
    ):
        captured["required_bands"] = tuple(required_bands)
        captured["normalize"] = bool(normalize)
        channels = {
            band: np.full((row_stop - row_start, col_stop - col_start), 1000, dtype=np.float32)
            for band in required_bands
        }
        return channels, []

    def _fake_run_cloud_inference(*, sensor, channels, threshold, backend, inference_device=None, include_shadows=True):
        captured["cloud_channels"] = tuple(channels.keys())
        captured["backend"] = backend
        return CloudMaskResult(
            probability=np.ones((4, 4), dtype=np.float32),
            mask=np.ones((4, 4), dtype=np.uint8),
            summary={
                "backend": backend,
                "sensor": sensor.sensor_key,
                "includes_shadows": include_shadows,
                "shadow_fraction": 0.25,
                "cloud_only_fraction": 0.75,
                "cloud_fraction": 1.0,
            },
        )

    monkeypatch.setattr("nimbuschain_mask_service.service.read_required_channels_window", _fake_read_required_channels_window)
    monkeypatch.setattr("nimbuschain_mask_service.service.run_cloud_inference", _fake_run_cloud_inference)

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-cloud-zip-aligned",
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
        threshold=0.99,
        include_shadows=True,
    )

    assert result["status"] == "written"
    assert captured["required_bands"] == ("B04", "B03", "B08")
    assert captured["normalize"] is False
    assert captured["cloud_channels"] == ("B04", "B03", "B08")


def test_heuristic_cloud_mask_service_keeps_wider_band_recipe_and_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_sentinel_scene(tmp_path)
    output = tmp_path / "masked-heuristic.zarr"
    captured: dict[str, object] = {}

    def _fake_read_required_channels_window(
        root,
        *,
        band_names,
        required_bands,
        scale_hint,
        row_start,
        row_stop,
        col_start,
        col_stop,
        normalize=True,
        time_index=0,
    ):
        captured["required_bands"] = tuple(required_bands)
        captured["normalize"] = bool(normalize)
        channels = {
            band: np.full((row_stop - row_start, col_stop - col_start), 1000, dtype=np.float32)
            for band in required_bands
        }
        return channels, []

    def _fake_run_cloud_inference(*, sensor, channels, threshold, backend, inference_device=None, include_shadows=True):
        captured["cloud_channels"] = tuple(channels.keys())
        captured["backend"] = backend
        return CloudMaskResult(
            probability=np.ones((4, 4), dtype=np.float32),
            mask=np.ones((4, 4), dtype=np.uint8),
            summary={
                "backend": backend,
                "sensor": sensor.sensor_key,
                "includes_shadows": include_shadows,
            },
        )

    monkeypatch.setattr("nimbuschain_mask_service.service.read_required_channels_window", _fake_read_required_channels_window)
    monkeypatch.setattr("nimbuschain_mask_service.service.run_cloud_inference", _fake_run_cloud_inference)

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
        include_shadows=True,
    )

    assert result["status"] == "written"
    assert captured["required_bands"] == ("B02", "B03", "B04", "B08", "B11", "B12")
    assert captured["normalize"] is True
    assert captured["cloud_channels"] == ("B02", "B03", "B04", "B08", "B11", "B12")


def test_landsat_omnicloudmask_service_normalizes_inputs_before_model_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_landsat_l1_scene(tmp_path)
    output = tmp_path / "landsat-masked.zarr"
    captured: dict[str, object] = {}

    def _fake_read_required_channels_window(
        root,
        *,
        band_names,
        required_bands,
        scale_hint,
        row_start,
        row_stop,
        col_start,
        col_stop,
        normalize=True,
        time_index=0,
    ):
        del root, band_names, scale_hint, time_index
        captured["required_bands"] = tuple(required_bands)
        captured["normalize"] = bool(normalize)
        channels = {
            band: np.full((row_stop - row_start, col_stop - col_start), 20000, dtype=np.float32)
            for band in required_bands
        }
        return channels, []

    def _fake_run_cloud_inference(*, sensor, channels, threshold, backend, inference_device=None, include_shadows=True):
        del threshold, inference_device, include_shadows
        captured["backend"] = backend
        captured["cloud_channels"] = tuple(channels.keys())
        sample = next(iter(channels.values()))
        return CloudMaskResult(
            probability=np.ones(sample.shape, dtype=np.float32),
            mask=np.ones(sample.shape, dtype=np.uint8),
            summary={
                "backend": backend,
                "sensor": sensor.sensor_key,
                "cloud_fraction": 1.0,
                "cloud_only_fraction": 1.0,
                "shadow_fraction": 0.0,
                "includes_shadows": True,
            },
        )

    monkeypatch.setattr("nimbuschain_mask_service.service.read_required_channels_window", _fake_read_required_channels_window)
    monkeypatch.setattr("nimbuschain_mask_service.service.run_cloud_inference", _fake_run_cloud_inference)

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-landsat-cloud-zip-aligned",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        scene_id="LC09_L1TP_199023_20260325_20260326_02_T1",
        acquisition_datetime="2026-03-25T10:39:33Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [30.0, 0.0, 399960.0, 0.0, -30.0, 5300040.0],
        },
        backend="omnicloudmask",
        threshold=0.62,
    )

    assert result["status"] == "written"
    assert captured["required_bands"] == ("B4", "B3", "B5")
    assert captured["normalize"] is True
    assert captured["cloud_channels"] == ("B4", "B3", "B5")
