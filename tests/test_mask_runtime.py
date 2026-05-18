from __future__ import annotations

import types
from pathlib import Path

from nimbuschain_fetch.models import JobMaskRequest
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_mask_service.contracts import MaskApplyRequest
from nimbuschain_mask_service import runtime
import nimbuschain_mask_service.service as mask_service_module
import nimbuschain_mask_service.omniwater as omniwater_module
from nimbuschain_mask_service.omniwater import _run_omniwater_model
from nimbuschain_mask_service.service import (
    _cloud_tile_size,
    _cloud_tile_sizing,
    _effective_cloud_backend_request,
    support_status,
)


def test_resolve_inference_device_prefers_explicit_value() -> None:
    assert runtime.resolve_inference_device(explicit="cpu", env_var="DOES_NOT_EXIST") == "cpu"


def test_resolve_inference_device_auto_detects_cuda(monkeypatch) -> None:
    fake_torch = types.SimpleNamespace(
        cuda=types.SimpleNamespace(is_available=lambda: True),
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: False)
        ),
    )
    monkeypatch.setattr(runtime.importlib, "import_module", lambda name: fake_torch)

    assert runtime.resolve_inference_device(explicit=None, env_var="DOES_NOT_EXIST") == "cuda"


def test_parallel_worker_count_uses_gpu_safe_default(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_TEST_TILE_WORKERS", raising=False)
    assert runtime.parallel_worker_count(
        device="cuda",
        env_var="NIMBUS_TEST_TILE_WORKERS",
        cpu_default=3,
        gpu_default=1,
    ) == 1
    assert runtime.parallel_worker_count(
        device="cpu",
        env_var="NIMBUS_TEST_TILE_WORKERS",
        cpu_default=3,
        gpu_default=1,
    ) == 3


def test_mask_failure_step_prefers_stage_specific_water_failure() -> None:
    assert NimbusFetcher._mask_failure_step_from_payloads(
        mask_types=["cloud", "water"],
        water_mask={"status": "failed", "reason": "disk full"},
        cloud_mask={"status": "written"},
    ) == "water_failed"


def test_pipeline_timeline_rebuild_detects_corrupted_multiple_active_steps() -> None:
    assert NimbusFetcher._pipeline_timeline_needs_rebuild(
        row={"pipeline_state": "running_water_inference"},
        pipeline_timeline={
            "current_stage": "convert",
            "steps": [
                {"key": "writing_chunks", "status": "running"},
                {"key": "running_water_inference", "status": "running"},
            ],
            "stages": [
                {"key": "convert", "status": "running"},
                {"key": "cloud", "status": "pending"},
                {"key": "water", "status": "running"},
            ],
        },
        mask_types=["cloud", "water"],
        cube_mode="none",
    )
    assert NimbusFetcher._mask_failure_step_from_items(
        mask_types=["cloud", "water"],
        items=[
            {
                "failed_step": "cloud_failed",
                "conversion_metadata": {},
            },
            {
                "conversion_metadata": {
                    "water_mask": {"status": "failed"},
                    "cloud_mask": {"status": "written"},
                }
            },
        ],
    ) == "water_failed"


def test_pipeline_timeline_rebuild_detects_missing_cube_stage_for_cube_building() -> None:
    assert NimbusFetcher._pipeline_timeline_needs_rebuild(
        row={"pipeline_state": "cube_building"},
        pipeline_timeline={
            "current_stage": "convert",
            "cube_mode": "after_mask",
            "steps": [
                {"key": "zarr_written", "status": "done"},
                {"key": "running_cloud_inference", "status": "done"},
                {"key": "running_water_inference", "status": "done"},
                {"key": "cube_building", "status": "running"},
            ],
            "stages": [
                {"key": "convert", "status": "running"},
                {"key": "cloud", "status": "done"},
                {"key": "water", "status": "done"},
                {"key": "ready", "status": "pending"},
            ],
        },
        mask_types=["cloud", "water"],
        cube_mode="after_mask",
    )


def test_pipeline_timeline_rebuild_detects_leaked_cube_stage_before_cube_starts() -> None:
    assert NimbusFetcher._pipeline_timeline_needs_rebuild(
        row={"pipeline_state": "zarr_converting"},
        pipeline_timeline={
            "current_stage": "convert",
            "cube_mode": "before_mask",
            "steps": [
                {"key": "writing_chunks", "status": "running"},
                {"key": "cube_written", "status": "done"},
            ],
            "stages": [
                {"key": "convert", "status": "running"},
                {"key": "cube", "status": "done"},
                {"key": "ready", "status": "pending"},
            ],
        },
        mask_types=[],
        cube_mode="before_mask",
    )


def test_omnicloudmask_tile_size_defaults_to_fixed_512(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_CLOUDMASK_TILE_SIZE", raising=False)
    s2_summary = {"shape": [1, 4, 10980, 10980], "pixel_size": [10.0, 10.0]}
    landsat_summary = {"shape": [1, 7, 23040, 23040], "pixel_size": [10.0, 10.0]}

    sentinel_cpu = _cloud_tile_sizing(
        backend_name="omnicloudmask",
        device="cpu",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        dataset_summary=s2_summary,
    )
    sentinel_mps = _cloud_tile_sizing(
        backend_name="omnicloudmask",
        device="mps",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        dataset_summary=s2_summary,
    )
    landsat_mps = _cloud_tile_sizing(
        backend_name="omnicloudmask",
        device="mps",
        provider="usgs",
        collection="LANDSAT_OT_C2_L2",
        product_type="L2SR",
        dataset_summary=landsat_summary,
    )

    assert sentinel_cpu["tile_size"] == 512
    assert sentinel_cpu["target_tiles_long_axis"] == 22
    assert sentinel_cpu["target_pixel_size_meters"] == 10.0
    assert sentinel_cpu["source"] == "fixed_default"

    assert sentinel_mps["tile_size"] == 512
    assert sentinel_mps["target_tiles_long_axis"] == 22
    assert sentinel_mps["estimated_tiles_long_axis"] == 22

    assert landsat_mps["tile_size"] == 512
    assert landsat_mps["collection_family"] == "landsat-8-9"
    assert _cloud_tile_size(backend_name="omnicloudmask", device="cpu") == 512


def test_water_tile_size_defaults_to_fixed_512(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_WATERMASK_TILE_SIZE", raising=False)
    summary = {"shape": [1, 6, 10980, 10980], "pixel_size": [10.0, 10.0]}

    decision = omniwater_module._watermask_tile_sizing(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        dataset_summary=summary,
        device="mps",
        model_patch_size=512,
    )

    assert decision["tile_size"] == 512
    assert decision["target_tiles_long_axis"] == 22
    assert decision["snap_multiple"] == 512
    assert decision["target_pixel_size_meters"] == 10.0
    assert decision["source"] == "fixed_default"


def test_explicit_tile_size_override_still_wins(monkeypatch) -> None:
    monkeypatch.setenv("NIMBUS_CLOUDMASK_TILE_SIZE", "1400")
    monkeypatch.setenv("NIMBUS_WATERMASK_TILE_SIZE", "3000")

    cloud = _cloud_tile_sizing(
        backend_name="omnicloudmask",
        device="mps",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        dataset_summary={"shape": [1, 4, 10980, 10980], "pixel_size": [10.0, 10.0]},
    )
    water = omniwater_module._watermask_tile_sizing(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        dataset_summary={"shape": [1, 6, 10980, 10980], "pixel_size": [10.0, 10.0]},
        device="mps",
        model_patch_size=512,
    )

    assert cloud["source"] == "env_override"
    assert cloud["tile_size"] == 1400
    assert water["source"] == "env_override"
    assert water["tile_size"] == 3000


def test_support_status_exposes_tile_sizing_policy(monkeypatch) -> None:
    monkeypatch.setenv("NIMBUS_CLOUDMASK_TILE_SIZE", "1536")
    monkeypatch.delenv("NIMBUS_WATERMASK_TILE_SIZE", raising=False)

    status = support_status()

    assert status["tile_sizing"]["cloud"]["env_var"] == "NIMBUS_CLOUDMASK_TILE_SIZE"
    assert status["tile_sizing"]["cloud"]["env_override"] == "1536"
    assert status["tile_sizing"]["cloud"]["default_tile_size"] == 512
    assert status["tile_sizing"]["cloud"]["mode"] == "fixed_default"
    assert status["tile_sizing"]["water"]["patch_quantum"] == 512


def test_auto_cloud_backend_always_uses_omnicloudmask(monkeypatch) -> None:
    monkeypatch.setattr(mask_service_module, "resolve_inference_device", lambda **kwargs: "cpu")
    assert _effective_cloud_backend_request(backend="auto", inference_device=None) == "omnicloudmask"


def test_legacy_heuristic_cloud_backend_is_remapped_in_public_requests() -> None:
    assert JobMaskRequest(backend="heuristic").backend == "omnicloudmask"
    request = MaskApplyRequest(
        source_zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="scene",
        mask_types=["cloud"],
        backend="heuristic",
    )
    assert request.cloud.backend == "omnicloudmask"


def test_explicit_omnicloudmask_backend_is_preserved_on_cpu(monkeypatch) -> None:
    monkeypatch.setattr(mask_service_module, "resolve_inference_device", lambda **kwargs: "cpu")
    assert _effective_cloud_backend_request(backend="omnicloudmask", inference_device=None) == "omnicloudmask"


def test_runtime_device_status_reports_requested_and_resolved_device(monkeypatch) -> None:
    monkeypatch.setenv("NIMBUS_TEST_DEVICE", "mps")
    monkeypatch.setattr(runtime, "_available_devices", lambda: {"cpu": True, "cuda": False, "mps": False})

    status = runtime.runtime_device_status(explicit=None, env_var="NIMBUS_TEST_DEVICE")

    assert status["env"] == "mps"
    assert status["resolved"] == "cpu"
    assert status["available"] == {"cpu": True, "cuda": False, "mps": False}


def test_integrated_mask_workers_only_parallelize_with_accelerator(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_MASK_SCENE_MAX_WORKERS", raising=False)
    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.os.cpu_count", lambda: 8)
    monkeypatch.setattr(
        "nimbuschain_fetch.engine.nimbus_fetcher.resolve_inference_device",
        lambda **kwargs: "cpu",
    )
    assert NimbusFetcher._integrated_mask_max_workers(
        total=5,
        inference_device=None,
        water_inference_device=None,
    ) == 1
    assert NimbusFetcher._integrated_mask_max_workers(
        total=5,
        inference_device=None,
        water_inference_device=None,
        preferred_parallelism=3,
    ) == 2
    assert NimbusFetcher._integrated_mask_max_workers(
        total=5,
        inference_device=None,
        water_inference_device=None,
        preferred_parallelism=3,
        remote_runtime={},
    ) == 1

    monkeypatch.setattr(
        "nimbuschain_fetch.engine.nimbus_fetcher.resolve_inference_device",
        lambda **kwargs: "mps",
    )
    assert NimbusFetcher._integrated_mask_max_workers(
        total=5,
        inference_device=None,
        water_inference_device=None,
    ) == 2


def test_integrated_mask_workers_use_remote_runtime_when_external_service_has_mps(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_MASK_SCENE_MAX_WORKERS", raising=False)
    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.os.cpu_count", lambda: 8)
    monkeypatch.setattr(
        "nimbuschain_fetch.engine.nimbus_fetcher.resolve_inference_device",
        lambda **kwargs: "cpu",
    )

    assert NimbusFetcher._integrated_mask_max_workers(
        total=4,
        inference_device=None,
        water_inference_device=None,
        remote_runtime={
            "cloud": {"resolved": "mps"},
            "water": {"resolved": "mps"},
        },
    ) == 2


def test_omniwater_model_enables_osm_priors_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_make_water_mask(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}

    monkeypatch.delenv("NIMBUS_WATERMASK_USE_OSM_WATER", raising=False)
    monkeypatch.delenv("NIMBUS_WATERMASK_USE_OSM_BUILDING", raising=False)
    monkeypatch.delenv("NIMBUS_WATERMASK_USE_OSM_ROADS", raising=False)
    monkeypatch.setenv("NIMBUS_WATERMASK_BATCH_SIZE", "2")
    monkeypatch.setattr(omniwater_module, "resolve_inference_device", lambda **kwargs: "mps")

    _run_omniwater_model(
        make_water_mask=fake_make_water_mask,
        scene_dir=Path("/tmp/scene"),
        scene_paths=[Path("/tmp/scene/tile.tif")],
        output_dir=Path("/tmp/out"),
        cache_dir=Path("/tmp/cache"),
        tile_size=512,
        inference_device=None,
    )

    assert captured["use_osm_water"] is True
    assert captured["use_osm_building"] is True
    assert captured["use_osm_roads"] is True
    assert captured["batch_size"] == 1
    assert captured["optimise_model"] is False


def test_omniwater_model_retries_with_compact_mps_profile(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def flaky_make_water_mask(**kwargs):
        calls.append(dict(kwargs))
        if len(calls) == 1:
            raise RuntimeError(
                "Sizes of tensors must match except in dimension 1. Expected size 2 but got size 1."
            )
        return {"status": "ok"}

    monkeypatch.delenv("NIMBUS_WATERMASK_BATCH_SIZE", raising=False)
    monkeypatch.delenv("NIMBUS_WATERMASK_MPS_SAFE_MODE", raising=False)
    monkeypatch.setenv("NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE", "512")
    monkeypatch.setenv("NIMBUS_WATERMASK_INFERENCE_OVERLAP_SIZE", "128")
    monkeypatch.setattr(omniwater_module, "resolve_inference_device", lambda **kwargs: "mps")

    result, runtime = _run_omniwater_model(
        make_water_mask=flaky_make_water_mask,
        scene_dir=Path("/tmp/scene"),
        scene_paths=[Path("/tmp/scene/tile.tif")],
        output_dir=Path("/tmp/out"),
        cache_dir=Path("/tmp/cache"),
        tile_size=1024,
        inference_device=None,
    )

    assert result == {"status": "ok"}
    assert runtime["attempt_count"] == 2
    assert calls[0]["batch_size"] == 1
    assert calls[0]["optimise_model"] is False
    assert calls[1]["batch_size"] == 1
    assert calls[1]["optimise_model"] is False
    assert calls[1]["inference_patch_size"] == 256
    assert calls[1]["inference_overlap_size"] == 64
