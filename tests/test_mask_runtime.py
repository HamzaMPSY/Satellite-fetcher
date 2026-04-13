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
from nimbuschain_mask_service.service import _cloud_tile_size, _effective_cloud_backend_request


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


def test_omnicloudmask_tile_size_defaults_are_stable(monkeypatch) -> None:
    monkeypatch.delenv("NIMBUS_CLOUDMASK_TILE_SIZE", raising=False)
    assert _cloud_tile_size(backend_name="omnicloudmask") == 1024
    assert _cloud_tile_size(backend_name="heuristic") == 1024


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
    assert captured["optimise_model"] is True
