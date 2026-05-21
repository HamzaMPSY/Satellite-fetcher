from __future__ import annotations

import importlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient
from nimbuschain_sen2like_service.main import create_app
from nimbuschain_sen2like_service.models import Sen2LikeNormalizeRequest
from nimbuschain_sen2like_service.runner import build_command, readiness_payload, run_sen2like


_REQUIRED_TEST_BANDS = ("B02", "B03", "B04", "B08", "B11", "B12")


def _load_packaging_step_module():
    vendor_root = (
        Path(__file__).resolve().parents[1]
        / "sen2like-service"
        / "vendor"
        / "Satellite-fetcher-feature-sen2like_reimplementation"
    )
    if str(vendor_root) not in sys.path:
        sys.path.insert(0, str(vendor_root))
    from Packaging import PackagingStep

    return PackagingStep


def _load_pipeline_module(monkeypatch, tmp_path: Path):
    vendor_root = (
        Path(__file__).resolve().parents[1]
        / "sen2like-service"
        / "vendor"
        / "Satellite-fetcher-feature-sen2like_reimplementation"
    )
    if str(vendor_root) not in sys.path:
        sys.path.insert(0, str(vendor_root))
    module = importlib.import_module("Pipeline")
    monkeypatch.setattr(module, "_BASE", tmp_path, raising=False)
    return module


def _write_valid_sen2like_output(work_dir: Path, scene_name: str) -> Path:
    scene_dir = work_dir / scene_name
    safe_dir = (
        scene_dir
        / "SAFE"
        / "S2L_MSIL2F_20260101T105501_N0500_R000_T31UDQ_20260101T105501.SAFE"
    )
    _write_complete_safe(safe_dir, timestamp="20260101T105501")
    (scene_dir / "manifest.json").write_text(
        json.dumps({"steps": {"packaging": {"status": "success"}}}),
        encoding="utf-8",
    )
    return safe_dir


def _write_complete_safe(safe_dir: Path, *, timestamp: str) -> None:
    granule_dir = safe_dir / "GRANULE" / f"L2F_T31UDQ_{timestamp}_N0500_R000"
    img_dir = granule_dir / "IMG_DATA" / "RESOLUTION_10M"
    img_dir.mkdir(parents=True)
    (safe_dir / "manifest.safe").write_text("SAFE", encoding="utf-8")
    (safe_dir / "MTD_MSIL2F.xml").write_text("<xml />", encoding="utf-8")
    (granule_dir / "MTD_TL.xml").write_text("<xml />", encoding="utf-8")
    for band in _REQUIRED_TEST_BANDS:
        (img_dir / f"T31UDQ_{timestamp}_{band}_10m.TIF").write_bytes(b"tif")


def test_sen2like_request_defaults_to_parallel_workers() -> None:
    assert Sen2LikeNormalizeRequest().workers == 4


def test_sen2like_command_preserves_pipeline_entrypoint(tmp_path: Path) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    working_dir = tmp_path / "work"
    request = Sen2LikeNormalizeRequest(
        products=["/data/raw/LC08_SCENE"],
        working_dir=str(working_dir),
        workers=6,
        steps=["geometric_processing", "packaging"],
        no_resume=True,
        router_fallback_ok=True,
    )

    command = build_command(request, vendor_root=vendor_root, working_dir=working_dir)

    assert command[1] == str(vendor_root / "Pipeline.py")
    assert "/data/raw/LC08_SCENE" in command
    assert command[command.index("--working-dir") + 1] == str(working_dir)
    assert command[command.index("--workers") + 1] == "6"
    assert command[command.index("--base-dir") + 1] == str(vendor_root)
    assert "--no-resume" in command
    assert "--router-fallback-ok" in command


def test_sen2like_runner_reports_outputs_and_duration(tmp_path: Path, monkeypatch) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    scene_dir = work_dir / "LC08_SCENE"
    safe_dir = _write_valid_sen2like_output(work_dir, "LC08_SCENE")

    def fake_vendor_root() -> Path:
        return vendor_root

    captured_env = {}

    def fake_run(*args, **kwargs):
        captured_env.update(dict(kwargs.get("env") or {}))
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setenv("SPARK_LOCAL_DIRS", "/data/downloads/sen2like/spark")
    monkeypatch.delenv("NIMBUS_SEN2LIKE_SPARK_DIR", raising=False)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE"],
            working_dir=str(work_dir),
            direct_zarr=False,
        )
    )

    assert response.status == "succeeded"
    assert response.outputs[0].manifest_path == str(scene_dir / "manifest.json")
    assert response.outputs[0].normalized_uri == str(safe_dir)
    assert response.duration_seconds >= 0
    assert captured_env["SPARK_LOCAL_DIRS"] == "/tmp/nimbus-sen2like-spark"
    assert response.metadata["spark_local_dirs"] == "/tmp/nimbus-sen2like-spark"
    assert response.metadata["execution_mode"] == "single_product_parallel_steps"
    assert response.metadata["product_parallelism"] is False
    assert response.metadata["tile_parallelism"] is True


def test_sen2like_runner_can_write_direct_zarr_output(tmp_path: Path, monkeypatch) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    safe_dir = _write_valid_sen2like_output(work_dir, "LC08_SCENE")
    zarr_dir = tmp_path / "zarr"
    calls: list[dict[str, object]] = []

    class FakeZarrConversionService:
        def convert(self, **kwargs):
            calls.append(dict(kwargs))
            output_uri = str(kwargs["output_uri"])
            return (
                output_uri,
                "optical",
                {"status": "written", "scene_id": kwargs["scene_id"]},
                {
                    "dimensions": ["time", "band", "y", "x"],
                    "shape": [1, 6, 512, 512],
                    "band_names": ["B02", "B03", "B04", "B08", "B11", "B12"],
                },
            )

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", lambda: vendor_root)
    import nimbuschain_zarr_service.service as zarr_service_module

    monkeypatch.setattr(
        zarr_service_module,
        "ZarrConversionService",
        FakeZarrConversionService,
    )
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE"],
            working_dir=str(work_dir),
            direct_zarr=True,
            zarr_output_dir=str(zarr_dir),
        )
    )

    assert response.status == "succeeded"
    assert response.outputs[0].normalized_uri == str(safe_dir)
    assert response.outputs[0].zarr_uri == str(zarr_dir / f"{safe_dir.name[:-5]}.zarr")
    assert response.outputs[0].zarr_exists is True
    assert response.metadata["direct_zarr_status"] == "written"
    assert response.metadata["direct_zarr_outputs"] == [response.outputs[0].zarr_uri]
    assert calls[0]["provider"] == "copernicus"
    assert calls[0]["collection"] == "SENTINEL-2"
    assert calls[0]["product_type"] == "S2MSI2A"
    assert calls[0]["raw_uri"] == str(safe_dir)


def test_sen2like_runner_reports_parallel_multi_product_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    captured_command = []

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        captured_command.extend(args[0])
        _write_valid_sen2like_output(work_dir, "LC08_SCENE_A")
        _write_valid_sen2like_output(work_dir, "LC09_SCENE_B")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE_A", "/data/raw/LC09_SCENE_B"],
            working_dir=str(work_dir),
            workers=4,
        )
    )

    assert response.status == "succeeded"
    assert "/data/raw/LC08_SCENE_A" in captured_command
    assert "/data/raw/LC09_SCENE_B" in captured_command
    assert captured_command[captured_command.index("--workers") + 1] == "4"
    assert response.metadata["execution_mode"] == "parallel_multi_product"
    assert response.metadata["product_count"] == 2
    assert response.metadata["workers"] == 4
    assert response.metadata["product_parallelism"] is True
    assert response.metadata["tile_parallelism"] is True
    assert response.metadata["band_parallelism"] is True
    assert response.metadata["nested_band_parallelism"] is True
    assert response.metadata["safe_retry_enabled"] is True
    assert response.metadata["subprocess_attempts"][0]["mode"] == "parallel_products_and_bands"


def test_sen2like_runner_retries_without_nested_band_parallelism_on_resource_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    captured_envs: list[dict[str, str]] = []

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        captured_envs.append(dict(kwargs.get("env") or {}))
        if len(captured_envs) == 1:
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=137,
                stdout="",
                stderr="killed by memory pressure",
            )
        _write_valid_sen2like_output(work_dir, "LC08_SCENE_A")
        _write_valid_sen2like_output(work_dir, "LC09_SCENE_B")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM", "true")
    monkeypatch.setenv("NIMBUS_SEN2LIKE_SAFE_RETRY", "true")

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE_A", "/data/raw/LC09_SCENE_B"],
            working_dir=str(work_dir),
            workers=4,
        )
    )

    assert response.status == "succeeded"
    assert captured_envs[0]["NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"] == "true"
    assert captured_envs[1]["NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM"] == "false"
    assert [item["mode"] for item in response.metadata["subprocess_attempts"]] == [
        "parallel_products_and_bands",
        "safe_product_parallel_only",
    ]


def test_sen2like_runner_extracts_tar_products_before_invoking_pipeline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    product_file = raw_dir / "LC08_SCENE_MTL.txt"
    product_file.write_text("LANDSAT_PRODUCT_ID = LC08_SCENE", encoding="utf-8")
    archive_path = tmp_path / "LC08_SCENE.tar"
    with tarfile.open(archive_path, "w") as archive:
        archive.add(product_file, arcname=product_file.name)
    work_dir = tmp_path / "work"
    captured_command = []

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        captured_command.extend(args[0])
        _write_valid_sen2like_output(work_dir, "LC08_SCENE")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=[str(archive_path)],
            working_dir=str(work_dir),
        )
    )

    extracted_input = work_dir / "_inputs" / "LC08_SCENE"
    assert str(extracted_input) in captured_command
    assert (extracted_input / product_file.name).exists()
    assert response.status == "succeeded"
    assert response.products == [str(archive_path)]
    assert response.outputs[0].output_dir == str(work_dir / "LC08_SCENE")
    assert response.metadata["prepared_products"][0]["extracted"] is True


def test_sen2like_runner_preprocesses_extracted_tar_rasters_to_target_shape(
    tmp_path: Path,
    monkeypatch,
) -> None:
    np = pytest.importorskip("numpy")
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin

    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    raster_path = raw_dir / "LC08_SCENE_B2.TIF"
    with rasterio.open(
        raster_path,
        "w",
        driver="GTiff",
        height=64,
        width=96,
        count=1,
        dtype="uint16",
        crs="EPSG:32631",
        transform=from_origin(500000, 5400000, 30, 30),
    ) as dst:
        dst.write(np.arange(64 * 96, dtype=np.uint16).reshape(64, 96), 1)
    mtl_path = raw_dir / "LC08_SCENE_MTL.txt"
    mtl_path.write_text(
        "\n".join(
            [
                "LANDSAT_PRODUCT_ID = LC08_SCENE",
                "REFLECTIVE_LINES = 64",
                "REFLECTIVE_SAMPLES = 96",
            ]
        ),
        encoding="utf-8",
    )
    archive_path = tmp_path / "LC08_SCENE.tar"
    with tarfile.open(archive_path, "w") as archive:
        archive.add(raster_path, arcname=raster_path.name)
        archive.add(mtl_path, arcname=mtl_path.name)
    work_dir = tmp_path / "work"

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        _write_valid_sen2like_output(work_dir, "LC08_SCENE")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=[str(archive_path)],
            working_dir=str(work_dir),
            preprocess_target_shape="16x16",
        )
    )

    extracted_input = work_dir / "_inputs" / "LC08_SCENE"
    with rasterio.open(extracted_input / raster_path.name) as src:
        assert (src.height, src.width) == (16, 16)
    assert "REFLECTIVE_LINES = 16" in (extracted_input / mtl_path.name).read_text()
    assert "REFLECTIVE_SAMPLES = 16" in (extracted_input / mtl_path.name).read_text()
    preprocess = response.metadata["prepared_products"][0]["preprocess"]
    assert preprocess["applied"] is True
    assert preprocess["target_shape"] == [16, 16]
    assert preprocess["files_resampled"] == 1


def test_sen2like_runner_rejects_stale_safe_with_wrong_acquisition_time(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    input_dir = tmp_path / "raw" / "LC09_SCENE"
    input_dir.mkdir(parents=True)
    (input_dir / "LC09_L1TP_199026_20260410_20260410_02_T1_MTL.txt").write_text(
        "\n".join(
            [
                'LANDSAT_PRODUCT_ID = "LC09_L1TP_199026_20260410_20260410_02_T1"',
                "DATE_ACQUIRED = 2026-04-10",
                'SCENE_CENTER_TIME = "10:40:36.9176670Z"',
            ]
        ),
        encoding="utf-8",
    )
    work_dir = tmp_path / "work"
    stale_safe = (
        work_dir
        / "LC09_SCENE"
        / "SAFE"
        / "S2L_MSIL2F_20260518T000000_N0500_R000_T31UDQ_20260518T090903.SAFE"
    )
    _write_complete_safe(stale_safe, timestamp="20260518T000000")

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=[str(input_dir)],
            working_dir=str(work_dir),
        )
    )

    assert response.status == "failed"
    assert response.outputs[0].normalized_uri is None
    issue_codes = {issue["code"] for issue in response.metadata["output_issues"]}
    assert "normalized_output_missing" in issue_codes


def test_sen2like_runner_fails_before_pyspark_when_tar_input_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    missing_archive = tmp_path / "LC08_MISSING.tar"
    subprocess_called = False

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*_args, **_kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=[str(missing_archive)],
            working_dir=str(work_dir),
        )
    )

    assert subprocess_called is False
    assert response.status == "failed"
    assert response.return_code == -1
    assert response.command == []
    assert response.outputs[0].output_dir == str(work_dir / "LC08_MISSING")
    assert response.metadata["tar_inputs_supported"] is True
    assert response.metadata["prepared_products"][0]["extracted"] is False
    assert response.metadata["input_issues"][0]["code"] == "input_missing"
    assert "Landsat tar input is missing" in response.metadata["input_issues"][0]["message"]


def test_sen2like_geometric_gri_lookup_does_not_fetch_when_disabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pipeline = _load_pipeline_module(monkeypatch, tmp_path)
    fake_gri_fetch = ModuleType("Geometric_Processing.gri_fetch")
    fake_gri_fetch.derive_mgrs_tile = lambda _scene: "T31UDQ"

    def fail_fetch(*_args, **_kwargs):
        raise AssertionError("get_or_fetch_gri must not run when auto-fetch is disabled")

    fake_gri_fetch.get_or_fetch_gri = fail_fetch
    monkeypatch.setitem(sys.modules, "Geometric_Processing.gri_fetch", fake_gri_fetch)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_GRI_AUTO_FETCH", "false")

    step = pipeline.GeometricProcessingStep({})
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={
            "landsat_path": str(tmp_path / "LC09_SCENE"),
            "gri_cache_dir": str(tmp_path / "gri-cache"),
            "gri_path": str(tmp_path / "gri-cache" / "GRI_T31UDQ.tif"),
        },
    )

    try:
        step._resolve_gri_path(ctx)
    except FileNotFoundError as exc:
        assert "auto-fetch is disabled" in str(exc)
        assert "T31UDQ" in str(exc)
    else:
        raise AssertionError("Expected missing local GRI to fail via FileNotFoundError.")


def test_sen2like_geometric_gri_lookup_rejects_unusable_cached_gri(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pipeline = _load_pipeline_module(monkeypatch, tmp_path)
    fake_gri_fetch = ModuleType("Geometric_Processing.gri_fetch")
    fake_gri_fetch.derive_mgrs_tile = lambda _scene: "T31UDQ"

    def fail_fetch(*_args, **_kwargs):
        raise AssertionError("get_or_fetch_gri must not run for an unusable cached GRI")

    fake_gri_fetch.get_or_fetch_gri = fail_fetch
    monkeypatch.setitem(sys.modules, "Geometric_Processing.gri_fetch", fake_gri_fetch)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_GRI_AUTO_FETCH", "false")

    cache_dir = tmp_path / "gri-cache"
    cache_dir.mkdir()
    cached_gri = cache_dir / "GRI_T31UDQ.tif"
    cached_gri.write_text("not a geotiff", encoding="utf-8")

    step = pipeline.GeometricProcessingStep({})
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={
            "landsat_path": str(tmp_path / "LC09_SCENE"),
            "gri_cache_dir": str(cache_dir),
            "gri_path": str(cached_gri),
        },
    )

    try:
        step._resolve_gri_path(ctx)
    except FileNotFoundError as exc:
        assert "auto-fetch is disabled" in str(exc)
        assert "not usable" in str(exc)
    else:
        raise AssertionError("Expected unusable local GRI to fail via FileNotFoundError.")


def test_sen2like_client_summarizes_sigkill_as_memory_pressure() -> None:
    message = Sen2LikeServiceClient._summarize_structured_error(
        {
            "return_code": -9,
            "stderr_tail": "Reprojection -> EPSG:32631 @ 10.0 m",
            "metadata": {
                "output_issues": [
                    {
                        "code": "normalized_output_missing",
                        "message": "Sen2Like did not produce a valid Sentinel-like SAFE output.",
                    }
                ]
            },
        }
    )

    assert "not have enough memory" in message


def test_sen2like_readiness_requires_sixs_executable(tmp_path: Path, monkeypatch) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    pipeline = vendor_root / "Pipeline.py"
    pipeline.write_text("print('upstream')", encoding="utf-8")

    monkeypatch.setenv("NIMBUS_SEN2LIKE_VENDOR_DIR", str(vendor_root))
    monkeypatch.setattr("nimbuschain_sen2like_service.runner.shutil.which", lambda _name: None)

    payload = readiness_payload()

    assert payload["status"] == "unavailable"
    assert payload["pipeline_py_exists"] is True
    assert payload["sixs_executable"] is None
    assert payload["sixs_executable_exists"] is False


def test_sen2like_readiness_reports_sixs_path(tmp_path: Path, monkeypatch) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    pipeline = vendor_root / "Pipeline.py"
    pipeline.write_text("print('upstream')", encoding="utf-8")

    monkeypatch.setenv("NIMBUS_SEN2LIKE_VENDOR_DIR", str(vendor_root))
    monkeypatch.setattr(
        "nimbuschain_sen2like_service.runner.shutil.which",
        lambda name: "/usr/local/bin/sixsV1.1" if name == "sixs" else None,
    )

    payload = readiness_payload()

    assert payload["status"] == "ok"
    assert payload["pipeline_py_exists"] is True
    assert payload["sixs_executable"] == "/usr/local/bin/sixsV1.1"
    assert payload["sixs_executable_exists"] is True


def test_sen2like_readiness_endpoint_fails_when_sixs_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        "nimbuschain_sen2like_service.main.readiness_payload",
        lambda: {
            "status": "unavailable",
            "pipeline_py_exists": True,
            "sixs_executable_exists": False,
        },
    )

    response = TestClient(create_app()).get("/readiness")

    assert response.status_code == 503
    assert response.json()["detail"]["sixs_executable_exists"] is False


def test_sen2like_runner_fails_when_manifest_contains_failed_steps(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    scene_dir = work_dir / "LC08_SCENE"
    scene_dir.mkdir(parents=True)
    (scene_dir / "manifest.json").write_text(
        json.dumps(
            {
                "steps": {
                    "atmospheric_correction": {
                        "status": "failed",
                        "error": "not a directory",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE"],
            working_dir=str(work_dir),
        )
    )

    assert response.status == "failed"
    issue_codes = {issue["code"] for issue in response.metadata["output_issues"]}
    assert "step_failed" in issue_codes
    assert "normalized_output_missing" in issue_codes


def test_sen2like_runner_reports_incomplete_manifest_steps(
    tmp_path: Path,
    monkeypatch,
) -> None:
    vendor_root = tmp_path / "vendor"
    vendor_root.mkdir()
    (vendor_root / "Pipeline.py").write_text("print('upstream')")
    work_dir = tmp_path / "work"
    scene_dir = work_dir / "LC08_SCENE"
    scene_dir.mkdir(parents=True)
    (scene_dir / "manifest.json").write_text(
        json.dumps(
            {
                "steps": {
                    "atmospheric_correction": {
                        "status": "running",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def fake_vendor_root() -> Path:
        return vendor_root

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("nimbuschain_sen2like_service.runner.resolve_vendor_root", fake_vendor_root)
    monkeypatch.setattr("subprocess.run", fake_run)

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE"],
            working_dir=str(work_dir),
        )
    )

    assert response.status == "failed"
    issue_codes = {issue["code"] for issue in response.metadata["output_issues"]}
    assert "step_incomplete" in issue_codes
    assert "normalized_output_missing" in issue_codes


def test_sen2like_packaging_reads_landsat_mtl_metadata_for_extracted_input(
    tmp_path: Path,
) -> None:
    packaging = _load_packaging_step_module()
    input_dir = tmp_path / "_inputs" / "LC09_L2SP_EXTRACTED"
    input_dir.mkdir(parents=True)
    (input_dir / "LC09_L2SP_198032_20260410_20260411_02_T1_MTL.txt").write_text(
        "\n".join(
            [
                'LANDSAT_PRODUCT_ID = "LC09_L2SP_198032_20260410_20260411_02_T1"',
                "DATE_ACQUIRED = 2026-04-10",
                'SCENE_CENTER_TIME = "10:40:36.9176670Z"',
            ]
        ),
        encoding="utf-8",
    )
    ctx = SimpleNamespace(
        product_id=str(input_dir),
        config={"landsat_path": str(input_dir)},
    )

    metadata = packaging._resolve_acquisition_metadata(ctx)

    assert metadata["date"] == "20260410"
    assert metadata["timestamp"] == "20260410T104036"
    assert metadata["iso"] == "2026-04-10T10:40:36.917667Z"
    assert metadata["product_id"] == "LC09_L2SP_198032_20260410_20260411_02_T1"
    assert metadata["source"].endswith("_MTL.txt")


def test_sen2like_packaging_uses_acquisition_time_in_safe_and_granule_names() -> None:
    packaging = _load_packaging_step_module()

    safe_name = packaging._build_safe_name("T31UDQ", "L2F", "05.00", "20260410T104036")
    granule_id = packaging._build_granule_id("L2F", "T31UDQ", "20260410T104036", "05.00")

    assert safe_name.startswith("S2L_MSIL2F_20260410T104036_N0500_R000_T31UDQ_")
    assert safe_name.endswith(".SAFE")
    assert granule_id == "L2F_T31UDQ_20260410T104036_N0500_R000"


def test_sen2like_packaging_refuses_current_day_fallback_for_path_like_product_id(
    tmp_path: Path,
) -> None:
    packaging = _load_packaging_step_module()
    ctx = SimpleNamespace(
        product_id=str(tmp_path / "_inputs" / "LC09_L2SP_EXTRACTED"),
        config={},
    )

    try:
        packaging._resolve_acquisition_metadata(ctx)
    except RuntimeError as exc:
        assert "synthetic acquisition date" in str(exc)
    else:
        raise AssertionError("Expected missing MTL metadata to fail clearly.")


def test_sen2like_packaging_acquisition_time_helpers_preserve_landsat_time() -> None:
    packaging = _load_packaging_step_module()

    assert packaging._build_acquisition_timestamp("20260418", "10:40:22.8698510Z") == (
        "20260418T104022"
    )
    assert packaging._build_acquisition_iso("20260418", "10:40:22.8698510Z") == (
        "2026-04-18T10:40:22.869851Z"
    )
