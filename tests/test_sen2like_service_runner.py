from __future__ import annotations

import json
import subprocess
import tarfile
from pathlib import Path

from nimbuschain_sen2like_service.models import Sen2LikeNormalizeRequest
from nimbuschain_sen2like_service.runner import build_command, run_sen2like


def _write_valid_sen2like_output(work_dir: Path, scene_name: str) -> Path:
    scene_dir = work_dir / scene_name
    safe_dir = scene_dir / "SAFE" / "S2L_MSIL2F_20260101T105501_N0500_R000_T31UDQ_20260101T105501.SAFE"
    img_dir = safe_dir / "GRANULE" / "L2F_T31UDQ_A20260101T105501" / "IMG_DATA" / "RESOLUTION_10M"
    img_dir.mkdir(parents=True)
    (safe_dir / "manifest.safe").write_text("SAFE", encoding="utf-8")
    (img_dir / "T31UDQ_20260101T105501_B02_10m.TIF").write_bytes(b"tif")
    (scene_dir / "manifest.json").write_text(
        json.dumps({"steps": {"packaging": {"status": "success"}}}),
        encoding="utf-8",
    )
    return safe_dir


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

    response = run_sen2like(
        Sen2LikeNormalizeRequest(
            job_id="job-1",
            products=["/data/raw/LC08_SCENE"],
            working_dir=str(work_dir),
        )
    )

    assert response.status == "succeeded"
    assert response.outputs[0].manifest_path == str(scene_dir / "manifest.json")
    assert response.outputs[0].normalized_uri == str(safe_dir)
    assert response.duration_seconds >= 0
    assert captured_env["SPARK_LOCAL_DIRS"] == "/tmp/nimbus-sen2like-spark"
    assert response.metadata["spark_local_dirs"] == "/tmp/nimbus-sen2like-spark"


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
