from __future__ import annotations

import subprocess
from pathlib import Path

from nimbuschain_sen2like_service.models import Sen2LikeNormalizeRequest
from nimbuschain_sen2like_service.runner import build_command, run_sen2like


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
    safe_dir = scene_dir / "T31UDQ_L2F"
    safe_dir.mkdir(parents=True)
    (scene_dir / "manifest.json").write_text("{}")

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

    assert response.status == "succeeded"
    assert response.outputs[0].manifest_path == str(scene_dir / "manifest.json")
    assert response.outputs[0].normalized_uri == str(safe_dir)
    assert response.duration_seconds >= 0
