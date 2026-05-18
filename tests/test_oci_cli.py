from __future__ import annotations

import json
from pathlib import Path

from nimbuschain_zarr_service import oci_cli
from nimbuschain_zarr_service.oci_storage import OCIStorageError


def test_oci_cp_downloads_file_into_existing_directory(monkeypatch, tmp_path: Path, capsys) -> None:
    class FakeStore:
        def is_dir(self, path: str) -> bool:
            return False

        def is_file(self, path: str) -> bool:
            return path == "raw/scene.SAFE.zip"

        def download_file(self, path: str, destination: Path) -> Path:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("ok", encoding="utf-8")
            return destination

    monkeypatch.setattr(
        oci_cli,
        "_build_store",
        lambda uri: (FakeStore(), "raw/scene.SAFE.zip"),
    )
    destination_dir = tmp_path / "downloads"
    destination_dir.mkdir()
    args = oci_cli.build_parser().parse_args(
        ["cp", "oci://bucket@namespace/raw/scene.SAFE.zip", str(destination_dir)]
    )

    assert oci_cli.run(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_kind"] == "file"
    assert Path(payload["local_path"]) == destination_dir / "scene.SAFE.zip"


def test_oci_cp_requires_recursive_for_directory(monkeypatch, tmp_path: Path) -> None:
    class FakeStore:
        def is_dir(self, path: str) -> bool:
            return True

        def is_file(self, path: str) -> bool:
            return False

    monkeypatch.setattr(
        oci_cli,
        "_build_store",
        lambda uri: (FakeStore(), "raw/scene.SAFE"),
    )
    args = oci_cli.build_parser().parse_args(
        ["cp", "oci://bucket@namespace/raw/scene.SAFE", str(tmp_path)]
    )

    try:
        oci_cli.run(args)
    except OCIStorageError as exc:
        assert "Re-run with --recursive" in str(exc)
    else:  # pragma: no cover - defensive guard
        raise AssertionError("Expected OCIStorageError when copying a directory without --recursive.")


def test_oci_cp_downloads_directory_into_explicit_target(monkeypatch, tmp_path: Path, capsys) -> None:
    class FakeStore:
        def is_dir(self, path: str) -> bool:
            return True

        def is_file(self, path: str) -> bool:
            return False

        def download_tree(self, path: str, destination_root: Path) -> Path:
            destination_root.mkdir(parents=True, exist_ok=True)
            return destination_root

    monkeypatch.setattr(
        oci_cli,
        "_build_store",
        lambda uri: (FakeStore(), "raw/scene.SAFE"),
    )
    explicit_target = tmp_path / "scene_copy.SAFE"
    args = oci_cli.build_parser().parse_args(
        ["cp", "oci://bucket@namespace/raw/scene.SAFE", str(explicit_target), "--recursive"]
    )

    assert oci_cli.run(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert Path(payload["local_path"]) == explicit_target
