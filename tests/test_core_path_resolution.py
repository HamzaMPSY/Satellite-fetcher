from __future__ import annotations

from pathlib import Path

from nimbuschain_zarr_service.core import resolve_output_path


def test_resolve_output_path_prefers_existing_local_workspace_path(tmp_path: Path) -> None:
    candidate = tmp_path / "data" / "downloads" / "zarr" / "scene.zarr"
    candidate.parent.mkdir(parents=True, exist_ok=True)

    resolved = resolve_output_path(str(candidate))

    assert resolved == candidate.resolve()
