from __future__ import annotations

import json
from pathlib import Path

from nimbuschain_fetch_ui.zarr_utils import inspect_local_zarr_store, requested_mask_state


def _make_store(tmp_path: Path, name: str, *, attrs: dict | None = None, mask_layers: list[str] | None = None) -> Path:
    store_path = tmp_path / name
    store_path.mkdir(parents=True, exist_ok=True)
    (store_path / ".zattrs").write_text(json.dumps(attrs or {}), encoding="utf-8")
    (store_path / "imagery").mkdir(parents=True, exist_ok=True)
    if mask_layers:
        masks_path = store_path / "masks"
        masks_path.mkdir(parents=True, exist_ok=True)
        for layer_name in mask_layers:
            (masks_path / layer_name).mkdir(parents=True, exist_ok=True)
    return store_path


def test_inspect_local_zarr_store_reports_unmasked_store(tmp_path: Path) -> None:
    store_path = _make_store(
        tmp_path,
        "scene.zarr",
        attrs={"reference_pixel_size": [10.0, 10.0]},
    )

    summary = inspect_local_zarr_store(str(store_path))

    assert summary["runtime_exists"] is True
    assert summary["mask_state"] == "none"
    assert summary["mask_state_label"] == "No masks"
    assert summary["reference_pixel_size"] == [10.0, 10.0]

    requested = requested_mask_state(summary, ["water", "cloud"])
    assert requested["status"] == "not_written"
    assert requested["missing_primary"] == ["water", "cloud"]


def test_requested_mask_state_detects_partial_store(tmp_path: Path) -> None:
    store_path = _make_store(
        tmp_path,
        "scene_partial.zarr",
        attrs={"water_mask_status": "written"},
        mask_layers=["water", "water_probability"],
    )

    summary = inspect_local_zarr_store(str(store_path))

    assert summary["mask_state"] == "water"
    assert summary["primary_masks"] == ["water"]

    water_only = requested_mask_state(summary, ["water"])
    assert water_only["status"] == "written"

    combined = requested_mask_state(summary, ["water", "cloud"])
    assert combined["status"] == "partial"
    assert combined["present_primary"] == ["water"]
    assert combined["missing_primary"] == ["cloud"]


def test_requested_mask_state_detects_complete_store(tmp_path: Path) -> None:
    store_path = _make_store(
        tmp_path,
        "scene_complete.zarr",
        attrs={
            "water_mask_status": "written",
            "cloud_mask_status": "written",
        },
        mask_layers=["water", "water_probability", "cloud", "cloud_probability"],
    )

    summary = inspect_local_zarr_store(str(store_path))

    assert summary["mask_state"] == "water+cloud"
    assert summary["mask_state_label"] == "Water + cloud"

    requested = requested_mask_state(summary, ["water", "cloud"])
    assert requested["status"] == "written"
    assert requested["present_primary"] == ["water", "cloud"]
