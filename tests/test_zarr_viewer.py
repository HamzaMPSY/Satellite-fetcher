from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import zarr

from nimbuschain_zarr_viewer.cli import (
    _default_rgb_bands,
    _resolve_step,
    build_launch_command,
    discover_local_zarr_stores,
    preset_viewer_options,
    summarize_store,
)


def _write_label_array(group: zarr.Group, key: str, labels: list[str]) -> None:
    values = np.asarray(labels, dtype="U16")
    group.create_array(key, data=values, overwrite=True)


def test_summarize_store_discovers_masks_and_labels(tmp_path: Path) -> None:
    store_path = tmp_path / "scene.zarr"
    group = zarr.open_group(store_path, mode="w")
    imagery = np.zeros((1, 3, 4, 5), dtype=np.uint16)
    group.create_array("imagery", data=imagery, overwrite=True)
    _write_label_array(group, "band", ["B04", "B03", "B02"])
    ancillary = np.zeros((1, 1, 4, 5), dtype=np.uint8)
    group.create_array("ancillary", data=ancillary, overwrite=True)
    _write_label_array(group, "ancillary_layer", ["SCL"])
    masks = group.create_group("masks", overwrite=True)
    cloud = np.zeros((1, 4, 5), dtype=np.uint8)
    cloud_probability = np.zeros((1, 4, 5), dtype=np.float32)
    masks.create_array("cloud", data=cloud, overwrite=True)
    masks.create_array(
        "cloud_probability",
        data=cloud_probability,
        overwrite=True,
    )

    summary = summarize_store(store_path)

    assert summary.band_names == ["B04", "B03", "B02"]
    assert summary.ancillary_names == ["SCL"]
    assert summary.mask_layers == ["cloud", "cloud_probability"]
    assert summary.imagery_shape == (1, 3, 4, 5)


def test_default_rgb_bands_supports_sentinel_and_landsat() -> None:
    assert _default_rgb_bands(["B01", "B04", "B03", "B02"]) == ("B04", "B03", "B02")
    assert _default_rgb_bands(["B1", "B4", "B3", "B2"]) == ("B4", "B3", "B2")
    assert _default_rgb_bands(["SR_B1", "SR_B4", "SR_B3", "SR_B2"]) == ("SR_B4", "SR_B3", "SR_B2")


def test_resolve_step_autoscales_large_rasters() -> None:
    assert _resolve_step((12, 10980, 10980), None) == 1
    assert _resolve_step((12, 24000, 20000), None) == 2
    assert _resolve_step((12, 40000, 40000), 4) == 4


def test_preset_viewer_options_cover_requested_modes() -> None:
    rgb = preset_viewer_options("rgb")
    assert rgb.show_rgb is True
    assert rgb.show_bands is False
    assert rgb.show_masks is False

    rgb_masks = preset_viewer_options("rgb-masks")
    assert rgb_masks.show_rgb is True
    assert rgb_masks.show_bands is False
    assert rgb_masks.show_masks is True
    assert rgb_masks.show_ancillary is False

    masks_only = preset_viewer_options("masks-only")
    assert masks_only.show_masks is True
    assert masks_only.show_rgb is False

    all_bands = preset_viewer_options("all-bands")
    assert all_bands.show_bands is True
    assert all_bands.show_ancillary is True


def test_discover_local_zarr_stores_sorts_by_mtime(tmp_path: Path) -> None:
    older = tmp_path / "older.zarr"
    newer = tmp_path / "newer.zarr"
    older.mkdir()
    newer.mkdir()
    os.utime(older, (1, 1))
    os.utime(newer, (2, 2))

    stores = discover_local_zarr_stores(tmp_path)

    assert stores[0].name == "newer.zarr"
    assert stores[1].name == "older.zarr"


def test_build_launch_command_includes_preset_and_step(tmp_path: Path) -> None:
    store_path = tmp_path / "scene.zarr"
    store_path.mkdir()
    expected_script = Path(__file__).resolve().parents[1] / "scripts" / "open_zarr_napari.py"

    command = build_launch_command(
        store_path,
        preset="rgb-masks",
        step=3,
        grid=True,
        python_executable="/tmp/python",
    )

    assert command[:2] == ["/tmp/python", str(expected_script)]
    assert command[-5:] == ["--preset", "rgb-masks", "--step", "3", "--grid"]
