from __future__ import annotations

import os
from pathlib import Path

import dask.array as da
import numpy as np
import zarr

from nimbuschain_zarr_viewer.cli import (
    _default_rgb_bands,
    _normalize_channel_for_display,
    _resolve_step,
    _sample_percentile_limits,
    build_launch_command,
    discover_local_zarr_stores,
    mask_state_label,
    preset_viewer_options,
    resolve_sensor_display_config,
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

    assert summary.provider is None
    assert summary.collection is None
    assert summary.product_type is None
    assert summary.band_names == ["B04", "B03", "B02"]
    assert summary.ancillary_names == ["SCL"]
    assert summary.mask_layers == ["cloud", "cloud_probability"]
    assert summary.mask_state_label == "Cloud only"
    assert summary.imagery_shape == (1, 3, 4, 5)


def test_mask_state_label_distinguishes_pipeline_mask_layouts() -> None:
    assert mask_state_label([]) == "Zarr only"
    assert mask_state_label(["water", "water_probability"]) == "Water only"
    assert mask_state_label(["cloud", "cloud_probability"]) == "Cloud only"
    assert mask_state_label(["water", "cloud", "water_probability", "cloud_probability"]) == "Water + cloud"
    assert mask_state_label(["water_probability"]) == "Probability layers only"


def test_default_rgb_bands_supports_sentinel_and_landsat() -> None:
    assert _default_rgb_bands(["B01", "B04", "B03", "B02"]) == ("B04", "B03", "B02")
    assert _default_rgb_bands(["B1", "B4", "B3", "B2"]) == ("B4", "B3", "B2")
    assert _default_rgb_bands(["SR_B1", "SR_B4", "SR_B3", "SR_B2"]) == ("SR_B4", "SR_B3", "SR_B2")


def test_resolve_sensor_display_config_matches_supported_products() -> None:
    sentinel = resolve_sensor_display_config(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        band_names=["B01", "B02", "B03", "B04"],
    )
    assert sentinel.sensor_key == "sentinel-2"
    assert sentinel.scale_hint == "reflectance_0_10000"
    assert sentinel.rgb_bands == ("B04", "B03", "B02")

    landsat_l1 = resolve_sensor_display_config(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        band_names=["B1", "B2", "B3", "B4"],
    )
    assert landsat_l1.sensor_key == "landsat-8-9-l1"
    assert landsat_l1.scale_hint == "landsat_l1_reflectance"
    assert landsat_l1.rgb_bands == ("B4", "B3", "B2")

    landsat_l2 = resolve_sensor_display_config(
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
        band_names=["SR_B1", "SR_B2", "SR_B3", "SR_B4"],
    )
    assert landsat_l2.sensor_key == "landsat-8-9-l2"
    assert landsat_l2.scale_hint == "landsat_l2_reflectance"
    assert landsat_l2.rgb_bands == ("SR_B4", "SR_B3", "SR_B2")


def test_normalize_channel_for_display_applies_sensor_scaling() -> None:
    sentinel = _normalize_channel_for_display(
        da.from_array(np.asarray([[1000, 5000]], dtype=np.float32)),
        scale_hint="reflectance_0_10000",
        band_name="B04",
        root_attrs={},
    ).compute()
    assert np.allclose(sentinel, np.asarray([[0.1, 0.5]], dtype=np.float32))

    landsat_l2 = _normalize_channel_for_display(
        da.from_array(np.asarray([[10000]], dtype=np.float32)),
        scale_hint="landsat_l2_reflectance",
        band_name="SR_B4",
        root_attrs={},
    ).compute()
    assert np.allclose(landsat_l2, np.asarray([[0.075]], dtype=np.float32))


def test_sample_percentile_limits_ignores_invalid_pixels() -> None:
    array = da.from_array(np.asarray([[0.0, 0.1, 0.2], [0.3, 0.4, 0.9]], dtype=np.float32))
    valid = da.from_array(np.asarray([[False, True, True], [True, True, False]], dtype=bool))

    low, high = _sample_percentile_limits(array, valid_mask=valid, low_percentile=0.0, high_percentile=100.0)

    assert np.isclose(low, 0.1)
    assert np.isclose(high, 0.4)


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
