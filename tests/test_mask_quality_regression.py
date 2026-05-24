from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("zarr")

import zarr

from nimbuschain_mask_service.inference import run_cloud_inference
from nimbuschain_mask_service.omniwater import _run_water_fallback_window
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec, resolve_sensor_mask_spec
from nimbuschain_mask_service.service import MaskService


def _sensor_case_specs() -> list[tuple[str, SensorMaskSpec]]:
    return [
        ("sentinel_l1c", resolve_sensor_mask_spec(provider="copernicus", collection="SENTINEL-2", product_type="S2MSI1C")),
        ("sentinel_l2a", resolve_sensor_mask_spec(provider="copernicus", collection="SENTINEL-2", product_type="S2MSI2A")),
        ("landsat_l1", resolve_sensor_mask_spec(provider="usgs", collection="LANDSAT_OT_C2_L1", product_type="L1TP")),
        ("landsat_l2", resolve_sensor_mask_spec(provider="usgs", collection="LANDSAT_OT_C2_L2", product_type="L2SR")),
    ]


def _band_map(sensor: SensorMaskSpec) -> dict[str, str]:
    if sensor.sensor_key == "sentinel-2":
        return {"blue": "B02", "green": "B03", "red": "B04", "nir": "B08", "swir1": "B11", "swir2": "B12"}
    if sensor.sensor_key == "landsat-8-9-l1":
        return {"blue": "B2", "green": "B3", "red": "B4", "nir": "B5", "swir1": "B6", "swir2": "B7"}
    return {"blue": "SR_B2", "green": "SR_B3", "red": "SR_B4", "nir": "SR_B5", "swir1": "SR_B6", "swir2": "SR_B7"}


def _synthetic_scene(sensor: SensorMaskSpec) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    required = {str(name) for name in sensor.cloud_required_bands + sensor.water_fallback_bands}
    height = width = 48
    channels = {
        name: np.zeros((height, width), dtype=np.float32)
        for name in required
    }
    rows, cols = np.indices((height, width))
    masks = {
        "open_water": (rows >= 2) & (rows < 12) & (cols >= 2) & (cols < 12),
        "turbid_water": (rows >= 2) & (rows < 12) & (cols >= 18) & (cols < 28),
        "dark_land": (rows >= 2) & (rows < 12) & (cols >= 34) & (cols < 44),
        "thick_cloud": (rows >= 18) & (rows < 28) & (cols >= 2) & (cols < 12),
        "thin_cloud": (rows >= 18) & (rows < 28) & (cols >= 18) & (cols < 28),
        "coastline": (rows >= 18) & (rows < 28) & (cols >= 34) & (cols < 44),
        "cloud_shadow": (rows >= 28) & (rows < 34) & (cols >= 6) & (cols < 16),
        "bright_land": (rows >= 34) & (rows < 44) & (cols >= 34) & (cols < 44),
    }

    signatures = {
        "open_water": dict(blue=0.14, green=0.38, red=0.06, nir=0.010, swir1=0.006, swir2=0.004),
        "turbid_water": dict(blue=0.20, green=0.31, red=0.18, nir=0.040, swir1=0.022, swir2=0.015),
        "thick_cloud": dict(blue=0.95, green=0.96, red=0.95, nir=0.34, swir1=0.78, swir2=0.72),
        "thin_cloud": dict(blue=0.72, green=0.74, red=0.72, nir=0.44, swir1=0.53, swir2=0.48),
        "cloud_shadow": dict(blue=0.09, green=0.08, red=0.07, nir=0.04, swir1=0.030, swir2=0.022),
        "bright_land": dict(blue=0.12, green=0.18, red=0.26, nir=0.50, swir1=0.44, swir2=0.38),
        "dark_land": dict(blue=0.08, green=0.09, red=0.10, nir=0.28, swir1=0.25, swir2=0.22),
        "coastline": dict(blue=0.18, green=0.24, red=0.16, nir=0.12, swir1=0.09, swir2=0.07),
    }

    mapping = _band_map(sensor)
    for region_name, region_mask in masks.items():
        signature = signatures[region_name]
        for role, band_name in mapping.items():
            channels[band_name][region_mask] = float(signature[role])
    return channels, masks


def _cloud_target_mask(masks: dict[str, np.ndarray]) -> np.ndarray:
    return np.logical_or(masks["thick_cloud"], masks["thin_cloud"])


@pytest.mark.parametrize("case_name,sensor", _sensor_case_specs())
def test_heuristic_cloud_quality_reaches_recall_targets(case_name: str, sensor: SensorMaskSpec) -> None:
    channels, masks = _synthetic_scene(sensor)
    result = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="heuristic",
        include_shadows=True,
    )

    mask = np.asarray(result.mask, dtype=np.uint8)
    cloud_target = _cloud_target_mask(masks)
    shadow_target = masks["cloud_shadow"]
    dry_controls = np.logical_or(masks["bright_land"], masks["dark_land"])

    cloud_recall = float(mask[cloud_target].mean())
    shadow_recall = float(mask[shadow_target].mean())
    false_positive = float(mask[dry_controls].mean())

    min_cloud_recall = 0.90 if sensor.sensor_key == "sentinel-2" else 0.85
    assert cloud_recall >= min_cloud_recall, f"{case_name}: cloud recall too low ({cloud_recall:.3f})"
    assert shadow_recall >= 0.70, f"{case_name}: shadow recall too low ({shadow_recall:.3f})"
    assert false_positive <= 0.15, f"{case_name}: false positives too high ({false_positive:.3f})"
    assert result.summary["threshold_used"] == pytest.approx(sensor.cloud_threshold_default)


@pytest.mark.parametrize("case_name,sensor", _sensor_case_specs())
def test_water_fallback_quality_reaches_recall_targets(case_name: str, sensor: SensorMaskSpec) -> None:
    channels, masks = _synthetic_scene(sensor)
    cloud_obstruction = np.logical_or(_cloud_target_mask(masks), masks["cloud_shadow"]).astype(np.uint8)
    probability, water_mask, summary = _run_water_fallback_window(
        sensor=sensor,
        channels=channels,
        threshold=float(sensor.water_threshold_default),
        cloud_mask=cloud_obstruction,
    )

    water_mask = np.asarray(water_mask, dtype=np.uint8)
    open_recall = float(water_mask[masks["open_water"]].mean())
    turbid_recall = float(water_mask[masks["turbid_water"]].mean())
    dry_controls = np.logical_or(masks["bright_land"], masks["dark_land"])
    false_positive = float(water_mask[dry_controls].mean())

    assert open_recall >= 0.80, f"{case_name}: open-water recall too low ({open_recall:.3f})"
    assert turbid_recall >= 0.65, f"{case_name}: turbid-water recall too low ({turbid_recall:.3f})"
    assert false_positive <= 0.10, f"{case_name}: water false positives too high ({false_positive:.3f})"
    assert summary["cloud_blocked_pixels"] > 0
    assert float(np.asarray(probability, dtype=np.float32).mean()) > 0.0


@pytest.mark.parametrize("case_name,sensor", _sensor_case_specs())
def test_cloud_first_obstruction_improves_water_output(case_name: str, sensor: SensorMaskSpec) -> None:
    channels, masks = _synthetic_scene(sensor)
    probability_no_cloud, water_no_cloud, _ = _run_water_fallback_window(
        sensor=sensor,
        channels=channels,
        threshold=float(sensor.water_threshold_default),
        cloud_mask=None,
    )
    _ = probability_no_cloud  # debug only
    probability_with_cloud, water_with_cloud, _ = _run_water_fallback_window(
        sensor=sensor,
        channels=channels,
        threshold=float(sensor.water_threshold_default),
        cloud_mask=np.logical_or(_cloud_target_mask(masks), masks["cloud_shadow"]).astype(np.uint8),
    )
    _ = probability_with_cloud
    shadow_region_without_cloud = float(np.asarray(water_no_cloud, dtype=np.uint8)[masks["cloud_shadow"]].mean())
    shadow_region_with_cloud = float(np.asarray(water_with_cloud, dtype=np.uint8)[masks["cloud_shadow"]].mean())
    assert shadow_region_with_cloud <= shadow_region_without_cloud, (
        f"{case_name}: cloud-first ordering should not increase shadow-as-water false positives"
    )


@pytest.mark.parametrize("case_name,sensor", _sensor_case_specs())
def test_water_fallback_allows_cloud_water_overlap(case_name: str, sensor: SensorMaskSpec) -> None:
    channels, masks = _synthetic_scene(sensor)
    probability, water_mask, summary = _run_water_fallback_window(
        sensor=sensor,
        channels=channels,
        threshold=float(sensor.water_threshold_default),
        cloud_mask=masks["open_water"].astype(np.uint8),
    )

    water_mask = np.asarray(water_mask, dtype=np.uint8)
    assert float(water_mask[masks["open_water"]].mean()) >= 0.80, (
        f"{case_name}: cloud overlap should not erase water pixels"
    )
    assert float(np.asarray(probability, dtype=np.float32)[masks["open_water"]].mean()) > 0.0
    assert summary["cloud_blocked_pixels"] == int(masks["open_water"].sum())


def _write_source_zarr_from_scene(
    root: Path,
    *,
    channels: dict[str, np.ndarray],
    sensor: SensorMaskSpec,
    invalid_mask: np.ndarray | None = None,
) -> Path:
    output = root / "source.zarr"
    band_names = list(dict.fromkeys(sensor.cloud_required_bands + sensor.water_fallback_bands))
    height, width = next(iter(channels.values())).shape
    group = zarr.open_group(str(output), mode="w", zarr_format=2)
    imagery = group.create_array(
        "imagery",
        shape=(1, len(band_names), height, width),
        chunks=(1, len(band_names), min(height, 32), min(width, 32)),
        dtype=np.uint16,
        overwrite=True,
    )
    for band_index, band_name in enumerate(band_names):
        band_data = np.clip(np.round(channels[band_name] * 10000.0), 0, 10000).astype(np.uint16)
        if invalid_mask is not None:
            band_data = np.where(invalid_mask, 0, band_data).astype(np.uint16, copy=False)
        imagery[0, band_index, :, :] = band_data
    group.create_array("band", shape=(len(band_names),), chunks=(len(band_names),), dtype="U8", overwrite=True)
    group["band"][:] = np.array(band_names, dtype="U8")
    band_metadata = {
        band_name: {
            "source_nodata": 0,
            "target_nodata": 0,
        }
        for band_name in band_names
    }
    attrs = {
        "provider": "copernicus" if sensor.sensor_key == "sentinel-2" else "usgs",
        "scene_id": "synthetic_scene",
        "dimensions": ["time", "band", "y", "x"],
        "shape": [1, len(band_names), height, width],
        "band_names": band_names,
        "crs": "EPSG:32631",
        "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        "reference_pixel_size": [10.0, 10.0],
        "band_metadata": band_metadata,
    }
    if sensor.sensor_key == "sentinel-2":
        attrs.update(
            {
                "collection": "SENTINEL-2",
                "product_type": "S2MSI2A",
            }
        )
    elif sensor.sensor_key == "landsat-8-9-l1":
        attrs.update(
            {
                "collection": "LANDSAT_OT_C2_L1",
                "product_type": "L1TP",
                "radiometric_metadata": {
                    "bands": {
                        band_name: {
                            "mult": 1.0e-4,
                            "add": 0.0,
                            "apply_sun_elevation": False,
                        }
                        for band_name in band_names
                    },
                    "sun_elevation": 45.0,
                },
            }
        )
    else:
        attrs.update(
            {
                "collection": "LANDSAT_OT_C2_L2",
                "product_type": "L2SR",
                "radiometric_metadata": {
                    "bands": {
                        band_name: {
                            "mult": 1.0e-4,
                            "add": 0.0,
                            "apply_sun_elevation": False,
                        }
                        for band_name in band_names
                    },
                    "sun_elevation": 45.0,
                },
            }
        )
    group.attrs.update(
        attrs
    )
    zarr.consolidate_metadata(str(output))
    return output


def test_combined_water_cloud_run_preserves_both_masks_and_probabilities(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sensor = resolve_sensor_mask_spec(provider="copernicus", collection="SENTINEL-2", product_type="S2MSI2A")
    channels, masks = _synthetic_scene(sensor)
    source = _write_source_zarr_from_scene(tmp_path, channels=channels, sensor=sensor)
    monkeypatch.setenv("NIMBUS_WATERMASK_RUNTIME_MODE", "fallback")

    result = MaskService().apply_masks_to_zarr(
        job_id="mask-quality",
        zarr_uri=str(source),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="synthetic_scene",
        acquisition_datetime="2026-03-30T00:00:00Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
            "shape": [1, len(dict.fromkeys(sensor.cloud_required_bands + sensor.water_fallback_bands)), 48, 48],
            "band_names": list(dict.fromkeys(sensor.cloud_required_bands + sensor.water_fallback_bands)),
        },
        mask_types=["water", "cloud"],
        backend="heuristic",
        threshold=0.45,
        overwrite=True,
        include_shadows=True,
    )

    assert result["status"] == "written"
    assert result["masked_zarr_uri"]
    assert Path(str(result["masked_zarr_uri"])).exists()
    assert str(result["masked_zarr_uri"]) == str(source)

    derived = zarr.open_group(str(result["masked_zarr_uri"]), mode="r", use_consolidated=False)
    assert "imagery" in derived
    assert "band" in derived
    assert "masks" in derived
    assert "cloud" in derived["masks"]
    assert "cloud_probability" in derived["masks"]
    assert "water" in derived["masks"]
    assert "water_probability" in derived["masks"]

    cloud_mask = np.asarray(derived["masks"]["cloud"][0], dtype=np.uint8)
    water_mask = np.asarray(derived["masks"]["water"][0], dtype=np.uint8)
    assert float(cloud_mask[_cloud_target_mask(masks)].mean()) >= 0.90
    assert float(cloud_mask[masks["cloud_shadow"]].mean()) >= 0.70
    assert float(water_mask[masks["open_water"]].mean()) >= 0.80


@pytest.mark.parametrize(
    "case_name,sensor,mask_types",
    [
        (
            "landsat_l1_cloud_only",
            resolve_sensor_mask_spec(provider="usgs", collection="LANDSAT_OT_C2_L1", product_type="L1TP"),
            ["cloud"],
        ),
        (
            "landsat_l2_water_only",
            resolve_sensor_mask_spec(provider="usgs", collection="LANDSAT_OT_C2_L2", product_type="L2SR"),
            ["water"],
        ),
        (
            "landsat_l2_water_cloud",
            resolve_sensor_mask_spec(provider="usgs", collection="LANDSAT_OT_C2_L2", product_type="L2SR"),
            ["water", "cloud"],
        ),
    ],
)
def test_landsat_invalid_wedge_stays_clear_in_mask_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case_name: str,
    sensor: SensorMaskSpec,
    mask_types: list[str],
) -> None:
    channels, _masks = _synthetic_scene(sensor)
    rows, cols = np.indices(next(iter(channels.values())).shape)
    invalid_mask = (rows >= 34) & (cols < 16)
    source = _write_source_zarr_from_scene(
        tmp_path / case_name,
        channels=channels,
        sensor=sensor,
        invalid_mask=invalid_mask,
    )
    monkeypatch.setenv("NIMBUS_WATERMASK_RUNTIME_MODE", "fallback")

    band_names = list(dict.fromkeys(sensor.cloud_required_bands + sensor.water_fallback_bands))
    result = MaskService().apply_masks_to_zarr(
        job_id=f"mask-{case_name}",
        zarr_uri=str(source),
        provider="usgs",
        collection="LANDSAT_OT_C2_L1" if sensor.sensor_key == "landsat-8-9-l1" else "LANDSAT_OT_C2_L2",
        product_type="L1TP" if sensor.sensor_key == "landsat-8-9-l1" else "L2SR",
        scene_id=case_name,
        acquisition_datetime="2026-03-30T00:00:00Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
            "shape": [1, len(band_names), 48, 48],
            "band_names": band_names,
        },
        mask_types=mask_types,
        backend="heuristic",
        threshold=0.45,
        overwrite=True,
        include_shadows=True,
    )

    assert result["status"] == "written"
    derived = zarr.open_group(str(source), mode="r", use_consolidated=False)
    masks_group = derived["masks"]

    if "cloud" in mask_types:
        cloud_mask = np.asarray(masks_group["cloud"][0], dtype=np.uint8)
        cloud_probability = np.asarray(masks_group["cloud_probability"][0], dtype=np.float32)
        assert int(cloud_mask[invalid_mask].sum()) == 0
        assert float(cloud_probability[invalid_mask].sum()) == pytest.approx(0.0)

    if "water" in mask_types:
        water_mask = np.asarray(masks_group["water"][0], dtype=np.uint8)
        water_probability = np.asarray(masks_group["water_probability"][0], dtype=np.float32)
        assert int(water_mask[invalid_mask].sum()) == 0
        assert float(water_probability[invalid_mask].sum()) == pytest.approx(0.0)
