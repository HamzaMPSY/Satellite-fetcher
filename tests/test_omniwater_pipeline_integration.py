from __future__ import annotations

from pathlib import Path
import sys
import types

import numpy as np
import pytest

pytest.importorskip("rasterio")
pytest.importorskip("zarr")

import rasterio
from rasterio.transform import from_origin
import zarr

from nimbuschain_mask_service.service import MaskService
from nimbuschain_zarr_service.service import ZarrConversionService


def _write_raster(path: Path, *, shape: tuple[int, int], pixel_size: float, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.full(shape, value, dtype=np.uint16)
    transform = from_origin(399960.0, 5300040.0, pixel_size, pixel_size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=shape[0],
        width=shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:32631",
        transform=transform,
    ) as dataset:
        dataset.write(data, 1)


def _build_s2_bundle(root: Path, *, scene_id: str) -> Path:
    safe_root = root / scene_id
    safe_root.mkdir(parents=True, exist_ok=True)
    (safe_root / "manifest.safe").write_text("SAFE", encoding="utf-8")
    img_root = safe_root / "GRANULE" / "T31TDN" / "IMG_DATA"
    for index, code in enumerate(("B02", "B03", "B04", "B08"), start=1):
        _write_raster(
            img_root / "R10m" / f"T31TDN_20260101T105501_{code}_10m.tif",
            shape=(6, 6),
            pixel_size=10.0,
            value=index,
        )
    for index, code in enumerate(("B05", "B06", "B07", "B8A", "B11", "B12"), start=10):
        _write_raster(
            img_root / "R20m" / f"T31TDN_20260101T105501_{code}_20m.tif",
            shape=(3, 3),
            pixel_size=20.0,
            value=index,
        )
    for index, code in enumerate(("B01", "B09", "B10"), start=20):
        _write_raster(
            img_root / "R60m" / f"T31TDN_20260101T105501_{code}_60m.tif",
            shape=(1, 1),
            pixel_size=60.0,
            value=index,
        )
    return safe_root


def _build_landsat_bundle(root: Path, *, scene_id: str, l1: bool) -> Path:
    bundle_root = root / scene_id
    bundle_root.mkdir(parents=True, exist_ok=True)
    (bundle_root / f"{scene_id}_MTL.txt").write_text(
        "\n".join(
            [
                f'LANDSAT_PRODUCT_ID = "{scene_id}"',
                f'LANDSAT_SCENE_ID = "{scene_id}"',
                'DATE_ACQUIRED = 2026-01-01',
                'SCENE_CENTER_TIME = "10:55:01.0240000Z"',
            ]
        ),
        encoding="utf-8",
    )
    if l1:
        for index in range(1, 12):
            pixel_size = 15.0 if index == 8 else (60.0 if index in {10, 11} else 30.0)
            shape = (8, 8) if pixel_size == 15.0 else ((2, 2) if pixel_size == 60.0 else (4, 4))
            _write_raster(
                bundle_root / f"{scene_id}_B{index}.TIF",
                shape=shape,
                pixel_size=pixel_size,
                value=index,
            )
    else:
        for index in range(1, 8):
            _write_raster(
                bundle_root / f"{scene_id}_SR_B{index}.TIF",
                shape=(4, 4),
                pixel_size=30.0,
                value=index,
            )
        _write_raster(
            bundle_root / f"{scene_id}_ST_B10.TIF",
            shape=(4, 4),
            pixel_size=30.0,
            value=20,
        )
    return bundle_root


def _install_fake_omniwatermask(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType("omniwatermask")
    module.__version__ = "test"

    def make_water_mask(*, scene_paths, band_order, output_dir, **_kwargs):
        assert band_order == [1, 2, 3, 4]
        outputs = []
        for scene in scene_paths:
            scene_path = Path(scene)
            mask_path = Path(output_dir) / f"{scene_path.stem}_water_mask.tif"
            with rasterio.open(scene_path) as src:
                red = src.read(1)
                nir = src.read(4)
                mask = (nir >= red).astype(np.uint8)
                profile = src.profile.copy()
                profile.update(count=1, dtype="uint8")
                with rasterio.open(mask_path, "w", **profile) as dst:
                    dst.write(mask, 1)
            outputs.append(mask_path)
        return outputs

    module.make_water_mask = make_water_mask
    monkeypatch.setitem(sys.modules, "omniwatermask", module)


def _assert_source_zarr_unchanged(zarr_uri: str) -> None:
    group = zarr.open_group(zarr_uri, mode="r")
    assert "masks" not in group or "water" not in group["masks"]
    assert group.attrs.get("water_mask_written") in (None, False)
    assert group.attrs.get("water_mask_status") in (None, "")
    assert group.attrs.get("water_mask_path") in (None, "")


def test_sentinel2_manual_watermask_creates_masked_copy_without_mutating_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_omniwatermask(monkeypatch)
    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "s2.zarr").resolve())

    written_uri, _family, summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    _assert_source_zarr_unchanged(written_uri)
    assert summary.get("water_mask") in (None, {})
    assert dataset_summary.get("water_mask_written") in (None, False)

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    masked_uri = str(result["output_zarr_uri"])
    assert result["input_zarr_uri"] == written_uri
    assert masked_uri != written_uri
    assert Path(masked_uri).exists()

    _assert_source_zarr_unchanged(written_uri)
    group = zarr.open_group(masked_uri, mode="r")
    assert "masks" in group and "water" in group["masks"]
    assert tuple(group["masks"]["water"].shape) == (1, 6, 6)
    assert result["status"] == "written"
    assert group.attrs["water_mask_written"] is True
    assert group.attrs["water_mask_path"] == "masks/water"
    assert group.attrs["water_mask_status"] == "written"


def test_landsat_manual_watermask_creates_masked_copy_without_mutating_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_omniwatermask(monkeypatch)
    scene_id = "LC08_L1TP_190026_20260101_20260101_02_T1"
    raw_root = _build_landsat_bundle(tmp_path / "raw", scene_id=scene_id, l1=True)
    output_uri = str((tmp_path / "out" / "landsat.zarr").resolve())

    written_uri, _family, _summary, dataset_summary = ZarrConversionService().convert(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="L1TP",
    )

    _assert_source_zarr_unchanged(written_uri)

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="usgs",
        collection="LANDSAT_OT_C2_L1",
        product_type="L1TP",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    masked_uri = str(result["output_zarr_uri"])
    assert result["input_zarr_uri"] == written_uri
    assert masked_uri != written_uri
    assert Path(masked_uri).exists()

    _assert_source_zarr_unchanged(written_uri)
    group = zarr.open_group(masked_uri, mode="r")
    assert "masks" in group and "water" in group["masks"]
    assert tuple(group["masks"]["water"].shape) == (1, 4, 4)
    assert result["status"] == "written"
    assert group.attrs["water_mask_written"] is True
    assert group.attrs["water_mask_path"] == "masks/water"
    assert group.attrs["water_mask_status"] == "written"


def test_manual_omniwater_stage_callback_reports_pipeline_steps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_omniwatermask(monkeypatch)
    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "callback.zarr").resolve())
    stages: list[tuple[str, dict[str, object]]] = []

    written_uri, _family, _summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
        stage_callback=lambda stage, payload: stages.append((stage, dict(payload))),
    )

    assert [name for name, _payload in stages] == [
        "water_masking_started",
        "water_masking_finished",
    ]
    assert stages[0][1]["zarr_uri"] == output_uri
    assert stages[1][1]["water_mask"]["status"] == "written"
    assert stages[1][1]["water_mask"]["input_zarr_uri"] == output_uri
    assert stages[1][1]["water_mask"]["output_zarr_uri"] != output_uri
    assert Path(str(stages[1][1]["water_mask"]["output_zarr_uri"])).exists()


def test_manual_omniwater_tiles_existing_zarr_and_persists_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_omniwatermask(monkeypatch)
    monkeypatch.setenv("NIMBUS_WATERMASK_TILE_SIZE", "3")
    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "tiled.zarr").resolve())

    written_uri, _family, _summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    assert result["status"] == "written"
    assert Path(str(result["artifact_uri"])).exists()
    assert Path(str(result["status_path"])).exists()
    assert result["input_zarr_uri"] == written_uri
    assert result["output_zarr_uri"] != written_uri
    assert Path(str(result["output_zarr_uri"])).exists()


def test_manual_omniwater_failure_preserves_zarr_and_records_mask_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NIMBUS_WATERMASK_RUNTIME_MODE", "model")
    module = types.ModuleType("omniwatermask")
    module.__version__ = "test"

    def make_water_mask(*, scene_paths, band_order, output_dir, **_kwargs):
        raise RuntimeError("simulated_omniwater_failure")

    module.make_water_mask = make_water_mask
    monkeypatch.setitem(sys.modules, "omniwatermask", module)

    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "callback.zarr").resolve())

    written_uri, _family, summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    assert written_uri == output_uri
    assert Path(written_uri).exists()
    assert summary.get("water_mask") in (None, {})
    assert result["status"] == "failed"
    assert result["reason"] == "simulated_omniwater_failure"
    assert result["input_zarr_uri"] == written_uri
    _assert_source_zarr_unchanged(written_uri)
    group = zarr.open_group(written_uri, mode="r")
    assert group.attrs.get("water_mask_status_path") in (None, "")


def test_manual_omniwater_uses_ndwi_fallback_when_legacy_fastai_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = types.ModuleType("omniwatermask")
    module.__version__ = "test"

    def make_water_mask(*, scene_paths, band_order, output_dir, **_kwargs):
        raise ImportError(
            "fastai is not installed. Please install with legacy model support to use model versions 1-3."
        )

    module.make_water_mask = make_water_mask
    monkeypatch.setitem(sys.modules, "omniwatermask", module)
    monkeypatch.setenv("NIMBUS_WATERMASK_TILE_SIZE", "3")

    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "fallback.zarr").resolve())

    written_uri, _family, _summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    assert result["status"] == "written"
    assert result["runtime_mode"] == "ndwi_fallback"
    assert result["input_zarr_uri"] == written_uri
    assert result["output_zarr_uri"] != written_uri
    assert Path(str(result["output_zarr_uri"])).exists()
    _assert_source_zarr_unchanged(written_uri)


def test_manual_omniwater_uses_internal_ndwi_fallback_when_module_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    real_import_module = importlib.import_module

    def fake_import_module(name: str, package: str | None = None):
        if name == "omniwatermask":
            raise ImportError("simulated missing omniwatermask")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setenv("NIMBUS_WATERMASK_TILE_SIZE", "3")

    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "fallback_no_module.zarr").resolve())

    written_uri, _family, _summary, dataset_summary = ZarrConversionService().convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    result = MaskService().apply_omniwater_to_zarr(
        zarr_uri=written_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI1C",
        scene_id=scene_id,
        acquisition_datetime=dataset_summary.get("acquisition_datetime"),
        dataset_summary=dataset_summary,
    )

    assert result["status"] == "written"
    assert result["runtime_mode"] == "ndwi_fallback"
    assert Path(str(result["artifact_uri"])).exists()
    assert result["input_zarr_uri"] == written_uri
    assert result["output_zarr_uri"] != written_uri
    assert Path(str(result["output_zarr_uri"])).exists()
