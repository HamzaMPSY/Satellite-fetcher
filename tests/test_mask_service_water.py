from __future__ import annotations

from pathlib import Path

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


def test_write_water_mask_to_existing_zarr_store(tmp_path: Path) -> None:
    scene_id = "S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path / "raw", scene_id=scene_id)
    output_uri = str((tmp_path / "out" / "scene.zarr").resolve())

    zarr_service = ZarrConversionService()
    written_uri, _data_family, summary, dataset_summary = zarr_service.convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI1C",
    )

    height = int(dataset_summary["shape"][2])
    width = int(dataset_summary["shape"][3])
    mask = np.fromfunction(lambda y, x: ((x + y) % 2).astype(np.uint8), (height, width), dtype=int)

    result = MaskService().write_water_mask(
        output_uri=written_uri,
        mask=mask,
        acquisition_datetime="2026-01-01T10:55:01Z",
        model_version="test",
        input_bands=["B04", "B03", "B02", "B08"],
        metadata={"source_scene_id": scene_id},
    )

    group = zarr.open_group(written_uri, mode="r")
    assert "masks" in group
    assert "water" in group["masks"]
    assert "water_probability" in group["masks"]
    water = group["masks"]["water"]
    water_probability = group["masks"]["water_probability"]
    assert tuple(water.shape) == (1, height, width)
    assert tuple(water_probability.shape) == (1, height, width)
    assert str(water.dtype) == "uint8"
    assert str(water_probability.dtype) == "float32"
    assert set(np.unique(water[0, :, :]).tolist()).issubset({0, 1})
    assert water.attrs["mask_name"] == "water"
    assert water.attrs["mask_path"] == "masks/water"
    assert water.attrs["input_bands"] == ["B04", "B03", "B02", "B08"]
    assert group.attrs["water_mask_path"] == "masks/water"
    assert group.attrs["water_mask_probability_path"] == "masks/water_probability"
    assert group.attrs["water_mask_written"] is True
    assert result["shape"] == [1, height, width]
    assert result["classes"] == {"0": "non-water", "1": "water"}
    assert summary["scene_id"] == scene_id.removesuffix(".SAFE")
