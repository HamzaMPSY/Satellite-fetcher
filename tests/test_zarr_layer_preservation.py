from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("rasterio")
pytest.importorskip("zarr")

import rasterio
from rasterio.transform import from_origin
import zarr

from nimbuschain_zarr_service.core import _create_array_compat
from nimbuschain_zarr_service.service import ZarrConversionService


def _labels(array) -> tuple[str, ...]:
    values = np.asarray(array[:]).tolist()
    return tuple(
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        for value in values
    )


@dataclass(frozen=True)
class LayerCase:
    name: str
    provider: str
    collection: str
    product_type: str
    scene_id: str
    expected_band_names: tuple[str, ...]
    expected_ancillary_names: tuple[str, ...]
    expected_pixel_size: tuple[float, float] | None


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


def test_create_array_compat_handles_legacy_zarr_without_data_keyword() -> None:
    class LegacyArray:
        def __init__(self, shape: tuple[int, ...], dtype: np.dtype):
            self.data = np.zeros(shape, dtype=dtype)

        def __setitem__(self, key: object, value: object) -> None:
            self.data[key] = value

    class LegacyGroup:
        def __init__(self) -> None:
            self.created_kwargs: dict[str, object] = {}
            self.array: LegacyArray | None = None

        def create_array(self, name: str, **kwargs: object) -> LegacyArray:
            if "data" in kwargs:
                raise TypeError("Group.create_array() got an unexpected keyword argument 'data'")
            self.created_kwargs = {"name": name, **kwargs}
            self.array = LegacyArray(
                tuple(int(v) for v in kwargs["shape"]),  # type: ignore[index]
                np.dtype(kwargs["dtype"]),
            )
            return self.array

    group = LegacyGroup()
    payload = np.asarray(["B02", "B03"], dtype="<U3")

    array = _create_array_compat(group, "band", data=payload, overwrite=True)

    assert group.created_kwargs["name"] == "band"
    assert group.created_kwargs["shape"] == payload.shape
    assert group.created_kwargs["dtype"] == payload.dtype
    assert group.created_kwargs["overwrite"] is True
    assert np.array_equal(array.data, payload)


def _build_s2_bundle(root: Path, *, scene_id: str, l1c: bool) -> Path:
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

    sixty_meter_codes = ("B01", "B09", "B10") if l1c else ("B01", "B09")
    for index, code in enumerate(sixty_meter_codes, start=20):
        _write_raster(
            img_root / "R60m" / f"T31TDN_20260101T105501_{code}_60m.tif",
            shape=(1, 1),
            pixel_size=60.0,
            value=index,
        )

    if not l1c:
        for index, layer in enumerate(("SCL", "AOT", "WVP"), start=30):
            resolution = 20.0 if layer == "SCL" else (10.0 if layer == "AOT" else 60.0)
            shape = (3, 3) if resolution == 20.0 else ((6, 6) if resolution == 10.0 else (1, 1))
            folder = "R20m" if resolution == 20.0 else ("R10m" if resolution == 10.0 else "R60m")
            _write_raster(
                img_root / folder / f"T31TDN_20260101T105501_{layer}_{int(resolution)}m.tif",
                shape=shape,
                pixel_size=resolution,
                value=index,
            )

    return safe_root


def _build_sen2like_bundle(root: Path, scene_id: str) -> Path:
    safe_root = (
        root
        / "S2L_MSIL2F_20260101T105501_N0500_R000_T31TDN_20260101T105501.SAFE"
    )
    safe_root.mkdir(parents=True, exist_ok=True)
    (safe_root / "manifest.safe").write_text("SAFE", encoding="utf-8")
    img_root = (
        safe_root
        / "GRANULE"
        / "L2F_T31TDN_A20260101T105501"
        / "IMG_DATA"
        / "RESOLUTION_10M"
    )
    for index, code in enumerate(("B02", "B03", "B04", "B08", "B11", "B12"), start=1):
        _write_raster(
            img_root / f"T31TDN_20260101T105501_{code}_10m.TIF",
            shape=(6, 6),
            pixel_size=10.0,
            value=index,
        )
    _write_raster(
        img_root / "T31TDN_20260101T105501_VALIDITY_MASK_10m.TIF",
        shape=(6, 6),
        pixel_size=10.0,
        value=1,
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
                'SUN_ELEVATION = 45.0',
                *[
                    f"REFLECTANCE_MULT_BAND_{index} = {'2.0000E-05' if l1 else '2.7500E-05'}"
                    for index in range(1, 8 if not l1 else 10)
                ],
                *[
                    f"REFLECTANCE_ADD_BAND_{index} = {'-0.100000' if l1 else '-0.200000'}"
                    for index in range(1, 8 if not l1 else 10)
                ],
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
        for index, layer in enumerate(("QA_PIXEL", "QA_RADSAT", "SAA"), start=50):
            _write_raster(
                bundle_root / f"{scene_id}_{layer}.TIF",
                shape=(4, 4),
                pixel_size=30.0,
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
        for index, layer in enumerate(("QA_PIXEL", "QA_RADSAT", "ST_QA", "SZA"), start=60):
            _write_raster(
                bundle_root / f"{scene_id}_{layer}.TIF",
                shape=(4, 4),
                pixel_size=30.0,
                value=index,
            )

    return bundle_root


def _build_s1_bundle(root: Path, *, scene_id: str) -> Path:
    safe_root = root / scene_id
    safe_root.mkdir(parents=True, exist_ok=True)
    (safe_root / "manifest.safe").write_text("SAFE", encoding="utf-8")
    measurement_root = safe_root / "measurement"
    annotation_root = safe_root / "annotation"
    for index, pol in enumerate(("VV", "VH"), start=1):
        _write_raster(
            measurement_root / f"s1a_measurement_{pol.lower()}.tiff",
            shape=(5, 4),
            pixel_size=10.0,
            value=index,
        )
    _write_raster(
        annotation_root / "s1a_inc_angle.tiff",
        shape=(5, 4),
        pixel_size=10.0,
        value=20,
    )
    return safe_root


@pytest.fixture()
def zarr_service() -> ZarrConversionService:
    return ZarrConversionService()


@pytest.mark.parametrize(
    ("case", "builder"),
    [
        (
            LayerCase(
                name="sentinel2_l1c",
                provider="copernicus",
                collection="SENTINEL-2",
                product_type="S2MSI1C",
                scene_id="S2A_MSIL1C_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE",
                expected_band_names=(
                    "B01",
                    "B02",
                    "B03",
                    "B04",
                    "B05",
                    "B06",
                    "B07",
                    "B08",
                    "B8A",
                    "B09",
                    "B10",
                    "B11",
                    "B12",
                ),
                expected_ancillary_names=(),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_s2_bundle(root, scene_id=scene_id, l1c=True),
        ),
        (
            LayerCase(
                name="sentinel2_l2a",
                provider="copernicus",
                collection="SENTINEL-2",
                product_type="S2MSI2A",
                scene_id="S2A_MSIL2A_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE",
                expected_band_names=(
                    "B01",
                    "B02",
                    "B03",
                    "B04",
                    "B05",
                    "B06",
                    "B07",
                    "B08",
                    "B8A",
                    "B09",
                    "B11",
                    "B12",
                ),
                expected_ancillary_names=("SCL", "AOT", "WVP"),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_s2_bundle(root, scene_id=scene_id, l1c=False),
        ),
        (
            LayerCase(
                name="sen2like_l2f",
                provider="copernicus",
                collection="SENTINEL-2",
                product_type="S2MSI2A",
                scene_id="LC08_L1TP_190026_20260101_20260101_02_T1",
                expected_band_names=("B02", "B03", "B04", "B08", "B11", "B12"),
                expected_ancillary_names=("VALIDITY_MASK",),
                expected_pixel_size=(10.0, 10.0),
            ),
            _build_sen2like_bundle,
        ),
        (
            LayerCase(
                name="landsat_l1",
                provider="usgs",
                collection="landsat_ot_c2_l1",
                product_type="L1TP",
                scene_id="LC08_L1TP_190026_20260101_20260101_02_T1",
                expected_band_names=(
                    "B1",
                    "B2",
                    "B3",
                    "B4",
                    "B5",
                    "B6",
                    "B7",
                    "B8",
                    "B9",
                    "B10",
                    "B11",
                ),
                expected_ancillary_names=("QA_PIXEL", "QA_RADSAT", "SAA"),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_landsat_bundle(root, scene_id=scene_id, l1=True),
        ),
        (
            LayerCase(
                name="landsat_l2",
                provider="usgs",
                collection="landsat_ot_c2_l2",
                product_type="L2SP",
                scene_id="LC08_L2SP_190026_20260101_20260105_02_T1",
                expected_band_names=(
                    "SR_B1",
                    "SR_B2",
                    "SR_B3",
                    "SR_B4",
                    "SR_B5",
                    "SR_B6",
                    "SR_B7",
                    "ST_B10",
                ),
                expected_ancillary_names=("QA_PIXEL", "QA_RADSAT", "ST_QA", "SZA"),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_landsat_bundle(root, scene_id=scene_id, l1=False),
        ),
        (
            LayerCase(
                name="sentinel1_grd",
                provider="copernicus",
                collection="SENTINEL-1",
                product_type="GRD",
                scene_id="S1A_IW_GRDH_1SDV_20260101T000000_20260101T000025_000001_000001_ABCD.SAFE",
                expected_band_names=("VV", "VH"),
                expected_ancillary_names=("INC_ANGLE",),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_s1_bundle(root, scene_id=scene_id),
        ),
        (
            LayerCase(
                name="sentinel1_slc",
                provider="copernicus",
                collection="SENTINEL-1",
                product_type="IW_SLC__1S",
                scene_id="S1A_IW_SLC__1SDV_20260101T000000_20260101T000025_000001_000001_ABCD.SAFE",
                expected_band_names=("VV", "VH"),
                expected_ancillary_names=("INC_ANGLE",),
                expected_pixel_size=(10.0, 10.0),
            ),
            lambda root, scene_id: _build_s1_bundle(root, scene_id=scene_id),
        ),
    ],
    ids=lambda item: item.name if isinstance(item, LayerCase) else None,
)
def test_converter_preserves_exact_native_layers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    zarr_service: ZarrConversionService,
    case: LayerCase,
    builder,
) -> None:
    monkeypatch.setenv("NIMBUS_ZARR_TARGET_SHAPE", "native")
    raw_root = builder(tmp_path, case.scene_id)
    output_uri = str((tmp_path / f"{case.name}.zarr").resolve())

    written_uri, data_family, normalization_summary, dataset_summary = zarr_service.convert(
        provider=case.provider,
        collection=case.collection,
        scene_id=case.scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type=case.product_type,
    )

    assert data_family in {"optical", "sar"}
    assert Path(written_uri).exists(), f"{case.name}: Zarr store was not created"
    assert tuple(dataset_summary["band_names"]) == case.expected_band_names, (
        f"{case.name}: imagery layers differ from expected source-exact set"
    )
    assert tuple(dataset_summary.get("ancillary_layer_names") or []) == case.expected_ancillary_names, (
        f"{case.name}: ancillary layers differ from expected source-exact set"
    )
    assert normalization_summary["validation"]["imagery_layer_count"] == len(case.expected_band_names)
    assert normalization_summary["validation"]["ancillary_layer_count"] == len(case.expected_ancillary_names)
    if case.expected_pixel_size is not None:
        assert tuple(dataset_summary["pixel_size"]) == case.expected_pixel_size
    assert dataset_summary.get("quadkey_schema_version") == 1
    assert dataset_summary.get("quadkey_coverage_mode") == "bbox_intersection"
    assert isinstance(dataset_summary.get("quadkey_zoom_index"), int)
    assert isinstance(dataset_summary.get("quadkeys_index"), list)
    assert dataset_summary.get("quadkeys_index"), f"{case.name}: expected non-empty quadkeys_index"

    group = zarr.open_group(str(written_uri), mode="r", zarr_format=2)
    assert _labels(group["band"]) == case.expected_band_names
    assert tuple(group["imagery"].shape) == tuple(dataset_summary["shape"])
    if case.expected_pixel_size is not None:
        assert tuple(group.attrs.get("reference_pixel_size") or []) == case.expected_pixel_size
    assert group.attrs["quadkey_schema_version"] == 1
    assert group.attrs["quadkey_coverage_mode"] == "bbox_intersection"
    assert isinstance(group.attrs["quadkeys_index"], list)
    assert group.attrs["quadkeys_index"], f"{case.name}: expected non-empty root quadkeys_index"
    assert dict(group.attrs.get("band_metadata") or {})
    if case.provider == "usgs":
        radiometric_metadata = dict(group.attrs.get("radiometric_metadata") or {})
        assert radiometric_metadata.get("bands")
    by_time = group.attrs["quadkeys_by_time"]
    assert isinstance(by_time, dict)
    assert len(by_time) == 1
    (_, time_payload), = by_time.items()
    assert time_payload["quadkeys_index"] == group.attrs["quadkeys_index"]

    if case.expected_ancillary_names:
        assert "ancillary" in group
        assert "ancillary_layer" in group
        assert _labels(group["ancillary_layer"]) == case.expected_ancillary_names
        assert tuple(group["ancillary"].shape) == tuple(dataset_summary["ancillary_shape"])
        assert dict(group.attrs.get("ancillary_metadata") or {})
    else:
        assert "ancillary" not in group


def test_converter_defaults_to_exact_512_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    zarr_service: ZarrConversionService,
) -> None:
    monkeypatch.delenv("NIMBUS_ZARR_TARGET_SHAPE", raising=False)
    scene_id = "S2A_MSIL2A_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE"
    raw_root = _build_s2_bundle(tmp_path, scene_id=scene_id, l1c=False)
    output_uri = str((tmp_path / "default_512.zarr").resolve())

    written_uri, _data_family, _normalization_summary, dataset_summary = zarr_service.convert(
        provider="copernicus",
        collection="SENTINEL-2",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="S2MSI2A",
    )

    assert dataset_summary["shape"][-2:] == [512, 512]
    assert dataset_summary["ancillary_shape"][-2:] == [512, 512]
    group = zarr.open_group(str(written_uri), mode="r", zarr_format=2)
    assert tuple(group["imagery"].shape[-2:]) == (512, 512)
    assert tuple(group["ancillary"].shape[-2:]) == (512, 512)
    assert group.attrs["output_shape_policy"] == "exact_target_shape"
    assert group.attrs["target_shape"] == [512, 512]
    assert _labels(group["band"]) == (
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B09",
        "B11",
        "B12",
    )
    assert group["band"].dtype.kind == "S"
    assert group["time"].dtype.kind == "S"


def test_landsat_converter_accepts_strict_satellite_prefixed_product_type(
    tmp_path: Path,
    zarr_service: ZarrConversionService,
) -> None:
    scene_id = "LC09_L1TP_190026_20260101_20260101_02_T1"
    raw_root = _build_landsat_bundle(tmp_path, scene_id=scene_id, l1=True)
    output_uri = str((tmp_path / "landsat_strict_prefixed.zarr").resolve())

    written_uri, _data_family, normalization_summary, _dataset_summary = zarr_service.convert(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        scene_id=scene_id,
        raw_uri=str(raw_root),
        output_uri=output_uri,
        product_type="9L1TP",
    )

    assert Path(written_uri).exists()
    assert normalization_summary["product_type"] == "L1TP"
    assert normalization_summary["product_type_short"] == "9L1TP"
    assert normalization_summary["grid"]["reference_band"]
    assert normalization_summary["grid"]["width"] > 0
    assert normalization_summary["grid"]["height"] > 0
    group = zarr.open_group(str(written_uri), mode="r")
    assert group.attrs["product_type"] == "L1TP"
    assert group.attrs["product_type_short"] == "9L1TP"
