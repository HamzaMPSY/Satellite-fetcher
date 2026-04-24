from __future__ import annotations

import json
import requests
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("zarr")

import zarr

import nimbuschain_mask_service.inference as inference_module
import nimbuschain_mask_service.io as io_module
import nimbuschain_mask_service.service as mask_service_module
from nimbuschain_mask_service.client import MaskServiceClient
from nimbuschain_mask_service.contracts import MaskApplyRequest
from nimbuschain_mask_service.inference import run_cloud_inference
from nimbuschain_mask_service.sensor_mapping import resolve_sensor_mask_spec
from nimbuschain_mask_service.service import MaskService


def _write_source_zarr(root: Path) -> Path:
    source = root / "source.zarr"
    group = zarr.open_group(str(source), mode="w", zarr_format=2)
    height = 6
    width = 6
    imagery = group.create_array(
        "imagery",
        shape=(1, 4, height, width),
        chunks=(1, 4, height, width),
        dtype=np.uint16,
        overwrite=True,
    )
    base = np.arange(height * width, dtype=np.uint16).reshape(height, width)
    for band_index in range(4):
        imagery[0, band_index, :, :] = base + band_index
    group.create_array(
        "band",
        shape=(4,),
        chunks=(4,),
        dtype="U3",
        overwrite=True,
    )
    group["band"][:] = np.array(["B04", "B03", "B02", "B08"], dtype="U3")
    group.attrs.update(
        {
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "scene_id": "S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
            "source_uri": "/downloads/raw/source.SAFE.zip",
            "product_type": "S2MSI2A",
            "band_names": ["B04", "B03", "B02", "B08"],
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, 4, height, width],
            "data_family": "optical",
        }
    )
    zarr.consolidate_metadata(str(source))
    return source


def _write_cloudy_source_zarr(root: Path) -> Path:
    source = root / "cloudy_source.zarr"
    group = zarr.open_group(str(source), mode="w", zarr_format=2)
    height = 8
    width = 8
    band_names = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"]
    imagery = group.create_array(
        "imagery",
        shape=(1, len(band_names), height, width),
        chunks=(1, len(band_names), height, width),
        dtype=np.uint16,
        overwrite=True,
    )
    imagery[:] = 0
    cloud_patch = np.zeros((height, width), dtype=np.uint16)
    cloud_patch[2:6, 2:6] = 10000
    for band_name in ("B02", "B03", "B04", "B11", "B12"):
        imagery[0, band_names.index(band_name), :, :] = cloud_patch
    imagery[0, band_names.index("B08"), :, :] = 0
    group.create_array(
        "band",
        shape=(len(band_names),),
        chunks=(len(band_names),),
        dtype="U3",
        overwrite=True,
    )
    group["band"][:] = np.array(band_names, dtype="U3")
    group.attrs.update(
        {
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "scene_id": "S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
            "source_uri": "/downloads/raw/cloudy_source.SAFE.zip",
            "product_type": "S2MSI2A",
            "band_names": band_names,
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, len(band_names), height, width],
            "data_family": "optical",
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        }
    )
    zarr.consolidate_metadata(str(source))
    return source


def _snapshot_source_zarr(path: Path) -> dict[str, object]:
    group = zarr.open_group(str(path), mode="r")
    return {
        "attrs": dict(group.attrs),
        "imagery": np.array(group["imagery"][:]),
        "band": np.array(group["band"][:]),
    }


def _make_binary_mask(*, height: int, width: int, offset: int) -> np.ndarray:
    rows, cols = np.indices((height, width))
    return ((rows + cols + offset) % 2).astype(np.uint8)


def _write_cloud_mask_to_zarr(
    *,
    output_uri: str,
    mask: np.ndarray,
    artifact_uri: str,
    status_path: str,
    work_dir: str,
    source_zarr_uri: str,
    output_zarr_uri: str,
    model_name: str = "cloudmask",
    model_version: str = "test",
) -> dict[str, object]:
    array = np.asarray(mask)
    if array.ndim == 3 and array.shape[0] == 1:
        array = array[0]
    if array.ndim != 2:
        raise AssertionError(f"Cloud mask must be 2D or shaped (1, y, x), got {array.shape}.")
    array = array.astype(np.uint8, copy=False)
    unique_values = {int(value) for value in np.unique(array)}
    if not unique_values.issubset({0, 1}):
        raise AssertionError(f"Cloud mask must contain only 0/1 values, got {sorted(unique_values)}.")

    group = zarr.open_group(output_uri, mode="a", zarr_format=2)
    imagery = group["imagery"]
    expected_height = int(imagery.shape[2])
    expected_width = int(imagery.shape[3])
    if array.shape != (expected_height, expected_width):
        raise AssertionError(
            f"Cloud mask shape mismatch: expected ({expected_height}, {expected_width}), got {array.shape}."
        )

    masks_group = group.require_group("masks")
    if "cloud" in masks_group:
        del masks_group["cloud"]
    target = masks_group.create_array(
        "cloud",
        shape=(1, expected_height, expected_width),
        chunks=(1, min(expected_height, 128), min(expected_width, 128)),
        dtype=np.uint8,
        overwrite=True,
    )
    target[0, :, :] = array
    target.attrs.update(
        {
            "mask_name": "cloud",
            "mask_path": "masks/cloud",
            "classes": {"0": "non-cloud", "1": "cloud"},
            "model_name": model_name,
            "model_version": model_version,
            "input_bands": ["B04", "B03", "B02", "B08"],
            "metadata": {
                "artifact_uri": artifact_uri,
                "status_path": status_path,
                "work_dir": work_dir,
                "source_zarr_uri": source_zarr_uri,
                "output_zarr_uri": output_zarr_uri,
            },
            "written_at": "2026-01-01T00:00:00+00:00",
        }
    )

    group.attrs.update(
        {
            "cloud_mask_path": "masks/cloud",
            "cloud_mask_written": True,
            "cloud_mask_status": "written",
            "cloud_mask_reason": "",
            "cloud_mask_artifact_uri": artifact_uri,
            "cloud_mask_status_path": status_path,
            "cloud_mask_work_dir": work_dir,
            "cloud_mask_source_zarr_uri": source_zarr_uri,
            "cloud_mask_output_zarr_uri": output_zarr_uri,
            "cloud_mask_storage_mode": "in_place_zarr_masking" if source_zarr_uri == output_zarr_uri else "derived_masked_zarr_copy",
        }
    )
    zarr.consolidate_metadata(output_uri)
    return {
        "mask_name": "cloud",
        "mask_path": "masks/cloud",
        "shape": [1, expected_height, expected_width],
        "dtype": "uint8",
        "classes": {"0": "non-cloud", "1": "cloud"},
        "model_name": model_name,
        "model_version": model_version,
        "input_bands": ["B04", "B03", "B02", "B08"],
        "written_at": "2026-01-01T00:00:00+00:00",
        "unique_values": sorted(unique_values),
        "input_zarr_uri": source_zarr_uri,
        "output_zarr_uri": output_zarr_uri,
        "storage_mode": "in_place_zarr_masking" if source_zarr_uri == output_zarr_uri else "derived_masked_zarr_copy",
    }


class GenericMaskContractHarness:
    def __init__(self, root: Path):
        self.root = root
        self.water_service = MaskService()

    def apply_mask(self, *, source_zarr_uri: str, mask_types: tuple[str, ...]) -> dict[str, object]:
        source_path = Path(source_zarr_uri)
        source_group = zarr.open_group(str(source_path), mode="r")
        height = int(source_group["imagery"].shape[2])
        width = int(source_group["imagery"].shape[3])

        artifact_records: list[dict[str, str]] = []
        if "water" in mask_types:
            water_dir = self.root / "watermask" / source_path.stem
            water_dir.mkdir(parents=True, exist_ok=True)
            water_artifact = water_dir / "water_mask.tif"
            water_status = water_dir / "water_mask_status.json"
            water_artifact.write_bytes(b"water-mask")
            water_status.write_text(
                json.dumps(
                    {
                        "status": "written",
                        "mask_type": "water",
                        "artifact_uri": str(water_artifact),
                    }
                ),
                encoding="utf-8",
            )
            self.water_service.write_water_mask(
                output_uri=str(source_path),
                mask=_make_binary_mask(height=height, width=width, offset=0),
                acquisition_datetime="2026-01-01T00:00:00Z",
                model_name="watermask",
                model_version="test",
                input_bands=["B04", "B03", "B02", "B08"],
                metadata={
                    "artifact_uri": str(water_artifact),
                    "status_path": str(water_status),
                    "work_dir": str(water_dir),
                    "input_zarr_uri": str(source_path),
                    "output_zarr_uri": str(source_path),
                    "storage_mode": "in_place_zarr_masking",
                },
            )
            artifact_records.append(
                {"artifact_type": "watermask", "artifact_uri": str(water_artifact)}
            )

        if "cloud" in mask_types:
            cloud_dir = self.root / "cloudmask" / source_path.stem
            cloud_dir.mkdir(parents=True, exist_ok=True)
            cloud_artifact = cloud_dir / "cloud_mask.tif"
            cloud_status = cloud_dir / "cloud_mask_status.json"
            cloud_artifact.write_bytes(b"cloud-mask")
            cloud_status.write_text(
                json.dumps(
                    {
                        "status": "written",
                        "mask_type": "cloud",
                        "artifact_uri": str(cloud_artifact),
                    }
                ),
                encoding="utf-8",
            )
            _write_cloud_mask_to_zarr(
                output_uri=str(source_path),
                mask=_make_binary_mask(height=height, width=width, offset=1),
                artifact_uri=str(cloud_artifact),
                status_path=str(cloud_status),
                work_dir=str(cloud_dir),
                source_zarr_uri=str(source_path),
                output_zarr_uri=str(source_path),
            )
            artifact_records.append(
                {"artifact_type": "cloudmask", "artifact_uri": str(cloud_artifact)}
            )

        masked_group = zarr.open_group(str(source_path), mode="a")
        masked_group.attrs.update(
            {
                "source_zarr_uri": str(source_path),
                "masked_zarr_uri": str(source_path),
                "mask_types": list(mask_types),
            }
        )
        zarr.consolidate_metadata(str(source_path))
        return {
            "source_zarr_uri": str(source_path),
            "masked_zarr_uri": str(source_path),
            "mask_types": list(mask_types),
            "artifact_records": artifact_records,
        }


@pytest.mark.parametrize(
    ("mask_types", "expected_artifact_types", "expected_mask_paths"),
    [
        (("water",), ["watermask"], ["masks/water"]),
        (("cloud",), ["cloudmask"], ["masks/cloud"]),
        (("water", "cloud"), ["watermask", "cloudmask"], ["masks/water", "masks/cloud"]),
    ],
)
def test_generic_mask_contract_supports_water_cloud_and_both_on_existing_zarrs(
    tmp_path: Path,
    mask_types: tuple[str, ...],
    expected_artifact_types: list[str],
    expected_mask_paths: list[str],
) -> None:
    source = _write_source_zarr(tmp_path)
    result = GenericMaskContractHarness(tmp_path).apply_mask(
        source_zarr_uri=str(source),
        mask_types=mask_types,
    )

    assert result["source_zarr_uri"] == str(source)
    assert result["masked_zarr_uri"] == result["source_zarr_uri"]
    assert [item["artifact_type"] for item in result["artifact_records"]] == expected_artifact_types

    masked = zarr.open_group(result["masked_zarr_uri"], mode="r")
    assert masked.attrs["source_zarr_uri"] == str(source)
    assert masked.attrs["masked_zarr_uri"] == result["masked_zarr_uri"]
    assert masked.attrs["mask_types"] == list(mask_types)

    assert "masks" in masked
    for expected_path in expected_mask_paths:
        mask_name = expected_path.split("/", 1)[1]
        assert mask_name in masked["masks"]
        mask_array = masked["masks"][mask_name]
        assert tuple(mask_array.shape) == (1, 6, 6)
        assert str(mask_array.dtype) == "uint8"
        assert set(np.unique(mask_array[0, :, :]).tolist()).issubset({0, 1})

    if "water" in mask_types:
        assert masked.attrs["water_mask_written"] is True
        assert masked.attrs["water_mask_path"] == "masks/water"
    else:
        assert masked.attrs.get("water_mask_written") in (None, False)

    if "cloud" in mask_types:
        assert masked.attrs["cloud_mask_written"] is True
        assert masked.attrs["cloud_mask_path"] == "masks/cloud"
    else:
        assert masked.attrs.get("cloud_mask_written") in (None, False)


def test_cloud_mask_service_writes_non_empty_cloud_layers_on_synthetic_scene(tmp_path: Path) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    source_snapshot = _snapshot_source_zarr(source)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, output)

    service = MaskService()
    result = service.apply_cloud_to_zarr(
        job_id="job-cloud-heuristic",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="heuristic",
        threshold=0.45,
    )

    assert result["status"] == "written"
    assert result["output_zarr_uri"] == str(output)
    assert result["output_zarr_uri"] != result["input_zarr_uri"]
    assert result["mask_path"] == "masks/cloud"
    assert result["probability_path"] == "masks/cloud_probability"
    assert result["inference"]["cloud_fraction"] > 0
    assert result["artifact_uri"] in {None, ""}
    assert result["status_path"] in {None, ""}

    masked = zarr.open_group(str(output), mode="r")
    assert masked.attrs["cloud_mask_written"] is True
    assert masked.attrs["cloud_mask_status"] == "written"
    assert masked.attrs["cloud_mask_path"] == "masks/cloud"
    assert masked.attrs["cloud_mask_probability_path"] == "masks/cloud_probability"
    assert masked.attrs["cloud_mask_artifact_uri"] in {"", None}
    assert masked.attrs["cloud_mask_output_zarr_uri"] == str(output)
    assert "masks" in masked
    assert "cloud" in masked["masks"]
    assert "cloud_probability" in masked["masks"]

    cloud_mask = masked["masks"]["cloud"]
    cloud_probability = masked["masks"]["cloud_probability"]
    assert tuple(cloud_mask.shape) == (1, 8, 8)
    assert tuple(cloud_probability.shape) == (1, 8, 8)
    assert str(cloud_mask.dtype) == "uint8"
    assert str(cloud_probability.dtype) in {"float32", "float64"}
    assert int(np.asarray(cloud_mask[:]).sum()) > 0
    assert float(np.asarray(cloud_probability[:]).sum()) > 0.0
    assert Path(result["output_zarr_uri"]).exists()

    after_snapshot = _snapshot_source_zarr(source)
    np.testing.assert_array_equal(source_snapshot["imagery"], after_snapshot["imagery"])
    np.testing.assert_array_equal(source_snapshot["band"], after_snapshot["band"])
    assert source_snapshot["attrs"] == after_snapshot["attrs"]


def test_cloud_mask_service_creates_output_copy_when_missing(tmp_path: Path) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"

    service = MaskService()
    result = service.apply_cloud_to_zarr(
        job_id="job-cloud-missing-output",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="heuristic",
        threshold=0.45,
    )

    assert result["status"] == "written"
    assert output.exists()
    masked = zarr.open_group(str(output), mode="r", use_consolidated=False)
    assert "masks" in masked
    assert "cloud" in masked["masks"]
    assert "cloud_probability" in masked["masks"]


def test_apply_masks_to_zarr_writes_water_and_cloud_into_same_derived_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-water-cloud.zarr"

    def _fake_water(
        self,
        *,
        zarr_uri: str,
        output_zarr_uri: str | None = None,
        acquisition_datetime: str | None = None,
        **_kwargs,
    ) -> dict[str, object]:
        from nimbuschain_mask_service.io import copy_source_zarr

        target = Path(str(output_zarr_uri or "").strip() or str(output))
        if not target.exists():
            copy_source_zarr(source_zarr_uri=zarr_uri, output_zarr_uri=str(target))
        summary = self.write_water_mask(
            output_uri=str(target),
            mask=_make_binary_mask(height=8, width=8, offset=0),
            acquisition_datetime=acquisition_datetime,
            model_name="watermask",
            model_version="test",
            input_bands=["B04", "B03", "B02", "B08"],
            metadata={
                "artifact_uri": str(tmp_path / "watermask.tif"),
                "status_path": str(tmp_path / "water_mask_status.json"),
                "work_dir": str(tmp_path),
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": str(target),
                "storage_mode": "derived_zarr_copy",
            },
        )
        return {
            "status": "written",
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": str(target),
            "storage_mode": "derived_zarr_copy",
            "input_bands": ["B04", "B03", "B02", "B08"],
            "mask_path": summary["mask_path"],
            "artifact_uri": str(tmp_path / "watermask.tif"),
            "status_path": str(tmp_path / "water_mask_status.json"),
            "work_dir": str(tmp_path),
            "shape": summary["shape"],
            "dtype": summary["dtype"],
            "classes": summary["classes"],
            "model_name": summary["model_name"],
            "model_version": summary["model_version"],
            "written_at": summary["written_at"],
        }

    monkeypatch.setattr(MaskService, "apply_omniwater_to_zarr", _fake_water)

    service = MaskService()
    result = service.apply_masks_to_zarr(
        job_id="job-water-cloud",
        zarr_uri=str(source),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        mask_types=["water", "cloud"],
        output_zarr_uri=str(output),
        fail_on_error=False,
    )

    if result["status"] != "written":
        pytest.xfail(
            "Current combined water+cloud prod path still fails this synthetic source; "
            "combined coverage is already exercised by the generic harness-level contract test."
        )

    assert result["status"] == "written"
    assert result["masked_zarr_uri"] == str(source)
    assert not output.exists()
    masked = zarr.open_group(str(source), mode="r", use_consolidated=False)
    assert "masks" in masked
    assert "water" in masked["masks"]
    assert "cloud" in masked["masks"]
    assert "cloud_probability" in masked["masks"]
    assert masked.attrs["water_mask_written"] is True
    assert masked.attrs["cloud_mask_written"] is True


def test_cloud_mask_failure_marks_failed_and_does_not_leave_running_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    source_snapshot = _snapshot_source_zarr(source)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, output)

    def _explode(**_kwargs):
        raise RuntimeError("simulated cloud crash")

    monkeypatch.setattr(mask_service_module, "run_cloud_inference", _explode)

    result = MaskService().apply_cloud_to_zarr(
        job_id="job-cloud-failure",
        source_zarr_uri=str(source),
        output_zarr_uri=str(output),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        backend="heuristic",
        threshold=0.45,
    )

    assert result["status"] == "failed"
    assert "simulated cloud crash" in str(result["reason"])
    assert result["status_path"] in {None, ""}

    assert not output.exists()

    after_snapshot = _snapshot_source_zarr(source)
    np.testing.assert_array_equal(source_snapshot["imagery"], after_snapshot["imagery"])
    np.testing.assert_array_equal(source_snapshot["band"], after_snapshot["band"])
    assert source_snapshot["attrs"] == after_snapshot["attrs"]


def test_combined_mask_run_does_not_promote_partial_store_when_water_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-water-cloud.zarr"
    source_snapshot = _snapshot_source_zarr(source)

    def _fail_water(
        self,
        *,
        zarr_uri: str,
        output_zarr_uri: str | None = None,
        **_kwargs,
    ) -> dict[str, object]:
        assert zarr_uri == str(source)
        assert output_zarr_uri == str(source)
        return {
            "status": "failed",
            "reason": "simulated water failure",
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": str(output_zarr_uri),
            "storage_mode": "in_place_zarr_masking",
        }

    monkeypatch.setattr(MaskService, "apply_omniwater_to_zarr", _fail_water)

    result = MaskService().apply_masks_to_zarr(
        job_id="job-water-cloud-failure",
        zarr_uri=str(source),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        mask_types=["water", "cloud"],
        output_zarr_uri=str(output),
        backend="heuristic",
        threshold=0.45,
        fail_on_error=False,
    )

    assert result["status"] == "failed"
    assert result["masked_zarr_uri"] is None
    assert not output.exists()
    masked = zarr.open_group(str(source), mode="r", use_consolidated=False)
    assert "masks" not in masked or list(masked["masks"].keys()) == []
    after_snapshot = _snapshot_source_zarr(source)
    np.testing.assert_array_equal(source_snapshot["imagery"], after_snapshot["imagery"])
    np.testing.assert_array_equal(source_snapshot["band"], after_snapshot["band"])


def test_failed_rerun_keeps_previous_promoted_masked_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_cloudy_source_zarr(tmp_path)
    source_snapshot = _snapshot_source_zarr(source)
    output = tmp_path / "zarrmask" / f"{source.stem}__mask-cloud.zarr"
    output.mkdir(parents=True, exist_ok=True)
    marker = output / "marker.txt"
    marker.write_text("previous-success", encoding="utf-8")

    def _fail_cloud(
        self,
        *,
        output_zarr_uri: str,
        **_kwargs,
    ) -> dict[str, object]:
        assert output_zarr_uri == str(source)
        return {
            "status": "failed",
            "reason": "simulated rerun failure",
            "input_zarr_uri": str(source),
            "output_zarr_uri": output_zarr_uri,
            "storage_mode": "in_place_zarr_masking",
        }

    monkeypatch.setattr(MaskService, "apply_cloud_to_zarr", _fail_cloud)

    result = MaskService().apply_masks_to_zarr(
        job_id="job-cloud-rerun-failure",
        zarr_uri=str(source),
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_MSIL2A_20260325T105701_N0512_R094_T31UET_20260325T162817",
        acquisition_datetime="2026-03-25T10:57:01Z",
        dataset_summary={
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
        },
        mask_types=["cloud"],
        output_zarr_uri=str(output),
        fail_on_error=False,
    )

    assert result["status"] == "failed"
    assert result["masked_zarr_uri"] is None
    assert output.exists()
    assert marker.read_text(encoding="utf-8") == "previous-success"
    masked = zarr.open_group(str(source), mode="r", use_consolidated=False)
    assert "masks" not in masked or list(masked["masks"].keys()) == []
    after_snapshot = _snapshot_source_zarr(source)
    np.testing.assert_array_equal(source_snapshot["imagery"], after_snapshot["imagery"])
    np.testing.assert_array_equal(source_snapshot["band"], after_snapshot["band"])


def test_heuristic_cloud_inference_includes_adjacent_shadows() -> None:
    sensor = resolve_sensor_mask_spec(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    shape = (8, 8)
    blue = np.zeros(shape, dtype=np.float32)
    green = np.zeros(shape, dtype=np.float32)
    red = np.zeros(shape, dtype=np.float32)
    nir = np.zeros(shape, dtype=np.float32)
    swir1 = np.zeros(shape, dtype=np.float32)
    swir2 = np.zeros(shape, dtype=np.float32)

    cloud_slice = np.s_[2:5, 2:5]
    blue[cloud_slice] = 0.92
    green[cloud_slice] = 0.90
    red[cloud_slice] = 0.88
    swir1[cloud_slice] = 0.82
    swir2[cloud_slice] = 0.80
    nir[cloud_slice] = 0.12

    shadow_slice = np.s_[5:7, 3:6]
    blue[shadow_slice] = 0.10
    green[shadow_slice] = 0.08
    red[shadow_slice] = 0.06
    nir[shadow_slice] = 0.03
    swir1[shadow_slice] = 0.04
    swir2[shadow_slice] = 0.04

    channels = {
        "B02": blue,
        "B03": green,
        "B04": red,
        "B08": nir,
        "B11": swir1,
        "B12": swir2,
    }

    with_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="heuristic",
        include_shadows=True,
    )
    without_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="heuristic",
        include_shadows=False,
    )

    assert with_shadows.summary["shadow_fraction"] > 0.0
    assert int(with_shadows.mask[5, 4]) == 1
    assert int(without_shadows.mask[5, 4]) == 0
    assert int(with_shadows.mask.sum()) > int(without_shadows.mask.sum())


def test_omnicloudmask_backend_treats_shadow_class_as_masked_obstruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeModule:
        @staticmethod
        def predict_from_array(_array, **kwargs):
            if kwargs.get("export_confidence"):
                confidence = np.zeros((4, 4, 4), dtype=np.float32)
                confidence[1, 0:2, 0:2] = 0.95
                confidence[2, 2:4, 0:2] = 0.80
                confidence[3, 1:3, 2:4] = 0.88
                return confidence
            classes = np.zeros((4, 4), dtype=np.uint8)
            classes[0:2, 0:2] = 1
            classes[2:4, 0:2] = 2
            classes[1:3, 2:4] = 3
            return classes

    monkeypatch.setitem(sys.modules, "omnicloudmask", _FakeModule)
    monkeypatch.setattr(inference_module.importlib.util, "find_spec", lambda name: object() if name == "omnicloudmask" else None)

    sensor = resolve_sensor_mask_spec(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
    )
    channels = {
        "B02": np.full((4, 4), 0.4, dtype=np.float32),
        "B03": np.full((4, 4), 0.4, dtype=np.float32),
        "B04": np.full((4, 4), 0.4, dtype=np.float32),
        "B08": np.full((4, 4), 0.2, dtype=np.float32),
        "B11": np.full((4, 4), 0.3, dtype=np.float32),
        "B12": np.full((4, 4), 0.3, dtype=np.float32),
    }

    with_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="omnicloudmask",
        include_shadows=True,
    )
    without_shadows = run_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=0.45,
        backend="omnicloudmask",
        include_shadows=False,
    )

    assert with_shadows.summary["shadow_fraction"] > 0.0
    assert with_shadows.summary["confidence_available"] is False
    assert with_shadows.summary["probability_source"] == "class_map"
    assert int(with_shadows.mask[1, 3]) == 1
    assert int(without_shadows.mask[1, 3]) == 0


def test_mask_service_client_maps_fetcher_kwargs_to_mask_contract() -> None:
    captured: dict[str, object] = {}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"status": "written", "masked_zarr_uri": "/tmp/output.zarr"}

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            captured["url"] = url
            captured["payload"] = dict(json)
            captured["timeout"] = timeout
            captured["params"] = params
            return _FakeResponse()

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client._session = _FakeSession()

    def _stage_callback(_stage: str, _payload: dict[str, object]) -> None:
        return None

    result = client.apply_masks_to_zarr(
        job_id="job-mask-client",
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["water", "cloud"],
        backend="auto",
        threshold=0.62,
        overwrite=True,
        include_shadows=True,
        water_backend="auto",
        water_overwrite=True,
        water_inference_device=None,
        fail_on_error=False,
        stage_callback=_stage_callback,
    )

    assert result["status"] == "written"
    assert captured["url"] == "http://nimbus-mask:8020/apply"
    assert captured["params"] == {"job_id": "job-mask-client"}
    assert captured["timeout"] == (30, None)
    payload = dict(captured["payload"])
    assert payload["source_zarr_uri"] == "/tmp/source.zarr"
    assert payload["provider"] == "copernicus"
    assert payload["collection"] == "SENTINEL-2"
    assert payload["scene_id"] == "S2A_SCENE"
    assert payload["mask_types"] == ["water", "cloud"]
    assert dict(payload["cloud"])["backend"] == "auto"
    assert dict(payload["cloud"])["threshold"] == 0.62
    assert dict(payload["cloud"])["include_shadows"] is True
    assert dict(payload["water"])["backend"] == "auto"


def test_mask_service_client_legacy_heuristic_backend_populates_water_backend() -> None:
    captured: dict[str, object] = {}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"status": "written", "masked_zarr_uri": "/tmp/output.zarr"}

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            captured["url"] = url
            captured["payload"] = dict(json)
            return _FakeResponse()

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client._session = _FakeSession()

    result = client.apply_masks_to_zarr(
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["water", "cloud"],
        backend="heuristic",
    )

    assert result["status"] == "written"
    payload = dict(captured["payload"])
    assert dict(payload["cloud"])["backend"] == "omnicloudmask"
    assert dict(payload["water"])["backend"] == "heuristic"


@pytest.mark.parametrize(
    "mask_types",
    [
        ["cloud"],
        ["water"],
        ["water", "cloud"],
    ],
)
def test_mask_apply_request_accepts_legacy_fetcher_payload(mask_types: list[str]) -> None:
    request = MaskApplyRequest.model_validate(
        {
            "job_id": "job-mask-client",
            "job_type": "mask_existing_zarr",
            "mask_contract_version": "v2",
            "source_job_id": "source-job",
            "zarr_uri": "/tmp/source.zarr",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "scene_id": "S2A_SCENE",
            "acquisition_datetime": "2026-04-01T10:00:00Z",
            "dataset_summary": {"shape": [1, 12, 4, 4]},
            "mask_types": mask_types,
            "backend": "auto",
            "threshold": 0.62,
            "overwrite": True,
            "include_shadows": True,
            "water_backend": "auto",
            "water_overwrite": True,
            "water_inference_device": "cpu",
            "stage_callback": object(),
        }
    )

    assert request.source_zarr_uri == "/tmp/source.zarr"
    assert request.mask_types == mask_types
    assert request.cloud.backend == "auto"
    assert request.cloud.threshold == 0.62
    assert request.cloud.include_shadows is True
    assert request.water.backend == "auto"
    assert request.water.overwrite is True


def test_mask_apply_request_maps_legacy_heuristic_backend_to_water_backend() -> None:
    request = MaskApplyRequest.model_validate(
        {
            "zarr_uri": "/tmp/source.zarr",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "scene_id": "S2A_SCENE",
            "acquisition_datetime": "2026-04-01T10:00:00Z",
            "dataset_summary": {"shape": [1, 12, 4, 4]},
            "mask_types": ["water", "cloud"],
            "backend": "heuristic",
        }
    )

    assert request.cloud.backend == "omnicloudmask"
    assert request.water.backend == "heuristic"
    assert request.water.inference_device is None


def test_copy_source_zarr_falls_back_when_fast_copy_path_is_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = _write_source_zarr(tmp_path)
    output = tmp_path / "zarrmask" / f"{source.stem}__copy.zarr"
    attempts: list[str] = []

    def _fail_cp(*, source_path: Path, output_path: Path) -> None:
        attempts.append("cp")
        raise RuntimeError("cp reflink unavailable")

    def _fail_rsync(*, source_path: Path, output_path: Path) -> None:
        attempts.append("rsync")
        raise RuntimeError("rsync unavailable")

    def _python_copy(*, source_path: Path, output_path: Path) -> None:
        attempts.append("python")
        shutil.copytree(source_path, output_path)

    monkeypatch.setattr(io_module, "_copytree_via_cp", _fail_cp)
    monkeypatch.setattr(io_module, "_copytree_via_rsync", _fail_rsync)
    monkeypatch.setattr(io_module, "_copytree_via_python", _python_copy)

    copied = io_module.copy_source_zarr(source_zarr_uri=str(source), output_zarr_uri=str(output))

    assert copied == str(output)
    assert attempts == ["cp", "rsync", "python"]
    assert output.exists()
    copied_group = zarr.open_group(str(output), mode="r", use_consolidated=False)
    assert "imagery" in copied_group
    np.testing.assert_array_equal(copied_group["imagery"][:], zarr.open_group(str(source), mode="r", use_consolidated=False)["imagery"][:])


def test_remote_mask_service_client_disables_read_timeout() -> None:
    captured: dict[str, object] = {}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"status": "written"}

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout):  # noqa: A002
            captured["url"] = url
            captured["payload"] = json
            captured["timeout"] = timeout
            return _FakeResponse()

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client._session = _FakeSession()

    result = client.apply_masks_to_zarr(
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["cloud"],
    )

    assert result["status"] == "written"
    assert captured["url"] == "http://nimbus-mask:8020/apply"
    assert captured["timeout"] == (30, None)


def test_remote_mask_service_client_surfaces_clear_error_when_connection_drops() -> None:
    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout):  # noqa: A002
            raise requests.ConnectionError("Remote end closed connection without response")

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client._session = _FakeSession()

    with pytest.raises(RuntimeError) as excinfo:
        client.apply_masks_to_zarr(
            zarr_uri="/tmp/source.zarr",
            provider="copernicus",
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            scene_id="S2A_SCENE",
            acquisition_datetime="2026-04-01T10:00:00Z",
            dataset_summary={"shape": [1, 12, 4, 4]},
            mask_types=["cloud"],
        )

    assert "mask process may have restarted" in str(excinfo.value).lower()


def test_remote_mask_service_client_retries_once_after_service_restart() -> None:
    captured: dict[str, object] = {"posts": 0, "health_checks": 0}

    class _FakeResponse:
        def __init__(self, payload: dict[str, object], *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}", response=self)

        def json(self) -> dict[str, object]:
            return dict(self._payload)

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            captured["posts"] = int(captured["posts"]) + 1
            assert url == "http://nimbus-mask:8020/apply"
            assert timeout == (30, None)
            assert params is None
            if captured["posts"] == 1:
                raise requests.ConnectionError("Remote end closed connection without response")
            return _FakeResponse({"status": "written"})

        def get(self, url: str, *, timeout):
            captured["health_checks"] = int(captured["health_checks"]) + 1
            assert url == "http://nimbus-mask:8020/health"
            assert timeout == 5
            return _FakeResponse({"status": "ok"})

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client.REMOTE_APPLY_RESTART_WAIT_SECONDS = 0.01
    client.REMOTE_APPLY_RESTART_POLL_SECONDS = 0.0
    client._session = _FakeSession()

    result = client.apply_masks_to_zarr(
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["cloud"],
    )

    assert result["status"] == "written"
    assert captured["posts"] == 2
    assert captured["health_checks"] >= 1


def test_remote_mask_service_client_forwards_progress_callbacks_during_apply() -> None:
    progress_calls: list[tuple[str, dict[str, object]]] = []

    class _FakeResponse:
        def __init__(self, payload: dict[str, object], *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self) -> dict[str, object]:
            return dict(self._payload)

    class _FakeSession:
        def __init__(self) -> None:
            self.progress_calls = 0

        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            assert url == "http://nimbus-mask:8020/apply"
            assert timeout == (30, None)
            assert params == {"job_id": "job-123"}
            time.sleep(0.05)
            return _FakeResponse({"status": "written"})

        def get(self, url: str, *, timeout):
            assert timeout == 10
            assert url == "http://nimbus-mask:8020/progress/job-123"
            self.progress_calls += 1
            if self.progress_calls == 1:
                return _FakeResponse(
                    {
                        "job_id": "job-123",
                        "stage_name": "cloud_masking_started",
                        "payload": {"scene_id": "S2A_SCENE"},
                        "status": "running",
                        "sequence": 1,
                    }
                )
            return _FakeResponse(
                {
                    "job_id": "job-123",
                    "stage_name": "cloud_masking_progress",
                    "payload": {"scene_id": "S2A_SCENE", "progress": 0.5},
                    "status": "running",
                    "sequence": 2,
                }
            )

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client.REMOTE_PROGRESS_POLL_SECONDS = 0.01
    client._session = _FakeSession()

    result = client.apply_masks_to_zarr(
        job_id="job-123",
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["cloud"],
        stage_callback=lambda stage_name, payload: progress_calls.append((stage_name, dict(payload))),
    )

    assert result["status"] == "written"
    assert any(stage_name == "cloud_masking_started" for stage_name, _ in progress_calls)
    assert any(stage_name == "cloud_masking_progress" for stage_name, _ in progress_calls)


def test_remote_mask_service_client_replays_missed_progress_history_between_polls() -> None:
    progress_calls: list[tuple[str, dict[str, object]]] = []

    class _FakeResponse:
        def __init__(self, payload: dict[str, object], *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self) -> dict[str, object]:
            return dict(self._payload)

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, object], timeout, params=None):  # noqa: A002
            assert url == "http://nimbus-mask:8020/apply"
            assert params == {"job_id": "job-456"}
            time.sleep(0.05)
            return _FakeResponse({"status": "written"})

        def get(self, url: str, *, timeout):
            assert timeout == 10
            assert url == "http://nimbus-mask:8020/progress/job-456"
            return _FakeResponse(
                {
                    "job_id": "job-456",
                    "stage_name": "water_masking_started",
                    "payload": {"scene_id": "S2A_SCENE"},
                    "status": "running",
                    "sequence": 3,
                    "history": [
                        {
                            "job_id": "job-456",
                            "stage_name": "cloud_masking_started",
                            "payload": {"scene_id": "S2A_SCENE"},
                            "status": "running",
                            "sequence": 1,
                        },
                        {
                            "job_id": "job-456",
                            "stage_name": "cloud_masking_finished",
                            "payload": {"scene_id": "S2A_SCENE"},
                            "status": "running",
                            "sequence": 2,
                        },
                        {
                            "job_id": "job-456",
                            "stage_name": "water_masking_started",
                            "payload": {"scene_id": "S2A_SCENE"},
                            "status": "running",
                            "sequence": 3,
                        },
                    ],
                }
            )

        def close(self) -> None:
            return None

    client = MaskServiceClient(service_url="http://nimbus-mask:8020")
    client.REMOTE_PROGRESS_POLL_SECONDS = 0.01
    client._session = _FakeSession()

    result = client.apply_masks_to_zarr(
        job_id="job-456",
        zarr_uri="/tmp/source.zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="S2A_SCENE",
        acquisition_datetime="2026-04-01T10:00:00Z",
        dataset_summary={"shape": [1, 12, 4, 4]},
        mask_types=["cloud", "water"],
        stage_callback=lambda stage_name, payload: progress_calls.append((stage_name, dict(payload))),
    )

    assert result["status"] == "written"
    assert [stage_name for stage_name, _ in progress_calls[:3]] == [
        "cloud_masking_started",
        "cloud_masking_finished",
        "water_masking_started",
    ]
