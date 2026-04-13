from __future__ import annotations

from pathlib import Path

import numpy as np
import zarr

from nimbuschain_mask_service.io import local_path_for_uri, open_zarr_group
from nimbuschain_mask_service.writers import write_water_mask_to_zarr
from nimbuschain_mask_service.service import MaskService


def _build_minimal_store(path: Path) -> None:
    root = zarr.open_group(str(path), mode="w", zarr_format=2)
    root.create_array("imagery", data=np.zeros((1, 3, 4, 4), dtype=np.uint16))
    root.create_array("band", data=np.asarray(["B02", "B03", "B04"], dtype="U3"))
    root.attrs["provider"] = "copernicus"
    root.attrs["collection"] = "SENTINEL-2"
    root.attrs["product_type"] = "S2MSI2A"
    root.attrs["scene_id"] = "scene"


def test_local_path_for_uri_maps_container_downloads_to_host_root(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "downloads"
    monkeypatch.setenv("NIMBUS_HOST_DATA_DIR", str(data_root))

    mapped = local_path_for_uri("/data/downloads/zarr/scene.zarr")

    assert mapped == (data_root / "zarr" / "scene.zarr").resolve()


def test_open_zarr_group_maps_container_downloads_to_host_store(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "downloads"
    store_path = data_root / "zarr" / "scene.zarr"
    store_path.parent.mkdir(parents=True, exist_ok=True)
    _build_minimal_store(store_path)
    monkeypatch.setenv("NIMBUS_HOST_DATA_DIR", str(data_root))

    root = open_zarr_group("/data/downloads/zarr/scene.zarr", mode="a")

    assert tuple(root["imagery"].shape) == (1, 3, 4, 4)
    assert root.attrs["scene_id"] == "scene"


def test_apply_masks_to_zarr_reports_written_in_place_store_for_mapped_host_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "downloads"
    store_path = data_root / "zarr" / "scene.zarr"
    store_path.parent.mkdir(parents=True, exist_ok=True)
    _build_minimal_store(store_path)
    monkeypatch.setenv("NIMBUS_HOST_DATA_DIR", str(data_root))

    canonical_uri = "/data/downloads/zarr/scene.zarr"

    def _fake_apply_cloud_to_zarr(self, **kwargs):  # type: ignore[no-untyped-def]
        return {
            "status": "written",
            "input_zarr_uri": canonical_uri,
            "output_zarr_uri": canonical_uri,
            "storage_mode": "in_place_zarr_masking",
        }

    monkeypatch.setattr(MaskService, "apply_cloud_to_zarr", _fake_apply_cloud_to_zarr)

    result = MaskService().apply_masks_to_zarr(
        zarr_uri=canonical_uri,
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        scene_id="scene",
        acquisition_datetime=None,
        dataset_summary={},
        mask_types=["cloud"],
        output_zarr_uri=canonical_uri,
    )

    assert result["status"] == "written"
    assert result["masked_zarr_uri"] == canonical_uri
    assert result["masked_zarr_outputs"] == [canonical_uri]


def test_write_water_mask_to_zarr_uses_mapped_host_store(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "downloads"
    store_path = data_root / "zarr" / "scene.zarr"
    store_path.parent.mkdir(parents=True, exist_ok=True)
    _build_minimal_store(store_path)
    monkeypatch.setenv("NIMBUS_HOST_DATA_DIR", str(data_root))

    canonical_uri = "/data/downloads/zarr/scene.zarr"
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[1:3, 1:3] = 1

    result = write_water_mask_to_zarr(
        output_uri=canonical_uri,
        mask=mask,
        metadata={"input_zarr_uri": canonical_uri, "output_zarr_uri": canonical_uri},
    )

    assert result["mask_path"] == "masks/water"
    assert result["output_zarr_uri"] == canonical_uri
    root = zarr.open_group(str(store_path), mode="r", zarr_format=2)
    assert "masks" in root
    assert "water" in root["masks"]
