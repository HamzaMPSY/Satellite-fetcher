from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("zarr")

import zarr

from nimbuschain_mask_service import cli as mask_cli


def _write_source_zarr(path: Path) -> Path:
    group = zarr.open_group(str(path), mode="w", zarr_format=2)
    imagery = group.create_array(
        "imagery",
        shape=(1, 4, 4, 4),
        chunks=(1, 4, 4, 4),
        dtype=np.uint16,
        overwrite=True,
    )
    imagery[:] = 1
    group.create_array("band", shape=(4,), chunks=(4,), dtype="U3", overwrite=True)
    group["band"][:] = np.array(["B04", "B03", "B02", "B08"], dtype="U3")
    group.attrs.update(
        {
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "scene_id": "S2A_TEST_SCENE",
            "band_names": ["B04", "B03", "B02", "B08"],
            "dimensions": ["time", "band", "y", "x"],
            "data_family": "optical",
            "crs": "EPSG:32631",
            "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
            "acquisition_datetime": "2026-04-10T08:00:21Z",
        }
    )
    zarr.consolidate_metadata(str(path))
    return path


def test_mask_cli_uses_zarr_metadata_when_args_are_omitted(monkeypatch, tmp_path: Path, capsys) -> None:
    store_path = _write_source_zarr(tmp_path / "source.zarr")
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, *, service_url: str | None = None):
            captured["service_url"] = service_url

        def apply_masks_to_zarr(self, **kwargs):
            captured.update(kwargs)
            return {"status": "written", "masked_zarr_uri": kwargs["zarr_uri"]}

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(mask_cli, "MaskServiceClient", FakeClient)
    args = mask_cli.build_parser().parse_args(
        [str(store_path), "--mask-types", "water,cloud"]
    )

    assert mask_cli.run(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert captured["provider"] == "copernicus"
    assert captured["collection"] == "SENTINEL-2"
    assert captured["scene_id"] == "S2A_TEST_SCENE"
    assert captured["mask_types"] == ["water", "cloud"]
    assert payload["masked_zarr_uri"] == str(store_path)
    assert captured["closed"] is True
