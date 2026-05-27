from __future__ import annotations

from pathlib import Path

import numpy as np
from fastapi.testclient import TestClient
import zarr

from nimbuschain_rgb_viewer_service.bootstrap import create_app
from nimbuschain_rgb_viewer_service.presets import choose_rgb_bands


def _write_scene_store(path: Path, *, provider: str, collection: str, bands: list[str]) -> None:
    group = zarr.open_group(path, mode="w", zarr_format=2)
    imagery = np.zeros((1, len(bands), 4, 6), dtype=np.uint16)
    for band_index, _band in enumerate(bands):
        imagery[0, band_index] = (band_index + 1) * 1000 + np.arange(24, dtype=np.uint16).reshape(4, 6)
    group.create_array("imagery", data=imagery, overwrite=True)
    group.create_array("band", data=np.asarray(bands, dtype="U8"), overwrite=True)
    group.create_array("time", data=np.asarray(["2026-04-08T10:00:00Z"], dtype="U32"), overwrite=True)
    group.attrs.update(
        {
            "provider": provider,
            "collection": collection,
            "product_type": "L1TP" if provider == "usgs" else "S2MSI2A",
            "scene_id": path.stem,
            "acquisition_datetime": "2026-04-08T10:00:00Z",
        }
    )


def test_choose_rgb_bands_uses_provider_recommended_presets() -> None:
    assert choose_rgb_bands(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        band_names=["B02", "B03", "B04", "B08"],
    ) == (["B04", "B03", "B02"], "sentinel2_true_color")
    assert choose_rgb_bands(
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        band_names=["B2", "B3", "B4", "B5"],
    ) == (["B4", "B3", "B2"], "landsat_l1_true_color")


def test_rgb_viewer_lists_scenes_and_renders_png(tmp_path: Path) -> None:
    scene = tmp_path / "LC92010262026098LGN00.zarr"
    _write_scene_store(
        scene,
        provider="usgs",
        collection="landsat_ot_c2_l1",
        bands=["B2", "B3", "B4", "B5"],
    )
    client = TestClient(create_app(zarr_root=tmp_path))

    scenes_response = client.get("/v1/scenes")
    assert scenes_response.status_code == 200
    payload = scenes_response.json()
    assert payload["items"][0]["scene_id"] == "LC92010262026098LGN00"
    assert payload["items"][0]["recommended_rgb"] == {
        "preset": "landsat_l1_true_color",
        "bands": ["B4", "B3", "B2"],
    }

    preview_response = client.get("/v1/preview", params={"uri": str(scene), "max_size": 128})
    assert preview_response.status_code == 200
    assert preview_response.headers["content-type"] == "image/png"
    assert preview_response.headers["x-nimbus-rgb-bands"] == "B4,B3,B2"
    assert preview_response.content.startswith(b"\x89PNG\r\n\x1a\n")

    relative_response = client.get("/v1/preview", params={"uri": scene.name, "max_size": 128})
    assert relative_response.status_code == 200
    assert relative_response.content.startswith(b"\x89PNG\r\n\x1a\n")


def test_rgb_viewer_filters_job_outputs_and_lists_cubes(tmp_path: Path) -> None:
    downloads_root = tmp_path / "downloads"
    zarr_root = downloads_root / "zarr"
    job_id = "job123"
    job_root = downloads_root / job_id
    job_root.mkdir(parents=True)
    (job_root / "manifest.json").write_text(
        '{"paths":["/data/downloads/job123/LC92010262026098LGN00.tar"]}',
        encoding="utf-8",
    )
    scene = zarr_root / "LC92010262026098LGN00.zarr"
    other_scene = zarr_root / "LC_OTHER.zarr"
    cube = zarr_root / "cubes" / job_id / "cube_200026_20260409_20260425_before_mask.zarr"
    for path in (scene, other_scene, cube):
        _write_scene_store(
            path,
            provider="usgs",
            collection="landsat_ot_c2_l1",
            bands=["B2", "B3", "B4", "B5"],
        )
    client = TestClient(create_app(zarr_root=zarr_root))

    response = client.get("/v1/scenes", params={"job_id": job_id})

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["scene_id"] for item in items] == [
        "LC92010262026098LGN00",
        "cube_200026_20260409_20260425_before_mask",
    ]
    assert [item["kind"] for item in items] == ["scene", "cube"]


def test_rgb_viewer_rejects_missing_custom_bands(tmp_path: Path) -> None:
    scene = tmp_path / "S2A_TEST.zarr"
    _write_scene_store(
        scene,
        provider="copernicus",
        collection="SENTINEL-2",
        bands=["B02", "B03", "B04"],
    )
    client = TestClient(create_app(zarr_root=tmp_path))

    response = client.get("/v1/preview", params={"uri": str(scene), "bands": "B04,B03,B08"})

    assert response.status_code == 400
    assert "B08" in response.json()["detail"]
