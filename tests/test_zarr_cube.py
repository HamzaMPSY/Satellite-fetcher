from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from nimbuschain_zarr_service.core import ConversionError
from nimbuschain_zarr_service.cube import (
    _copy_time_slice_in_chunks,
    build_grouped_time_cubes,
    build_time_cube,
)


class _ChunkGuardArray:
    def __init__(self, data: np.ndarray, *, chunks: tuple[int, ...]) -> None:
        self.data = np.asarray(data)
        self.shape = self.data.shape
        self.dtype = self.data.dtype
        self.chunks = chunks
        self.max_read_spans = [0 for _ in range(self.data.ndim)]

    def __getitem__(self, key):
        normalized = self._normalize_key(key)
        for axis, item in enumerate(normalized):
            span = 1
            if isinstance(item, slice):
                span = max(0, int(item.stop or 0) - int(item.start or 0))
            self.max_read_spans[axis] = max(self.max_read_spans[axis], span)
        return self.data[normalized]

    def __setitem__(self, key, value) -> None:
        self.data[key] = value

    def _normalize_key(self, key):
        if not isinstance(key, tuple):
            key = (key,)
        normalized = []
        for axis, item in enumerate(key):
            if isinstance(item, slice):
                start = 0 if item.start is None else int(item.start)
                stop = int(self.shape[axis]) if item.stop is None else int(item.stop)
                normalized.append(slice(start, stop, item.step))
            else:
                normalized.append(int(item))
        return tuple(normalized)


def _write_scene_store(
    path: Path,
    *,
    scene_id: str,
    acquisition_time: str,
    imagery_value: int,
    band_names: list[str],
    transform: list[float] | None = None,
    ancillary_names: list[str] | None = None,
    ancillary_value: int = 1,
) -> None:
    transform = transform or [10.0, 0.0, 500000.0, 0.0, -10.0, 4100000.0]
    group = zarr.open_group(path, mode="w", zarr_format=2)
    imagery = np.full((1, len(band_names), 2, 3), imagery_value, dtype=np.uint16)
    group.create_array("imagery", data=imagery, overwrite=True)
    group.create_array("band", data=np.asarray(band_names, dtype="U16"), overwrite=True)
    group.create_array("time", data=np.asarray([acquisition_time], dtype="U32"), overwrite=True)
    group.create_array(
        "x",
        data=np.asarray([500005.0, 500015.0, 500025.0], dtype=np.float64),
        overwrite=True,
    )
    group.create_array(
        "y",
        data=np.asarray([4099995.0, 4099985.0], dtype=np.float64),
        overwrite=True,
    )
    if ancillary_names:
        ancillary = np.full((1, len(ancillary_names), 2, 3), ancillary_value, dtype=np.uint8)
        group.create_array("ancillary", data=ancillary, overwrite=True)
        group.create_array(
            "ancillary_layer",
            data=np.asarray(ancillary_names, dtype="U16"),
            overwrite=True,
        )
        group.attrs["ancillary_metadata"] = {
            name: {"source_band_index": index + 1}
            for index, name in enumerate(ancillary_names)
        }

    group.attrs.update(
        {
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "data_family": "optical",
            "scene_id": scene_id,
            "acquisition_datetime": acquisition_time,
            "crs": "EPSG:32631",
            "transform": transform,
            "reference_band": "B04",
            "reference_pixel_size": [10.0, 10.0],
            "band_names": list(band_names),
            "band_metadata": {
                name: {
                    "path": f"/tmp/{scene_id}_{name}.tif",
                    "source_band_index": index + 1,
                    "resampled_to_reference": False,
                }
                for index, name in enumerate(band_names)
            },
            "zarr_format_version": 1,
        }
    )


def test_build_time_cube_stacks_scenes_in_timestamp_order(tmp_path: Path) -> None:
    first = tmp_path / "scene_b.zarr"
    second = tmp_path / "scene_a.zarr"
    output = tmp_path / "cube.zarr"

    _write_scene_store(
        first,
        scene_id="scene-b",
        acquisition_time="2026-01-02T10:00:00Z",
        imagery_value=22,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        second,
        scene_id="scene-a",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=11,
        band_names=["B04", "B03"],
    )

    summary = build_time_cube([str(first), str(second)], str(output))

    root = zarr.open_group(output, mode="r", zarr_format=2)
    assert summary["time_values"] == [
        "2026-01-01T10:00:00+00:00",
        "2026-01-02T10:00:00+00:00",
    ]
    assert root["time"][:].tolist() == summary["time_values"]
    assert root["scene_id"][:].tolist() == ["scene-a", "scene-b"]
    assert root["source_zarr_uri"][:].tolist() == [str(second.resolve()), str(first.resolve())]
    assert tuple(root["imagery"].shape) == (2, 2, 2, 3)
    assert np.all(root["imagery"][0, :, :, :] == 11)
    assert np.all(root["imagery"][1, :, :, :] == 22)
    assert root.attrs["cube_kind"] == "time_series"
    assert root.attrs["source_scene_count"] == 2
    assert root.attrs["band_metadata"]["B04"].get("path") is None
    assert "quadkeys_by_time" not in root.attrs


def test_build_time_cube_stacks_ancillary_when_schema_matches(tmp_path: Path) -> None:
    first = tmp_path / "scene_1.zarr"
    second = tmp_path / "scene_2.zarr"
    output = tmp_path / "cube_ancillary.zarr"

    _write_scene_store(
        first,
        scene_id="scene-1",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=1,
        band_names=["B04", "B03"],
        ancillary_names=["SCL", "AOT"],
        ancillary_value=7,
    )
    _write_scene_store(
        second,
        scene_id="scene-2",
        acquisition_time="2026-01-03T10:00:00Z",
        imagery_value=2,
        band_names=["B04", "B03"],
        ancillary_names=["SCL", "AOT"],
        ancillary_value=9,
    )

    summary = build_time_cube([str(first), str(second)], str(output))

    root = zarr.open_group(output, mode="r", zarr_format=2)
    assert summary["ancillary_written"] is True
    assert summary["ancillary_layer_names"] == ["SCL", "AOT"]
    assert root["ancillary_layer"][:].tolist() == ["SCL", "AOT"]
    assert tuple(root["ancillary"].shape) == (2, 2, 2, 3)
    assert np.all(root["ancillary"][0, :, :, :] == 7)
    assert np.all(root["ancillary"][1, :, :, :] == 9)


def test_build_time_cube_skips_ancillary_when_schema_differs(tmp_path: Path) -> None:
    first = tmp_path / "scene_1.zarr"
    second = tmp_path / "scene_2.zarr"
    output = tmp_path / "cube_skip_ancillary.zarr"

    _write_scene_store(
        first,
        scene_id="scene-1",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=1,
        band_names=["B04", "B03"],
        ancillary_names=["SCL", "AOT"],
    )
    _write_scene_store(
        second,
        scene_id="scene-2",
        acquisition_time="2026-01-02T10:00:00Z",
        imagery_value=2,
        band_names=["B04", "B03"],
        ancillary_names=["SCL"],
    )

    summary = build_time_cube([str(first), str(second)], str(output))

    root = zarr.open_group(output, mode="r", zarr_format=2)
    assert summary["ancillary_written"] is False
    assert "ancillary" not in root
    assert root.attrs["ancillary_omitted_reason"]


def test_build_time_cube_rejects_mismatched_band_names(tmp_path: Path) -> None:
    first = tmp_path / "scene_1.zarr"
    second = tmp_path / "scene_2.zarr"
    output = tmp_path / "cube_fail.zarr"

    _write_scene_store(
        first,
        scene_id="scene-1",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=1,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        second,
        scene_id="scene-2",
        acquisition_time="2026-01-02T10:00:00Z",
        imagery_value=2,
        band_names=["B08", "B03"],
    )

    with pytest.raises(ConversionError, match="band_names"):
        build_time_cube([str(first), str(second)], str(output))


def test_build_time_cube_rejects_mismatched_transform(tmp_path: Path) -> None:
    first = tmp_path / "scene_1.zarr"
    second = tmp_path / "scene_2.zarr"
    output = tmp_path / "cube_fail_transform.zarr"

    _write_scene_store(
        first,
        scene_id="scene-1",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=1,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        second,
        scene_id="scene-2",
        acquisition_time="2026-01-02T10:00:00Z",
        imagery_value=2,
        band_names=["B04", "B03"],
        transform=[20.0, 0.0, 500000.0, 0.0, -20.0, 4100000.0],
    )

    with pytest.raises(ConversionError, match="transform"):
        build_time_cube([str(first), str(second)], str(output))


def test_build_grouped_time_cubes_filters_dates_and_deduplicates_same_timestamp(tmp_path: Path) -> None:
    scene_a = tmp_path / "scene_a.zarr"
    scene_b = tmp_path / "scene_b.zarr"
    scene_duplicate = tmp_path / "scene_duplicate.zarr"
    other_tile = tmp_path / "other_tile.zarr"
    output_dir = tmp_path / "cubes"

    _write_scene_store(
        scene_a,
        scene_id="S2A_MSIL2A_20260101T100000_N0512_R078_T37RDP_20260101T120000",
        acquisition_time="2026-01-01T10:00:00Z",
        imagery_value=1,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        scene_b,
        scene_id="S2A_MSIL2A_20260103T100000_N0512_R078_T37RDP_20260103T120000",
        acquisition_time="2026-01-03T10:00:00Z",
        imagery_value=2,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        scene_duplicate,
        scene_id="S2B_MSIL2A_20260103T100000_N0512_R035_T37RDP_20260103T120500",
        acquisition_time="2026-01-03T10:00:00Z",
        imagery_value=3,
        band_names=["B04", "B03"],
    )
    _write_scene_store(
        other_tile,
        scene_id="S2A_MSIL2A_20260102T100000_N0512_R078_T37RCN_20260102T120000",
        acquisition_time="2026-01-02T10:00:00Z",
        imagery_value=4,
        band_names=["B04", "B03"],
    )

    summary = build_grouped_time_cubes(
        [str(scene_a), str(scene_b), str(scene_duplicate), str(other_tile)],
        str(output_dir),
        start_date="2026-01-01",
        end_date="2026-01-03",
        stage_label="before_mask",
    )

    assert summary["status"] == "written"
    assert summary["stage_label"] == "before_mask"
    assert summary["cube_outputs"] == [str((output_dir / "cube_37RDP_20260101_20260103_before_mask.zarr").resolve())]
    assert summary["tiles_built"] == ["37RDP"]
    assert summary["tiles_skipped"][0]["group_key"] == "37RCN"
    assert summary["items"][0]["skipped_duplicate_scene_ids"] == [
        "S2B_MSIL2A_20260103T100000_N0512_R035_T37RDP_20260103T120500"
    ]


def test_build_grouped_time_cubes_groups_compact_landsat_scene_ids(tmp_path: Path) -> None:
    scene_a = tmp_path / "landsat_a.zarr"
    scene_b = tmp_path / "landsat_b.zarr"
    scene_c = tmp_path / "landsat_c.zarr"
    other_path_row = tmp_path / "landsat_other.zarr"
    output_dir = tmp_path / "landsat_cubes"

    _write_scene_store(
        scene_a,
        scene_id="LC92010262026098LGN00",
        acquisition_time="2026-04-08T10:00:00Z",
        imagery_value=1,
        band_names=["B4", "B3"],
    )
    _write_scene_store(
        scene_b,
        scene_id="LC82010262026106LGN00",
        acquisition_time="2026-04-16T10:00:00Z",
        imagery_value=2,
        band_names=["B4", "B3"],
    )
    _write_scene_store(
        scene_c,
        scene_id="LC92010262026114LGN00",
        acquisition_time="2026-04-24T10:00:00Z",
        imagery_value=3,
        band_names=["B4", "B3"],
    )
    _write_scene_store(
        other_path_row,
        scene_id="LC82020262026097LGN00",
        acquisition_time="2026-04-07T10:00:00Z",
        imagery_value=4,
        band_names=["B4", "B3"],
    )

    summary = build_grouped_time_cubes(
        [str(scene_a), str(scene_b), str(scene_c), str(other_path_row)],
        str(output_dir),
        start_date="2026-04-07",
        end_date="2026-04-24",
        stage_label="before_mask",
    )

    assert summary["status"] == "written"
    assert summary["cube_outputs"] == [
        str((output_dir / "cube_201026_20260408_20260424_before_mask.zarr").resolve())
    ]
    assert summary["tiles_built"] == ["201026"]
    assert summary["tiles_skipped"] == [
        {
            "group_key": "202026",
            "reason": "fewer_than_two_unique_times",
            "candidate_scene_ids": ["LC82020262026097LGN00"],
        }
    ]
    assert summary["items"][0]["source_scene_count"] == 3


def test_copy_time_slice_in_chunks_reads_imagery_by_chunk() -> None:
    source_data = np.arange(1 * 3 * 4 * 5, dtype=np.uint16).reshape(1, 3, 4, 5)
    source = _ChunkGuardArray(source_data, chunks=(1, 1, 2, 2))
    target = _ChunkGuardArray(np.zeros((2, 3, 4, 5), dtype=np.uint16), chunks=(1, 1, 2, 2))

    blocks_written = _copy_time_slice_in_chunks(
        source,
        target,
        source_time_index=0,
        target_time_index=1,
        layer_name="imagery",
        label_names=["B01", "B02", "B03"],
    )

    np.testing.assert_array_equal(target.data[1], source_data[0])
    assert blocks_written == 18
    assert source.max_read_spans[1] <= 1
    assert source.max_read_spans[2] <= 2
    assert source.max_read_spans[3] <= 2


def test_copy_time_slice_in_chunks_reads_masks_by_chunk() -> None:
    source_data = np.arange(1 * 4 * 5, dtype=np.uint8).reshape(1, 4, 5)
    source = _ChunkGuardArray(source_data, chunks=(1, 2, 2))
    target = _ChunkGuardArray(np.zeros((2, 4, 5), dtype=np.uint8), chunks=(1, 2, 2))

    blocks_written = _copy_time_slice_in_chunks(
        source,
        target,
        source_time_index=0,
        target_time_index=1,
        layer_name="masks/cloud",
    )

    np.testing.assert_array_equal(target.data[1], source_data[0])
    assert blocks_written == 6
    assert source.max_read_spans[1] <= 2
    assert source.max_read_spans[2] <= 2
