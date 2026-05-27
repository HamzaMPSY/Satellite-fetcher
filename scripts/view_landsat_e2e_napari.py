from __future__ import annotations

from pathlib import Path

import dask.array as da
import napari
import zarr


ROOT = Path(__file__).resolve().parents[1]

SCENES = [
    (
        "2026-04-30",
        ROOT / "data/downloads/zarr/S2L_MSIL2F_20260430T075226_N0500_R000_T37REP_20260526T171058.zarr",
    ),
    (
        "2026-04-22",
        ROOT / "data/downloads/zarr/S2L_MSIL2F_20260422T075243_N0500_R000_T37REP_20260526T173021.zarr",
    ),
]


def _band_names(group: zarr.Group) -> list[str]:
    names: list[str] = []
    for value in group["band"][:]:
        if isinstance(value, bytes):
            names.append(value.decode("utf-8"))
        else:
            names.append(str(value))
    return names


def _dask_array(group: zarr.Group, key: str) -> da.Array:
    array = group[key]
    return da.from_array(array, chunks=array.chunks)


def _add_scene(viewer: napari.Viewer, label: str, path: Path, *, x_offset: int) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing Zarr scene: {path}")

    group = zarr.open_group(path, mode="r")
    bands = _band_names(group)
    band_index = {band: index for index, band in enumerate(bands)}
    rgb_indexes = [band_index[band] for band in ("B04", "B03", "B02")]

    imagery = _dask_array(group, "imagery")
    rgb = da.stack([imagery[0, index, :, :] for index in rgb_indexes], axis=-1)
    cloud = _dask_array(group, "masks/cloud")[0]
    water = _dask_array(group, "masks/water")[0]
    cloud_probability = _dask_array(group, "masks/cloud_probability")[0]
    water_probability = _dask_array(group, "masks/water_probability")[0]

    translate = (0, x_offset)
    viewer.add_image(
        rgb,
        name=f"{label} RGB B04/B03/B02",
        rgb=True,
        translate=translate,
    )
    viewer.add_image(
        cloud,
        name=f"{label} cloud mask",
        colormap="magenta",
        contrast_limits=(0, 1),
        opacity=0.38,
        blending="additive",
        translate=translate,
    )
    viewer.add_image(
        water,
        name=f"{label} water mask",
        colormap="cyan",
        contrast_limits=(0, 1),
        opacity=0.55,
        blending="additive",
        translate=translate,
    )
    viewer.add_image(
        cloud_probability,
        name=f"{label} cloud probability",
        colormap="magma",
        contrast_limits=(0.0, 1.0),
        opacity=0.35,
        blending="additive",
        translate=translate,
        visible=False,
    )
    viewer.add_image(
        water_probability,
        name=f"{label} water probability",
        colormap="viridis",
        contrast_limits=(0.0, 1.0),
        opacity=0.35,
        blending="additive",
        translate=translate,
        visible=False,
    )


def main() -> None:
    viewer = napari.Viewer(title="NimbusChain Landsat E2E: RGB + masks")
    for index, (label, path) in enumerate(SCENES):
        _add_scene(viewer, label, path, x_offset=index * 11500)
    viewer.dims.ndisplay = 2
    napari.run()


if __name__ == "__main__":
    main()
