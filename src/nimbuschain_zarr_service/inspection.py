from __future__ import annotations

from typing import Any

from nimbuschain_zarr_service.models import DatasetInspectionRecord


def inspect_dataset_summary(zarr_uri: str) -> dict[str, Any]:
    try:
        import zarr
    except Exception as exc:
        raise ValueError(f"Unable to inspect existing Zarr output because zarr is unavailable ({exc}).") from exc

    from nimbuschain_zarr_service.core import _open_existing_output_store

    root = zarr.open_group(_open_existing_output_store(zarr_uri), mode="r")
    imagery = root.get("imagery")
    if imagery is None:
        raise ValueError("The selected Zarr output does not contain an imagery array.")
    band_names = list(root.attrs.get("band_names") or [])
    if not band_names and "band" in root:
        band_names = [_decode_label(item) for item in root["band"][:].tolist()]
    acquisition_datetime = None
    if "time" in root and int(getattr(root["time"], "shape", [0])[0] or 0) > 0:
        raw_time = root["time"][0]
        acquisition_datetime = _decode_label(raw_time.item() if hasattr(raw_time, "item") else raw_time)
    attrs = dict(root.attrs)
    transform = list(attrs.get("transform") or [])
    if len(transform) < 6 and "x" in root and "y" in root:
        derived_transform = derive_transform_from_xy(
            x_values=root["x"][:].tolist(),
            y_values=root["y"][:].tolist(),
        )
        if derived_transform:
            transform = derived_transform
    return DatasetInspectionRecord(
        dimensions=["time", "band", "y", "x"],
        shape=list(imagery.shape),
        band_names=[_decode_label(item) for item in band_names],
        ancillary_layer_names=list(root.attrs.get("ancillary_layer_names") or []),
        acquisition_datetime=acquisition_datetime,
        crs=attrs.get("crs"),
        transform=transform,
        dtype=str(attrs.get("dtype") or imagery.dtype),
        pixel_size=list(attrs.get("reference_pixel_size") or []),
        reference_pixel_size=list(attrs.get("reference_pixel_size") or []),
        band_metadata=dict(attrs.get("band_metadata") or {}),
        ancillary_metadata=dict(attrs.get("ancillary_metadata") or {}),
    ).to_dict()


def _decode_label(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def derive_transform_from_xy(*, x_values: list[Any], y_values: list[Any]) -> list[float]:
    if len(x_values) < 2 or len(y_values) < 2:
        return []
    try:
        x0 = float(x_values[0])
        x1 = float(x_values[1])
        y0 = float(y_values[0])
        y1 = float(y_values[1])
    except (TypeError, ValueError):
        return []
    x_res = x1 - x0
    y_res = y1 - y0
    if x_res == 0.0 or y_res == 0.0:
        return []
    return [x_res, 0.0, x0 - (x_res / 2.0), 0.0, y_res, y0 - (y_res / 2.0)]
