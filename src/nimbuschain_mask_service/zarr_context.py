from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from nimbuschain_mask_service.path_resolution import local_path_for_uri
from nimbuschain_shared.zarr import ConversionError


@dataclass(frozen=True)
class ZarrMaskContext:
    zarr_uri: str
    provider: str | None
    collection: str | None
    product_type: str | None
    scene_id: str | None
    band_names: tuple[str, ...]
    imagery_shape: tuple[int, ...]


def open_zarr_group(zarr_uri: str, *, mode: str = "r") -> Any:
    try:
        import zarr
    except ImportError as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError(f"Masking requires zarr runtime support ({exc}).") from exc

    store_path = local_path_for_uri(zarr_uri)
    if not store_path.exists():
        raise ConversionError(f"Output store does not exist yet: {store_path}")
    return zarr.open_group(str(store_path), mode=mode, zarr_format=2)


def delete_mask_layers(zarr_uri: str, *, layer_names: tuple[str, ...]) -> None:
    if not layer_names:
        return
    try:
        import zarr
    except ImportError:
        return

    try:
        root = open_zarr_group(zarr_uri, mode="a")
    except Exception:
        return

    masks_group = root.get("masks")
    if masks_group is None:
        return

    touched = False
    for layer_name in layer_names:
        if layer_name in masks_group:
            del masks_group[layer_name]
            touched = True

    if not touched:
        return

    try:
        zarr.consolidate_metadata(root.store)
    except Exception:
        pass


def read_context(root: Any, *, zarr_uri: str) -> ZarrMaskContext:
    if "imagery" not in root:
        raise ConversionError("Zarr store does not contain an 'imagery' array.")
    imagery = root["imagery"]
    if len(imagery.shape) != 4:
        raise ConversionError(
            f"Expected imagery to be 4D (time, band, y, x), got shape={tuple(imagery.shape)}"
        )

    band_names: list[str] = []
    if "band" in root:
        raw_values = np.asarray(root["band"][:]).tolist()
        for value in raw_values:
            if isinstance(value, bytes):
                band_names.append(value.decode("utf-8", errors="replace"))
            else:
                band_names.append(str(value))
    else:
        band_names = [str(value) for value in (root.attrs.get("band_names") or [])]

    if not band_names:
        raise ConversionError("Zarr store does not expose band names (band coord/attrs missing).")

    return ZarrMaskContext(
        zarr_uri=zarr_uri,
        provider=_attr_as_text(root.attrs.get("provider")),
        collection=_attr_as_text(root.attrs.get("collection")),
        product_type=_attr_as_text(root.attrs.get("product_type")),
        scene_id=_attr_as_text(root.attrs.get("scene_id")),
        band_names=tuple(band_names),
        imagery_shape=tuple(int(v) for v in imagery.shape),
    )


def _attr_as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = ["ZarrMaskContext", "delete_mask_layers", "open_zarr_group", "read_context"]
