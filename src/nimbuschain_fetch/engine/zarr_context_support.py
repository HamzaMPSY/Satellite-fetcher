from __future__ import annotations

import re
from typing import Any

from nimbuschain_fetch.domain.metadata import ConversionItemRecord, ConversionMetadataRecord


class FetcherZarrContextSupport:
    """Zarr context and dataset inspection helpers for the fetcher facade."""

    def __init__(self, *, converter: Any, provider_name: Any, scene_id_from_raw_uri: Any) -> None:
        self._converter = converter
        self._provider_name = provider_name
        self._scene_id_from_raw_uri = scene_id_from_raw_uri

    def resolve_zarr_context(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result: dict[str, Any],
        zarr_uri: str,
        scene_id_override: str | None,
        product_type_override: str | None,
    ) -> dict[str, Any]:
        _ = job_id
        conversion_metadata = ConversionMetadataRecord.from_mapping(
            result.get("conversion_metadata") or row.get("conversion_metadata")
        )
        items = [ConversionItemRecord.from_mapping(item) for item in conversion_metadata.items]
        matching_item = next(
            (
                item
                for item in items
                if str(item.zarr_uri or "").strip() == zarr_uri
            ),
            None,
        )
        dataset_summary = dict(matching_item.dataset_summary if matching_item is not None else {})
        summary = dict(matching_item.summary if matching_item is not None else {})
        grid_summary = dict(summary.get("grid") or {})
        if not dataset_summary.get("crs") and grid_summary.get("crs"):
            dataset_summary["crs"] = grid_summary.get("crs")
        if not dataset_summary.get("transform") and grid_summary.get("transform"):
            dataset_summary["transform"] = list(grid_summary.get("transform") or [])
        if not dataset_summary.get("dtype") and grid_summary.get("dtype"):
            dataset_summary["dtype"] = str(grid_summary.get("dtype") or "")
        if not dataset_summary.get("shape") and grid_summary.get("height") and grid_summary.get("width"):
            band_names = list(dataset_summary.get("band_names") or summary.get("normalized_band_order") or [])
            dataset_summary["shape"] = [1, len(band_names), int(grid_summary["height"]), int(grid_summary["width"])]
        if not dataset_summary.get("band_names") and summary.get("normalized_band_order"):
            dataset_summary["band_names"] = [str(item) for item in list(summary.get("normalized_band_order") or [])]
        if not dataset_summary.get("ancillary_layer_names") and summary.get("ancillary_layer_names"):
            dataset_summary["ancillary_layer_names"] = [
                str(item) for item in list(summary.get("ancillary_layer_names") or [])
            ]
        if not dataset_summary:
            dataset_summary = self.inspect_zarr_dataset(zarr_uri)
        provider_name = self._provider_name(summary.get("provider") or row.get("provider"))
        collection = str(summary.get("collection") or row.get("collection") or "").strip()
        scene_id = (
            str(scene_id_override or "").strip()
            or str(matching_item.scene_id if matching_item is not None else "").strip()
            or str(summary.get("scene_id") or "").strip()
            or self._scene_id_from_raw_uri(zarr_uri)
        )
        product_type = (
            str(summary.get("product_type") or "").strip()
            or str(product_type_override or "").strip()
            or str(row.get("product_type") or "").strip()
            or None
        )
        acquisition_datetime = (
            str(dataset_summary.get("acquisition_datetime") or "").strip()
            or str(summary.get("acquisition_datetime") or "").strip()
            or None
        )
        if not collection:
            raise ValueError("Unable to infer collection for the selected Zarr output.")
        if not dataset_summary:
            raise ValueError("Unable to infer dataset summary for the selected Zarr output.")
        return {
            "provider": provider_name,
            "collection": collection,
            "scene_id": scene_id,
            "product_type": product_type,
            "acquisition_datetime": acquisition_datetime,
            "dataset_summary": dataset_summary,
        }

    def inspect_zarr_dataset(self, zarr_uri: str) -> dict[str, Any]:
        return self._converter().inspect_dataset(zarr_uri=zarr_uri)

    @staticmethod
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
        return [
            x_res,
            0.0,
            x0 - (x_res / 2.0),
            0.0,
            y_res,
            y0 - (y_res / 2.0),
        ]

    @staticmethod
    def infer_crs_from_scene_metadata(*, scene_id: str, source_uri: str) -> str | None:
        text = f"{scene_id} {source_uri}"
        match = re.search(r"T(?P<zone>\d{2})(?P<band>[A-Z]{3})", text)
        if not match:
            return None
        zone = int(match.group("zone"))
        latitude_band = match.group("band")[0]
        epsg_base = 326 if latitude_band >= "N" else 327
        return f"EPSG:{epsg_base}{zone:02d}"
