from __future__ import annotations

from typing import Any

from nimbuschain_zarr_service.core import summarize_dataset
from nimbuschain_zarr_service.landsat import convert_landsat_to_zarr
from nimbuschain_zarr_service.readers import CopernicusProductReader, LandsatProductReader
from nimbuschain_zarr_service.writers import ZarrWriter


class ZarrConversionService:
    def __init__(self) -> None:
        self._copernicus_reader = CopernicusProductReader()
        self._landsat_reader = LandsatProductReader()
        self._writer = ZarrWriter()

    def convert(
        self,
        *,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        if provider == "copernicus":
            dataset, summary = self._copernicus_reader.read(
                raw_uri=raw_uri,
                provider=provider,
                collection=collection,
                scene_id=scene_id,
                product_type=product_type,
            )
        elif provider == "usgs":
            return convert_landsat_to_zarr(
                raw_uri=raw_uri,
                provider=provider,
                collection=collection,
                scene_id=scene_id,
                output_uri=output_uri,
                product_type=product_type,
            )
        else:
            raise ValueError(f"Unsupported provider: {provider}")

        written_uri = self._writer.write(dataset, output_uri)
        dataset_summary = summarize_dataset(
            dataset,
            data_family=str(summary.get("data_family", "unknown")),
            zarr_uri=written_uri,
        )
        return written_uri, str(summary.get("data_family", "unknown")), summary, dataset_summary
