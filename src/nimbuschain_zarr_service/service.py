from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nimbuschain_zarr_service.copernicus import convert_copernicus_to_zarr
from nimbuschain_zarr_service.landsat import convert_landsat_to_zarr


class ZarrConversionService:
    def __init__(self) -> None:
        pass

    def convert(
        self,
        *,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        if provider == "copernicus":
            return convert_copernicus_to_zarr(
                raw_uri=raw_uri,
                provider=provider,
                collection=collection,
                scene_id=scene_id,
                output_uri=output_uri,
                product_type=product_type,
                progress_callback=progress_callback,
            )
        elif provider == "usgs":
            return convert_landsat_to_zarr(
                raw_uri=raw_uri,
                provider=provider,
                collection=collection,
                scene_id=scene_id,
                output_uri=output_uri,
                product_type=product_type,
                progress_callback=progress_callback,
            )
        else:
            raise ValueError(f"Unsupported provider: {provider}")
