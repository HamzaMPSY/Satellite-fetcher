from __future__ import annotations

from typing import Any

from nimbuschain_zarr_service.landsat import build_landsat_dataset


class LandsatProductReader:
    def read(
        self,
        *,
        raw_uri: str,
        provider: str,
        collection: str,
        scene_id: str,
        product_type: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        return build_landsat_dataset(
            raw_uri=raw_uri,
            provider=provider,
            collection=collection,
            scene_id=scene_id,
            product_type=product_type,
        )
