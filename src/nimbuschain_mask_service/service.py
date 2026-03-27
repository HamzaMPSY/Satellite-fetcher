from __future__ import annotations

from typing import Any

import numpy as np

from nimbuschain_mask_service.omniwater import apply_omniwatermask_to_zarr
from nimbuschain_mask_service.writers import write_water_mask_to_zarr


class MaskService:
    """Internal masking runtime used by the unified backend pipeline."""

    def write_water_mask(
        self,
        *,
        output_uri: str,
        mask: np.ndarray,
        acquisition_datetime: str | None = None,
        model_name: str = "omniwatermask",
        model_version: str | None = None,
        input_bands: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return write_water_mask_to_zarr(
            output_uri=output_uri,
            mask=mask,
            acquisition_datetime=acquisition_datetime,
            model_name=model_name,
            model_version=model_version,
            input_bands=input_bands,
            metadata=metadata,
        )

    def apply_omniwater_to_zarr(
        self,
        *,
        job_id: str | None = None,
        zarr_uri: str,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, Any],
        fail_on_error: bool = False,
        stage_callback: Any = None,
    ) -> dict[str, Any]:
        return apply_omniwatermask_to_zarr(
            job_id=job_id,
            zarr_uri=zarr_uri,
            provider=provider,
            collection=collection,
            product_type=product_type,
            scene_id=scene_id,
            acquisition_datetime=acquisition_datetime,
            dataset_summary=dataset_summary,
            fail_on_error=fail_on_error,
            stage_callback=stage_callback,
        )
