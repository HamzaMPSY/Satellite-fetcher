from __future__ import annotations

from typing import Any

import numpy as np

from nimbuschain_mask_service.registry import CloudBackendRegistry, WaterBackendRegistry
from nimbuschain_mask_service.runtime import resolve_inference_device
from nimbuschain_mask_service.workflows import (
    CloudMaskWorkflowService,
    CombinedMaskWorkflowService,
    WaterMaskWorkflowService,
    _cloud_tile_size,
    _cloud_tile_sizing,
    _effective_cloud_backend_request,
    run_cloud_inference,
    support_status,
)


class MaskService:
    """Internal masking runtime used by the unified backend pipeline."""

    def __init__(
        self,
        *,
        cloud_registry: CloudBackendRegistry | None = None,
        water_registry: WaterBackendRegistry | None = None,
    ) -> None:
        self._cloud_registry = cloud_registry or CloudBackendRegistry.default()
        self._water_registry = water_registry or WaterBackendRegistry.default()
        self._cloud_workflow = CloudMaskWorkflowService(registry=self._cloud_registry)
        self._water_workflow = WaterMaskWorkflowService(registry=self._water_registry)
        self._combined_workflow = CombinedMaskWorkflowService(
            cloud_workflow=self._cloud_workflow,
            water_workflow=self._water_workflow,
        )

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
        return self._water_workflow.write_water_mask(
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
        source_zarr_uri: str | None = None,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, Any],
        output_zarr_uri: str | None = None,
        backend: str | None = None,
        overwrite: bool = True,
        inference_device: str | None = None,
        fail_on_error: bool = False,
        stage_callback: Any = None,
    ) -> dict[str, Any]:
        return self._water_workflow.apply_to_zarr(
            job_id=job_id,
            zarr_uri=zarr_uri,
            source_zarr_uri=source_zarr_uri,
            provider=provider,
            collection=collection,
            product_type=product_type,
            scene_id=scene_id,
            acquisition_datetime=acquisition_datetime,
            dataset_summary=dataset_summary,
            output_zarr_uri=output_zarr_uri,
            overwrite=overwrite,
            inference_device=inference_device,
            fail_on_error=fail_on_error,
            stage_callback=stage_callback,
        )

    def apply_masks_to_zarr(
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
        mask_types: list[str],
        output_zarr_uri: str | None = None,
        fail_on_error: bool = False,
        stage_callback: Any = None,
        backend: str = "auto",
        threshold: float = 0.45,
        overwrite: bool = True,
        inference_device: str | None = None,
        include_shadows: bool = True,
        water_backend: str | None = None,
        water_overwrite: bool = True,
        water_inference_device: str | None = None,
    ) -> dict[str, Any]:
        return self._combined_workflow.apply_to_zarr(
            job_id=job_id,
            zarr_uri=zarr_uri,
            provider=provider,
            collection=collection,
            product_type=product_type,
            scene_id=scene_id,
            acquisition_datetime=acquisition_datetime,
            dataset_summary=dataset_summary,
            mask_types=mask_types,
            output_zarr_uri=output_zarr_uri,
            fail_on_error=fail_on_error,
            stage_callback=stage_callback,
            backend=backend,
            threshold=threshold,
            overwrite=overwrite,
            inference_device=inference_device,
            include_shadows=include_shadows,
            water_backend=water_backend,
            water_overwrite=water_overwrite,
            water_inference_device=water_inference_device,
            cloud_runner=self.apply_cloud_to_zarr,
            water_runner=self.apply_omniwater_to_zarr,
        )

    def apply_cloud_to_zarr(
        self,
        *,
        job_id: str | None,
        source_zarr_uri: str,
        output_zarr_uri: str,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, Any],
        fail_on_error: bool = False,
        stage_callback: Any = None,
        threshold: float = 0.45,
        backend: str = "auto",
        overwrite: bool = True,
        inference_device: str | None = None,
        include_shadows: bool = True,
    ) -> dict[str, Any]:
        return self._cloud_workflow.apply_to_zarr(
            job_id=job_id,
            source_zarr_uri=source_zarr_uri,
            output_zarr_uri=output_zarr_uri,
            provider=provider,
            collection=collection,
            product_type=product_type,
            scene_id=scene_id,
            acquisition_datetime=acquisition_datetime,
            dataset_summary=dataset_summary,
            fail_on_error=fail_on_error,
            stage_callback=stage_callback,
            threshold=threshold,
            backend=backend,
            overwrite=overwrite,
            inference_device=inference_device,
            include_shadows=include_shadows,
        )
