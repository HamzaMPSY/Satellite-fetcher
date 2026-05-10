from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from nimbuschain_fetch.engine.conversion_runtime import convert_raw_outputs
from nimbuschain_shared.dto import ZarrConversionRequest


class FetcherConversionSupport:
    """Conversion facade helpers bridging fetcher workflows and converter ports."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def convert_single_raw_output(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        product_type: str | None,
        raw_uri: str,
        scene_id: str,
        output_uri: str,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        converter = self._rt._converter()
        convert_request = ZarrConversionRequest(
            job_id=job_id,
            pipeline_id=job_id,
            trace_id=uuid.uuid4().hex,
            provider=provider_name,
            collection=self._rt._normalize_collection_for_zarr(provider_name, collection),
            scene_id=scene_id,
            raw_uri=raw_uri,
            output_uri=output_uri,
            product_type=self._rt._normalize_product_type_for_zarr(product_type),
            progress_callback=progress_callback,
        )
        written_uri, data_family, conversion_summary, dataset_summary = self._run_converter(
            converter,
            convert_request,
        )
        return {
            "raw_uri": raw_uri,
            "scene_id": scene_id,
            "zarr_uri": written_uri,
            "data_family": data_family,
            "summary": conversion_summary,
            "dataset_summary": dataset_summary,
        }

    def convert_raw_outputs(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        product_type: str | None,
        raw_outputs: list[str],
        is_cancelled: Callable[[], bool],
        scene_id_override: str | None = None,
        output_uri_override: str | None = None,
        pipeline_metadata: dict[str, Any] | None = None,
        conversion_provider_name: str | None = None,
        conversion_collection: str | None = None,
        conversion_product_type: str | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        return convert_raw_outputs(
            self._rt,
            job_id=job_id,
            provider_name=provider_name,
            collection=collection,
            product_type=product_type,
            raw_outputs=raw_outputs,
            is_cancelled=is_cancelled,
            scene_id_override=scene_id_override,
            output_uri_override=output_uri_override,
            pipeline_metadata=pipeline_metadata,
            conversion_provider_name=conversion_provider_name,
            conversion_collection=conversion_collection,
            conversion_product_type=conversion_product_type,
        )

    def convert_existing_job(
        self,
        job_id: str,
        request: Any,
        *,
        continue_pipeline: bool = False,
    ):
        return self._rt._manual_conversion_service.convert_existing_job(
            job_id,
            request,
            continue_pipeline=continue_pipeline,
        )

    def _run_converter(
        self,
        converter: Any,
        request: ZarrConversionRequest,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        if hasattr(converter, "convert_request"):
            return converter.convert_request(request)
        try:
            return converter.convert(**self._full_convert_kwargs(request))
        except TypeError as exc:
            if not self._is_legacy_convert_signature_error(exc):
                raise
            return converter.convert(**self._legacy_convert_kwargs(request))

    @staticmethod
    def _full_convert_kwargs(request: ZarrConversionRequest) -> dict[str, Any]:
        return {
            "job_id": request.job_id,
            "pipeline_id": request.pipeline_id,
            "trace_id": request.trace_id,
            "provider": request.provider,
            "collection": request.collection,
            "scene_id": request.scene_id,
            "raw_uri": request.raw_uri,
            "output_uri": request.output_uri,
            "product_type": request.product_type,
            "progress_callback": request.progress_callback,
        }

    @staticmethod
    def _legacy_convert_kwargs(request: ZarrConversionRequest) -> dict[str, Any]:
        return {
            "provider": request.provider,
            "collection": request.collection,
            "scene_id": request.scene_id,
            "raw_uri": request.raw_uri,
            "output_uri": request.output_uri,
            "product_type": request.product_type,
            "progress_callback": request.progress_callback,
        }

    @staticmethod
    def _is_legacy_convert_signature_error(exc: TypeError) -> bool:
        message = str(exc)
        return "unexpected keyword argument" in message or "positional argument" in message
