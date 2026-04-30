from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nimbuschain_zarr_service.models import (
    ConversionOutcomeRecord,
    ConversionProgressRecord,
    ConversionRequestRecord,
)
from nimbuschain_zarr_service.ports import ProgressCallback
from nimbuschain_zarr_service.registry import ConversionProviderRegistry


class ZarrConversionService:
    def __init__(self, *, registry: ConversionProviderRegistry | None = None) -> None:
        self._registry = registry or ConversionProviderRegistry.default()

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
        request = ConversionRequestRecord(
            provider=str(provider).strip().lower(),
            collection=collection,
            scene_id=scene_id,
            raw_uri=raw_uri,
            output_uri=output_uri,
            product_type=product_type,
        )
        provider_adapter = self._registry.resolve(request.provider)
        result = provider_adapter.convert(
            request,
            progress_callback=self._wrap_legacy_progress_callback(progress_callback),
        )
        return result.as_tuple()

    def convert_record(
        self,
        *,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> ConversionOutcomeRecord:
        request = ConversionRequestRecord(
            provider=provider,
            collection=collection,
            scene_id=scene_id,
            raw_uri=raw_uri,
            output_uri=output_uri,
            product_type=product_type,
        )
        provider_adapter = self._registry.resolve(request.provider)
        return provider_adapter.convert(request, progress_callback=progress_callback)

    @staticmethod
    def _wrap_legacy_progress_callback(
        callback: Callable[[dict[str, Any]], None] | None,
    ) -> ProgressCallback | None:
        if callback is None:
            return None

        def _wrapped(progress: ConversionProgressRecord) -> None:
            callback(progress.to_dict())

        return _wrapped
