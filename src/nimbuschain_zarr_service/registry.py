from __future__ import annotations

from dataclasses import dataclass

from nimbuschain_zarr_service.copernicus import build_copernicus_dataset, convert_copernicus_to_zarr
from nimbuschain_zarr_service.landsat import build_landsat_dataset, convert_landsat_to_zarr
from nimbuschain_zarr_service.models import (
    ConversionOutcomeRecord,
    ConversionProgressRecord,
    ConversionRequestRecord,
)
from nimbuschain_zarr_service.ports import ConversionProviderPort, ProgressCallback


@dataclass
class _FunctionConversionProvider:
    name: str
    _convert_fn: object
    _build_dataset_fn: object

    def convert(
        self,
        request: ConversionRequestRecord,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> ConversionOutcomeRecord:
        callback = None
        if progress_callback is not None:
            def _callback(payload: dict[str, object]) -> None:
                progress_callback(ConversionProgressRecord.from_mapping(payload))
            callback = _callback
        written_uri, data_family, summary, dataset_summary = self._convert_fn(
            raw_uri=request.raw_uri,
            provider=request.provider,
            collection=request.collection,
            scene_id=request.scene_id,
            output_uri=request.output_uri,
            product_type=request.product_type,
            progress_callback=callback,
        )
        return ConversionOutcomeRecord(
            written_uri=written_uri,
            data_family=data_family,
            summary=dict(summary),
            dataset_summary=dict(dataset_summary),
        )

    def build_dataset(self, request: ConversionRequestRecord):
        return self._build_dataset_fn(
            raw_uri=request.raw_uri,
            provider=request.provider,
            collection=request.collection,
            scene_id=request.scene_id,
            product_type=request.product_type,
        )


class ConversionProviderRegistry:
    def __init__(self, providers: dict[str, ConversionProviderPort] | None = None) -> None:
        self._providers: dict[str, ConversionProviderPort] = {}
        for name, provider in (providers or {}).items():
            self.register(name, provider)

    @classmethod
    def default(cls) -> "ConversionProviderRegistry":
        return cls(
            {
                "copernicus": _FunctionConversionProvider(
                    name="copernicus",
                    _convert_fn=convert_copernicus_to_zarr,
                    _build_dataset_fn=build_copernicus_dataset,
                ),
                "usgs": _FunctionConversionProvider(
                    name="usgs",
                    _convert_fn=convert_landsat_to_zarr,
                    _build_dataset_fn=build_landsat_dataset,
                ),
            }
        )

    def register(self, name: str, provider: ConversionProviderPort) -> None:
        self._providers[str(name).strip().lower()] = provider

    def resolve(self, name: str) -> ConversionProviderPort:
        normalized = str(name or "").strip().lower()
        provider = self._providers.get(normalized)
        if provider is None:
            raise ValueError(f"Unsupported provider: {name}")
        return provider

    def list(self) -> list[ConversionProviderPort]:
        return list(self._providers.values())
