from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from nimbuschain_zarr_service.models import (
    ConversionOutcomeRecord,
    ConversionProgressRecord,
    ConversionRequestRecord,
)


ProgressCallback = Callable[[ConversionProgressRecord], None]


@runtime_checkable
class ConversionProviderPort(Protocol):
    @property
    def name(self) -> str:
        ...

    def convert(
        self,
        request: ConversionRequestRecord,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> ConversionOutcomeRecord:
        ...

    def build_dataset(
        self,
        request: ConversionRequestRecord,
    ) -> tuple[Any, dict[str, Any]]:
        ...
