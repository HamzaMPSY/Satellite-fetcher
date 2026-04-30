from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import Protocol, runtime_checkable

from nimbuschain_mask_service.inference import CloudMaskResult
from nimbuschain_mask_service.models import (
    CloudBackendRunRequest,
    StageEventPayload,
    WaterBackendRunRequest,
    WaterMaskState,
)


StageCallback = Callable[[str, StageEventPayload], None]


@runtime_checkable
class CloudMaskBackendPort(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def kind(self) -> str:
        ...

    def available(self) -> bool:
        ...

    def required_bands(self, sensor: Any) -> tuple[str, ...]:
        ...

    def normalize_inputs(self, sensor: Any) -> bool:
        ...

    def run(
        self,
        request: CloudBackendRunRequest,
    ) -> CloudMaskResult:
        ...


@runtime_checkable
class WaterMaskBackendPort(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def kind(self) -> str:
        ...

    def available(self) -> bool:
        ...

    def run(self, request: WaterBackendRunRequest) -> WaterMaskState:
        ...
