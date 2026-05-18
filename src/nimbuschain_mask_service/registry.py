from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType
from typing import Any

from nimbuschain_mask_service.backends.cloud import heuristic_backend as cloud_heuristic_backend
from nimbuschain_mask_service.backends.cloud import omnicloudmask_backend
from nimbuschain_mask_service.backends.water import heuristic_backend as water_heuristic_backend
from nimbuschain_mask_service.backends.water import omniwatermask_backend
from nimbuschain_mask_service.models import (
    BackendAvailabilityRecord,
    CloudBackendRunRequest,
    DatasetSummaryRecord,
    RegistryStatusRecord,
    WaterBackendRunRequest,
    WaterMaskState,
)
from nimbuschain_mask_service.ports import CloudMaskBackendPort, WaterMaskBackendPort


@dataclass(frozen=True)
class BackendDescriptor:
    name: str
    kind: str
    module: ModuleType
    available: bool

    def required_bands(self, sensor: Any) -> tuple[str, ...]:
        func = getattr(self.module, "required_bands", None)
        if callable(func):
            return tuple(func(sensor))
        return tuple()

    def normalize_inputs(self, sensor: Any) -> bool:
        func = getattr(self.module, "normalize_inputs", None)
        if callable(func):
            return bool(func(sensor))
        return True

    def run(self, request: CloudBackendRunRequest | WaterBackendRunRequest) -> Any:
        if isinstance(request, CloudBackendRunRequest):
            return self.module.run(
                sensor=request.sensor,
                channels=request.channels,
                threshold=request.threshold,
                inference_device=request.inference_device,
                include_shadows=request.include_shadows,
                valid_mask=request.valid_mask,
            )
        return self.module.run(
            job_id=request.job_id,
            zarr_uri=request.zarr_uri,
            source_zarr_uri=request.source_zarr_uri,
            provider=request.provider,
            collection=request.collection,
            product_type=request.product_type,
            scene_id=request.scene_id,
            acquisition_datetime=request.acquisition_datetime,
            dataset_summary=request.dataset_summary.to_dict(),
            output_zarr_uri=request.output_zarr_uri,
            overwrite=request.overwrite,
            inference_device=request.inference_device,
            fail_on_error=request.fail_on_error,
            stage_callback=request.stage_callback,
        )


class _ModuleCloudBackendAdapter:
    def __init__(self, module: ModuleType):
        self._module = module

    @property
    def name(self) -> str:
        return str(getattr(self._module, "NAME"))

    @property
    def kind(self) -> str:
        return str(getattr(self._module, "KIND"))

    def available(self) -> bool:
        return bool(self._module.available())

    def required_bands(self, sensor: Any) -> tuple[str, ...]:
        func = getattr(self._module, "required_bands", None)
        if callable(func):
            return tuple(func(sensor))
        return tuple()

    def normalize_inputs(self, sensor: Any) -> bool:
        func = getattr(self._module, "normalize_inputs", None)
        if callable(func):
            return bool(func(sensor))
        return True

    def run(
        self,
        request: CloudBackendRunRequest,
    ) -> Any:
        return self._module.run(
            sensor=request.sensor,
            channels=request.channels,
            threshold=request.threshold,
            inference_device=request.inference_device,
            include_shadows=request.include_shadows,
            valid_mask=request.valid_mask,
        )


class _ModuleWaterBackendAdapter:
    def __init__(self, module: ModuleType):
        self._module = module

    @property
    def name(self) -> str:
        return str(getattr(self._module, "NAME"))

    @property
    def kind(self) -> str:
        return str(getattr(self._module, "KIND"))

    def available(self) -> bool:
        return bool(self._module.available())

    def run(self, request: WaterBackendRunRequest) -> WaterMaskState:
        return WaterMaskState.from_mapping(
            self._module.run(
                job_id=request.job_id,
                zarr_uri=request.zarr_uri,
                source_zarr_uri=request.source_zarr_uri,
                provider=request.provider,
                collection=request.collection,
                product_type=request.product_type,
                scene_id=request.scene_id,
                acquisition_datetime=request.acquisition_datetime,
                dataset_summary=request.dataset_summary.to_dict(),
                output_zarr_uri=request.output_zarr_uri,
                overwrite=request.overwrite,
                inference_device=request.inference_device,
                fail_on_error=request.fail_on_error,
                stage_callback=request.stage_callback,
            )
        )


_CLOUD_BACKENDS = {
    omnicloudmask_backend.NAME: omnicloudmask_backend,
    cloud_heuristic_backend.NAME: cloud_heuristic_backend,
}

_WATER_BACKENDS = {
    omniwatermask_backend.NAME: omniwatermask_backend,
    water_heuristic_backend.NAME: water_heuristic_backend,
}


def _descriptor(kind: str, name: str, module: ModuleType) -> BackendDescriptor:
    available = bool(getattr(module, "available")())
    return BackendDescriptor(name=name, kind=kind, module=module, available=available)


class CloudBackendRegistry:
    def __init__(self, backends: dict[str, CloudMaskBackendPort] | None = None):
        self._backends: dict[str, CloudMaskBackendPort] = {}
        for name, backend in (backends or {}).items():
            self.register(name, backend)

    @classmethod
    def default(cls) -> "CloudBackendRegistry":
        return cls(
            {
                omnicloudmask_backend.NAME: _ModuleCloudBackendAdapter(omnicloudmask_backend),
                cloud_heuristic_backend.NAME: _ModuleCloudBackendAdapter(cloud_heuristic_backend),
            }
        )

    def register(self, name: str, backend: CloudMaskBackendPort) -> None:
        self._backends[str(name).strip().lower()] = backend

    def list(self) -> list[CloudMaskBackendPort]:
        return list(self._backends.values())

    def resolve(self, name: str | None) -> CloudMaskBackendPort:
        requested = str(name or "auto").strip().lower()
        if requested in {"", "auto"}:
            requested = "omnicloudmask"
        backend = self._backends.get(requested)
        if backend is None:
            raise ValueError(f"Unsupported cloud backend: {name}")
        return backend


class WaterBackendRegistry:
    def __init__(self, backends: dict[str, WaterMaskBackendPort] | None = None):
        self._backends: dict[str, WaterMaskBackendPort] = {}
        for name, backend in (backends or {}).items():
            self.register(name, backend)

    @classmethod
    def default(cls) -> "WaterBackendRegistry":
        return cls(
            {
                omniwatermask_backend.NAME: _ModuleWaterBackendAdapter(omniwatermask_backend),
                water_heuristic_backend.NAME: _ModuleWaterBackendAdapter(water_heuristic_backend),
            }
        )

    def register(self, name: str, backend: WaterMaskBackendPort) -> None:
        self._backends[str(name).strip().lower()] = backend

    def list(self) -> list[WaterMaskBackendPort]:
        return list(self._backends.values())

    def resolve(self, name: str | None = None) -> WaterMaskBackendPort:
        requested = str(name or "auto").strip().lower()
        if requested in {"", "auto"}:
            primary = self._backends.get("omniwatermask")
            if primary is not None and primary.available():
                return primary
            requested = "heuristic"
        if requested in {"fallback", "ndwi"}:
            requested = "heuristic"
        backend = self._backends.get(requested)
        if backend is None:
            raise ValueError(f"Unsupported water backend: {name}")
        return backend


def list_backends(kind: str) -> list[BackendDescriptor]:
    if kind == "cloud":
        return [
            BackendDescriptor(
                name=item.name,
                kind=item.kind,
                module=_CLOUD_BACKENDS[item.name],
                available=item.available(),
            )
            for item in CloudBackendRegistry.default().list()
        ]
    return [
        BackendDescriptor(
            name=item.name,
            kind=item.kind,
            module=_WATER_BACKENDS[item.name],
            available=item.available(),
        )
        for item in WaterBackendRegistry.default().list()
    ]


def resolve_cloud_backend(name: str | None) -> BackendDescriptor:
    backend = CloudBackendRegistry.default().resolve(name)
    return _descriptor("cloud", backend.name, _CLOUD_BACKENDS[backend.name])


def resolve_water_backend(name: str | None = None) -> BackendDescriptor:
    backend = WaterBackendRegistry.default().resolve(name)
    return _descriptor("water", backend.name, _WATER_BACKENDS[backend.name])


def registry_status() -> dict[str, object]:
    return RegistryStatusRecord(
        cloud=[
            BackendAvailabilityRecord(
                name=item.name,
                available=item.available,
                primary=item.name == "omnicloudmask",
            )
            for item in list_backends("cloud")
        ],
        water=[
            BackendAvailabilityRecord(
                name=item.name,
                available=item.available,
                primary=item.name == "omniwatermask",
            )
            for item in list_backends("water")
        ],
    ).to_dict()
