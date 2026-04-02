from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType
from typing import Any

from nimbuschain_mask_service.backends.cloud import heuristic_backend as cloud_heuristic_backend
from nimbuschain_mask_service.backends.cloud import omnicloudmask_backend
from nimbuschain_mask_service.backends.water import heuristic_backend as water_heuristic_backend
from nimbuschain_mask_service.backends.water import omniwatermask_backend


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

    def run(self, **kwargs: Any) -> Any:
        return self.module.run(**kwargs)


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


def list_backends(kind: str) -> list[BackendDescriptor]:
    modules = _CLOUD_BACKENDS if kind == "cloud" else _WATER_BACKENDS
    return [_descriptor(kind, name, module) for name, module in modules.items()]


def resolve_cloud_backend(name: str | None) -> BackendDescriptor:
    requested = str(name or "auto").strip().lower()
    if requested in {"", "auto"}:
        requested = "omnicloudmask" if omnicloudmask_backend.available() else "heuristic"
    module = _CLOUD_BACKENDS.get(requested)
    if module is None:
        raise ValueError(f"Unsupported cloud backend: {name}")
    return _descriptor("cloud", requested, module)


def resolve_water_backend(name: str | None = None) -> BackendDescriptor:
    requested = str(name or "auto").strip().lower()
    if requested in {"", "auto"}:
        requested = "omniwatermask" if omniwatermask_backend.available() else "heuristic"
    if requested in {"fallback", "ndwi"}:
        requested = "heuristic"
    module = _WATER_BACKENDS.get(requested)
    if module is None:
        raise ValueError(f"Unsupported water backend: {name}")
    return _descriptor("water", requested, module)


def registry_status() -> dict[str, object]:
    return {
        "cloud": [
            {"name": item.name, "available": item.available, "primary": item.name == "omnicloudmask"}
            for item in list_backends("cloud")
        ],
        "water": [
            {"name": item.name, "available": item.available, "primary": item.name == "omniwatermask"}
            for item in list_backends("water")
        ],
    }
