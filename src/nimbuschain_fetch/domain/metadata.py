from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any


def _as_payload(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


@dataclass(slots=True)
class PayloadRecord(Mapping[str, Any]):
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "PayloadRecord":
        return cls(payload=_as_payload(value))

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)

    def __getitem__(self, key: str) -> Any:
        return self.payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)

    def __bool__(self) -> bool:
        return bool(self.payload)


@dataclass(slots=True)
class PipelineMetadataRecord(PayloadRecord):

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "PipelineMetadataRecord":
        return cls(payload=_as_payload(value))

    @property
    def mask_types(self) -> list[str]:
        return [str(item).strip() for item in list(self.payload.get("mask_types") or []) if str(item).strip()]

    @property
    def cube_mode(self) -> str | None:
        value = str(self.payload.get("cube_mode") or "").strip()
        return value or None

    @property
    def mask_status(self) -> str | None:
        value = str(self.payload.get("mask_status") or self.payload.get("status") or "").strip()
        return value or None

    @property
    def source_job_id(self) -> str | None:
        value = str(self.payload.get("source_job_id") or "").strip()
        return value or None

    @property
    def source_zarr_uri(self) -> str | None:
        value = str(self.payload.get("source_zarr_uri") or "").strip()
        return value or None

    @property
    def masked_zarr_uri(self) -> str | None:
        value = str(self.payload.get("masked_zarr_uri") or "").strip()
        return value or None

    @property
    def water_mask(self) -> dict[str, Any]:
        return _as_payload(self.payload.get("water_mask")) if isinstance(self.payload.get("water_mask"), Mapping) else {}

    @property
    def cloud_mask(self) -> dict[str, Any]:
        return _as_payload(self.payload.get("cloud_mask")) if isinstance(self.payload.get("cloud_mask"), Mapping) else {}

    def merged_with(self, other: Mapping[str, Any] | None) -> "PipelineMetadataRecord":
        merged = dict(self.payload)
        merged.update(_as_payload(other))
        return PipelineMetadataRecord(payload=merged)


@dataclass(slots=True)
class ConversionMetadataRecord(PayloadRecord):

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "ConversionMetadataRecord":
        return cls(payload=_as_payload(value))

    @property
    def mask_status(self) -> str | None:
        mask_payload = _as_payload(self.payload.get("mask")) if isinstance(self.payload.get("mask"), Mapping) else {}
        value = str(mask_payload.get("status") or self.payload.get("status") or "").strip()
        return value or None

    @property
    def source_job_id(self) -> str | None:
        value = str(self.payload.get("source_job_id") or "").strip()
        return value or None

    @property
    def source_zarr_uri(self) -> str | None:
        value = str(self.payload.get("source_zarr_uri") or "").strip()
        return value or None

    @property
    def masked_zarr_uri(self) -> str | None:
        value = str(self.payload.get("masked_zarr_uri") or "").strip()
        return value or None

    @property
    def water_mask(self) -> dict[str, Any]:
        return _as_payload(self.payload.get("water_mask")) if isinstance(self.payload.get("water_mask"), Mapping) else {}

    @property
    def cloud_mask(self) -> dict[str, Any]:
        return _as_payload(self.payload.get("cloud_mask")) if isinstance(self.payload.get("cloud_mask"), Mapping) else {}

    @property
    def items(self) -> list[dict[str, Any]]:
        return [_as_payload(item) for item in list(self.payload.get("items") or []) if isinstance(item, Mapping)]

    def merged_with(self, other: Mapping[str, Any] | None) -> "ConversionMetadataRecord":
        merged = dict(self.payload)
        merged.update(_as_payload(other))
        return ConversionMetadataRecord(payload=merged)


@dataclass(slots=True)
class MaskStateRecord:
    water_mask: dict[str, Any] = field(default_factory=dict)
    cloud_mask: dict[str, Any] = field(default_factory=dict)
    mask_quality: dict[str, Any] = field(default_factory=dict)
    water_fraction: float = 0.0
    cloud_fraction: float = 0.0
    cloud_only_fraction: float = 0.0
    shadow_fraction: float = 0.0

    @classmethod
    def from_sources(cls, *sources: Mapping[str, Any] | None) -> "MaskStateRecord":
        def _first_mapping(key: str) -> dict[str, Any]:
            for source in sources:
                payload = _as_payload(source)
                value = payload.get(key)
                if isinstance(value, Mapping):
                    mapped = _as_payload(value)
                    if mapped:
                        return mapped
            return {}

        def _first_float(key: str) -> float:
            for source in sources:
                payload = _as_payload(source)
                value = payload.get(key)
                try:
                    if value is not None:
                        return float(value)
                except (TypeError, ValueError):
                    continue
            return 0.0

        return cls(
            water_mask=_first_mapping("water_mask"),
            cloud_mask=_first_mapping("cloud_mask"),
            mask_quality=_first_mapping("mask_quality"),
            water_fraction=_first_float("water_fraction"),
            cloud_fraction=_first_float("cloud_fraction"),
            cloud_only_fraction=_first_float("cloud_only_fraction"),
            shadow_fraction=_first_float("shadow_fraction"),
        )

    def apply_failure(self, reason: str) -> None:
        for payload in (self.water_mask, self.cloud_mask):
            if payload:
                payload["status"] = "failed"
                payload["reason"] = reason

    def to_metadata_fields(self) -> dict[str, Any]:
        return {
            "water_mask": dict(self.water_mask),
            "cloud_mask": dict(self.cloud_mask),
            "mask_quality": dict(self.mask_quality),
            "water_fraction": self.water_fraction,
            "cloud_fraction": self.cloud_fraction,
            "cloud_only_fraction": self.cloud_only_fraction,
            "shadow_fraction": self.shadow_fraction,
        }


@dataclass(slots=True)
class ConversionItemRecord:
    raw_uri: str | None = None
    scene_id: str | None = None
    zarr_uri: str | None = None
    data_family: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    dataset_summary: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "ConversionItemRecord":
        payload = _as_payload(value)
        return cls(
            raw_uri=str(payload.get("raw_uri") or "").strip() or None,
            scene_id=str(payload.get("scene_id") or "").strip() or None,
            zarr_uri=str(payload.get("zarr_uri") or "").strip() or None,
            data_family=str(payload.get("data_family") or "").strip() or None,
            summary=_as_payload(payload.get("summary")) if isinstance(payload.get("summary"), Mapping) else {},
            dataset_summary=(
                _as_payload(payload.get("dataset_summary"))
                if isinstance(payload.get("dataset_summary"), Mapping)
                else {}
            ),
        )


@dataclass(slots=True)
class StringMapRecord(Mapping[str, str]):
    payload: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "StringMapRecord":
        if not isinstance(value, Mapping):
            return cls()
        return cls(
            payload={
                str(key): str(item)
                for key, item in value.items()
                if str(key).strip()
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)

    def __getitem__(self, key: str) -> str:
        return self.payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)

    def __bool__(self) -> bool:
        return bool(self.payload)
