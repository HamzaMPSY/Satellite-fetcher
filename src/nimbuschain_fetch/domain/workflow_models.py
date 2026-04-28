from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MaskWorkflowItem:
    zarr_uri: str
    status: str
    pipeline_metadata: dict[str, Any] = field(default_factory=dict)
    conversion_metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    failed_step: str | None = None

    @classmethod
    def from_execution(
        cls,
        *,
        zarr_uri: str,
        mask_execution: dict[str, Any],
    ) -> "MaskWorkflowItem":
        return cls(
            zarr_uri=zarr_uri,
            status=str(mask_execution.get("status") or ""),
            pipeline_metadata=dict(mask_execution.get("pipeline_metadata") or {}),
            conversion_metadata=dict(mask_execution.get("conversion_metadata") or {}),
            errors=[str(item) for item in list(mask_execution.get("errors") or []) if str(item).strip()],
            failed_step=(
                str(mask_execution.get("failed_step") or "").strip() or None
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "zarr_uri": self.zarr_uri,
            "status": self.status,
            "pipeline_metadata": dict(self.pipeline_metadata),
            "conversion_metadata": dict(self.conversion_metadata),
            "errors": list(self.errors),
            "failed_step": self.failed_step,
        }


@dataclass(slots=True)
class MaskWorkflowSummary:
    status: str
    mask_types: list[str] = field(default_factory=list)
    mask_mode: str = "integrated"
    items: list[MaskWorkflowItem] = field(default_factory=list)
    masked_zarr_uri: str | None = None
    water_mask: dict[str, Any] = field(default_factory=dict)
    cloud_mask: dict[str, Any] = field(default_factory=dict)
    mask_quality: dict[str, Any] = field(default_factory=dict)
    failed_step: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "mask_types": list(self.mask_types),
            "mask_mode": self.mask_mode,
            "items": [item.to_payload() for item in self.items],
        }
        if self.masked_zarr_uri:
            payload["masked_zarr_uri"] = self.masked_zarr_uri
        if self.water_mask:
            payload["water_mask"] = dict(self.water_mask)
        if self.cloud_mask:
            payload["cloud_mask"] = dict(self.cloud_mask)
        if self.mask_quality:
            payload["mask_quality"] = dict(self.mask_quality)
        if self.failed_step:
            payload["failed_step"] = self.failed_step
        return payload
