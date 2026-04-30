from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nimbuschain_fetch.domain.metadata import (
    ConversionMetadataRecord,
    PayloadRecord,
    PipelineMetadataRecord,
)


@dataclass(slots=True)
class MaskWorkflowItem:
    zarr_uri: str
    status: str
    pipeline_metadata: PipelineMetadataRecord | dict[str, Any] = field(
        default_factory=PipelineMetadataRecord
    )
    conversion_metadata: ConversionMetadataRecord | dict[str, Any] = field(
        default_factory=ConversionMetadataRecord
    )
    errors: list[str] = field(default_factory=list)
    failed_step: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.pipeline_metadata, PipelineMetadataRecord):
            self.pipeline_metadata = PipelineMetadataRecord.from_mapping(self.pipeline_metadata)
        if not isinstance(self.conversion_metadata, ConversionMetadataRecord):
            self.conversion_metadata = ConversionMetadataRecord.from_mapping(self.conversion_metadata)

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
            pipeline_metadata=PipelineMetadataRecord.from_mapping(mask_execution.get("pipeline_metadata")),
            conversion_metadata=ConversionMetadataRecord.from_mapping(mask_execution.get("conversion_metadata")),
            errors=[str(item) for item in list(mask_execution.get("errors") or []) if str(item).strip()],
            failed_step=(
                str(mask_execution.get("failed_step") or "").strip() or None
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "zarr_uri": self.zarr_uri,
            "status": self.status,
            "pipeline_metadata": self.pipeline_metadata.to_dict(),
            "conversion_metadata": self.conversion_metadata.to_dict(),
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
    water_mask: PayloadRecord | dict[str, Any] = field(default_factory=PayloadRecord)
    cloud_mask: PayloadRecord | dict[str, Any] = field(default_factory=PayloadRecord)
    mask_quality: PayloadRecord | dict[str, Any] = field(default_factory=PayloadRecord)
    failed_step: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.water_mask, PayloadRecord):
            self.water_mask = PayloadRecord.from_mapping(self.water_mask)
        if not isinstance(self.cloud_mask, PayloadRecord):
            self.cloud_mask = PayloadRecord.from_mapping(self.cloud_mask)
        if not isinstance(self.mask_quality, PayloadRecord):
            self.mask_quality = PayloadRecord.from_mapping(self.mask_quality)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "mask_types": list(self.mask_types),
            "mask_mode": self.mask_mode,
            "items": [item.to_payload() for item in self.items],
        }
        if self.masked_zarr_uri:
            payload["masked_zarr_uri"] = self.masked_zarr_uri
        if self.water_mask.payload:
            payload["water_mask"] = self.water_mask.to_dict()
        if self.cloud_mask.payload:
            payload["cloud_mask"] = self.cloud_mask.to_dict()
        if self.mask_quality.payload:
            payload["mask_quality"] = self.mask_quality.to_dict()
        if self.failed_step:
            payload["failed_step"] = self.failed_step
        return payload
