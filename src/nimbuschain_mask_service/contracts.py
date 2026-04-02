from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CloudMaskOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["auto", "heuristic", "omnicloudmask"] = "auto"
    threshold: float = Field(default=0.45, ge=0.0, le=1.0)
    overwrite: bool = True
    inference_device: str | None = None
    include_shadows: bool = True


class WaterMaskOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["auto", "heuristic", "omniwatermask", "fallback", "ndwi"] = "auto"
    overwrite: bool = True
    inference_device: str | None = None


class MaskApplyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_zarr_uri: str
    output_zarr_uri: str | None = None
    provider: str
    collection: str
    product_type: str | None = None
    scene_id: str
    acquisition_datetime: str | None = None
    dataset_summary: dict[str, Any] = Field(default_factory=dict)
    mask_types: list[Literal["water", "cloud"]] = Field(min_length=1)
    fail_on_error: bool = False
    cloud: CloudMaskOptions = Field(default_factory=CloudMaskOptions)
    water: WaterMaskOptions = Field(default_factory=WaterMaskOptions)
    backend: Literal["auto", "heuristic", "omnicloudmask"] | None = None
    threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    overwrite: bool | None = None
    inference_device: str | None = None
    include_shadows: bool | None = None

    @model_validator(mode="before")
    @classmethod
    def _accept_legacy_fetcher_payloads(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        legacy_source = normalized.get("source_zarr_uri") or normalized.get("zarr_uri")
        if legacy_source and not normalized.get("source_zarr_uri"):
            normalized["source_zarr_uri"] = legacy_source
        normalized.pop("zarr_uri", None)

        water = dict(normalized.get("water") or {})
        if normalized.get("water_backend") is not None:
            water.setdefault("backend", normalized.pop("water_backend"))
        if normalized.get("water_overwrite") is not None:
            water.setdefault("overwrite", normalized.pop("water_overwrite"))
        if normalized.get("water_inference_device") is not None:
            water.setdefault("inference_device", normalized.pop("water_inference_device"))
        if water:
            normalized["water"] = water

        for key in (
            "job_id",
            "job_type",
            "source_job_id",
            "mask_contract_version",
            "stage_callback",
        ):
            normalized.pop(key, None)

        return normalized

    @field_validator("mask_types")
    @classmethod
    def _normalize_mask_types(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            candidate = str(value or "").strip().lower()
            if candidate not in {"water", "cloud"}:
                raise ValueError("mask_types must contain only 'water' and/or 'cloud'.")
            if candidate not in normalized:
                normalized.append(candidate)
        if not normalized:
            raise ValueError("mask_types cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def _merge_legacy_top_level_fields(self) -> "MaskApplyRequest":
        cloud = self.cloud.model_copy(update={})
        water = self.water.model_copy(update={})
        if self.backend is not None:
            cloud = cloud.model_copy(update={"backend": self.backend})
        if self.threshold is not None:
            cloud = cloud.model_copy(update={"threshold": self.threshold})
        if self.overwrite is not None:
            cloud = cloud.model_copy(update={"overwrite": self.overwrite})
            water = water.model_copy(update={"overwrite": self.overwrite})
        if self.inference_device is not None:
            cloud = cloud.model_copy(update={"inference_device": self.inference_device})
            water = water.model_copy(update={"inference_device": self.inference_device})
        if self.include_shadows is not None:
            cloud = cloud.model_copy(update={"include_shadows": self.include_shadows})
        object.__setattr__(self, "cloud", cloud)
        object.__setattr__(self, "water", water)
        return self
