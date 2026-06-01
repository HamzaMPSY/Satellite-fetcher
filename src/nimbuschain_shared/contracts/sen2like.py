from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


ALLOWED_STEPS = {
    "geometric_processing",
    "atmospheric_correction",
    "sbaf",
    "valid_pixel_mask",
    "brdf_adjustment",
    "data_fusion",
    "packaging",
    "validation",
    "cleanup",
}

ALLOWED_CLEANUP_MODES = {"none", "light", "medium", "aggressive", "strict"}


class Sen2LikeNormalizeRequest(BaseModel):
    job_id: str | None = None
    pipeline_id: str | None = None
    trace_id: str | None = None
    products: list[str] = Field(default_factory=list)
    landsat_path: str | None = None
    working_dir: str | None = None
    workers: int = Field(default=4, ge=1, le=128)
    steps: list[str] | None = None
    s2_path: str | None = None
    no_resume: bool = False
    no_routing: bool = False
    router_fallback_ok: bool = False
    exclude_water: bool = False
    cleanup_mode: str = "none"
    cleanup_dry_run: bool = False
    timeout_seconds: float | None = Field(default=None, gt=0)
    spark_master: str | None = None
    nested_band_parallelism: bool | None = None
    band_workers: int | None = Field(default=None, ge=1, le=32)
    safe_retry: bool | None = None
    preprocess_target_shape: str | None = None
    direct_zarr: bool | None = None
    zarr_output_dir: str | None = None
    extra_args: list[str] = Field(default_factory=list)

    @field_validator("products")
    @classmethod
    def _strip_products(cls, value: list[str]) -> list[str]:
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("steps")
    @classmethod
    def _validate_steps(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized = [str(item).strip() for item in value if str(item).strip()]
        unknown = sorted(set(normalized) - ALLOWED_STEPS)
        if unknown:
            raise ValueError(f"Unknown Sen2Like steps: {', '.join(unknown)}")
        return normalized

    @field_validator("cleanup_mode")
    @classmethod
    def _validate_cleanup_mode(cls, value: str) -> str:
        normalized = str(value or "none").strip().lower()
        if normalized not in ALLOWED_CLEANUP_MODES:
            raise ValueError(
                "cleanup_mode must be one of: "
                + ", ".join(sorted(ALLOWED_CLEANUP_MODES))
            )
        return normalized

    @field_validator("preprocess_target_shape")
    @classmethod
    def _strip_preprocess_target_shape(cls, value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @field_validator("zarr_output_dir")
    @classmethod
    def _strip_zarr_output_dir(cls, value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    def product_inputs(self) -> list[str]:
        products = list(self.products)
        if self.landsat_path and self.landsat_path.strip():
            products.insert(0, self.landsat_path.strip())
        deduped: list[str] = []
        for product in products:
            if product not in deduped:
                deduped.append(product)
        return deduped


class Sen2LikeProductOutput(BaseModel):
    product: str
    output_dir: str
    manifest_path: str | None = None
    normalized_uri: str | None = None
    zarr_uri: str | None = None
    zarr_exists: bool = False
    zarr_data_family: str | None = None
    zarr_summary: dict[str, Any] | None = None
    zarr_dataset_summary: dict[str, Any] | None = None
    zarr_error: str | None = None
    exists: bool = False


class Sen2LikeNormalizeResponse(BaseModel):
    status: str
    job_id: str | None = None
    pipeline_id: str | None = None
    trace_id: str | None = None
    products: list[str]
    working_dir: str
    outputs: list[Sen2LikeProductOutput]
    duration_seconds: float
    return_code: int
    command: list[str]
    stdout_tail: str = ""
    stderr_tail: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
