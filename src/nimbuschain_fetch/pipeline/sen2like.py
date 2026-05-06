from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from nimbuschain_fetch.pipeline.core import PipelineContext, StageResult


def is_landsat_context(context: PipelineContext) -> bool:
    provider = context.provider.strip().lower()
    collection = context.collection.strip().lower()
    product_type = (context.product_type or "").strip().lower()
    if provider != "usgs":
        return False
    return "landsat" in collection or product_type.startswith("l")


@dataclass(frozen=True, slots=True)
class Sen2LikeStage:
    name: str = "sen2like"
    depends_on: tuple[str, ...] = ("fetch",)
    service_url: str | None = None
    skip_reason: str = "not_landsat_or_usgs"

    def should_run(self, context: PipelineContext) -> bool:
        return is_landsat_context(context)

    def run(self, context: PipelineContext) -> StageResult:
        configured_url = self._resolved_service_url()
        if not configured_url:
            return StageResult.skipped_result(
                self.name,
                reason="sen2like_service_url_missing",
                metadata={
                    "provider": context.provider,
                    "collection": context.collection,
                    "product_type": context.product_type,
                    "message": (
                        "Landsat Sen2Like normalization hook is enabled, but "
                        "NIMBUS_SEN2LIKE_SERVICE_URL is not configured yet."
                    ),
                },
            )

        metadata: dict[str, Any] = {
            "provider": context.provider,
            "collection": context.collection,
            "product_type": context.product_type,
            "service_url": configured_url,
            "service_url_configured": True,
            "message": (
                "Sen2Like service URL is configured. The HTTP client and "
                "microservice execution will be wired in the next migration step."
            ),
        }
        return StageResult.succeeded_result(
            self.name,
            outputs=[f"stage://{self.name}/{context.job_id or 'manual'}"],
            metadata=metadata,
        )

    def _resolved_service_url(self) -> str | None:
        raw_value = self.service_url
        if raw_value is None:
            raw_value = os.getenv("NIMBUS_SEN2LIKE_SERVICE_URL")
        value = str(raw_value or "").strip()
        return value or None
