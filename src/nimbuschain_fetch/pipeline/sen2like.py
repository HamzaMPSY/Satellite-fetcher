from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from nimbuschain_fetch.pipeline.core import PipelineContext, StageResult


Sen2LikeServiceClient: Any | None = None


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

        landsat_input = self._landsat_input(context)
        if not landsat_input:
            return StageResult.skipped_result(
                self.name,
                reason="sen2like_input_missing",
                metadata={
                    "provider": context.provider,
                    "collection": context.collection,
                    "product_type": context.product_type,
                    "service_url": configured_url,
                    "message": (
                        "Sen2Like service is configured, but this stage did not "
                        "receive a raw Landsat product path yet."
                    ),
                },
            )

        payload = self._normalize(configured_url, context, landsat_input)
        outputs = [
            str(item.get("normalized_uri") or item.get("output_dir"))
            for item in list(payload.get("outputs") or [])
            if item.get("normalized_uri") or item.get("output_dir")
        ]
        metadata: dict[str, Any] = {
            "provider": context.provider,
            "collection": context.collection,
            "product_type": context.product_type,
            "service_url": configured_url,
            "service_url_configured": True,
            "landsat_input": landsat_input,
            "sen2like_response": payload,
        }
        return StageResult.succeeded_result(
            self.name,
            outputs=outputs or [f"stage://{self.name}/{context.job_id or 'manual'}"],
            metadata=metadata,
        )

    def _resolved_service_url(self) -> str | None:
        raw_value = self.service_url
        if raw_value is None:
            raw_value = os.getenv("NIMBUS_SEN2LIKE_SERVICE_URL")
        value = str(raw_value or "").strip()
        return value or None

    @staticmethod
    def _landsat_input(context: PipelineContext) -> str | None:
        for key in ("landsat_path", "raw_uri", "source_uri", "product_path"):
            value = context.payload.get(key)
            if value:
                return str(value).strip()
            value = context.get(key)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _normalize(
        service_url: str,
        context: PipelineContext,
        landsat_input: str,
    ) -> dict[str, Any]:
        global Sen2LikeServiceClient
        if Sen2LikeServiceClient is None:
            from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient as _Client

            Sen2LikeServiceClient = _Client
        workers = int(context.payload.get("sen2like_workers") or context.get("sen2like_workers", 4))
        client = Sen2LikeServiceClient(service_url=service_url)
        try:
            return client.normalize(
                products=[landsat_input],
                job_id=context.job_id or None,
                pipeline_id=str(context.payload.get("pipeline_id") or "") or None,
                trace_id=str(context.payload.get("trace_id") or "") or None,
                working_dir=(
                    str(context.payload.get("sen2like_working_dir") or "").strip()
                    or context.get("sen2like_working_dir")
                    or None
                ),
                workers=workers,
            )
        finally:
            client.close()
