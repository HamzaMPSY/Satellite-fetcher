from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from nimbuschain_fetch.pipeline.core import PipelineContext, StageResult


Sen2LikeServiceClient: Any | None = None


def is_landsat_selection(
    *,
    provider: str,
    collection: str,
    product_type: str | None = None,
) -> bool:
    normalized_provider = provider.strip().lower()
    normalized_collection = collection.strip().lower()
    normalized_product_type = (product_type or "").strip().lower()
    if normalized_provider != "usgs":
        return False
    return "landsat" in normalized_collection or normalized_product_type.startswith("l")


def is_landsat_context(context: PipelineContext) -> bool:
    return is_landsat_selection(
        provider=context.provider,
        collection=context.collection,
        product_type=context.product_type,
    )


@dataclass(frozen=True, slots=True)
class Sen2LikeStage:
    name: str = "sen2like"
    depends_on: tuple[str, ...] = ("fetch",)
    service_url: str | None = None
    allow_raw_fallback: bool = False
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

        landsat_inputs = self._landsat_inputs(context)
        if not landsat_inputs:
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

        try:
            payload = self._normalize(configured_url, context, landsat_inputs)
        except Exception as exc:
            if self.allow_raw_fallback:
                return self._fallback_to_raw(
                    context,
                    configured_url=configured_url,
                    landsat_inputs=landsat_inputs,
                    reason="sen2like_service_failed",
                    error=exc,
                )
            return self._failed_result(
                context,
                configured_url=configured_url,
                landsat_inputs=landsat_inputs,
                reason="sen2like_service_failed",
                error=exc,
            )
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
            "landsat_input": landsat_inputs[0],
            "landsat_inputs": landsat_inputs,
            "sen2like_response": payload,
        }
        if not outputs:
            if self.allow_raw_fallback:
                return self._fallback_to_raw(
                    context,
                    configured_url=configured_url,
                    landsat_inputs=landsat_inputs,
                    reason="sen2like_output_missing",
                    response=payload,
                )
            return self._failed_result(
                context,
                configured_url=configured_url,
                landsat_inputs=landsat_inputs,
                reason="sen2like_output_missing",
                response=payload,
            )

        resolved_outputs = outputs
        context.set("sen2like_outputs", resolved_outputs)
        context.set("zarr_inputs", resolved_outputs)
        return StageResult.succeeded_result(
            self.name,
            outputs=resolved_outputs,
            metadata=metadata,
        )

    def _resolved_service_url(self) -> str | None:
        raw_value = self.service_url
        if raw_value is None:
            raw_value = os.getenv("NIMBUS_SEN2LIKE_SERVICE_URL")
        value = str(raw_value or "").strip()
        return value or None

    def _fallback_to_raw(
        self,
        context: PipelineContext,
        *,
        configured_url: str,
        landsat_inputs: list[str],
        reason: str,
        error: Exception | None = None,
        response: dict[str, Any] | None = None,
    ) -> StageResult:
        metadata: dict[str, Any] = {
            "provider": context.provider,
            "collection": context.collection,
            "product_type": context.product_type,
            "service_url": configured_url,
            "service_url_configured": True,
            "landsat_input": landsat_inputs[0],
            "landsat_inputs": landsat_inputs,
            "fallback_to_raw": True,
            "fallback_reason": reason,
        }
        if error is not None:
            metadata["fallback_error"] = str(error)
            metadata["fallback_error_type"] = type(error).__name__
        if response is not None:
            metadata["sen2like_response"] = response
        context.set("sen2like_outputs", landsat_inputs)
        context.set("zarr_inputs", landsat_inputs)
        context.set("sen2like_fallback_to_raw", True)
        return StageResult.succeeded_result(
            self.name,
            outputs=landsat_inputs,
            metadata=metadata,
        )

    def _failed_result(
        self,
        context: PipelineContext,
        *,
        configured_url: str,
        landsat_inputs: list[str],
        reason: str,
        error: Exception | None = None,
        response: dict[str, Any] | None = None,
    ) -> StageResult:
        metadata: dict[str, Any] = {
            "provider": context.provider,
            "collection": context.collection,
            "product_type": context.product_type,
            "service_url": configured_url,
            "service_url_configured": True,
            "landsat_input": landsat_inputs[0],
            "landsat_inputs": landsat_inputs,
            "fallback_to_raw": False,
            "fallback_allowed": False,
            "failure_reason": reason,
        }
        if error is not None:
            metadata["error_type"] = type(error).__name__
        if response is not None:
            metadata["sen2like_response"] = response
        message = str(error) if error is not None else "Sen2Like service returned no normalized outputs."
        return StageResult.failed_result(
            self.name,
            error=message,
            metadata=metadata,
        )

    @staticmethod
    def _landsat_inputs(context: PipelineContext) -> list[str]:
        values: list[str] = []
        for key in ("landsat_path", "raw_uri", "source_uri", "product_path"):
            value = context.payload.get(key)
            if value:
                values.append(str(value).strip())
            value = context.get(key)
            if value:
                values.append(str(value).strip())
        for key in ("landsat_paths", "raw_uris", "source_uris", "product_paths", "raw_outputs"):
            raw_values = context.payload.get(key) or context.get(key)
            if isinstance(raw_values, (list, tuple, set)):
                values.extend(str(item).strip() for item in raw_values)
        seen: set[str] = set()
        normalized: list[str] = []
        for value in values:
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @staticmethod
    def _normalize(
        service_url: str,
        context: PipelineContext,
        landsat_inputs: list[str],
    ) -> dict[str, Any]:
        global Sen2LikeServiceClient
        if Sen2LikeServiceClient is None:
            from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient as _Client

            Sen2LikeServiceClient = _Client
        workers = int(context.payload.get("sen2like_workers") or context.get("sen2like_workers", 1))
        client = Sen2LikeServiceClient(service_url=service_url)
        try:
            return client.normalize(
                products=landsat_inputs,
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
