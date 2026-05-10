from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nimbuschain_fetch.domain.metadata import PipelineMetadataRecord
from nimbuschain_fetch.models import PipelineState
from nimbuschain_fetch.pipeline.sen2like import is_landsat_selection
from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient


@dataclass(frozen=True, slots=True)
class Sen2LikeRoutingResult:
    conversion_inputs: list[str]
    pipeline_metadata: PipelineMetadataRecord
    routed: bool
    response: dict[str, Any]


class Sen2LikeNormalizationRouter:
    """Route Landsat raw products through Sen2Like before Zarr conversion."""

    def __init__(self, runtime: Any):
        self._rt = runtime

    @staticmethod
    def requires_normalization(
        *,
        provider: str,
        collection: str,
        product_type: str | None,
    ) -> bool:
        return is_landsat_selection(
            provider=provider,
            collection=collection,
            product_type=product_type,
        )

    def normalize_if_required(
        self,
        *,
        job_id: str,
        provider: str,
        collection: str,
        product_type: str | None,
        raw_outputs: list[str],
        pipeline_metadata: PipelineMetadataRecord | dict[str, Any],
        is_cancelled_now,
    ) -> Sen2LikeRoutingResult:
        metadata_record = (
            pipeline_metadata
            if isinstance(pipeline_metadata, PipelineMetadataRecord)
            else PipelineMetadataRecord.from_mapping(pipeline_metadata)
        )
        if not self.requires_normalization(
            provider=provider,
            collection=collection,
            product_type=product_type,
        ):
            return Sen2LikeRoutingResult(
                conversion_inputs=list(raw_outputs),
                pipeline_metadata=metadata_record.merged_with(
                    {
                        "sen2like_required": False,
                        "zarr_input_source": "raw",
                        "zarr_input_outputs": list(raw_outputs),
                    }
                ),
                routed=False,
                response={},
            )

        if not raw_outputs:
            return Sen2LikeRoutingResult(
                conversion_inputs=[],
                pipeline_metadata=metadata_record.merged_with(
                    {
                        "sen2like_required": True,
                        "sen2like_status": "skipped",
                        "sen2like_skip_reason": "no_raw_outputs",
                        "zarr_input_source": "raw",
                        "zarr_input_outputs": [],
                    }
                ),
                routed=False,
                response={},
            )

        service_url = str(self._rt.settings.nimbus_sen2like_service_url or "").strip()
        if not service_url:
            error = (
                "NIMBUS_SEN2LIKE_SERVICE_URL is required for Landsat jobs before Zarr conversion."
            )
            failed_metadata = metadata_record.merged_with(
                {
                    "sen2like_required": True,
                    "sen2like_status": "failed",
                    "sen2like_error": error,
                    "sen2like_inputs": list(raw_outputs),
                    "zarr_input_source": "sen2like",
                    "zarr_input_outputs": [],
                }
            )
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.sen2like_failed,
                pipeline_step=PipelineState.sen2like_failed.value,
                pipeline_progress=71.0,
                pipeline_metadata=failed_metadata.to_dict(),
                raw_outputs=raw_outputs,
                event_type="job.sen2like_failed",
                event_payload={"error": error, "input_count": len(raw_outputs)},
            )
            raise RuntimeError(
                error
            )

        queued_metadata = metadata_record.merged_with(
            {
                "sen2like_required": True,
                "sen2like_status": "queued",
                "sen2like_input_count": len(raw_outputs),
                "sen2like_inputs": list(raw_outputs),
                "sen2like_service_url": service_url,
            }
        )
        self._rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.sen2like_queued,
            pipeline_step=PipelineState.sen2like_queued.value,
            pipeline_progress=71.0,
            pipeline_metadata=queued_metadata.to_dict(),
            raw_outputs=raw_outputs,
            event_type="job.sen2like_queued",
            event_payload={"input_count": len(raw_outputs)},
        )
        if is_cancelled_now():
            raise self._rt.job_cancelled_error_cls("Job cancellation requested.")

        running_metadata = queued_metadata.merged_with({"sen2like_status": "running"})
        self._rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.sen2like_running,
            pipeline_step=PipelineState.sen2like_running.value,
            pipeline_progress=72.0,
            pipeline_metadata=running_metadata.to_dict(),
            raw_outputs=raw_outputs,
            event_type="job.sen2like_running",
            event_payload={"input_count": len(raw_outputs)},
        )

        try:
            response = self._normalize(
                service_url=service_url,
                job_id=job_id,
                products=raw_outputs,
            )
            outputs = _sen2like_outputs(response)
            if not outputs:
                raise RuntimeError("Sen2Like service returned no normalized outputs.")
        except Exception as exc:
            failed_metadata = running_metadata.merged_with(
                {
                    "sen2like_status": "failed",
                    "sen2like_error": str(exc),
                    "zarr_input_source": "sen2like",
                    "zarr_input_outputs": [],
                }
            )
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.sen2like_failed,
                pipeline_step=PipelineState.sen2like_failed.value,
                pipeline_progress=72.0,
                pipeline_metadata=failed_metadata.to_dict(),
                raw_outputs=raw_outputs,
                event_type="job.sen2like_failed",
                event_payload={"error": str(exc), "input_count": len(raw_outputs)},
            )
            raise

        finished_metadata = running_metadata.merged_with(
            {
                "sen2like_status": "written",
                "sen2like_outputs": outputs,
                "sen2like_output_count": len(outputs),
                "sen2like_response": response,
                "zarr_input_source": "sen2like",
                "zarr_input_outputs": outputs,
                "zarr_conversion_provider": "copernicus",
                "zarr_conversion_collection": "SENTINEL-2",
                "zarr_conversion_product_type": "S2MSI2A",
            }
        )
        self._rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.sen2like_written,
            pipeline_step=PipelineState.sen2like_written.value,
            pipeline_progress=74.0,
            pipeline_metadata=finished_metadata.to_dict(),
            raw_outputs=raw_outputs,
            event_type="job.sen2like_written",
            event_payload={
                "input_count": len(raw_outputs),
                "output_count": len(outputs),
                "sen2like_outputs": outputs,
            },
        )
        return Sen2LikeRoutingResult(
            conversion_inputs=outputs,
            pipeline_metadata=finished_metadata,
            routed=True,
            response=response,
        )

    def _normalize(
        self,
        *,
        service_url: str,
        job_id: str,
        products: list[str],
    ) -> dict[str, Any]:
        client = Sen2LikeServiceClient(service_url=service_url)
        try:
            return client.normalize(
                products=products,
                job_id=job_id,
                pipeline_id=job_id,
                working_dir=self._rt.settings.nimbus_sen2like_work_dir,
                workers=int(self._rt.settings.nimbus_sen2like_workers),
                timeout_seconds=self._rt.settings.nimbus_sen2like_timeout_seconds,
            )
        finally:
            client.close()


def _sen2like_outputs(response: dict[str, Any]) -> list[str]:
    outputs: list[str] = []
    for item in list(response.get("outputs") or []):
        if not isinstance(item, dict):
            continue
        output_uri = str(item.get("normalized_uri") or item.get("output_dir") or "").strip()
        if output_uri and output_uri not in outputs:
            outputs.append(output_uri)
    return outputs
