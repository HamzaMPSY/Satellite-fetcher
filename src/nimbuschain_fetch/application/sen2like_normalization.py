from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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

        previous_error = str(metadata_record.payload.get("sen2like_error") or "").strip()
        previous_status = str(metadata_record.payload.get("sen2like_status") or "").strip().lower()
        previous_fallback = _raw_fallback_payload(
            settings=self._rt.settings,
            error=previous_error,
            raw_outputs=raw_outputs,
            service_url=service_url,
        )
        if previous_status == "failed" and previous_fallback is not None:
            fallback_metadata = metadata_record.merged_with(
                {
                    "sen2like_status": "raw_fallback",
                    "sen2like_fallback_reason": previous_fallback["reason"],
                    "sen2like_fallback_message": previous_fallback["message"],
                    "zarr_input_source": "raw",
                    "zarr_input_outputs": list(raw_outputs),
                }
            )
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.sen2like_written,
                pipeline_step="sen2like_fallback",
                pipeline_progress=74.0,
                pipeline_metadata=fallback_metadata.to_dict(),
                raw_outputs=raw_outputs,
                event_type="job.sen2like_fallback",
                event_payload={
                    "error": previous_error,
                    "input_count": len(raw_outputs),
                    "reason": previous_fallback["reason"],
                    "from_previous_failure": True,
                },
            )
            return Sen2LikeRoutingResult(
                conversion_inputs=list(raw_outputs),
                pipeline_metadata=fallback_metadata,
                routed=False,
                response=previous_fallback,
            )

        queued_metadata = metadata_record.merged_with(
            {
                "sen2like_required": True,
                "sen2like_status": "queued",
                "sen2like_input_count": len(raw_outputs),
                "sen2like_inputs": list(raw_outputs),
                "sen2like_service_url": service_url,
                "sen2like_execution_mode": "sequential_single_product",
                "sen2like_working_dir": _job_working_dir(
                    self._rt.settings.nimbus_sen2like_work_dir,
                    job_id,
                ),
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
            fallback = _raw_fallback_payload(
                settings=self._rt.settings,
                error=str(exc),
                raw_outputs=raw_outputs,
                service_url=service_url,
            )
            if fallback is not None:
                fallback_metadata = running_metadata.merged_with(
                    {
                        "sen2like_status": "raw_fallback",
                        "sen2like_error": str(exc),
                        "sen2like_fallback_reason": fallback["reason"],
                        "sen2like_fallback_message": fallback["message"],
                        "zarr_input_source": "raw",
                        "zarr_input_outputs": list(raw_outputs),
                    }
                )
                self._rt._update_pipeline(
                    job_id,
                    pipeline_state=PipelineState.sen2like_written,
                    pipeline_step="sen2like_fallback",
                    pipeline_progress=74.0,
                    pipeline_metadata=fallback_metadata.to_dict(),
                    raw_outputs=raw_outputs,
                    event_type="job.sen2like_fallback",
                    event_payload={
                        "error": str(exc),
                        "input_count": len(raw_outputs),
                        "reason": fallback["reason"],
                    },
                )
                return Sen2LikeRoutingResult(
                    conversion_inputs=list(raw_outputs),
                    pipeline_metadata=fallback_metadata,
                    routed=False,
                    response=fallback,
                )

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
        working_dir = _job_working_dir(
            self._rt.settings.nimbus_sen2like_work_dir,
            job_id,
        )
        working_path = Path(working_dir)
        client: Sen2LikeServiceClient | None = None
        responses: list[dict[str, Any]] = []
        outputs: list[dict[str, Any]] = []
        duration_seconds = 0.0
        reused_existing_count = 0
        try:
            for index, product in enumerate(products, start=1):
                product_job_id = f"{job_id}-{index}" if len(products) > 1 else job_id
                existing_output = _existing_sen2like_output(
                    product=product,
                    working_dir=working_path,
                )
                if existing_output is not None:
                    reused_existing_count += 1
                    outputs.append(existing_output)
                    responses.append(
                        {
                            "status": "succeeded",
                            "job_id": product_job_id,
                            "pipeline_id": job_id,
                            "products": [str(product)],
                            "working_dir": working_dir,
                            "outputs": [existing_output],
                            "duration_seconds": 0.0,
                            "return_code": 0,
                            "metadata": {
                                "reused_existing_output": True,
                                "execution_mode": "resume_existing_safe",
                            },
                        }
                    )
                    continue

                if client is None:
                    client = Sen2LikeServiceClient(service_url=service_url)
                try:
                    response = client.normalize(
                        products=[product],
                        job_id=product_job_id,
                        pipeline_id=job_id,
                        working_dir=working_dir,
                        workers=int(self._rt.settings.nimbus_sen2like_workers),
                        no_resume=True,
                        timeout_seconds=self._rt.settings.nimbus_sen2like_timeout_seconds,
                    )
                except Exception as exc:
                    product_name = Path(str(product)).name or str(product)
                    raise RuntimeError(f"Sen2Like failed for {product_name}: {exc}") from exc
                responses.append(response)
                outputs.extend(
                    dict(item)
                    for item in list(response.get("outputs") or [])
                    if isinstance(item, dict)
                )
                try:
                    duration_seconds += float(response.get("duration_seconds") or 0.0)
                except (TypeError, ValueError):
                    pass
            return {
                "status": "succeeded",
                "job_id": job_id,
                "pipeline_id": job_id,
                "products": list(products),
                "working_dir": working_dir,
                "outputs": outputs,
                "duration_seconds": duration_seconds,
                "return_code": 0,
                "responses": responses,
                "metadata": {
                    "execution_mode": "sequential_single_product",
                    "product_count": len(products),
                    "reused_existing_output_count": reused_existing_count,
                    "service_url": service_url,
                },
            }
        finally:
            if client is not None:
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


def _job_working_dir(base_working_dir: str | None, job_id: str) -> str:
    base = Path(str(base_working_dir or "/data/downloads/sen2like").strip())
    safe_job_id = str(job_id or "job").strip().replace("/", "_")
    return str(base / safe_job_id)


def _existing_sen2like_output(
    *,
    product: str,
    working_dir: Path,
) -> dict[str, Any] | None:
    output_dir = working_dir / _sen2like_product_output_name(product)
    normalized_uri = _existing_normalized_output_uri(output_dir)
    if not normalized_uri:
        return None
    manifest_path = output_dir / "manifest.json"
    return {
        "product": str(product),
        "output_dir": str(output_dir),
        "manifest_path": str(manifest_path) if manifest_path.exists() else None,
        "normalized_uri": normalized_uri,
        "exists": True,
        "reused_existing": True,
    }


def _sen2like_product_output_name(product: str) -> str:
    name = Path(str(product)).name
    lowered = name.lower()
    for suffix in (".tar.gz", ".tgz", ".tar"):
        if lowered.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem or name


def _existing_normalized_output_uri(output_dir: Path) -> str | None:
    if not output_dir.exists():
        return None
    candidates = [
        *sorted((output_dir / "SAFE").glob("*.SAFE")),
        *sorted(output_dir.glob("*.SAFE")),
        *sorted(output_dir.glob("*_L2F")),
    ]
    for candidate in candidates:
        if _valid_existing_sen2like_output(candidate):
            return str(candidate)
    return None


def _valid_existing_sen2like_output(path: Path) -> bool:
    if not path.is_dir():
        return False
    if path.suffix.upper() == ".SAFE" and not (path / "manifest.safe").exists():
        return False
    for raster in path.rglob("*"):
        if raster.is_file() and raster.suffix.lower() in {".tif", ".tiff", ".jp2"}:
            return True
    return False


def _raw_fallback_payload(
    *,
    settings: Any,
    error: str,
    raw_outputs: list[str],
    service_url: str,
) -> dict[str, Any] | None:
    if not bool(getattr(settings, "nimbus_sen2like_raw_fallback", False)):
        return None
    if not _is_sen2like_resource_exhaustion(error):
        return None
    return {
        "status": "raw_fallback",
        "reason": "sen2like_resource_exhausted",
        "message": (
            "Sen2Like was killed by the runtime, so Nimbus will continue with the "
            "downloaded Landsat raw products."
        ),
        "service_url": service_url,
        "products": list(raw_outputs),
        "outputs": [
            {
                "product": str(product),
                "output_dir": None,
                "normalized_uri": None,
                "exists": False,
            }
            for product in raw_outputs
        ],
        "metadata": {
            "fallback_to_raw": True,
            "error": error,
        },
    }


def _is_sen2like_resource_exhaustion(error: str) -> bool:
    lowered = str(error or "").strip().lower()
    if not lowered:
        return False
    markers = (
        "killed during processing",
        "not have enough memory",
        "out of memory",
        "oom",
        "exit code -9",
        "exit code 137",
        "return_code=-9",
        "return_code 137",
    )
    return any(marker in lowered for marker in markers)
