from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import tarfile
from typing import Any

from nimbuschain_fetch.domain.metadata import PipelineMetadataRecord
from nimbuschain_fetch.models import PipelineState
from nimbuschain_fetch.pipeline.sen2like import is_landsat_selection
from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient


_MTL_KV_RE = re.compile(r'^\s*([A-Z0-9_]+)\s*=\s*"?([^"]*)"?\s*$')
_REQUIRED_SAFE_BANDS = frozenset({"B02", "B03", "B04", "B08", "B11", "B12"})


@dataclass(frozen=True, slots=True)
class Sen2LikeRoutingResult:
    conversion_inputs: list[str]
    sen2like_outputs: list[str]
    direct_zarr_outputs: list[str]
    direct_zarr_items: list[dict[str, Any]]
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
                sen2like_outputs=[],
                direct_zarr_outputs=[],
                direct_zarr_items=[],
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
                sen2like_outputs=[],
                direct_zarr_outputs=[],
                direct_zarr_items=[],
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
                sen2like_outputs=[],
                direct_zarr_outputs=[],
                direct_zarr_items=[],
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
                "sen2like_execution_mode": _sen2like_execution_mode(
                    product_count=len(raw_outputs),
                    worker_count=int(self._rt.settings.nimbus_sen2like_workers),
                ),
                "sen2like_workers": int(self._rt.settings.nimbus_sen2like_workers),
                "sen2like_nested_band_parallelism": _settings_bool(
                    self._rt.settings,
                    "nimbus_sen2like_nested_band_parallelism",
                    default=True,
                ),
                "sen2like_band_workers": _settings_int(
                    self._rt.settings,
                    "nimbus_sen2like_band_workers",
                    default=2,
                ),
                "sen2like_safe_retry": _settings_bool(
                    self._rt.settings,
                    "nimbus_sen2like_safe_retry",
                    default=True,
                ),
                "sen2like_error": None,
                "sen2like_resume_failed": False,
                "sen2like_preprocess_target_shape": _settings_str(
                    self._rt.settings,
                    "nimbus_sen2like_preprocess_target_shape",
                    default="native",
                ),
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
            routed_outputs = _sen2like_routed_outputs(response)
            outputs = list(routed_outputs["sen2like_outputs"])
            conversion_inputs = list(routed_outputs["conversion_inputs"])
            direct_zarr_outputs = list(routed_outputs["direct_zarr_outputs"])
            direct_zarr_items = list(routed_outputs["direct_zarr_items"])
            if not outputs and not direct_zarr_outputs:
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
                    sen2like_outputs=[],
                    direct_zarr_outputs=[],
                    direct_zarr_items=[],
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
                "sen2like_direct_zarr_outputs": direct_zarr_outputs,
                "sen2like_direct_zarr_output_count": len(direct_zarr_outputs),
                "sen2like_direct_zarr_items": direct_zarr_items,
                "sen2like_direct_zarr_status": str(
                    dict(response.get("metadata") or {}).get("direct_zarr_status") or ""
                ),
                "sen2like_response": response,
                "sen2like_error": None,
                "sen2like_resume_failed": False,
                "zarr_input_source": "sen2like",
                "zarr_input_outputs": conversion_inputs,
                "zarr_prebuilt_outputs": direct_zarr_outputs,
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
            conversion_inputs=conversion_inputs,
            sen2like_outputs=outputs,
            direct_zarr_outputs=direct_zarr_outputs,
            direct_zarr_items=direct_zarr_items,
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
        missing_products: list[str] = []
        try:
            for index, product in enumerate(products, start=1):
                product_job_id = f"{job_id}-{index}" if len(products) > 1 else job_id
                existing_output = _existing_sen2like_output(
                    product=product,
                    working_dir=working_path,
                )
                if existing_output is not None:
                    existing_output = _with_existing_direct_zarr_output(
                        existing_output,
                        settings=self._rt.settings,
                    )
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
                missing_products.append(product)

            if missing_products:
                if client is None:
                    client = Sen2LikeServiceClient(service_url=service_url)
                try:
                    response = client.normalize(
                        products=missing_products,
                        job_id=job_id,
                        pipeline_id=job_id,
                        working_dir=working_dir,
                        workers=int(self._rt.settings.nimbus_sen2like_workers),
                        no_resume=True,
                        timeout_seconds=self._rt.settings.nimbus_sen2like_timeout_seconds,
                        preprocess_target_shape=_settings_str(
                            self._rt.settings,
                            "nimbus_sen2like_preprocess_target_shape",
                            default="native",
                        ),
                        direct_zarr=_settings_bool(
                            self._rt.settings,
                            "nimbus_sen2like_direct_zarr",
                            default=False,
                        ),
                        zarr_output_dir=_settings_optional_str(
                            self._rt.settings,
                            "nimbus_sen2like_zarr_dir",
                        ),
                    )
                except Exception as exc:
                    product_names = ", ".join(
                        Path(str(item)).name or str(item)
                        for item in missing_products
                    )
                    raise RuntimeError(f"Sen2Like failed for {product_names}: {exc}") from exc
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
            direct_zarr_metadata = _direct_zarr_metadata_from_outputs(
                outputs,
                settings=self._rt.settings,
            )
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
                    "execution_mode": _sen2like_execution_mode(
                        product_count=len(products),
                        worker_count=int(self._rt.settings.nimbus_sen2like_workers),
                        missing_product_count=len(missing_products),
                    ),
                    "product_count": len(products),
                    "batched_product_count": len(missing_products),
                    "workers": int(self._rt.settings.nimbus_sen2like_workers),
                    "nested_band_parallelism": _settings_bool(
                        self._rt.settings,
                        "nimbus_sen2like_nested_band_parallelism",
                        default=True,
                    ),
                    "band_workers": _settings_int(
                        self._rt.settings,
                        "nimbus_sen2like_band_workers",
                        default=2,
                    ),
                    "safe_retry": _settings_bool(
                        self._rt.settings,
                        "nimbus_sen2like_safe_retry",
                        default=True,
                    ),
                    "preprocess_target_shape": _settings_str(
                        self._rt.settings,
                        "nimbus_sen2like_preprocess_target_shape",
                        default="native",
                    ),
                    "product_parallelism": (
                        len(missing_products) > 1
                        and int(self._rt.settings.nimbus_sen2like_workers) > 1
                    ),
                    "tile_parallelism": int(self._rt.settings.nimbus_sen2like_workers) > 1,
                    "band_parallelism": int(self._rt.settings.nimbus_sen2like_workers) > 1,
                    "reused_existing_output_count": reused_existing_count,
                    "service_url": service_url,
                    "direct_zarr_requested": _settings_bool(
                        self._rt.settings,
                        "nimbus_sen2like_direct_zarr",
                        default=False,
                    ),
                    "direct_zarr_output_dir": _settings_optional_str(
                        self._rt.settings,
                        "nimbus_sen2like_zarr_dir",
                    ),
                    **direct_zarr_metadata,
                },
            }
        finally:
            if client is not None:
                client.close()


def _sen2like_execution_mode(
    *,
    product_count: int,
    worker_count: int,
    missing_product_count: int | None = None,
) -> str:
    runnable_products = product_count if missing_product_count is None else missing_product_count
    if runnable_products <= 0:
        return "resume_existing_safe"
    if runnable_products == 1:
        return "single_product_parallel_steps" if worker_count > 1 else "single_product"
    if worker_count > 1:
        return "parallel_multi_product"
    return "batched_multi_product_single_worker"


def _settings_bool(settings: Any, name: str, *, default: bool) -> bool:
    value = getattr(settings, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _settings_int(settings: Any, name: str, *, default: int) -> int:
    value = getattr(settings, name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _settings_str(settings: Any, name: str, *, default: str) -> str:
    value = getattr(settings, name, default)
    normalized = str(value or "").strip()
    return normalized or str(default)


def _settings_optional_str(settings: Any, name: str) -> str | None:
    normalized = str(getattr(settings, name, "") or "").strip()
    return normalized or None


def _direct_zarr_metadata_from_outputs(
    outputs: list[dict[str, Any]],
    *,
    settings: Any,
) -> dict[str, Any]:
    routed = _sen2like_routed_outputs({"outputs": outputs})
    direct_zarr_outputs = list(routed["direct_zarr_outputs"])
    direct_zarr_items = list(routed["direct_zarr_items"])
    sen2like_outputs = list(routed["sen2like_outputs"])
    if not _settings_bool(settings, "nimbus_sen2like_direct_zarr", default=False):
        status = "skipped"
    elif direct_zarr_outputs and len(direct_zarr_outputs) >= len(sen2like_outputs):
        status = "written"
    elif direct_zarr_outputs:
        status = "partial"
    else:
        status = "failed" if sen2like_outputs else "skipped"
    return {
        "direct_zarr_status": status,
        "direct_zarr_outputs": direct_zarr_outputs,
        "direct_zarr_items": direct_zarr_items,
        "direct_zarr_output_count": len(direct_zarr_outputs),
    }


def _sen2like_routed_outputs(response: dict[str, Any]) -> dict[str, Any]:
    sen2like_outputs: list[str] = []
    conversion_inputs: list[str] = []
    direct_zarr_outputs: list[str] = []
    output_direct_items: list[dict[str, Any]] = []
    for item in list(response.get("outputs") or []):
        if not isinstance(item, dict):
            continue
        output_uri = str(item.get("normalized_uri") or item.get("output_dir") or "").strip()
        zarr_uri = str(item.get("zarr_uri") or "").strip()
        zarr_exists_value = item.get("zarr_exists")
        zarr_exists = bool(zarr_uri) if zarr_exists_value is None else bool(zarr_exists_value)
        if output_uri and output_uri not in sen2like_outputs:
            sen2like_outputs.append(output_uri)
        if zarr_uri and zarr_exists:
            if zarr_uri not in direct_zarr_outputs:
                direct_zarr_outputs.append(zarr_uri)
            output_direct_items.append(
                {
                    "product": item.get("product"),
                    "normalized_uri": output_uri,
                    "scene_id": _scene_id_from_uri(output_uri),
                    "zarr_uri": zarr_uri,
                    "data_family": item.get("zarr_data_family") or "optical",
                    "summary": dict(item.get("zarr_summary") or {}),
                    "dataset_summary": dict(item.get("zarr_dataset_summary") or {}),
                }
            )
        elif output_uri and output_uri not in conversion_inputs:
            conversion_inputs.append(output_uri)

    metadata = dict(response.get("metadata") or {})
    metadata_items = [
        dict(item)
        for item in list(metadata.get("direct_zarr_items") or [])
        if isinstance(item, dict) and str(item.get("zarr_uri") or "").strip()
    ]
    direct_items = metadata_items or output_direct_items
    if metadata_items:
        for item in metadata_items:
            zarr_uri = str(item.get("zarr_uri") or "").strip()
            if zarr_uri and zarr_uri not in direct_zarr_outputs:
                direct_zarr_outputs.append(zarr_uri)

    return {
        "sen2like_outputs": sen2like_outputs,
        "conversion_inputs": conversion_inputs,
        "direct_zarr_outputs": direct_zarr_outputs,
        "direct_zarr_items": direct_items,
    }


def _sen2like_outputs(response: dict[str, Any]) -> list[str]:
    return list(_sen2like_routed_outputs(response)["sen2like_outputs"])


def _scene_id_from_uri(uri: str) -> str:
    name = Path(str(uri).rstrip("/")).name
    if name.upper().endswith(".SAFE"):
        return name[:-5]
    return Path(name).stem or name


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
    expected_timestamp = _expected_acquisition_timestamp(
        product=product,
        working_dir=working_dir,
    )
    normalized_uri = _existing_normalized_output_uri(
        output_dir,
        expected_acquisition_timestamp=expected_timestamp,
    )
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


def _with_existing_direct_zarr_output(
    output: dict[str, Any],
    *,
    settings: Any,
) -> dict[str, Any]:
    if not _settings_bool(settings, "nimbus_sen2like_direct_zarr", default=False):
        return output
    normalized_uri = str(output.get("normalized_uri") or "").strip()
    if not normalized_uri:
        return output
    configured_root = _settings_optional_str(settings, "nimbus_sen2like_zarr_dir")
    root = (
        Path(configured_root)
        if configured_root
        else Path(getattr(settings, "nimbus_data_dir", "/data/downloads")) / "zarr"
    )
    scene_id = _scene_id_from_uri(normalized_uri)
    zarr_uri = str(root / f"{_safe_output_stem(scene_id)}.zarr")
    if Path(zarr_uri).is_dir():
        return {
            **output,
            "zarr_uri": zarr_uri,
            "zarr_exists": True,
            "zarr_data_family": "optical",
        }
    return output


def _safe_output_stem(scene_id: str) -> str:
    safe_scene = "".join(
        ch if ch.isalnum() or ch in "._-" else "_"
        for ch in str(scene_id or "scene")
    ).strip("._-")
    return safe_scene or "scene"


def _sen2like_product_output_name(product: str) -> str:
    name = Path(str(product)).name
    lowered = name.lower()
    for suffix in (".tar.gz", ".tgz", ".tar"):
        if lowered.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem or name


def _existing_normalized_output_uri(
    output_dir: Path,
    *,
    expected_acquisition_timestamp: str | None = None,
) -> str | None:
    if not output_dir.exists():
        return None
    candidates = [
        *sorted((output_dir / "SAFE").glob("*.SAFE")),
        *sorted(output_dir.glob("*.SAFE")),
        *sorted(output_dir.glob("*_L2F")),
    ]
    for candidate in candidates:
        if _valid_existing_sen2like_output(
            candidate,
            expected_acquisition_timestamp=expected_acquisition_timestamp,
        ):
            return str(candidate)
    return None


def _valid_existing_sen2like_output(
    path: Path,
    *,
    expected_acquisition_timestamp: str | None = None,
) -> bool:
    if not path.is_dir():
        return False
    if path.suffix.upper() != ".SAFE":
        return any(
            raster.is_file() and raster.suffix.lower() in {".tif", ".tiff", ".jp2"}
            for raster in path.rglob("*")
        )
    if expected_acquisition_timestamp and f"_{expected_acquisition_timestamp}_" not in path.name:
        return False
    if not (path / "manifest.safe").exists():
        return False
    if not any(path.glob("MTD_MSI*.xml")):
        return False
    granule_dirs = [item for item in (path / "GRANULE").glob("*") if item.is_dir()]
    if not granule_dirs:
        return False
    granule_dir = granule_dirs[0]
    if not (granule_dir / "MTD_TL.xml").exists():
        return False
    img_dir = granule_dir / "IMG_DATA" / "RESOLUTION_10M"
    if not img_dir.exists():
        return False
    return _REQUIRED_SAFE_BANDS.issubset(_safe_band_ids(img_dir))


def _safe_band_ids(img_dir: Path) -> set[str]:
    found: set[str] = set()
    for raster in img_dir.glob("*_10m.TIF"):
        for part in raster.stem.split("_"):
            candidate = part.upper()
            if candidate in _REQUIRED_SAFE_BANDS:
                found.add(candidate)
    return found


def _expected_acquisition_timestamp(*, product: str, working_dir: Path) -> str | None:
    output_name = _sen2like_product_output_name(product)
    candidates = [
        working_dir / "_inputs" / output_name,
        Path(str(product)),
    ]
    for candidate in candidates:
        values = _read_landsat_mtl_values(candidate)
        date_value = _normalize_landsat_mtl_date(values.get("DATE_ACQUIRED"))
        if not date_value:
            continue
        return _build_landsat_acquisition_timestamp(
            date_value,
            values.get("SCENE_CENTER_TIME"),
        )
    return None


def _read_landsat_mtl_values(path: Path) -> dict[str, str]:
    if path.is_dir():
        for mtl_path in sorted(path.glob("*_MTL.txt")) + sorted(path.glob("**/*_MTL.txt")):
            values = _read_mtl_text(mtl_path)
            if values:
                return values
        return {}
    if _looks_like_tar_product(path) and path.exists():
        try:
            with tarfile.open(path) as archive:
                for member in archive.getmembers():
                    if not member.isfile() or not member.name.endswith("_MTL.txt"):
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    return _parse_mtl_lines(
                        extracted.read().decode("utf-8", errors="ignore").splitlines()
                    )
        except (OSError, tarfile.TarError):
            return {}
    if path.is_file() and path.name.endswith("_MTL.txt"):
        return _read_mtl_text(path)
    return {}


def _looks_like_tar_product(path: Path) -> bool:
    return str(path.name).lower().endswith((".tar", ".tar.gz", ".tgz"))


def _read_mtl_text(path: Path) -> dict[str, str]:
    try:
        return _parse_mtl_lines(path.read_text(encoding="utf-8", errors="ignore").splitlines())
    except OSError:
        return {}


def _parse_mtl_lines(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        match = _MTL_KV_RE.match(line)
        if not match:
            continue
        key, value = match.groups()
        values[key] = value.strip().strip('"')
    return values


def _normalize_landsat_mtl_date(value: str | None) -> str:
    text = str(value or "").strip().strip('"')
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return "".join(match.groups())
    if re.fullmatch(r"\d{8}", text):
        return text
    return ""


def _build_landsat_acquisition_timestamp(
    acq_date: str,
    scene_center_time: str | None,
) -> str:
    match = re.search(r"(\d{2}):(\d{2}):(\d{2})", str(scene_center_time or ""))
    if not match:
        return f"{acq_date}T000000"
    return f"{acq_date}T{match.group(1)}{match.group(2)}{match.group(3)}"


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
