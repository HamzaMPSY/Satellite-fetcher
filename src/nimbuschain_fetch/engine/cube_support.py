from __future__ import annotations

import os
import time
import uuid
from typing import Any

from nimbuschain_fetch.domain.metadata import PipelineMetadataRecord
from nimbuschain_fetch.models import ArtifactType, PipelineState
from nimbuschain_shared.dto import GroupedCubeBuildRequest
from nimbuschain_shared.runtime import normalize_device_name, resolve_inference_device


class FetcherCubeSupport:
    """Cube-output orchestration and scene-parallelism helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def cube_outputs_for_job(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        row: dict[str, Any],
    ) -> list[str]:
        outputs = self._rt._merge_paths([], list(result.get("cube_outputs") or []))
        if outputs:
            return outputs
        pipeline_metadata = PipelineMetadataRecord.from_mapping(
            result.get("pipeline_metadata") or row.get("pipeline_metadata")
        )
        outputs = self._rt._merge_paths([], list(pipeline_metadata.payload.get("cube_outputs") or []))
        if outputs:
            return outputs
        artifacts = self._rt.list_artifacts(
            artifact_type=ArtifactType.zarr_cube.value,
            provider=None,
            collection=None,
            scene_id=None,
            job_id=job_id,
            uri_query=None,
            date_from=None,
            date_to=None,
            page=1,
            page_size=500,
        )
        return [str(item.artifact_uri).strip() for item in artifacts.items if str(item.artifact_uri).strip()]

    def build_cube_outputs(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        source_zarr_outputs: list[str],
        cube_mode: str,
        cube_start_date: Any,
        cube_end_date: Any,
        pipeline_metadata: dict[str, Any],
        stage_start_progress: float,
        stage_end_progress: float,
    ) -> dict[str, Any]:
        if not source_zarr_outputs:
            return {
                "status": "skipped",
                "reason": "no_source_zarrs",
                "cube_outputs": [],
                "items": [],
                "pipeline_metadata": {
                    **pipeline_metadata,
                    "cube_mode": cube_mode,
                    "cube_stage": cube_mode,
                    "cube_status": "skipped",
                    "cube_reason": "no_source_zarrs",
                    "cube_output_count": 0,
                    "cube_outputs": [],
                },
            }

        output_dir = self._rt._default_cube_output_dir(job_id)
        requested_date_range = {
            "start_date": cube_start_date.isoformat() if hasattr(cube_start_date, "isoformat") else cube_start_date,
            "end_date": cube_end_date.isoformat() if hasattr(cube_end_date, "isoformat") else cube_end_date,
        }
        queued_metadata = {
            **pipeline_metadata,
            "cube_mode": cube_mode,
            "cube_stage": cube_mode,
            "cube_status": "queued",
            "cube_output_dir": output_dir,
            "cube_source_count": len(source_zarr_outputs),
            "cube_date_range": requested_date_range,
        }
        self._rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.cube_queued,
            pipeline_step="cube_queued",
            pipeline_progress=stage_start_progress,
            pipeline_metadata=queued_metadata,
            event_type="job.cube_queued",
            event_payload={
                "cube_mode": cube_mode,
                "cube_source_count": len(source_zarr_outputs),
                "cube_output_dir": output_dir,
                "cube_date_range": requested_date_range,
            },
        )

        self._rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.cube_building,
            pipeline_step="cube_building",
            pipeline_progress=min(
                99.0,
                stage_start_progress + max(0.5, (stage_end_progress - stage_start_progress) * 0.5),
            ),
            pipeline_metadata={**queued_metadata, "cube_status": "running"},
            event_type="job.cube_building",
            event_payload={
                "cube_mode": cube_mode,
                "cube_source_count": len(source_zarr_outputs),
            },
        )

        last_cube_emit = {"mono": 0.0, "progress": float(stage_start_progress)}

        def _emit_cube_progress(payload: dict[str, Any]) -> None:
            group_total = max(1, int(payload.get("group_total") or 1))
            group_index = min(group_total, max(1, int(payload.get("group_index") or 1)))
            cube_fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
            aggregate_fraction = min(
                1.0,
                max(0.0, ((group_index - 1) + cube_fraction) / group_total),
            )
            stage_span = max(0.0, float(stage_end_progress) - float(stage_start_progress))
            stage_cap = (
                max(float(stage_start_progress), float(stage_end_progress) - 0.1)
                if stage_span > 0.1
                else float(stage_end_progress)
            )
            candidate_progress = (
                float(stage_start_progress) + stage_span * aggregate_fraction
                if stage_span > 0.0
                else float(stage_end_progress)
            )
            pipeline_progress = max(
                float(last_cube_emit["progress"]),
                min(stage_cap, candidate_progress),
            )
            now_mono = time.monotonic()
            if not self._rt._should_emit_zarr_progress(
                now_mono=now_mono,
                last_emit=float(last_cube_emit["mono"]),
                progress_pct=pipeline_progress,
                last_progress=float(last_cube_emit["progress"]),
            ):
                return
            last_cube_emit["mono"] = now_mono
            last_cube_emit["progress"] = pipeline_progress
            progress_metadata = {
                **queued_metadata,
                "cube_status": "running",
                "cube_active_group": str(payload.get("group_key") or "").strip(),
                "cube_group_index": group_index,
                "cube_group_total": group_total,
                "cube_current_scene_id": str(payload.get("scene_id") or "").strip(),
                "cube_current_layer": str(payload.get("layer_name") or "").strip(),
                "cube_current_band": str(payload.get("band_name") or "").strip(),
                "cube_blocks_written": int(payload.get("blocks_written") or 0),
                "cube_total_blocks": int(payload.get("total_blocks") or 0),
                "cube_fraction": round(cube_fraction, 6),
                "cube_aggregate_fraction": round(aggregate_fraction, 6),
                "cube_output_uri": str(payload.get("group_output_uri") or "").strip(),
            }
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_building,
                pipeline_step="cube_building",
                pipeline_progress=pipeline_progress,
                pipeline_metadata=progress_metadata,
            )

        try:
            cube_request = GroupedCubeBuildRequest(
                job_id=job_id,
                pipeline_id=job_id,
                trace_id=uuid.uuid4().hex,
                source_zarr_uris=list(source_zarr_outputs),
                output_dir=output_dir,
                start_date=cube_start_date,
                end_date=cube_end_date,
                stage_label=cube_mode,
                cube_layout=str(pipeline_metadata.get("cube_layout") or "grouped_time"),
                target_crs=pipeline_metadata.get("cube_target_crs"),
                target_resolution_m=int(pipeline_metadata.get("cube_target_resolution_m") or 10),
                overlap_policy=str(pipeline_metadata.get("cube_overlap_policy") or "least_cloud"),
                progress_callback=_emit_cube_progress,
            )
            converter = self._rt._converter()
            if hasattr(converter, "build_grouped_cubes_request"):
                cube_summary = converter.build_grouped_cubes_request(cube_request)
            else:
                cube_summary = converter.build_grouped_cubes(
                    job_id=cube_request.job_id,
                    pipeline_id=cube_request.pipeline_id,
                    trace_id=cube_request.trace_id,
                    source_zarr_uris=cube_request.source_zarr_uris,
                    output_dir=cube_request.output_dir,
                    start_date=cube_request.start_date,
                    end_date=cube_request.end_date,
                    stage_label=cube_request.stage_label,
                    cube_layout=cube_request.cube_layout,
                    target_crs=cube_request.target_crs,
                    target_resolution_m=cube_request.target_resolution_m,
                    overlap_policy=cube_request.overlap_policy,
                    progress_callback=cube_request.progress_callback,
                )
        except Exception as exc:
            if self._is_optional_cube_rejection(exc):
                reason = str(exc)
                skipped_metadata = {
                    **pipeline_metadata,
                    "cube_mode": cube_mode,
                    "cube_stage": cube_mode,
                    "cube_status": "skipped",
                    "cube_reason": reason,
                    "cube_output_dir": output_dir,
                    "cube_output_count": 0,
                    "cube_outputs": [],
                    "cube_items": [],
                    "cube_tiles_built": [],
                    "cube_tiles_skipped": [
                        {
                            "reason": "unsupported_cube_layout_for_inputs",
                            "message": reason,
                        }
                    ],
                    "cube_date_range": requested_date_range,
                }
                self._rt._update_pipeline(
                    job_id,
                    pipeline_state=PipelineState.cube_building,
                    pipeline_step="cube_skipped",
                    pipeline_progress=stage_end_progress,
                    pipeline_metadata=skipped_metadata,
                    event_type="job.cube_skipped",
                    event_payload={
                        "cube_mode": cube_mode,
                        "reason": reason,
                    },
                )
                return {
                    "status": "skipped",
                    "reason": reason,
                    "cube_outputs": [],
                    "items": [],
                    "tiles_built": [],
                    "tiles_skipped": list(skipped_metadata["cube_tiles_skipped"]),
                    "stage_label": cube_mode,
                    "date_range": requested_date_range,
                    "pipeline_metadata": skipped_metadata,
                }
            failed_metadata = {
                **queued_metadata,
                "cube_status": "failed",
                "cube_reason": str(exc),
                "cube_output_count": 0,
                "cube_outputs": [],
            }
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_failed,
                pipeline_step="cube_failed",
                pipeline_progress=min(99.0, stage_end_progress),
                pipeline_metadata=failed_metadata,
                event_type="job.cube_failed",
                event_payload={
                    "cube_mode": cube_mode,
                    "error": str(exc),
                },
            )
            raise

        cube_outputs = self._rt._merge_paths([], list(cube_summary.get("cube_outputs") or []))
        cube_items = [dict(item) for item in list(cube_summary.get("items") or [])]
        for item in cube_items:
            self._rt._register_cube_artifact(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                cube_stage=cube_mode,
                cube_summary=item,
            )

        final_metadata = {
            **pipeline_metadata,
            "cube_mode": cube_mode,
            "cube_stage": cube_mode,
            "cube_status": str(cube_summary.get("status") or "").strip() or "skipped",
            "cube_reason": str(cube_summary.get("reason") or "").strip() or "",
            "cube_output_dir": output_dir,
            "cube_output_count": len(cube_outputs),
            "cube_outputs": cube_outputs,
            "cube_items": cube_items,
            "cube_tiles_built": list(cube_summary.get("tiles_built") or []),
            "cube_tiles_skipped": list(cube_summary.get("tiles_skipped") or []),
            "cube_date_range": dict(cube_summary.get("date_range") or requested_date_range),
        }
        if cube_outputs:
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_written,
                pipeline_step="cube_written",
                pipeline_progress=stage_end_progress,
                pipeline_metadata=final_metadata,
                event_type="job.cube_written",
                event_payload={
                    "cube_mode": cube_mode,
                    "cube_outputs": cube_outputs,
                    "cube_output_count": len(cube_outputs),
                },
            )
        else:
            self._rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.cube_building,
                pipeline_step="cube_skipped",
                pipeline_progress=stage_end_progress,
                pipeline_metadata=final_metadata,
                event_type="job.cube_skipped",
                event_payload={
                    "cube_mode": cube_mode,
                    "reason": str(cube_summary.get("reason") or "").strip() or "no_groups_with_multiple_times",
                },
            )
        return {
            **cube_summary,
            "cube_outputs": cube_outputs,
            "items": cube_items,
            "pipeline_metadata": final_metadata,
        }

    @staticmethod
    def _is_optional_cube_rejection(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "daily mosaic cube is currently supported only for sentinel-2 scene zarr inputs"
            in message
        )

    def resume_base_result_paths(
        self,
        *,
        result: dict[str, Any],
        raw_outputs: list[str],
    ) -> list[str]:
        existing_paths = self._rt._merge_paths([], list(result.get("paths") or []))
        obsolete_paths = set(
            self._rt._merge_paths(
                list(result.get("zarr_outputs") or []),
                list(result.get("cube_outputs") or []),
            )
        )
        preserved = [path for path in existing_paths if path not in obsolete_paths]
        return self._rt._merge_paths(preserved, raw_outputs)

    def scene_parallelism_target_from_download(
        self,
        *,
        pipeline_metadata: dict[str, Any] | None,
        total: int,
    ) -> int:
        if total <= 1:
            return 1
        metadata = dict(pipeline_metadata or {})
        selected_accounts = int(metadata.get("account_pool_selected_accounts", 0) or 0)
        account_labels: set[str] = set()
        for item in list(metadata.get("account_pool_assignments") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        for item in list(dict(metadata.get("download_telemetry") or {}).get("accounts") or []):
            label = str((item or {}).get("account_label") or "").strip()
            if label:
                account_labels.add(label)
        if account_labels:
            selected_accounts = max(selected_accounts, len(account_labels))
        if selected_accounts <= 1:
            selected_accounts = min(total, 4)
        return max(
            1,
            min(
                int(selected_accounts or 1),
                max(1, total),
                max(1, int(self._rt.settings.nimbus_max_jobs or 1)),
                4,
            ),
        )

    @staticmethod
    def zarr_convert_max_workers(
        *,
        total: int,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
        os_module: Any = os,
    ) -> int:
        raw = str(os_module.getenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        cpu_budget = max(1, min(4, max(1, int((os_module.cpu_count() or 2) / 2))))
        default_value = min(max(1, int(preferred_parallelism or 1)), cpu_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))

    @staticmethod
    def integrated_mask_max_workers(
        *,
        total: int,
        inference_device: str | None,
        water_inference_device: str | None,
        remote_runtime: dict[str, Any] | None = None,
        preferred_parallelism: int | None = None,
        max_limit: int = 4,
        os_module: Any = os,
        resolve_inference_device_fn: Any = resolve_inference_device,
        normalize_device_name_fn: Any = normalize_device_name,
    ) -> int:
        raw = str(os_module.getenv("NIMBUS_MASK_SCENE_MAX_WORKERS") or "").strip()
        try:
            configured = int(raw) if raw else None
        except ValueError:
            configured = None
        resolved_cloud = resolve_inference_device_fn(
            explicit=inference_device,
            env_var="NIMBUS_CLOUDMASK_DEVICE",
        )
        resolved_water = resolve_inference_device_fn(
            explicit=water_inference_device,
            env_var="NIMBUS_WATERMASK_DEVICE",
        )
        runtime_payload = dict(remote_runtime or {})
        remote_cloud = normalize_device_name_fn(
            dict(runtime_payload.get("cloud") or {}).get("resolved")
        )
        remote_water = normalize_device_name_fn(
            dict(runtime_payload.get("water") or {}).get("resolved")
        )
        if remote_cloud not in {"", "auto"}:
            resolved_cloud = remote_cloud
        if remote_water not in {"", "auto"}:
            resolved_water = remote_water
        remote_service = remote_runtime is not None
        has_accelerator = any(device in {"cuda", "mps"} for device in {resolved_cloud, resolved_water})
        cpu_budget = max(1, min(4, max(1, int((os_module.cpu_count() or 2) / 2))))
        if remote_service:
            heuristic_budget = min(cpu_budget, 2 if has_accelerator else 1)
        else:
            heuristic_budget = min(cpu_budget, 3 if has_accelerator else 2)
        default_target = (
            max(1, int(preferred_parallelism or 1))
            if preferred_parallelism is not None
            else (2 if total > 1 and has_accelerator else 1)
        )
        default_value = min(default_target, heuristic_budget)
        value = configured if configured is not None else default_value
        return max(1, min(int(value), max(1, total), max(1, int(max_limit or 1))))
