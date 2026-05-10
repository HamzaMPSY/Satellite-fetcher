from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from nimbuschain_fetch.models import PipelineState


def convert_raw_outputs(
    rt: Any,
    *,
    job_id: str,
    provider_name: str,
    collection: str,
    product_type: str | None,
    raw_outputs: list[str],
    is_cancelled: Callable[[], bool],
    scene_id_override: str | None = None,
    output_uri_override: str | None = None,
    pipeline_metadata: dict[str, Any] | None = None,
    conversion_provider_name: str | None = None,
    conversion_collection: str | None = None,
    conversion_product_type: str | None = None,
) -> tuple[list[str], dict[str, Any]]:
    if not raw_outputs:
        return [], {"status": "skipped", "reason": "no_raw_outputs"}

    total = max(1, len(raw_outputs))
    effective_conversion_provider = str(conversion_provider_name or provider_name).strip()
    effective_conversion_collection = str(conversion_collection or collection).strip()
    effective_conversion_product_type = (
        str(conversion_product_type).strip()
        if conversion_product_type is not None
        else product_type
    )
    zarr_outputs: list[str] = []
    conversions: list[dict[str, Any]] = []
    prepared_items: list[dict[str, Any]] = []
    rt._update_pipeline(
        job_id,
        pipeline_state=PipelineState.zarr_queued,
        pipeline_step="zarr_queued",
        pipeline_progress=0.0,
        raw_outputs=raw_outputs,
        conversion_metadata={
            "status": "queued",
            "stage": "zarr_queued",
            "current_index": 0,
            "total": total,
        },
        event_type="job.zarr_queued",
        event_payload={"raw_output_count": len(raw_outputs)},
    )
    for index, raw_uri in enumerate(raw_outputs, start=1):
        if is_cancelled():
            raise rt.job_cancelled_error_cls("Job cancellation requested.")
        scene_id = (
            str(scene_id_override or "").strip()
            if index == 1 and scene_id_override
            else rt._scene_id_from_raw_uri(raw_uri)
        )
        output_uri = (
            str(output_uri_override or "").strip()
            if index == 1 and output_uri_override
            else rt._default_zarr_output_uri(scene_id)
        )
        prepared_items.append(
            {
                "index": index,
                "raw_uri": raw_uri,
                "scene_id": scene_id,
                "output_uri": output_uri,
            }
        )

    max_workers = rt._zarr_convert_max_workers(
        total=total,
        preferred_parallelism=rt._scene_parallelism_target_from_download(
            pipeline_metadata=pipeline_metadata,
            total=total,
        ),
        max_limit=min(4, max(1, int(rt.settings.nimbus_max_jobs or 1))),
    )
    if max_workers <= 1:
        for item in prepared_items:
            if is_cancelled():
                raise rt.job_cancelled_error_cls("Job cancellation requested.")
            index = int(item["index"])
            raw_uri = str(item["raw_uri"])
            scene_id = str(item["scene_id"])
            output_uri = str(item["output_uri"])
            per_item_progress = ((index - 1) / total) * 100.0
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="writing_chunks",
                pipeline_progress=per_item_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata={
                    "status": "running",
                    "stage": "writing_chunks",
                    "current_raw_uri": raw_uri,
                    "current_scene_id": scene_id,
                    "current_output_uri": output_uri,
                    "current_index": index,
                    "total": total,
                    "parallel_workers": 1,
                },
                event_type="job.zarr_converting",
                event_payload={
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": output_uri,
                    "index": index,
                    "total": total,
                    "stage": "writing_chunks",
                    "parallel_workers": 1,
                },
            )
            progress_callback = rt._build_zarr_progress_callback(
                job_id=job_id,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                raw_uri=raw_uri,
                scene_id=scene_id,
                output_uri=output_uri,
                index=index,
                total=total,
                parallel_workers=max_workers,
            )
            converted = rt._convert_single_raw_output(
                job_id=job_id,
                provider_name=effective_conversion_provider,
                collection=effective_conversion_collection,
                product_type=effective_conversion_product_type,
                raw_uri=raw_uri,
                scene_id=scene_id,
                output_uri=output_uri,
                progress_callback=progress_callback,
            )
            zarr_outputs.append(str(converted["zarr_uri"]))
            conversions.append(converted)
            register_progress = min(
                99.0,
                ((index - 1) / total) * 100.0 + (100.0 / total) * 0.85,
            )
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="registering_artifact",
                pipeline_progress=register_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata={
                    "status": "running",
                    "stage": "registering_artifact",
                    "current_raw_uri": raw_uri,
                    "current_scene_id": scene_id,
                    "current_output_uri": converted["zarr_uri"],
                    "current_index": index,
                    "total": total,
                    "parallel_workers": 1,
                },
                event_type="job.zarr_converting",
                event_payload={
                    "raw_uri": raw_uri,
                    "scene_id": scene_id,
                    "output_uri": converted["zarr_uri"],
                    "index": index,
                    "total": total,
                    "stage": "registering_artifact",
                    "parallel_workers": 1,
                },
            )
            rt._register_zarr_artifact(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                scene_id=scene_id,
                raw_uri=raw_uri,
                zarr_uri=str(converted["zarr_uri"]),
                data_family=str(converted["data_family"]),
                conversion_summary=dict(converted["summary"]),
                dataset_summary=dict(converted["dataset_summary"]),
            )
        return zarr_outputs, {
            "status": "written",
            "count": len(zarr_outputs),
            "items": conversions,
            "parallel_workers": 1,
            "conversion_provider": effective_conversion_provider,
            "conversion_collection": effective_conversion_collection,
            "conversion_product_type": effective_conversion_product_type,
        }

    rt._update_pipeline(
        job_id,
        pipeline_state=PipelineState.zarr_converting,
        pipeline_step="writing_chunks",
        pipeline_progress=0.0,
        raw_outputs=raw_outputs,
        zarr_outputs=zarr_outputs,
        conversion_metadata={
            "status": "running",
            "stage": "writing_chunks",
            "current_index": 0,
            "total": total,
            "parallel_workers": max_workers,
        },
        event_type="job.zarr_converting",
        event_payload={
            "raw_output_count": len(raw_outputs),
            "stage": "writing_chunks",
            "parallel_workers": max_workers,
        },
    )
    completed_by_index: dict[int, dict[str, Any]] = {}
    progress_lock = threading.Lock()
    progress_by_index: dict[int, float] = {
        int(item["index"]): 0.0 for item in prepared_items
    }
    last_emit = {"mono": 0.0, "progress": -1.0}

    def make_parallel_progress_callback(item: dict[str, Any]) -> Callable[[dict[str, Any]], None]:
        index = int(item["index"])
        raw_uri = str(item["raw_uri"])
        scene_id = str(item["scene_id"])
        output_uri = str(item["output_uri"])

        def callback(payload: dict[str, Any]) -> None:
            fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
            blocks_written = int(payload.get("blocks_written") or 0)
            total_blocks = int(payload.get("total_blocks") or 0)
            source_array_name = str(
                payload.get("source_array_name") or payload.get("array_name") or ""
            ).strip()
            band_name = str(payload.get("band_name") or "").strip()
            now_mono = time.monotonic()
            with progress_lock:
                progress_by_index[index] = max(progress_by_index.get(index, 0.0), fraction)
                aggregate_fraction = sum(progress_by_index.values()) / total
                pipeline_progress = min(99.0, aggregate_fraction * 85.0)
                if not rt._should_emit_zarr_progress(
                    now_mono=now_mono,
                    last_emit=float(last_emit["mono"]),
                    progress_pct=pipeline_progress,
                    last_progress=float(last_emit["progress"]),
                ):
                    return
                last_emit["mono"] = now_mono
                last_emit["progress"] = pipeline_progress
                items_completed = sum(
                    1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                )
                items_active = sum(
                    1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                )
            conversion_payload = {
                "status": "running",
                "stage": "writing_chunks",
                "current_raw_uri": raw_uri,
                "current_scene_id": scene_id,
                "current_output_uri": output_uri,
                "current_index": index,
                "total": total,
                "parallel_workers": max_workers,
                "chunk_fraction": round(fraction, 6),
                "aggregate_fraction": round(aggregate_fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": items_completed,
                "items_active": items_active,
            }
            if source_array_name:
                conversion_payload["current_array"] = source_array_name
            if band_name:
                conversion_payload["current_band"] = band_name
            event_payload = {
                "raw_uri": raw_uri,
                "scene_id": scene_id,
                "output_uri": output_uri,
                "index": index,
                "total": total,
                "stage": "writing_chunks",
                "parallel_workers": max_workers,
                "chunk_fraction": round(fraction, 6),
                "aggregate_fraction": round(aggregate_fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": items_completed,
                "items_active": items_active,
            }
            if source_array_name:
                event_payload["array_name"] = source_array_name
            if band_name:
                event_payload["band_name"] = band_name
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="writing_chunks",
                pipeline_progress=pipeline_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata=conversion_payload,
                event_type="job.zarr_converting",
                event_payload=event_payload,
            )

        return callback

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="zarr-convert") as executor:
        future_to_item = {
            executor.submit(
                rt._convert_single_raw_output,
                job_id=job_id,
                provider_name=effective_conversion_provider,
                collection=effective_conversion_collection,
                product_type=effective_conversion_product_type,
                raw_uri=str(item["raw_uri"]),
                scene_id=str(item["scene_id"]),
                output_uri=str(item["output_uri"]),
                progress_callback=make_parallel_progress_callback(item),
            ): item
            for item in prepared_items
        }
        for future in as_completed(future_to_item):
            if is_cancelled():
                raise rt.job_cancelled_error_cls("Job cancellation requested.")
            item = future_to_item[future]
            converted = future.result()
            index = int(item["index"])
            with progress_lock:
                progress_by_index[index] = 1.0
                items_completed = sum(
                    1 for current_fraction in progress_by_index.values() if current_fraction >= 1.0
                )
                items_active = sum(
                    1 for current_fraction in progress_by_index.values() if 0.0 < current_fraction < 1.0
                )
            completed_by_index[index] = converted
            ordered_indices = sorted(completed_by_index)
            zarr_outputs = [str(completed_by_index[current]["zarr_uri"]) for current in ordered_indices]
            conversions = [completed_by_index[current] for current in ordered_indices]
            register_progress = min(
                99.0,
                (len(completed_by_index) / total) * 85.0,
            )
            rt._update_pipeline(
                job_id,
                pipeline_state=PipelineState.zarr_converting,
                pipeline_step="registering_artifact",
                pipeline_progress=register_progress,
                raw_outputs=raw_outputs,
                zarr_outputs=zarr_outputs,
                conversion_metadata={
                    "status": "running",
                    "stage": "registering_artifact",
                    "current_raw_uri": item["raw_uri"],
                    "current_scene_id": item["scene_id"],
                    "current_output_uri": converted["zarr_uri"],
                    "current_index": len(completed_by_index),
                    "total": total,
                    "parallel_workers": max_workers,
                    "items_total": total,
                    "items_completed": items_completed,
                    "items_active": items_active,
                },
                event_type="job.zarr_converting",
                event_payload={
                    "raw_uri": item["raw_uri"],
                    "scene_id": item["scene_id"],
                    "output_uri": converted["zarr_uri"],
                    "index": len(completed_by_index),
                    "total": total,
                    "stage": "registering_artifact",
                    "parallel_workers": max_workers,
                    "items_total": total,
                    "items_completed": items_completed,
                    "items_active": items_active,
                },
            )
            rt._register_zarr_artifact(
                job_id=job_id,
                provider_name=provider_name,
                collection=collection,
                scene_id=str(item["scene_id"]),
                raw_uri=str(item["raw_uri"]),
                zarr_uri=str(converted["zarr_uri"]),
                data_family=str(converted["data_family"]),
                conversion_summary=dict(converted["summary"]),
                dataset_summary=dict(converted["dataset_summary"]),
            )
    return zarr_outputs, {
        "status": "written",
        "count": len(zarr_outputs),
        "items": conversions,
        "parallel_workers": max_workers,
        "conversion_provider": effective_conversion_provider,
        "conversion_collection": effective_conversion_collection,
        "conversion_product_type": effective_conversion_product_type,
    }
