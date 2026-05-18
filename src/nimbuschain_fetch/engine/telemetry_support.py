from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from nimbuschain_fetch.models import PipelineState


class FetcherTelemetrySupport:
    """Download/Zarr telemetry and progress-emission helpers for the fetcher facade."""

    @staticmethod
    def download_account_label(context: dict[str, Any] | None) -> str:
        label = str((context or {}).get("account_label") or "primary").strip()
        return label or "primary"

    @classmethod
    def build_download_telemetry(
        cls,
        *,
        pipeline_metadata: dict[str, Any],
        file_progress: dict[str, dict[str, Any]],
        bytes_downloaded: int,
        bytes_total: int,
        progress_pct: float,
        speed_bps: float,
        retry_state: dict[str, dict[str, Any]],
        phase: str,
        last_file: str | None = None,
    ) -> dict[str, Any]:
        metadata = dict(pipeline_metadata or {})
        assignments = list(metadata.get("account_pool_assignments") or [])
        planned_counts: dict[str, int] = {}
        ordered_labels: list[str] = []
        for entry in assignments:
            label = str((entry or {}).get("account_label") or "").strip()
            if not label:
                continue
            if label not in ordered_labels:
                ordered_labels.append(label)
            planned_counts[label] = max(0, int((entry or {}).get("product_count") or 0))

        for file_state in file_progress.values():
            label = cls.download_account_label(file_state)
            if label not in ordered_labels:
                ordered_labels.append(label)
            planned_counts.setdefault(label, 0)

        for label in retry_state:
            normalized = str(label or "").strip()
            if not normalized:
                continue
            if normalized not in ordered_labels:
                ordered_labels.append(normalized)
            planned_counts.setdefault(normalized, 0)

        products_found = int(metadata.get("products_found") or metadata.get("products_requested") or 0)
        products_downloaded = int(metadata.get("products_downloaded") or 0)
        total_known_files = len(file_progress)
        total_completed_files = sum(1 for entry in file_progress.values() if bool(entry.get("completed")))
        if str(phase).strip().lower() == "completed" and products_downloaded > 0:
            total_completed_files = max(total_completed_files, products_downloaded)

        account_rows: list[dict[str, Any]] = []
        for label in ordered_labels:
            observed = [
                dict(item)
                for item in file_progress.values()
                if cls.download_account_label(item) == label
            ]
            assigned = max(int(planned_counts.get(label, 0) or 0), len(observed))
            bytes_done = sum(max(0, int(item.get("downloaded") or 0)) for item in observed)
            bytes_known_total = sum(
                int(item.get("total") or 0)
                for item in observed
                if item.get("total") is not None
            )
            files_completed = sum(1 for item in observed if bool(item.get("completed")))
            if str(phase).strip().lower() == "completed" and assigned > 0:
                files_completed = max(files_completed, assigned)
            active_items = [item for item in observed if not bool(item.get("completed"))]
            latest_items = sorted(
                observed,
                key=lambda item: float(item.get("last_update_mono") or 0.0),
                reverse=True,
            )
            current_item = (active_items or latest_items or [None])[0]
            retry_info = dict(retry_state.get(label) or {})
            retry_status = str(retry_info.get("status") or "").strip().lower()
            rate_limited = str(retry_info.get("last_reason") or "").strip().lower() == "http_429"
            if str(phase).strip().lower() == "completed" and assigned > 0:
                account_status = "completed"
            elif retry_status == "rate_limited" and not active_items:
                account_status = "rate_limited"
            elif retry_status == "retrying" and not active_items:
                account_status = "retrying"
            elif files_completed >= assigned > 0:
                account_status = "completed"
            elif bytes_done > 0 or observed:
                account_status = "running"
            else:
                account_status = "queued"

            account_progress = 0.0
            if bytes_known_total > 0:
                account_progress = min(100.0, 100.0 * bytes_done / bytes_known_total)
            elif account_status == "completed" and assigned > 0:
                account_progress = 100.0

            account_rows.append(
                {
                    "account_label": label,
                    "status": account_status,
                    "product_count_assigned": assigned,
                    "files_observed": len(observed),
                    "files_completed": files_completed,
                    "active_file_count": len(active_items),
                    "bytes_downloaded": bytes_done,
                    "bytes_total": bytes_known_total,
                    "progress_pct": round(account_progress, 2),
                    "retry_count": int(retry_info.get("retry_count", 0) or 0),
                    "rate_limited": rate_limited,
                    "last_retry_at": retry_info.get("last_retry_at"),
                    "last_retry_reason": retry_info.get("last_reason"),
                    "current_file": str((current_item or {}).get("file_name") or "").strip() or None,
                }
            )

        total_bytes = max(int(bytes_total or 0), int(bytes_downloaded or 0))
        eta_seconds: float | None = None
        if speed_bps > 0 and total_bytes > int(bytes_downloaded or 0):
            eta_seconds = max(0.0, (total_bytes - int(bytes_downloaded or 0)) / max(speed_bps, 1.0))
        selected_accounts = int(metadata.get("account_pool_selected_accounts", 0) or 0)
        if selected_accounts <= 0 and account_rows:
            selected_accounts = len(
                [
                    row
                    for row in account_rows
                    if int(row.get("product_count_assigned", 0) or 0) > 0
                    or int(row.get("files_observed", 0) or 0) > 0
                    or int(row.get("bytes_downloaded", 0) or 0) > 0
                ]
            )
        try:
            download_window_seconds = (
                float(metadata.get("download_window_seconds"))
                if metadata.get("download_window_seconds") is not None
                else None
            )
        except (TypeError, ValueError):
            download_window_seconds = None

        return {
            "status": str(phase or "running").strip().lower() or "running",
            "strategy": str(metadata.get("download_strategy") or "default").strip().lower() or "default",
            "selected_accounts": selected_accounts,
            "pool_size": int(metadata.get("account_pool_size", 0) or 0),
            "per_account_concurrency": int(metadata.get("account_pool_per_account_concurrency", 0) or 0),
            "products_found": products_found,
            "products_downloaded": products_downloaded,
            "files_known": total_known_files,
            "files_completed": total_completed_files,
            "bytes_downloaded": int(bytes_downloaded or 0),
            "bytes_total": total_bytes,
            "progress_pct": round(float(progress_pct or 0.0), 2),
            "speed_bps": float(speed_bps or 0.0),
            "eta_seconds": eta_seconds,
            "started_at": str(metadata.get("download_started_at") or "").strip() or None,
            "finished_at": str(metadata.get("download_finished_at") or "").strip() or None,
            "duration_seconds": download_window_seconds,
            "last_file": str(last_file or "").strip() or None,
            "retry_count_total": sum(int((entry or {}).get("retry_count", 0) or 0) for entry in retry_state.values()),
            "rate_limited_accounts": sum(1 for row in account_rows if bool(row.get("rate_limited"))),
            "accounts": account_rows,
        }

    @staticmethod
    def should_emit_download_progress(
        *,
        delta: int,
        now_mono: float,
        last_emit: float,
        bytes_downloaded: int,
        last_bytes: int,
        progress_pct: float,
        last_progress: float,
        bytes_total: int,
        min_interval_seconds: float,
        max_interval_seconds: float,
        min_bytes: int,
        min_percent: float,
    ) -> bool:
        if delta == 0 or last_emit <= 0:
            return True

        elapsed = max(0.0, now_mono - last_emit)
        if bytes_total > 0 and bytes_downloaded >= bytes_total:
            return True
        if elapsed < min_interval_seconds:
            return False
        if elapsed >= max_interval_seconds:
            return True
        if max(0, bytes_downloaded - last_bytes) >= min_bytes:
            return True
        if max(0.0, progress_pct - last_progress) >= min_percent:
            return True
        return False

    @staticmethod
    def should_emit_zarr_progress(
        *,
        now_mono: float,
        last_emit: float,
        progress_pct: float,
        last_progress: float,
        min_interval_seconds: float,
        min_percent: float,
    ) -> bool:
        if last_emit <= 0 or last_progress < 0.0:
            return True
        if progress_pct >= 100.0:
            return True
        if max(0.0, progress_pct - last_progress) >= min_percent:
            return True
        return max(0.0, now_mono - last_emit) >= min_interval_seconds

    @classmethod
    def build_zarr_progress_callback(
        cls,
        rt: Any,
        *,
        job_id: str,
        raw_outputs: list[str],
        zarr_outputs: list[str],
        raw_uri: str,
        scene_id: str,
        output_uri: str,
        index: int,
        total: int,
        parallel_workers: int = 1,
    ) -> Callable[[dict[str, Any]], None]:
        last_emit = {"mono": 0.0, "progress": -1.0}
        base_progress = ((index - 1) / total) * 100.0
        progress_span = (100.0 / total) * 0.85

        def _callback(payload: dict[str, Any]) -> None:
            fraction = min(1.0, max(0.0, float(payload.get("fraction") or 0.0)))
            pipeline_progress = min(99.0, base_progress + progress_span * fraction)
            now_mono = time.monotonic()
            if not cls.should_emit_zarr_progress(
                now_mono=now_mono,
                last_emit=float(last_emit["mono"]),
                progress_pct=pipeline_progress,
                last_progress=float(last_emit["progress"]),
                min_interval_seconds=rt.ZARR_PROGRESS_MIN_INTERVAL_SECONDS,
                min_percent=rt.ZARR_PROGRESS_MIN_PERCENT,
            ):
                return
            last_emit["mono"] = now_mono
            last_emit["progress"] = pipeline_progress
            blocks_written = int(payload.get("blocks_written") or 0)
            total_blocks = int(payload.get("total_blocks") or 0)
            source_array_name = str(payload.get("source_array_name") or payload.get("array_name") or "").strip()
            band_name = str(payload.get("band_name") or "").strip()
            metadata = {
                "status": "running",
                "stage": "writing_chunks",
                "current_raw_uri": raw_uri,
                "current_scene_id": scene_id,
                "current_output_uri": output_uri,
                "current_index": index,
                "total": total,
                "parallel_workers": parallel_workers,
                "chunk_fraction": round(fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": max(0, index - 1),
                "items_active": 1,
            }
            if source_array_name:
                metadata["current_array"] = source_array_name
            if band_name:
                metadata["current_band"] = band_name
            event_payload = {
                "raw_uri": raw_uri,
                "scene_id": scene_id,
                "output_uri": output_uri,
                "index": index,
                "total": total,
                "stage": "writing_chunks",
                "parallel_workers": parallel_workers,
                "chunk_fraction": round(fraction, 6),
                "blocks_written": blocks_written,
                "total_blocks": total_blocks,
                "items_total": total,
                "items_completed": max(0, index - 1),
                "items_active": 1,
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
                conversion_metadata=metadata,
                event_type="job.zarr_converting",
                event_payload=event_payload,
            )

        return _callback
