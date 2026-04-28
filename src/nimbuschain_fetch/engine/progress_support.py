from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nimbuschain_fetch.engine.telemetry_support import FetcherTelemetrySupport


class FetcherProgressSupport:
    """Progress-throttling and telemetry helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    @staticmethod
    def download_account_label(context: dict[str, Any] | None) -> str:
        return FetcherTelemetrySupport.download_account_label(context)

    def build_download_telemetry(
        self,
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
        return FetcherTelemetrySupport.build_download_telemetry(
            pipeline_metadata=pipeline_metadata,
            file_progress=file_progress,
            bytes_downloaded=bytes_downloaded,
            bytes_total=bytes_total,
            progress_pct=progress_pct,
            speed_bps=speed_bps,
            retry_state=retry_state,
            phase=phase,
            last_file=last_file,
        )

    def should_emit_download_progress(
        self,
        *,
        delta: int,
        now_mono: float,
        last_emit: float,
        bytes_downloaded: int,
        last_bytes: int,
        progress_pct: float,
        last_progress: float,
        bytes_total: int,
    ) -> bool:
        return FetcherTelemetrySupport.should_emit_download_progress(
            delta=delta,
            now_mono=now_mono,
            last_emit=last_emit,
            bytes_downloaded=bytes_downloaded,
            last_bytes=last_bytes,
            progress_pct=progress_pct,
            last_progress=last_progress,
            bytes_total=bytes_total,
            min_interval_seconds=self._rt.DOWNLOAD_PROGRESS_MIN_INTERVAL_SECONDS,
            max_interval_seconds=self._rt.DOWNLOAD_PROGRESS_MAX_INTERVAL_SECONDS,
            min_bytes=self._rt.DOWNLOAD_PROGRESS_MIN_BYTES,
            min_percent=self._rt.DOWNLOAD_PROGRESS_MIN_PERCENT,
        )

    def should_emit_zarr_progress(
        self,
        *,
        now_mono: float,
        last_emit: float,
        progress_pct: float,
        last_progress: float,
    ) -> bool:
        return FetcherTelemetrySupport.should_emit_zarr_progress(
            now_mono=now_mono,
            last_emit=last_emit,
            progress_pct=progress_pct,
            last_progress=last_progress,
            min_interval_seconds=self._rt.ZARR_PROGRESS_MIN_INTERVAL_SECONDS,
            min_percent=self._rt.ZARR_PROGRESS_MIN_PERCENT,
        )

    def build_zarr_progress_callback(
        self,
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
        return FetcherTelemetrySupport.build_zarr_progress_callback(
            self._rt,
            job_id=job_id,
            raw_outputs=raw_outputs,
            zarr_outputs=zarr_outputs,
            raw_uri=raw_uri,
            scene_id=scene_id,
            output_uri=output_uri,
            index=index,
            total=total,
            parallel_workers=parallel_workers,
        )
