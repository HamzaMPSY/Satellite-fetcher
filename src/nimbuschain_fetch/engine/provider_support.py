from __future__ import annotations

from pathlib import Path
from typing import Any

from nimbuschain_fetch.download.coordinator import DownloadBatchResult, DownloadCoordinator
from nimbuschain_fetch.engine.provider_runtime import run_provider_job
from nimbuschain_fetch.models import JobCreateRequest, JobState, PipelineState, ProviderName


class FetcherProviderSupport:
    """Provider/download facade helpers for the fetcher runtime."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def run_provider_job(
        self,
        job_id: str,
        request: JobCreateRequest,
        output_dir: Any,
        progress_callback: Any,
        retry_callback: Any,
        is_cancelled: Any,
        *,
        download_manager_cls: Any,
    ) -> dict[str, Any]:
        return run_provider_job(
            self._rt,
            job_id=job_id,
            request=request,
            output_dir=output_dir,
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            is_cancelled=is_cancelled,
            download_manager_cls=download_manager_cls,
        )

    @staticmethod
    def provider_name(value: ProviderName | str) -> str:
        if isinstance(value, ProviderName):
            return value.value
        return str(value).strip().lower()

    def build_provider(self, provider_name: str, download_manager: Any):
        return self._rt._provider_registry.create(
            provider_name,
            settings=self._rt.settings,
            download_manager=download_manager,
        )

    def download_coordinator_instance(self, *, coordinator_cls: type[DownloadCoordinator] = DownloadCoordinator) -> DownloadCoordinator:
        if self._rt._download_coordinator is None:
            self._rt._download_coordinator = coordinator_cls(self._rt.settings)
        return self._rt._download_coordinator

    @staticmethod
    def supports_download_coordinator(
        provider_name: str,
        provider: Any,
    ) -> bool:
        _ = provider_name
        return callable(getattr(provider, "download_with_coordinator", None))

    def download_with_coordinator(
        self,
        *,
        job_id: str,
        provider_name: str,
        provider: Any,
        collection: str,
        product_ids: list[str],
        output_dir: Path,
        progress_callback: Any,
        retry_callback: Any,
        cancel_checker: Any,
        download_strategy: str,
        coordinator_cls: type[DownloadCoordinator] = DownloadCoordinator,
    ) -> DownloadBatchResult:
        coordinator = self.download_coordinator_instance(coordinator_cls=coordinator_cls)
        result = coordinator.download_products(
            job_id=job_id,
            provider_name=provider_name,
            provider=provider,
            collection=collection,
            product_ids=product_ids,
            output_dir=str(output_dir),
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            cancel_checker=cancel_checker,
            download_strategy=download_strategy,
        )
        setattr(provider, "last_download_metadata", dict(result.metadata or {}))
        return result

    def mark_cancelled(self, job_id: str, reason: str) -> None:
        current_row = self._rt._get_job_row_payload(job_id)
        self._rt.store.update_job(
            job_id,
            state=JobState.cancelled.value,
            finished_at=self._rt._now_iso(),
            pipeline_state=PipelineState.cancelled.value,
            pipeline_step="cancelled",
            pipeline_progress=current_row.get("pipeline_progress"),
        )
        self._rt.store.append_event(
            job_id,
            "job.cancelled",
            {
                "status": JobState.cancelled.value,
                "reason": reason,
                "pipeline_state": PipelineState.cancelled.value,
            },
        )
