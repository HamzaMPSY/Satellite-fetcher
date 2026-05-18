from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import CancelChecker, ProgressCallback, RetryCallback
from nimbuschain_fetch.ports import ProviderCapabilities, ProviderDownloadManagerConfig
from nimbuschain_fetch.settings import Settings


class ProviderBase(ABC):
    @classmethod
    def download_manager_config(
        cls,
        *,
        settings: Settings,
        data_plane_limit: int,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        requested_download_strategy: str,
    ) -> ProviderDownloadManagerConfig:
        _ = (settings, requested_download_strategy)
        return ProviderDownloadManagerConfig(
            max_concurrent=max(1, int(data_plane_limit)),
            progress_callback=progress_callback,
            cancel_checker=cancel_checker,
            retry_callback=retry_callback,
        )

    @classmethod
    def create_provider(
        cls,
        *,
        settings: Settings,
        download_manager: Any,
        requested_download_strategy: str,
    ) -> "ProviderBase":
        _ = requested_download_strategy
        return cls(settings, download_manager)

    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities()

    def configure_job(
        self,
        *,
        collection: str | None = None,
        product_type: str | None = None,
        download_strategy: str = "default",
    ) -> None:
        _ = (collection, product_type, download_strategy)

    @abstractmethod
    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        raise NotImplementedError

    def plan_download_metadata(self, product_count: int) -> dict[str, Any]:
        _ = product_count
        return {}

    def download_metadata(self) -> dict[str, Any]:
        return dict(getattr(self, "last_download_metadata", {}) or {})

    def coordinator_submission_metadata(
        self,
        *,
        product_count: int,
        download_strategy: str,
    ) -> dict[str, Any]:
        base = dict(self.plan_download_metadata(product_count))
        base["download_strategy"] = str(download_strategy or "default").strip().lower() or "default"
        return base

    def finalize_coordinator_batch_metadata(
        self,
        *,
        base: dict[str, Any],
        rows: list[dict[str, Any]],
        coordinator: Any,
    ) -> dict[str, Any]:
        _ = (rows, coordinator)
        return base

    @classmethod
    def single_download_manager_config(
        cls,
        *,
        settings: Settings,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        bandwidth_limiter: Any | None,
    ) -> ProviderDownloadManagerConfig:
        _ = settings
        return ProviderDownloadManagerConfig(
            max_concurrent=1,
            max_retries=5,
            initial_delay=2.0,
            backoff_factor=1.5,
            max_retry_delay=120.0,
            connect_timeout=30.0,
            chunk_size=128 * 1024,
            max_connections=4,
            max_connections_per_host=1,
            progress_callback=progress_callback,
            cancel_checker=cancel_checker,
            retry_callback=retry_callback,
            bandwidth_limiter=bandwidth_limiter,
        )

    @classmethod
    def handle_retry_feedback(
        cls,
        *,
        coordinator: Any,
        reason: str,
        retry_after: float | None,
        merged_context: dict[str, Any],
    ) -> None:
        _ = (coordinator, reason, retry_after, merged_context)
