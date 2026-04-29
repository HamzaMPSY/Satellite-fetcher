from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.download_manager import CancelChecker, ProgressCallback, RetryCallback
from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import ArtifactListResponse, ArtifactRecord, ArtifactUpsertRequest
from nimbuschain_fetch.settings import Settings

if TYPE_CHECKING:
    from nimbuschain_fetch.download.coordinator import DownloadBatchResult


@dataclass(frozen=True, slots=True)
class ZarrConversionRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    provider: str
    collection: str
    scene_id: str
    raw_uri: str
    output_uri: str
    product_type: str | None = None
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class GroupedCubeBuildRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    source_zarr_uris: list[str]
    output_dir: str
    include_ancillary: bool = True
    include_masks: bool | None = None
    start_date: str | None = None
    end_date: str | None = None
    stage_label: str | None = None
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class CubeBuildRequest:
    job_id: str
    pipeline_id: str
    trace_id: str
    source_zarr_uris: list[str]
    output_uri: str
    include_ancillary: bool = True
    include_masks: bool = False
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class MaskExecutionRequest:
    source_zarr_uri: str
    provider: str
    collection: str
    product_type: str | None
    scene_id: str
    acquisition_datetime: str | None
    dataset_summary: dict[str, Any] = field(default_factory=dict)
    mask_types: list[str] = field(default_factory=list)
    backend: str = "auto"
    threshold: float | None = None
    overwrite: bool = True
    inference_device: str | None = None
    include_shadows: bool = True
    water_backend: str = "auto"
    water_overwrite: bool = True
    water_inference_device: str | None = None
    fail_on_error: bool = False


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    supports_download_coordinator: bool = False


@dataclass(frozen=True, slots=True)
class ProviderDownloadManagerConfig:
    max_concurrent: int
    max_retries: int = 5
    initial_delay: float = 2.0
    backoff_factor: float = 1.5
    max_retry_delay: float = 120.0
    connect_timeout: float = 30.0
    read_timeout: float | None = None
    chunk_size: int = 128 * 1024
    max_connections: int | None = 50
    max_connections_per_host: int | None = 2
    enable_resume: bool = True
    min_resume_size: int = 1024 * 1024
    gateway_timeout_retries: int = 3
    gateway_timeout_floor_delay: float = 8.0
    progress_callback: ProgressCallback | None = None
    cancel_checker: CancelChecker | None = None
    retry_callback: RetryCallback | None = None
    bandwidth_limiter: Any | None = None

    def to_kwargs(self) -> dict[str, Any]:
        return {
            "max_concurrent": self.max_concurrent,
            "max_retries": self.max_retries,
            "initial_delay": self.initial_delay,
            "backoff_factor": self.backoff_factor,
            "max_retry_delay": self.max_retry_delay,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
            "chunk_size": self.chunk_size,
            "max_connections": self.max_connections,
            "max_connections_per_host": self.max_connections_per_host,
            "enable_resume": self.enable_resume,
            "min_resume_size": self.min_resume_size,
            "gateway_timeout_retries": self.gateway_timeout_retries,
            "gateway_timeout_floor_delay": self.gateway_timeout_floor_delay,
            "progress_callback": self.progress_callback,
            "cancel_checker": self.cancel_checker,
            "retry_callback": self.retry_callback,
            "bandwidth_limiter": self.bandwidth_limiter,
        }


@runtime_checkable
class ConverterPort(Protocol):
    def close(self) -> None:
        ...

    def convert_request(
        self,
        request: ZarrConversionRequest,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        ...

    def convert(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        ...

    def build_grouped_cubes(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        source_zarr_uris: list[str],
        output_dir: str,
        include_ancillary: bool = True,
        include_masks: bool | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        stage_label: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        ...

    def build_grouped_cubes_request(
        self,
        request: GroupedCubeBuildRequest,
    ) -> dict[str, Any]:
        ...

    def build_cube(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        source_zarr_uris: list[str],
        output_uri: str,
        include_ancillary: bool = True,
        include_masks: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        ...

    def build_cube_request(
        self,
        request: CubeBuildRequest,
    ) -> dict[str, Any]:
        ...

    def inspect_dataset(self, *, zarr_uri: str) -> dict[str, Any]:
        ...


@runtime_checkable
class MaskPort(Protocol):
    @property
    def supports_stage_callbacks(self) -> bool:
        ...

    def close(self) -> None:
        ...

    def health(self) -> dict[str, Any]:
        ...

    def schema(self) -> dict[str, Any]:
        ...

    def apply_mask_request(
        self,
        request: MaskExecutionRequest,
        *,
        job_id: str | None = None,
        stage_callback: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        ...

    def apply_masks_to_zarr(self, **kwargs: Any) -> dict[str, Any]:
        ...


@runtime_checkable
class ArtifactRegistryPort(Protocol):
    def upsert(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        ...

    def list_artifacts(
        self,
        *,
        artifact_type: str | None,
        provider: str | None,
        collection: str | None,
        scene_id: str | None,
        job_id: str | None,
        uri_query: str | None,
        date_from: Any,
        date_to: Any,
        page: int,
        page_size: int,
    ) -> ArtifactListResponse:
        ...


@runtime_checkable
class ProviderPort(Protocol):
    def capabilities(self) -> ProviderCapabilities:
        ...

    def configure_job(
        self,
        *,
        collection: str | None = None,
        product_type: str | None = None,
        download_strategy: str = "default",
    ) -> None:
        ...

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi: BaseGeometry | None,
        tile_id: str | None = None,
    ) -> list[str]:
        ...

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        ...

    def plan_download_metadata(self, product_count: int) -> dict[str, Any]:
        ...

    def download_metadata(self) -> dict[str, Any]:
        ...


@runtime_checkable
class CoordinatorAwareProviderPort(ProviderPort, Protocol):
    def download_with_coordinator(
        self,
        *,
        job_id: str,
        collection: str,
        product_ids: list[str],
        output_dir: Path,
        progress_callback: ProgressCallback | None,
        retry_callback: RetryCallback | None,
        cancel_checker: CancelChecker | None,
        download_strategy: str,
    ) -> "DownloadBatchResult":
        ...


@runtime_checkable
class ProviderDefinitionPort(Protocol):
    def download_manager_config(
        self,
        *,
        settings: Settings,
        data_plane_limit: int,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        requested_download_strategy: str,
    ) -> ProviderDownloadManagerConfig:
        ...

    def create_provider(
        self,
        *,
        settings: Settings,
        download_manager: Any,
        requested_download_strategy: str,
    ) -> ProviderPort:
        ...

    def single_download_manager_config(
        self,
        *,
        settings: Settings,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        bandwidth_limiter: Any | None,
    ) -> ProviderDownloadManagerConfig:
        ...

    def handle_retry_feedback(
        self,
        *,
        coordinator: Any,
        reason: str,
        retry_after: float | None,
        merged_context: dict[str, Any],
    ) -> None:
        ...


StoreFactory = Callable[[Settings], JobStore]
ExecutorFactory = Callable[..., ExecutorBackend]
ProviderRegistryMapping = Mapping[str, type[Any] | ProviderDefinitionPort]
