from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from shapely.geometry.base import BaseGeometry

from nimbuschain_fetch.download.coordinator import DownloadBatchResult
from nimbuschain_fetch.download.download_manager import CancelChecker, ProgressCallback, RetryCallback
from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import ArtifactListResponse, ArtifactRecord, ArtifactUpsertRequest
from nimbuschain_fetch.settings import Settings


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
    ) -> DownloadBatchResult:
        ...


StoreFactory = Callable[[Settings], JobStore]
ExecutorFactory = Callable[..., ExecutorBackend]
ProviderFactory = Callable[[Settings, Any], ProviderPort]
ProviderRegistryMapping = Mapping[str, type[Any] | ProviderFactory]
