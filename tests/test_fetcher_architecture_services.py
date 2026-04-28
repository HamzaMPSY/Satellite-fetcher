from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from nimbuschain_fetch.application.artifact_registry import ArtifactRegistryService
from nimbuschain_fetch.application.job_execution import JobExecutionContext, JobExecutionRegistry
from nimbuschain_fetch.application.pipeline_state import PipelineStateService
from nimbuschain_fetch.domain.metadata import ConversionItemRecord, ConversionMetadataRecord, MaskStateRecord, PipelineMetadataRecord
from nimbuschain_fetch.domain.records import ArtifactRowRecord, JobEventRecord, JobResultRecord, JobRowRecord
from nimbuschain_fetch.domain.workflow_models import MaskWorkflowItem, MaskWorkflowSummary
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.models import ArtifactType, JobStatusResponse, PipelineState
from nimbuschain_fetch.registries import ExecutorRegistry, ProviderRegistry, StoreRegistry
from nimbuschain_fetch.settings import Settings


class DummyStore:
    def __init__(self):
        self.job_rows: dict[str, dict[str, Any]] = {}
        self.artifact_rows: list[dict[str, Any]] = []
        self.events: list[tuple[str, str, dict[str, Any]]] = []
        self.result_rows: dict[str, dict[str, Any]] = {}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self.job_rows.get(job_id)
        return dict(row) if row is not None else None

    def get_job_record(self, job_id: str) -> JobRowRecord | None:
        row = self.get_job(job_id)
        return JobRowRecord.from_row(row) if row is not None else None

    def update_job(self, job_id: str, **fields: Any) -> None:
        row = dict(self.job_rows.get(job_id) or {"job_id": job_id})
        row.update(fields)
        self.job_rows[job_id] = row

    def claim_job_for_execution(self, job_id: str, worker_id: str) -> bool:
        _ = worker_id
        return job_id in self.job_rows

    def append_event(self, job_id: str, event_type: str, payload: dict[str, Any], timestamp: Any = None) -> int:
        _ = timestamp
        self.events.append((job_id, event_type, dict(payload)))
        return len(self.events)

    def set_result(self, job_id: str, result_payload: dict[str, Any]) -> None:
        self.result_rows[job_id] = dict(result_payload)

    def set_result_record(self, result: JobResultRecord) -> None:
        self.result_rows[result.job_id] = result.to_row()

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        row = self.result_rows.get(job_id)
        return dict(row) if row is not None else None

    def get_result_record(self, job_id: str) -> JobResultRecord | None:
        row = self.get_result(job_id)
        return JobResultRecord.from_row(job_id, row) if row is not None else None

    def list_event_records(self, job_id: str | None, since_id: int | None, limit: int = 200) -> list[JobEventRecord]:
        _ = limit
        items: list[JobEventRecord] = []
        for index, (event_job_id, event_type, payload) in enumerate(self.events, start=1):
            if job_id is not None and event_job_id != job_id:
                continue
            if since_id is not None and index <= since_id:
                continue
            items.append(
                JobEventRecord(
                    id=index,
                    job_id=event_job_id,
                    type=event_type,
                    payload=dict(payload),
                )
            )
        return items

    def upsert_artifact(self, artifact_payload: dict[str, Any]) -> dict[str, Any]:
        self.artifact_rows.append(dict(artifact_payload))
        return dict(artifact_payload)

    def upsert_artifact_record(self, artifact: ArtifactRowRecord) -> ArtifactRowRecord:
        row = self.upsert_artifact(artifact.to_row())
        return ArtifactRowRecord.from_row(row)

    def list_artifact_records(self, filters: Any) -> tuple[list[ArtifactRowRecord], int]:
        _ = filters
        return [ArtifactRowRecord.from_row(row) for row in self.artifact_rows], len(self.artifact_rows)


def test_pipeline_state_service_updates_job_and_event() -> None:
    store = DummyStore()
    store.job_rows["job-1"] = {
        "job_id": "job-1",
        "job_type": "search_download",
        "state": "running",
        "request": {"mask_types": ["cloud"], "cube_mode": "after_mask"},
        "pipeline_metadata": {},
    }
    service = PipelineStateService(
        store=store,  # type: ignore[arg-type]
        lock=RLock(),
        now_iso=lambda: "2026-04-28T00:00:00+00:00",
        job_kind_for_type=lambda job_type: "mask" if job_type == "mask_existing_zarr" else "fetch",
        normalized_mask_types=lambda values: [str(item).lower() for item in list(values or [])],
        timeline_cube_mode_for_row=lambda row, metadata=None: str(
            (metadata or {}).get("cube_mode") or dict(row.get("request") or {}).get("cube_mode") or "none"
        ),
    )

    service.update(
        "job-1",
        pipeline_state=PipelineState.zarr_converting,
        pipeline_step="writing_zarr",
        pipeline_progress=42.0,
        pipeline_metadata={"cube_mode": "after_mask"},
        event_type="job.zarr_converting",
        event_payload={"detail": "started"},
    )

    updated = store.job_rows["job-1"]
    assert updated["pipeline_state"] == PipelineState.zarr_converting.value
    assert updated["pipeline_step"] == "writing_zarr"
    assert updated["pipeline_progress"] == 42.0
    assert updated["pipeline_metadata"]["timeline"]["current_stage"] == "convert"
    assert updated["pipeline_metadata"]["timeline"]["stages"][2]["status"] == "running"
    assert store.events[-1][1] == "job.zarr_converting"


def test_artifact_registry_service_hashes_and_normalizes_payload() -> None:
    store = DummyStore()
    service = ArtifactRegistryService(
        store=store,  # type: ignore[arg-type]
        normalize_backend_path=lambda value: str(value).replace("/legacy", "/normalized"),
        normalize_artifact_row=lambda row: dict(row),
        path_size_bytes=lambda path: 123 if path else None,
        water_mask_quality_fields=lambda payload: {"quality": payload.get("status")},
        cloud_mask_quality_fields=lambda payload: {"quality": payload.get("status")},
        mask_quality_fields=lambda **kwargs: kwargs,
    )

    service.register_cube(
        job_id="job-2",
        provider_name="copernicus",
        collection="SENTINEL-2",
        cube_stage="after_mask",
        cube_summary={
            "zarr_uri": "/legacy/cubes/example.zarr",
            "scene_ids": ["scene-1"],
            "source_zarr_uris": ["/legacy/zarr/scene-1.zarr"],
            "data_family": "optical",
            "band_names": ["B02"],
            "dimensions": ["time", "band", "y", "x"],
            "shape": [1, 1, 2, 2],
        },
    )

    stored = store.artifact_rows[-1]
    assert stored["artifact_type"] == ArtifactType.zarr_cube.value
    assert stored["artifact_uri"] == "/normalized/cubes/example.zarr"
    assert stored["size_bytes"] == 123
    assert stored["artifact_id"]


def test_mask_workflow_models_serialize_cleanly() -> None:
    item = MaskWorkflowItem(
        zarr_uri="/tmp/scene.zarr",
        status="written",
        pipeline_metadata={"masked_zarr_uri": "/tmp/scene.zarr"},
        conversion_metadata={"cloud_mask": {"uri": "/tmp/cloud.tif"}},
        errors=[],
    )
    summary = MaskWorkflowSummary(
        status="written",
        mask_types=["cloud"],
        items=[item],
        masked_zarr_uri="/tmp/scene.zarr",
        cloud_mask={"uri": "/tmp/cloud.tif"},
        mask_quality={"cloud_fraction": 0.2},
    )

    payload = summary.to_payload()

    assert payload["status"] == "written"
    assert payload["items"][0]["zarr_uri"] == "/tmp/scene.zarr"
    assert payload["cloud_mask"]["uri"] == "/tmp/cloud.tif"


def test_metadata_records_extract_nested_state() -> None:
    pipeline = PipelineMetadataRecord.from_mapping({"cube_mode": "after_mask", "mask_status": "written"})
    conversion = ConversionMetadataRecord.from_mapping(
        {
            "mask": {"status": "failed"},
            "items": [{"zarr_uri": "/tmp/scene.zarr", "scene_id": "scene-1", "summary": {"grid": {"crs": "EPSG:32631"}}}],
            "water_mask": {"artifact_uri": "/tmp/water.tif"},
            "cloud_mask": {"artifact_uri": "/tmp/cloud.tif"},
            "water_fraction": 0.3,
        }
    )
    mask_state = MaskStateRecord.from_sources(conversion.to_dict(), pipeline.to_dict())
    item = ConversionItemRecord.from_mapping(conversion.items[0])

    assert pipeline.cube_mode == "after_mask"
    assert conversion.mask_status == "failed"
    assert mask_state.water_mask["artifact_uri"] == "/tmp/water.tif"
    assert mask_state.water_fraction == 0.3
    assert item.zarr_uri == "/tmp/scene.zarr"


def test_fetcher_get_result_reads_typed_store_record() -> None:
    store = DummyStore()
    store.job_rows["job-r"] = {
        "job_id": "job-r",
        "job_type": "search_download",
        "provider": "copernicus",
        "collection": "SENTINEL-2",
        "pipeline_metadata": {},
        "conversion_metadata": {},
    }
    store.set_result_record(
        JobResultRecord(
            job_id="job-r",
            paths=["/tmp/raw.zip", "/tmp/out.zarr"],
            raw_outputs=["/tmp/raw.zip"],
            zarr_outputs=["/tmp/out.zarr"],
            metadata={"source_job_id": "src-1"},
            pipeline_metadata={"stage": "written"},
            conversion_metadata={"status": "written"},
        )
    )
    fetcher = NimbusFetcher(
        settings=Settings(
            NIMBUS_DB_BACKEND="sqlite",
            NIMBUS_DB_PATH=str(Path("data/test.db")),
            NIMBUS_RUNTIME_ROLE="api",
        ),
        store=store,  # type: ignore[arg-type]
    )

    result = fetcher.get_result("job-r")

    assert result.job_id == "job-r"
    assert result.zarr_outputs == ["/tmp/out.zarr"]
    assert result.metadata["source_job_id"] == "src-1"


@dataclass
class FakeProvider:
    settings: Settings
    download_manager: Any

    def search_products(self, collection: str, product_type: str, start_date: str, end_date: str, aoi: Any, tile_id: str | None = None) -> list[str]:
        _ = (collection, product_type, start_date, end_date, aoi, tile_id)
        return []

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        _ = (product_ids, output_dir)
        return []


class FakeExecutor(ExecutorBackend):
    def __init__(self, **kwargs: Any):
        self.kwargs = dict(kwargs)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def submit(self, job_id: str) -> None:
        _ = job_id
        return None

    async def cancel(self, job_id: str) -> None:
        _ = job_id
        return None


def test_registries_and_fetcher_use_injected_factories() -> None:
    settings = Settings(
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(Path("data/test.db")),
        NIMBUS_RUNTIME_ROLE="api",
    )
    store_instance = DummyStore()
    store_registry = StoreRegistry({"sqlite": lambda _: store_instance})  # type: ignore[arg-type]
    provider_registry = ProviderRegistry({"fake": FakeProvider})
    executor_registry = ExecutorRegistry({"inprocess": lambda **kwargs: FakeExecutor(**kwargs)})

    fetcher = NimbusFetcher(
        settings=settings,
        store_registry=store_registry,
        provider_registry=provider_registry,
        executor_registry=executor_registry,
    )

    provider = fetcher._build_provider("fake", download_manager=object())
    assert isinstance(provider, FakeProvider)
    assert fetcher.store is store_instance


def test_job_execution_registry_dispatches_mask_workflow() -> None:
    store = DummyStore()
    store.job_rows["mask-job"] = {
        "job_id": "mask-job",
        "job_type": "mask_existing_zarr",
        "provider": "copernicus",
        "collection": "SENTINEL-2",
        "state": "queued",
        "request": {},
    }
    handled: list[JobExecutionContext] = []

    class Handler:
        def execute(self, context: JobExecutionContext) -> None:
            handled.append(context)

    fetcher = NimbusFetcher(
        settings=Settings(
            NIMBUS_DB_BACKEND="sqlite",
            NIMBUS_DB_PATH=str(Path("data/test.db")),
            NIMBUS_RUNTIME_ROLE="api",
        ),
        store=store,  # type: ignore[arg-type]
        job_execution_registry=JobExecutionRegistry({"mask_existing_zarr": Handler()}),
    )

    asyncio.run(fetcher._execute_job("mask-job", lambda: False))

    assert len(handled) == 1
    assert handled[0].job_id == "mask-job"


def test_job_execution_registry_awaits_async_fetch_workflow() -> None:
    store = DummyStore()
    store.job_rows["fetch-job"] = {
        "job_id": "fetch-job",
        "job_type": "search_download",
        "provider": "copernicus",
        "collection": "SENTINEL-2",
        "state": "queued",
        "request": {},
    }
    handled: list[str] = []

    class AsyncHandler:
        async def execute(self, context: JobExecutionContext) -> None:
            handled.append(context.job_id)

    fetcher = NimbusFetcher(
        settings=Settings(
            NIMBUS_DB_BACKEND="sqlite",
            NIMBUS_DB_PATH=str(Path("data/test.db")),
            NIMBUS_RUNTIME_ROLE="api",
        ),
        store=store,  # type: ignore[arg-type]
        job_execution_registry=JobExecutionRegistry({"search_download": AsyncHandler()}),
    )

    asyncio.run(fetcher._execute_job("fetch-job", lambda: False))

    assert handled == ["fetch-job"]


def test_fetcher_delegates_resume_pipeline_to_fetch_workflow() -> None:
    fetcher = NimbusFetcher(
        settings=Settings(
            NIMBUS_DB_BACKEND="sqlite",
            NIMBUS_DB_PATH=str(Path("data/test.db")),
            NIMBUS_RUNTIME_ROLE="api",
        ),
        store=DummyStore(),  # type: ignore[arg-type]
    )
    captured: dict[str, Any] = {}

    class Workflow:
        def continue_remaining_pipeline_after_zarr(self, **kwargs: Any) -> JobStatusResponse:
            captured.update(kwargs)
            return JobStatusResponse(
                job_id="job-9",
                state="succeeded",
                provider="copernicus",
                collection="SENTINEL-2",
            )

    fetcher._fetch_job_workflow = Workflow()  # type: ignore[assignment]

    response = fetcher._continue_remaining_pipeline_after_zarr(
        job_id="job-9",
        row={"request": {}},
        result={},
        raw_outputs=["/tmp/raw"],
        zarr_outputs=["/tmp/zarr"],
        conversion_metadata={"status": "written"},
    )

    assert response.job_id == "job-9"
    assert captured["job_id"] == "job-9"
    assert captured["zarr_outputs"] == ["/tmp/zarr"]


def test_fetcher_delegates_manual_conversion_to_service() -> None:
    fetcher = NimbusFetcher(
        settings=Settings(
            NIMBUS_DB_BACKEND="sqlite",
            NIMBUS_DB_PATH=str(Path("data/test.db")),
            NIMBUS_RUNTIME_ROLE="api",
        ),
        store=DummyStore(),  # type: ignore[arg-type]
    )
    captured: dict[str, Any] = {}

    class ConversionService:
        def convert_existing_job(self, job_id: str, request: Any, *, continue_pipeline: bool = False) -> JobStatusResponse:
            captured["job_id"] = job_id
            captured["request"] = request
            captured["continue_pipeline"] = continue_pipeline
            return JobStatusResponse(
                job_id=job_id,
                state="succeeded",
                provider="copernicus",
                collection="SENTINEL-2",
            )

    fetcher._manual_conversion_service = ConversionService()  # type: ignore[assignment]

    response = fetcher.convert_existing_job("job-11", object(), continue_pipeline=True)

    assert response.job_id == "job-11"
    assert captured["job_id"] == "job-11"
    assert captured["continue_pipeline"] is True
