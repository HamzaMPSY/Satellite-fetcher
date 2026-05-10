from __future__ import annotations

import asyncio
from typing import Any

from nimbuschain_fetch.application.job_execution import JobExecutionContext
from nimbuschain_fetch.application.pipeline_execution import ModularPipelineJobExecutionHandler
from nimbuschain_fetch.domain.records import JobRowRecord


class FakeStore:
    def __init__(self, row: JobRowRecord):
        self.row = row
        self.result: dict[str, Any] = {}
        self.events: list[tuple[str, dict[str, Any]]] = []

    def get_job_record(self, _job_id: str) -> JobRowRecord | None:
        return self.row

    def update_job(self, _job_id: str, **fields: Any) -> None:
        payload = self.row.to_row()
        payload.update(fields)
        self.row = JobRowRecord.from_row(payload)

    def get_result(self, _job_id: str) -> dict[str, Any] | None:
        return dict(self.result) if self.result else None

    def set_result(self, _job_id: str, result_payload: dict[str, Any]) -> None:
        self.result = dict(result_payload)

    def append_event(self, _job_id: str, event_type: str, payload: dict[str, Any], *_args: Any) -> int:
        self.events.append((event_type, dict(payload)))
        return len(self.events)


class FakeRuntime:
    def __init__(self, store: FakeStore):
        self.store = store

    @staticmethod
    def _normalized_mask_types(values: list[str] | tuple[str, ...] | None) -> list[str]:
        normalized: list[str] = []
        for item in list(values or []):
            value = str(item).strip().lower()
            if value in {"water", "cloud"} and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _normalized_cube_mode(value: Any) -> str:
        candidate = str(value or "").strip().lower()
        return candidate if candidate in {"before_mask", "after_mask"} else "none"


class FakeWorkflow:
    def __init__(self, store: FakeStore):
        self.store = store

    async def execute_from_context(self, context: JobExecutionContext) -> None:
        timeline = {
            "stages": [
                {"key": "search", "status": "done", "duration_seconds": 1.0},
                {"key": "download", "status": "done", "duration_seconds": 2.0},
                {"key": "convert", "status": "done", "duration_seconds": 3.0},
                {"key": "cube", "status": "done", "duration_seconds": 4.0},
                {"key": "cloud", "status": "done", "duration_seconds": 5.0},
                {"key": "water", "status": "done", "duration_seconds": 6.0},
            ]
        }
        pipeline_metadata = {
            "timeline": timeline,
            "mask_status": "written",
            "mask_types": ["water", "cloud"],
            "cube_mode": "before_mask",
        }
        self.store.update_job(
            context.job_id,
            state="succeeded",
            pipeline_state="masked_zarr_written",
            pipeline_step="masked_zarr_written",
            pipeline_metadata=pipeline_metadata,
            raw_outputs=["/data/raw/LC08_SCENE"],
            zarr_outputs=["/data/zarr/LC08_SCENE.zarr"],
            cube_outputs=["/data/cubes/LC08_SCENE.zarr"],
        )
        self.store.set_result(
            context.job_id,
            {
                "raw_outputs": ["/data/raw/LC08_SCENE"],
                "zarr_outputs": ["/data/zarr/LC08_SCENE.zarr"],
                "cube_outputs": ["/data/cubes/LC08_SCENE.zarr"],
                "pipeline_metadata": pipeline_metadata,
            },
        )


def test_modular_pipeline_handler_records_plan_and_stage_results_for_api_job() -> None:
    row = JobRowRecord(
        job_id="job-1",
        job_type="search_download",
        provider="usgs",
        collection="landsat_ot_c2_l1",
        product_type="L1TP",
        state="running",
        request={
            "job_type": "search_download",
            "provider": "usgs",
            "collection": "landsat_ot_c2_l1",
            "product_type": "L1TP",
            "mask_types": ["water", "cloud"],
            "cube_mode": "before_mask",
        },
    )
    store = FakeStore(row)
    handler = ModularPipelineJobExecutionHandler(
        runtime=FakeRuntime(store),
        workflow=FakeWorkflow(store),
    )

    asyncio.run(
        handler.execute(
            JobExecutionContext(
                job_id="job-1",
                row=row.to_row(),
                is_cancelled_now=lambda: False,
            )
        )
    )

    metadata = store.row.pipeline_metadata.to_dict()
    assert [stage["name"] for stage in metadata["stage_plan"]] == [
        "fetch",
        "sen2like",
        "zarr",
        "cube",
        "mask",
    ]
    stage_results = metadata["stage_results"]
    assert [stage["name"] for stage in stage_results] == [
        "fetch",
        "sen2like",
        "zarr",
        "cube",
        "mask",
    ]
    assert {stage["name"]: stage["status"] for stage in stage_results} == {
        "fetch": "succeeded",
        "sen2like": "skipped",
        "zarr": "succeeded",
        "cube": "succeeded",
        "mask": "succeeded",
    }
    assert metadata["orchestrator"]["status"] == "succeeded"
    assert metadata["orchestrator"]["stage_results"] == stage_results
    assert store.result["pipeline_metadata"]["stage_results"] == stage_results
    assert store.events[-1][0] == "job.pipeline_orchestrated"


def test_modular_pipeline_handler_targets_fetch_only_for_download_only_jobs() -> None:
    row = JobRowRecord(
        job_id="job-2",
        job_type="search_download",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        state="running",
        request={
            "job_type": "search_download",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "download_only": True,
        },
    )
    store = FakeStore(row)
    handler = ModularPipelineJobExecutionHandler(
        runtime=FakeRuntime(store),
        workflow=FakeWorkflow(store),
    )

    assert [stage["name"] for stage in handler._stage_plan(row.to_row())] == ["fetch"]
