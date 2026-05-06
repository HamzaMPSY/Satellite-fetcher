from __future__ import annotations

import pytest

from nimbuschain_fetch.pipeline import (
    FunctionStage,
    PipelineConfigurationError,
    PipelineContext,
    PipelineOrchestrator,
    StageResult,
    StageStatus,
)


def test_pipeline_orchestrator_runs_stages_in_dependency_order() -> None:
    calls: list[str] = []

    def record(name: str):
        def _run(context: PipelineContext) -> StageResult:
            calls.append(name)
            context.set(name, True)
            return StageResult.succeeded_result(name, outputs=[f"{name}.out"])

        return _run

    orchestrator = PipelineOrchestrator(
        [
            FunctionStage("zarr", record("zarr"), depends_on=("fetch",)),
            FunctionStage("fetch", record("fetch")),
            FunctionStage("mask", record("mask"), depends_on=("zarr",)),
        ]
    )

    results = orchestrator.run(PipelineContext(job_id="job-1"))

    assert calls == ["fetch", "zarr", "mask"]
    assert [result.name for result in results] == ["fetch", "zarr", "mask"]
    assert all(result.status == StageStatus.succeeded for result in results)
    assert results[0].duration_seconds >= 0
    assert results[0].started_at
    assert results[0].finished_at


def test_pipeline_orchestrator_skips_conditionally_disabled_stage() -> None:
    orchestrator = PipelineOrchestrator(
        [
            FunctionStage("fetch", lambda _context: None),
            FunctionStage(
                "sen2like",
                lambda _context: pytest.fail("stage should be skipped"),
                depends_on=("fetch",),
                condition=lambda context: context.provider == "usgs",
                skip_reason="not_landsat",
            ),
        ]
    )

    results = orchestrator.run(PipelineContext(provider="copernicus"))

    assert results[0].status == StageStatus.succeeded
    assert results[1].status == StageStatus.skipped
    assert results[1].metadata["reason"] == "not_landsat"


def test_pipeline_orchestrator_blocks_downstream_after_failed_dependency() -> None:
    orchestrator = PipelineOrchestrator(
        [
            FunctionStage("fetch", lambda _context: {"status": "failed", "error": "boom"}),
            FunctionStage(
                "zarr",
                lambda _context: pytest.fail("zarr should not run"),
                depends_on=("fetch",),
            ),
        ]
    )

    results = orchestrator.run(PipelineContext())

    assert results[0].status == StageStatus.failed
    assert results[0].error == "boom"
    assert results[1].status == StageStatus.skipped
    assert results[1].metadata["reason"] == "dependency_not_succeeded"
    assert results[1].metadata["blocked_by"] == ["fetch"]


def test_pipeline_orchestrator_can_plan_target_stage_with_dependencies() -> None:
    orchestrator = PipelineOrchestrator(
        [
            FunctionStage("fetch", lambda _context: None),
            FunctionStage("sen2like", lambda _context: None, depends_on=("fetch",)),
            FunctionStage("zarr", lambda _context: None, depends_on=("sen2like",)),
            FunctionStage("mask", lambda _context: None, depends_on=("zarr",)),
        ]
    )

    assert orchestrator.plan(target_stage="zarr") == ["fetch", "sen2like", "zarr"]


def test_pipeline_orchestrator_rejects_missing_dependencies() -> None:
    with pytest.raises(PipelineConfigurationError):
        PipelineOrchestrator([FunctionStage("zarr", lambda _context: None, depends_on=("fetch",))])
