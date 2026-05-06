from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class StageStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    skipped = "skipped"
    failed = "failed"


@dataclass(frozen=True, slots=True)
class StageResult:
    name: str
    status: StageStatus
    outputs: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float = 0.0

    @classmethod
    def succeeded_result(
        cls,
        name: str,
        *,
        outputs: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> "StageResult":
        return cls(
            name=name,
            status=StageStatus.succeeded,
            outputs=[str(item) for item in list(outputs or [])],
            metadata=dict(metadata or {}),
        )

    @classmethod
    def skipped_result(
        cls,
        name: str,
        *,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> "StageResult":
        return cls(
            name=name,
            status=StageStatus.skipped,
            metadata={"reason": reason, **dict(metadata or {})},
        )

    @classmethod
    def failed_result(
        cls,
        name: str,
        *,
        error: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> "StageResult":
        return cls(
            name=name,
            status=StageStatus.failed,
            error=str(error),
            metadata=dict(metadata or {}),
        )

    def with_timing(
        self,
        *,
        started_at: str,
        finished_at: str,
        duration_seconds: float,
    ) -> "StageResult":
        return replace(
            self,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=max(0.0, float(duration_seconds)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "outputs": list(self.outputs),
            "metadata": dict(self.metadata),
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
        }


@dataclass(slots=True)
class PipelineContext:
    job_id: str = ""
    provider: str = ""
    collection: str = ""
    product_type: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    values: dict[str, Any] = field(default_factory=dict)
    results: dict[str, StageResult] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.values[key] = value

    def record_result(self, result: StageResult) -> None:
        self.results[result.name] = result
        self.values[f"result_{result.name}"] = result.to_dict()


@runtime_checkable
class PipelineStage(Protocol):
    name: str
    depends_on: Sequence[str]

    def should_run(self, context: PipelineContext) -> bool:
        ...

    def run(self, context: PipelineContext) -> StageResult | Mapping[str, Any] | None:
        ...


StageCallable = Callable[[PipelineContext], StageResult | Mapping[str, Any] | None]
StageCondition = Callable[[PipelineContext], bool]


@dataclass(slots=True)
class FunctionStage:
    name: str
    run_callable: StageCallable
    depends_on: Sequence[str] = field(default_factory=tuple)
    condition: StageCondition | None = None
    skip_reason: str = "condition_not_met"

    def should_run(self, context: PipelineContext) -> bool:
        if self.condition is None:
            return True
        return bool(self.condition(context))

    def run(self, context: PipelineContext) -> StageResult | Mapping[str, Any] | None:
        return self.run_callable(context)


class PipelineConfigurationError(ValueError):
    pass


class PipelineOrchestrator:
    def __init__(self, stages: Sequence[PipelineStage]):
        self._stages = {stage.name: stage for stage in stages}
        if len(self._stages) != len(stages):
            raise PipelineConfigurationError("Pipeline stages must have unique names.")
        self._validate_dependencies()

    @property
    def stages(self) -> dict[str, PipelineStage]:
        return dict(self._stages)

    def plan(self, *, target_stage: str | None = None) -> list[str]:
        selected = self._selected_stage_names(target_stage)
        remaining = set(selected)
        ordered: list[str] = []
        while remaining:
            ready = sorted(
                name
                for name in remaining
                if all(dep not in remaining for dep in self._stages[name].depends_on)
            )
            if not ready:
                raise PipelineConfigurationError(
                    f"Circular dependency detected in stages: {sorted(remaining)}"
                )
            ordered.extend(ready)
            remaining.difference_update(ready)
        return ordered

    def describe_plan(self, *, target_stage: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "name": name,
                "depends_on": list(self._stages[name].depends_on),
            }
            for name in self.plan(target_stage=target_stage)
        ]

    def run(
        self,
        context: PipelineContext,
        *,
        target_stage: str | None = None,
        raise_on_failure: bool = False,
    ) -> list[StageResult]:
        results: list[StageResult] = []
        for name in self.plan(target_stage=target_stage):
            stage = self._stages[name]
            blocked_by = [
                dep
                for dep in stage.depends_on
                if context.results.get(dep) is None
                or context.results[dep].status != StageStatus.succeeded
            ]
            if blocked_by:
                result = StageResult.skipped_result(
                    name,
                    reason="dependency_not_succeeded",
                    metadata={"blocked_by": blocked_by},
                )
                result = self._with_elapsed_timing(result, started_mono=time.perf_counter())
                context.record_result(result)
                results.append(result)
                continue

            started_mono = time.perf_counter()
            try:
                if not stage.should_run(context):
                    reason = getattr(stage, "skip_reason", "condition_not_met")
                    result = StageResult.skipped_result(name, reason=str(reason))
                else:
                    result = self._coerce_result(name, stage.run(context))
            except Exception as exc:
                result = StageResult.failed_result(name, error=str(exc))

            result = self._with_elapsed_timing(result, started_mono=started_mono)
            context.record_result(result)
            results.append(result)
            if raise_on_failure and result.status == StageStatus.failed:
                raise RuntimeError(f"Pipeline stage '{name}' failed: {result.error}")
        return results

    def _validate_dependencies(self) -> None:
        missing: list[str] = []
        for stage in self._stages.values():
            for dependency in stage.depends_on:
                if dependency not in self._stages:
                    missing.append(f"{stage.name}->{dependency}")
        if missing:
            raise PipelineConfigurationError(
                f"Pipeline stage dependencies are missing: {', '.join(sorted(missing))}"
            )

    def _selected_stage_names(self, target_stage: str | None) -> set[str]:
        if target_stage is None:
            return set(self._stages)
        target = str(target_stage).strip()
        if target not in self._stages:
            raise PipelineConfigurationError(f"Unknown pipeline stage: {target}")
        selected: set[str] = set()

        def visit(name: str) -> None:
            if name in selected:
                return
            for dependency in self._stages[name].depends_on:
                visit(dependency)
            selected.add(name)

        visit(target)
        return selected

    @staticmethod
    def _coerce_result(
        name: str,
        value: StageResult | Mapping[str, Any] | None,
    ) -> StageResult:
        if isinstance(value, StageResult):
            return value if value.name == name else replace(value, name=name)
        if value is None:
            return StageResult.succeeded_result(name)
        status = StageStatus(str(value.get("status") or StageStatus.succeeded.value))
        outputs = [str(item) for item in list(value.get("outputs") or [])]
        metadata = dict(value.get("metadata") or {})
        error = value.get("error")
        return StageResult(
            name=name,
            status=status,
            outputs=outputs,
            metadata=metadata,
            error=str(error) if error else None,
        )

    @staticmethod
    def _with_elapsed_timing(result: StageResult, *, started_mono: float) -> StageResult:
        now = datetime.now(timezone.utc)
        duration = time.perf_counter() - started_mono
        # The stage start timestamp is derived from finish time minus duration to
        # keep one clock source for wall-clock values while measuring elapsed time.
        started_at = datetime.fromtimestamp(now.timestamp() - duration, tz=timezone.utc)
        return result.with_timing(
            started_at=started_at.isoformat(),
            finished_at=now.isoformat(),
            duration_seconds=duration,
        )
