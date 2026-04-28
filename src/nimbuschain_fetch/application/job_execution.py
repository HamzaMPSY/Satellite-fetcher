from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class JobExecutionContext:
    job_id: str
    row: dict[str, Any]
    is_cancelled_now: Callable[[], bool]


class JobExecutionHandler(Protocol):
    def execute(self, context: JobExecutionContext) -> Any:
        ...


class CallbackJobExecutionHandler:
    def __init__(self, callback: Callable[[JobExecutionContext], Any]):
        self._callback = callback

    def execute(self, context: JobExecutionContext) -> Any:
        return self._callback(context)


class JobExecutionRegistry:
    def __init__(
        self,
        handlers: Mapping[str, JobExecutionHandler] | None = None,
    ):
        self._handlers: dict[str, JobExecutionHandler] = {}
        for job_type, handler in (handlers or {}).items():
            self.register(job_type, handler)

    def register(self, job_type: str, handler: JobExecutionHandler) -> None:
        self._handlers[str(job_type).strip().lower()] = handler

    def resolve(self, job_type: str | None) -> JobExecutionHandler | None:
        return self._handlers.get(str(job_type or "").strip().lower())

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))
