from __future__ import annotations

from abc import ABC, abstractmethod


class ExecutorBackend(ABC):
    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def submit(self, job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def cancel(self, job_id: str) -> None:
        raise NotImplementedError
