from __future__ import annotations

from nimbuschain_fetch.jobs.executor_base import ExecutorBackend


class CeleryExecutorStub(ExecutorBackend):
    """Placeholder for future Celery integration."""

    async def start(self) -> None:
        raise NotImplementedError("Celery executor backend is not implemented yet.")

    async def stop(self) -> None:
        raise NotImplementedError("Celery executor backend is not implemented yet.")

    async def submit(self, job_id: str) -> None:
        raise NotImplementedError("Celery executor backend is not implemented yet.")

    async def cancel(self, job_id: str) -> None:
        raise NotImplementedError("Celery executor backend is not implemented yet.")


class RQExecutorStub(ExecutorBackend):
    """Placeholder for future RQ integration."""

    async def start(self) -> None:
        raise NotImplementedError("RQ executor backend is not implemented yet.")

    async def stop(self) -> None:
        raise NotImplementedError("RQ executor backend is not implemented yet.")

    async def submit(self, job_id: str) -> None:
        raise NotImplementedError("RQ executor backend is not implemented yet.")

    async def cancel(self, job_id: str) -> None:
        raise NotImplementedError("RQ executor backend is not implemented yet.")


class ArqExecutorStub(ExecutorBackend):
    """Placeholder for future Arq integration."""

    async def start(self) -> None:
        raise NotImplementedError("Arq executor backend is not implemented yet.")

    async def stop(self) -> None:
        raise NotImplementedError("Arq executor backend is not implemented yet.")

    async def submit(self, job_id: str) -> None:
        raise NotImplementedError("Arq executor backend is not implemented yet.")

    async def cancel(self, job_id: str) -> None:
        raise NotImplementedError("Arq executor backend is not implemented yet.")
