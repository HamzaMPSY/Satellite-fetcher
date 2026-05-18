from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["InProcessExecutor", "SQLiteJobStore", "MongoJobStore", "create_job_store"]


def __getattr__(name: str) -> Any:
    if name == "InProcessExecutor":
        return getattr(import_module("nimbuschain_fetch.jobs.executor_inprocess"), name)
    if name == "SQLiteJobStore":
        return getattr(import_module("nimbuschain_fetch.jobs.sqlite_store"), name)
    if name == "MongoJobStore":
        return getattr(import_module("nimbuschain_fetch.jobs.mongodb_store"), name)
    if name == "create_job_store":
        return getattr(import_module("nimbuschain_fetch.jobs.store_factory"), name)
    raise AttributeError(name)
