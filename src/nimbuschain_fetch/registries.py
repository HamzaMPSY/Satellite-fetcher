from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from nimbuschain_fetch.jobs.executor_backends import ArqExecutorStub, CeleryExecutorStub, RQExecutorStub
from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.jobs.executor_inprocess import InProcessExecutor
from nimbuschain_fetch.jobs.mongodb_store import MongoJobStore
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.ports import ExecutorFactory, ProviderFactory, ProviderPort, ProviderRegistryMapping, StoreFactory
from nimbuschain_fetch.providers import CopernicusProvider, UsgsProvider
from nimbuschain_fetch.settings import Settings


def _build_mongodb_store(settings: Settings) -> JobStore:
    return MongoJobStore(
        uri=settings.nimbus_mongodb_uri,
        db_name=settings.nimbus_mongodb_db,
    )


def _build_sqlite_store(settings: Settings) -> JobStore:
    return SQLiteJobStore(settings.nimbus_db_path)


def _build_inprocess_executor(**kwargs: Any) -> ExecutorBackend:
    return InProcessExecutor(**kwargs)


def _build_celery_executor(**_: Any) -> ExecutorBackend:
    return CeleryExecutorStub()


def _build_rq_executor(**_: Any) -> ExecutorBackend:
    return RQExecutorStub()


def _build_arq_executor(**_: Any) -> ExecutorBackend:
    return ArqExecutorStub()


class StoreRegistry:
    def __init__(self, factories: Mapping[str, StoreFactory] | None = None):
        self._factories: dict[str, StoreFactory] = {
            "mongodb": _build_mongodb_store,
            "sqlite": _build_sqlite_store,
        }
        if factories is not None:
            for name, factory in factories.items():
                self.register(name, factory)

    def register(self, name: str, factory: StoreFactory) -> None:
        self._factories[str(name).strip().lower()] = factory

    def create(self, settings: Settings, backend: str | None = None) -> JobStore:
        key = str(backend or settings.nimbus_db_backend).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported NIMBUS_DB_BACKEND='{backend or settings.nimbus_db_backend}'")
        return factory(settings)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


class ExecutorRegistry:
    def __init__(self, factories: Mapping[str, ExecutorFactory] | None = None):
        self._factories: dict[str, ExecutorFactory] = {
            "inprocess": _build_inprocess_executor,
            "celery": _build_celery_executor,
            "rq": _build_rq_executor,
            "arq": _build_arq_executor,
        }
        if factories is not None:
            for name, factory in factories.items():
                self.register(name, factory)

    def register(self, name: str, factory: ExecutorFactory) -> None:
        self._factories[str(name).strip().lower()] = factory

    def create(self, backend: str, **kwargs: Any) -> ExecutorBackend:
        key = str(backend).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported executor backend '{backend}'.")
        return factory(**kwargs)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


class ProviderRegistry:
    def __init__(self, factories: ProviderRegistryMapping | None = None):
        self._factories: dict[str, ProviderFactory] = {}
        for name, factory in (factories or self.default_factories()).items():
            self.register(name, factory)

    @staticmethod
    def default_factories() -> ProviderRegistryMapping:
        return {
            "copernicus": CopernicusProvider,
            "usgs": UsgsProvider,
        }

    @staticmethod
    def _normalize_factory(factory: type[Any] | ProviderFactory) -> ProviderFactory:
        if isinstance(factory, type):
            return lambda settings, download_manager, cls=factory: cls(settings, download_manager)
        return factory

    def register(self, name: str, factory: type[Any] | ProviderFactory) -> None:
        self._factories[str(name).strip().lower()] = self._normalize_factory(factory)

    def create(self, name: str, *, settings: Settings, download_manager: Any) -> ProviderPort:
        key = str(name).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported provider '{name}'.")
        return factory(settings, download_manager)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))
