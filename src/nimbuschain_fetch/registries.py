from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from nimbuschain_fetch.download.download_manager import CancelChecker, ProgressCallback, RetryCallback
from nimbuschain_fetch.jobs.executor_backends import ArqExecutorStub, CeleryExecutorStub, RQExecutorStub
from nimbuschain_fetch.jobs.executor_base import ExecutorBackend
from nimbuschain_fetch.jobs.executor_inprocess import InProcessExecutor
from nimbuschain_fetch.jobs.mongodb_store import MongoJobStore
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.ports import (
    ExecutorFactory,
    ProviderDefinitionPort,
    ProviderDownloadManagerConfig,
    ProviderPort,
    ProviderRegistryMapping,
    StoreFactory,
)
from nimbuschain_fetch.providers.base import ProviderBase
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
        self._factories: dict[str, ProviderDefinitionPort] = {}
        for name, factory in (factories or self.default_factories()).items():
            self.register(name, factory)

    @staticmethod
    def default_factories() -> ProviderRegistryMapping:
        return {
            "copernicus": CopernicusProvider,
            "usgs": UsgsProvider,
        }

    @staticmethod
    def _normalize_factory(factory: type[Any] | ProviderDefinitionPort) -> ProviderDefinitionPort:
        if isinstance(factory, type):
            return _ProviderDefinitionAdapter(factory)
        return factory

    def register(self, name: str, factory: type[Any] | ProviderDefinitionPort) -> None:
        self._factories[str(name).strip().lower()] = self._normalize_factory(factory)

    def download_manager_config(
        self,
        name: str,
        *,
        settings: Settings,
        data_plane_limit: int,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        requested_download_strategy: str,
    ) -> ProviderDownloadManagerConfig:
        key = str(name).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported provider '{name}'.")
        return factory.download_manager_config(
            settings=settings,
            data_plane_limit=data_plane_limit,
            progress_callback=progress_callback,
            cancel_checker=cancel_checker,
            retry_callback=retry_callback,
            requested_download_strategy=requested_download_strategy,
        )

    def create(
        self,
        name: str,
        *,
        settings: Settings,
        download_manager: Any,
        requested_download_strategy: str = "default",
    ) -> ProviderPort:
        key = str(name).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported provider '{name}'.")
        return factory.create_provider(
            settings=settings,
            download_manager=download_manager,
            requested_download_strategy=requested_download_strategy,
        )

    def definition(self, name: str) -> ProviderDefinitionPort:
        key = str(name).strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"Unsupported provider '{name}'.")
        return factory

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


class _ProviderDefinitionAdapter:
    def __init__(self, provider_cls: type[Any]) -> None:
        self._provider_cls = provider_cls

    def download_manager_config(
        self,
        *,
        settings: Settings,
        data_plane_limit: int,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        requested_download_strategy: str,
    ) -> ProviderDownloadManagerConfig:
        provider_cls = self._provider_cls
        if isinstance(provider_cls, type) and issubclass(provider_cls, ProviderBase):
            return provider_cls.download_manager_config(
                settings=settings,
                data_plane_limit=data_plane_limit,
                progress_callback=progress_callback,
                cancel_checker=cancel_checker,
                retry_callback=retry_callback,
                requested_download_strategy=requested_download_strategy,
            )
        return ProviderDownloadManagerConfig(
            max_concurrent=max(1, int(data_plane_limit)),
            progress_callback=progress_callback,
            cancel_checker=cancel_checker,
            retry_callback=retry_callback,
        )

    def create_provider(
        self,
        *,
        settings: Settings,
        download_manager: Any,
        requested_download_strategy: str,
    ) -> ProviderPort:
        provider_cls = self._provider_cls
        if isinstance(provider_cls, type) and issubclass(provider_cls, ProviderBase):
            return provider_cls.create_provider(
                settings=settings,
                download_manager=download_manager,
                requested_download_strategy=requested_download_strategy,
            )
        try:
            return provider_cls(settings, download_manager)
        except TypeError:
            return provider_cls()

    def single_download_manager_config(
        self,
        *,
        settings: Settings,
        progress_callback: ProgressCallback | None,
        cancel_checker: CancelChecker | None,
        retry_callback: RetryCallback | None,
        bandwidth_limiter: Any | None,
    ) -> ProviderDownloadManagerConfig:
        provider_cls = self._provider_cls
        if isinstance(provider_cls, type) and issubclass(provider_cls, ProviderBase):
            return provider_cls.single_download_manager_config(
                settings=settings,
                progress_callback=progress_callback,
                cancel_checker=cancel_checker,
                retry_callback=retry_callback,
                bandwidth_limiter=bandwidth_limiter,
            )
        return ProviderDownloadManagerConfig(
            max_concurrent=1,
            max_connections=4,
            max_connections_per_host=1,
            progress_callback=progress_callback,
            cancel_checker=cancel_checker,
            retry_callback=retry_callback,
            bandwidth_limiter=bandwidth_limiter,
        )

    def handle_retry_feedback(
        self,
        *,
        coordinator: Any,
        reason: str,
        retry_after: float | None,
        merged_context: dict[str, Any],
    ) -> None:
        provider_cls = self._provider_cls
        if isinstance(provider_cls, type) and issubclass(provider_cls, ProviderBase):
            provider_cls.handle_retry_feedback(
                coordinator=coordinator,
                reason=reason,
                retry_after=retry_after,
                merged_context=merged_context,
            )
