"""NimbusChain Fetch core engine package."""

from __future__ import annotations

from typing import Any

__all__ = ["NimbusFetcher", "NimbusFetcherClient"]


def __getattr__(name: str) -> Any:
    if name == "NimbusFetcher":
        from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher

        return NimbusFetcher
    if name == "NimbusFetcherClient":
        from nimbuschain_fetch.client import NimbusFetcherClient

        return NimbusFetcherClient
    raise AttributeError(f"module 'nimbuschain_fetch' has no attribute {name!r}")
