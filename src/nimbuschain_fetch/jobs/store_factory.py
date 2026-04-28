from __future__ import annotations

from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.registries import StoreRegistry
from nimbuschain_fetch.settings import Settings


def create_job_store(settings: Settings) -> JobStore:
    return StoreRegistry().create(settings)
