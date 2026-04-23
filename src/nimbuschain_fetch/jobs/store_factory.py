from __future__ import annotations

from nimbuschain_fetch.jobs.mongodb_store import MongoJobStore
from nimbuschain_fetch.jobs.sqlite_store import SQLiteJobStore
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.settings import Settings


def create_job_store(settings: Settings) -> JobStore:
    backend = settings.nimbus_db_backend.strip().lower()
    if backend == "mongodb":
        return MongoJobStore(
            uri=settings.nimbus_mongodb_uri,
            db_name=settings.nimbus_mongodb_db,
        )
    if backend == "sqlite":
        return SQLiteJobStore(settings.nimbus_db_path)
    raise ValueError(f"Unsupported NIMBUS_DB_BACKEND='{settings.nimbus_db_backend}'")
