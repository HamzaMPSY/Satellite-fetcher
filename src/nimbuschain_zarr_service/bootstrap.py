from __future__ import annotations

import logging
import os

from fastapi import FastAPI

from nimbuschain_zarr_service.api import router
from nimbuschain_zarr_service.constants import APP_VERSION
from nimbuschain_zarr_service.logging_config import configure_logging
from nimbuschain_zarr_service.middleware import RequestTelemetryMiddleware


configure_logging(
    level=str(os.getenv("NIMBUS_LOG_LEVEL") or "INFO"),
    json_logs=str(os.getenv("NIMBUS_LOG_JSON") or "").strip().lower() in {"1", "true", "yes", "on"},
)
logger = logging.getLogger("nimbus.zarr")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Nimbus Zarr Converter Service",
        version=APP_VERSION,
        description=(
            "Active microservice that reads supported raw products, normalizes them "
            "into time/band/y/x datasets, and writes local Zarr stores."
        ),
    )
    app.add_middleware(RequestTelemetryMiddleware)
    app.include_router(router)
    return app
