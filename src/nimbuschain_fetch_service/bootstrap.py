from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import Settings, get_settings
from nimbuschain_fetch_service.api.artifacts import router as artifacts_router
from nimbuschain_fetch_service.api.converter import router as converter_router
from nimbuschain_fetch_service.api.events import router as events_router
from nimbuschain_fetch_service.api.health import router as health_router
from nimbuschain_fetch_service.api.jobs import router as jobs_router
from nimbuschain_fetch_service.api.metrics import router as metrics_router
from nimbuschain_fetch_service.api.preview import router as preview_router
from nimbuschain_fetch_service.api.providers import router as providers_router
from nimbuschain_fetch_service.logging_config import configure_logging
from nimbuschain_fetch_service.middleware import (
    APIKeyMiddleware,
    MaxBodySizeMiddleware,
    RequestTelemetryMiddleware,
)
from nimbuschain_fetch_service.services import (
    FetchServiceContainer,
    create_fetch_service_container,
)


def create_fetcher(*, settings: Settings) -> NimbusFetcher:
    return NimbusFetcher(settings=settings)


def create_service_container(fetcher: NimbusFetcher) -> FetchServiceContainer:
    return create_fetch_service_container(fetcher)


def _root_page(runtime_role: str) -> str:
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>NimbusChain Fetch Service</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; color: #111827; }
      h1 { margin-bottom: .25rem; }
      p { color: #4b5563; }
      .card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 1rem; max-width: 700px; }
      code { background: #f3f4f6; padding: .15rem .4rem; border-radius: 6px; }
      ul { line-height: 1.9; }
      a { color: #0f766e; text-decoration: none; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>NimbusChain Fetch Service</h1>
      <p>Service is running. Runtime role: <code>%s</code></p>
      <ul>
        <li><a href="/docs">OpenAPI docs</a></li>
        <li><a href="/v1/health">Health check</a></li>
        <li><code>POST /v1/jobs</code></li>
        <li><code>GET /v1/events</code> (SSE)</li>
      </ul>
    </div>
  </body>
</html>
""" % runtime_role


def create_app(*, settings: Settings | None = None) -> FastAPI:
    runtime_settings = settings or get_settings()
    configure_logging(
        level=runtime_settings.nimbus_log_level,
        json_logs=runtime_settings.nimbus_log_json,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        fetcher = create_fetcher(settings=runtime_settings)
        await fetcher.start()
        app.state.fetcher = fetcher
        app.state.services = create_service_container(fetcher)
        app.state.settings = runtime_settings
        try:
            yield
        finally:
            await fetcher.stop()

    app = FastAPI(
        title="NimbusChain Fetch Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(RequestTelemetryMiddleware)
    app.add_middleware(
        MaxBodySizeMiddleware,
        max_body_bytes=runtime_settings.nimbus_max_request_mb * 1024 * 1024,
    )
    app.add_middleware(APIKeyMiddleware, api_key=runtime_settings.nimbus_api_key)

    if runtime_settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=runtime_settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.include_router(health_router)
    app.include_router(jobs_router)
    app.include_router(converter_router)
    app.include_router(artifacts_router)
    app.include_router(events_router)
    app.include_router(metrics_router)
    app.include_router(preview_router)
    app.include_router(providers_router)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def root_page() -> str:
        return _root_page(runtime_settings.runtime_role)

    return app


__all__ = [
    "FetchServiceContainer",
    "create_app",
    "create_fetcher",
    "create_service_container",
]
