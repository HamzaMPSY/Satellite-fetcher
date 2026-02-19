from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.settings import get_settings
from nimbuschain_fetch_service.api.events import router as events_router
from nimbuschain_fetch_service.api.health import router as health_router
from nimbuschain_fetch_service.api.jobs import router as jobs_router
from nimbuschain_fetch_service.api.metrics import router as metrics_router
from nimbuschain_fetch_service.logging_config import configure_logging
from nimbuschain_fetch_service.middleware import (
    APIKeyMiddleware,
    MaxBodySizeMiddleware,
    RequestTelemetryMiddleware,
)

settings = get_settings()
configure_logging(level=settings.nimbus_log_level, json_logs=settings.nimbus_log_json)


@asynccontextmanager
async def lifespan(app: FastAPI):
    fetcher = NimbusFetcher(settings=settings)
    await fetcher.start()
    app.state.fetcher = fetcher
    app.state.settings = settings
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
app.add_middleware(MaxBodySizeMiddleware, max_body_bytes=settings.nimbus_max_request_mb * 1024 * 1024)
app.add_middleware(APIKeyMiddleware, api_key=settings.nimbus_api_key)

if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(health_router)
app.include_router(jobs_router)
app.include_router(events_router)
app.include_router(metrics_router)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root_page() -> str:
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
""" % settings.runtime_role
