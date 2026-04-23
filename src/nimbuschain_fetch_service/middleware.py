from __future__ import annotations

import logging
import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from nimbuschain_fetch_service.observability import record_http_request


logger = logging.getLogger("nimbus.api")


class APIKeyMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, api_key: str | None):
        super().__init__(app)
        self._api_key = api_key.strip() if api_key else None

    async def dispatch(self, request: Request, call_next):
        if not self._api_key:
            return await call_next(request)

        if request.url.path in {"/", "/v1/health"}:
            return await call_next(request)

        incoming = request.headers.get("X-API-Key")
        if incoming != self._api_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid API key."})

        return await call_next(request)


class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_body_bytes: int):
        super().__init__(app)
        self._max_body_bytes = max(1, int(max_body_bytes))

    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdigit():
            if int(content_length) > self._max_body_bytes:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Payload too large."},
                )

        if request.method in {"POST", "PUT", "PATCH"} and not content_length:
            body = await request.body()
            if len(body) > self._max_body_bytes:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Payload too large."},
                )

        return await call_next(request)


class RequestTelemetryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = request_id
        started = time.monotonic()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = int(response.status_code)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            elapsed = max(0.0, time.monotonic() - started)
            route = request.scope.get("route")
            route_path = getattr(route, "path", request.url.path)
            record_http_request(request.method, str(route_path), status_code, elapsed)
            logger.info(
                "request_completed method=%s path=%s status=%s duration_s=%.4f",
                request.method,
                route_path,
                status_code,
                elapsed,
                extra={"request_id": request_id},
            )
