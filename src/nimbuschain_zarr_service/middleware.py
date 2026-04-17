from __future__ import annotations

import logging
import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger("nimbus.zarr")


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
            logger.info(
                "request_completed method=%s path=%s status=%s duration_s=%.4f",
                request.method,
                route_path,
                status_code,
                elapsed,
                extra={"request_id": request_id},
            )
