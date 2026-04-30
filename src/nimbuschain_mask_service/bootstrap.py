from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from nimbuschain_mask_service.service import MaskService


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mask_service = MaskService()
    try:
        yield
    finally:
        app.state.mask_service = None


def create_app() -> FastAPI:
    from nimbuschain_mask_service.api import router

    app = FastAPI(
        title="NimbusChain Mask Service",
        version="0.1.0",
        description="Internal/dev-only mask runtime harness for Zarr-derived masks.",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


__all__ = ["create_app", "lifespan"]
