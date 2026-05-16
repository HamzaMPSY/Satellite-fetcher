from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from nimbuschain_rgb_viewer_service import __version__
from nimbuschain_rgb_viewer_service.api import create_router


DEFAULT_ZARR_ROOT = Path("data/downloads/zarr")


def create_app(zarr_root: str | Path | None = None) -> FastAPI:
    resolved_root = Path(
        zarr_root
        or os.getenv("NIMBUS_RGB_VIEWER_ZARR_ROOT")
        or os.getenv("NIMBUS_DATA_ZARR_ROOT")
        or DEFAULT_ZARR_ROOT
    ).expanduser()
    app = FastAPI(
        title="Nimbus RGB Viewer Service",
        version=__version__,
        description="Small local service that renders recommended RGB previews from Nimbus Zarr stores.",
    )
    app.include_router(create_router(zarr_root=resolved_root))
    return app
