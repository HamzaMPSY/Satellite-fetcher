from __future__ import annotations

import logging

import uvicorn

from nimbuschain_zarr_service.bootstrap import create_app
from nimbuschain_zarr_service.constants import DEFAULT_PORT


logger = logging.getLogger("nimbus.zarr")
app = create_app()


def run() -> None:
    logger.info(
        "zarr_service_starting host=%s port=%s",
        "0.0.0.0",
        DEFAULT_PORT,
    )
    uvicorn.run(
        "nimbuschain_zarr_service.main:app",
        host="0.0.0.0",
        port=DEFAULT_PORT,
        reload=False,
    )


if __name__ == "__main__":
    run()
