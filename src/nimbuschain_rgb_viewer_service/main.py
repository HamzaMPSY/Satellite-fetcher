from __future__ import annotations

import os

import uvicorn

from nimbuschain_rgb_viewer_service.bootstrap import create_app


DEFAULT_PORT = 8040
app = create_app()


def run() -> None:
    uvicorn.run(
        "nimbuschain_rgb_viewer_service.main:app",
        host=str(os.getenv("NIMBUS_RGB_VIEWER_HOST") or "127.0.0.1"),
        port=int(os.getenv("NIMBUS_RGB_VIEWER_PORT") or DEFAULT_PORT),
        reload=False,
    )


if __name__ == "__main__":
    run()
