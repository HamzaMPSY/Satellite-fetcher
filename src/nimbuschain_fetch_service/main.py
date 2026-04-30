from __future__ import annotations

import uvicorn

from nimbuschain_fetch.settings import get_settings
from nimbuschain_fetch_service.bootstrap import create_app
from nimbuschain_fetch_service.logging_config import configure_logging

settings = get_settings()
configure_logging(level=settings.nimbus_log_level, json_logs=settings.nimbus_log_json)

app = create_app(settings=settings)


def run() -> None:
    uvicorn.run(
        "nimbuschain_fetch_service.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    run()
