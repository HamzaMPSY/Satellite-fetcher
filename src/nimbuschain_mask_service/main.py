from __future__ import annotations

import uvicorn

from nimbuschain_mask_service.bootstrap import create_app


app = create_app()


def run() -> None:
    uvicorn.run("nimbuschain_mask_service.main:app", host="0.0.0.0", port=8020, reload=False)
