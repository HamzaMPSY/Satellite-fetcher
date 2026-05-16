from __future__ import annotations

import logging
import subprocess

import uvicorn
from fastapi import FastAPI, HTTPException

from nimbuschain_sen2like_service import __version__
from nimbuschain_sen2like_service.models import (
    ALLOWED_CLEANUP_MODES,
    ALLOWED_STEPS,
    Sen2LikeNormalizeRequest,
    Sen2LikeNormalizeResponse,
)
from nimbuschain_sen2like_service.runner import readiness_payload, run_sen2like


logger = logging.getLogger("nimbus.sen2like")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Nimbus Sen2Like Service",
        version=__version__,
        description=(
            "HTTP wrapper around the vendored PySpark Sen2Like Landsat "
            "normalization pipeline."
        ),
    )

    @app.get("/health")
    def health() -> dict[str, object]:
        payload = readiness_payload()
        return {
            **payload,
            "status": "ok",
            "version": __version__,
        }

    @app.get("/readiness")
    def readiness() -> dict[str, object]:
        payload = readiness_payload()
        if not payload["pipeline_py_exists"] or not payload["sixs_executable_exists"]:
            raise HTTPException(status_code=503, detail=payload)
        return payload

    @app.get("/schema")
    def schema() -> dict[str, object]:
        return {
            "steps": sorted(ALLOWED_STEPS),
            "cleanup_modes": sorted(ALLOWED_CLEANUP_MODES),
            "default_port": 8030,
            "entrypoint": "Pipeline.py",
            "execution": "subprocess",
            "pyspark": True,
        }

    @app.post("/normalize", response_model=Sen2LikeNormalizeResponse)
    @app.post("/v1/normalize", response_model=Sen2LikeNormalizeResponse)
    def normalize(request: Sen2LikeNormalizeRequest) -> Sen2LikeNormalizeResponse:
        try:
            response = run_sen2like(request)
        except subprocess.TimeoutExpired as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("sen2like_normalize_failed")
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if response.status != "succeeded":
            raise HTTPException(status_code=500, detail=response.model_dump())
        return response

    return app


app = create_app()


def run() -> None:
    uvicorn.run(
        "nimbuschain_sen2like_service.main:app",
        host="0.0.0.0",
        port=8030,
        reload=False,
    )


if __name__ == "__main__":
    run()
