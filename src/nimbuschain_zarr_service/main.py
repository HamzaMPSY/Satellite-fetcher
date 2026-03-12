"""FastAPI entrypoint for NimbusChain Zarr Service.

Provides a minimal health endpoint so the container can start under uvicorn.
Extend with real converter APIs as needed.
"""

from fastapi import FastAPI


app = FastAPI(title="NimbusChain Zarr Service", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def run() -> None:
    import uvicorn

    uvicorn.run("nimbuschain_zarr_service.main:app", host="0.0.0.0", port=8010)


if __name__ == "__main__":
    run()