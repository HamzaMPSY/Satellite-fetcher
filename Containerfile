ARG PYTHON_BASE=python:3.11.11-slim-bookworm
FROM ${PYTHON_BASE} AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/home/appuser \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libexpat1 && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin appuser

COPY requirements/fetch-service.txt /app/requirements/fetch-service.txt
COPY requirements/runtime-common.txt /app/requirements/runtime-common.txt
RUN python -m pip install -r /app/requirements/fetch-service.txt
COPY --chown=appuser:appuser src/nimbuschain_fetch /app/src/nimbuschain_fetch
COPY --chown=appuser:appuser src/nimbuschain_fetch_service /app/src/nimbuschain_fetch_service
COPY --chown=appuser:appuser src/nimbuschain_shared /app/src/nimbuschain_shared

USER appuser

EXPOSE 8000

CMD ["uvicorn", "nimbuschain_fetch_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
