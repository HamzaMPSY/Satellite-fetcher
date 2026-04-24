FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends libexpat1 && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 appuser

COPY requirements/fetch-service.txt /app/requirements/fetch-service.txt
COPY requirements/runtime-common.txt /app/requirements/runtime-common.txt
COPY src/nimbuschain_fetch /app/src/nimbuschain_fetch
COPY src/nimbuschain_fetch_service /app/src/nimbuschain_fetch_service
COPY src/nimbuschain_shared /app/src/nimbuschain_shared
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements/fetch-service.txt

USER appuser

EXPOSE 8000

CMD ["uvicorn", "nimbuschain_fetch_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
