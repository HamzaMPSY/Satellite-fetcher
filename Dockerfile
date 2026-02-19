FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

COPY pyproject.toml README.md /build/
COPY src /build/src

RUN pip install --no-cache-dir --upgrade pip build && \
    python -m build --wheel --outdir /dist

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser

COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /tmp/*.whl && \
    rm -rf /tmp/*.whl

USER appuser

EXPOSE 8000

CMD ["uvicorn", "nimbuschain_fetch_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
