#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
BUILD="${BUILD:-0}"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi

if command -v podman machine >/dev/null 2>&1; then
  podman machine start >/dev/null 2>&1 || true
fi

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  COMPOSE_CMD=(podman compose)
fi

ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml up -d)
if [ "${BUILD}" = "1" ]; then
  ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml up -d --build)
fi

cd "${PROJECT_ROOT}"
"${COMPOSE_CMD[@]}" "${ARGS[@]}"

echo
echo "Stack started."
echo "API: http://127.0.0.1:8000"
echo "UI:  http://127.0.0.1:8501"
echo "ZARR: http://127.0.0.1:${NIMBUS_ZARR_PORT:-8010}"
