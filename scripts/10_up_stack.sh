#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  COMPOSE_CMD=(podman compose)
fi

cd "${PROJECT_ROOT}"
"${COMPOSE_CMD[@]}" --env-file "${ENV_FILE}" -f podman-compose.yml up --build -d

echo
echo "Stack started."
echo "API: http://127.0.0.1:8000"
echo "UI:  http://127.0.0.1:8501"
echo
echo "Tip: ${COMPOSE_CMD[*]} -f podman-compose.yml logs -f nimbus-api nimbus-worker"
