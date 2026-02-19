#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed." >&2
  exit 1
fi

cd "${PROJECT_ROOT}"
docker compose --env-file "${ENV_FILE}" up --build -d
docker compose ps

echo
echo "Stack started (docker compose)."
echo "API: http://127.0.0.1:8000"
echo "UI:  http://127.0.0.1:8501"
