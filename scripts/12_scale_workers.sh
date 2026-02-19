#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <worker_count>" >&2
  exit 1
fi

COUNT="$1"
if ! [[ "${COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: worker_count must be a positive integer." >&2
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
cd "${PROJECT_ROOT}"

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  COMPOSE_CMD=(podman compose)
fi

"${COMPOSE_CMD[@]}" --env-file "${ENV_FILE}" -f podman-compose.yml up -d --scale nimbus-worker="${COUNT}"

echo "Scaled nimbus-worker to ${COUNT} replicas."
