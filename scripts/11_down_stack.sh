#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PODMAN_CMD=("${PROJECT_ROOT}/scripts/09_podman_doctor.sh")
cd "${PROJECT_ROOT}"

if [ ! -x "${PODMAN_CMD[0]}" ]; then
  echo "ERROR: ${PODMAN_CMD[0]} is missing or not executable." >&2
  exit 1
fi

if WAIT_SECONDS="${WAIT_SECONDS:-30}" "${PODMAN_CMD[@]}" compose version >/dev/null 2>&1; then
  COMPOSE_CMD=("${PODMAN_CMD[@]}" compose)
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  echo "ERROR: neither 'podman compose' nor 'podman-compose' is available." >&2
  exit 1
fi

COMPOSE_ARGS=(-f podman-compose.yml)
if [ -f podman-compose.mask-external.yml ]; then
  COMPOSE_ARGS+=(-f podman-compose.mask-external.yml)
fi

"${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" down --remove-orphans

echo "Stack stopped."
