#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

if command -v podman machine >/dev/null 2>&1; then
  podman machine start >/dev/null 2>&1 || true
fi

wait_podman() {
  local deadline=$((SECONDS + 30))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if podman info >/dev/null 2>&1; then
      return 0
    fi
    podman system connection default podman-machine-default >/dev/null 2>&1 || true
    if command -v podman machine >/dev/null 2>&1; then
      podman machine start >/dev/null 2>&1 || true
    fi
    sleep 2
  done
  echo "ERROR: podman did not become ready for stack shutdown." >&2
  exit 1
}

wait_podman

if podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  echo "ERROR: neither 'podman compose' nor 'podman-compose' is available." >&2
  exit 1
fi

"${COMPOSE_CMD[@]}" -f podman-compose.yml down --remove-orphans

echo "Stack stopped."
