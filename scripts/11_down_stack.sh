#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  COMPOSE_CMD=(podman compose)
fi

"${COMPOSE_CMD[@]}" -f podman-compose.yml down --remove-orphans

echo "Stack stopped."
