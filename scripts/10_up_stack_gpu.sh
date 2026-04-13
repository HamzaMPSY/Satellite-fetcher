#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"

if [ "$(uname -s)" = "Darwin" ]; then
  echo "ERROR: GPU-enabled container mode is not available on macOS." >&2
  echo "Use './scripts/10_up_stack_external_mask.sh' plus './scripts/12_up_mask_service_native.sh' to run nimbus-mask on the host with MPS." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required for the NVIDIA GPU stack." >&2
  exit 1
fi

cd "${PROJECT_ROOT}"
docker compose \
  --env-file "${ENV_FILE}" \
  -f docker-compose.yml \
  -f docker-compose.gpu.yml \
  up -d --build
