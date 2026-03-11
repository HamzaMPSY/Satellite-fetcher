#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
BUILD="${BUILD:-0}"
WAIT_SECONDS="${WAIT_SECONDS:-45}"

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

EXPECTED_CONTAINERS=(
  "nimbus-mongodb"
  "backendnimbus_nimbus-api_1"
  "backendnimbus_nimbus-worker_1"
  "backendnimbus_nimbus-ui_1"
  "backendnimbus_nimbus-zarr_1"
)

existing_containers=()
stopped_containers=()
missing_containers=()
container_snapshot="$(podman ps -a --format '{{.Names}}|{{.Status}}' 2>/dev/null || true)"

container_status() {
  local name="$1"
  printf '%s\n' "${container_snapshot}" | awk -F'|' -v target="${name}" '$1 == target { print $2; exit }'
}

for name in "${EXPECTED_CONTAINERS[@]}"; do
  status="$(container_status "${name}")"
  if [ -n "${status}" ]; then
    existing_containers+=("${name}")
    case "${status}" in
      Up*)
        ;;
      *)
        stopped_containers+=("${name}")
        ;;
    esac
  else
    missing_containers+=("${name}")
  fi
done

if [ "${#existing_containers[@]}" -gt 0 ] && [ "${#missing_containers[@]}" -eq 0 ] && [ "${BUILD}" != "1" ]; then
  if [ "${#stopped_containers[@]}" -gt 0 ]; then
    echo "Starting existing containers..."
    podman start "${stopped_containers[@]}" >/dev/null
  else
    echo "Stack containers already exist and are running; skipping compose up."
  fi
else
ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml up -d)
if [ "${BUILD}" = "1" ]; then
  ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml up -d --build)
fi

cd "${PROJECT_ROOT}"
"${COMPOSE_CMD[@]}" "${ARGS[@]}"
fi

wait_http() {
  local label="$1"
  local url="$2"
  local deadline=$((SECONDS + WAIT_SECONDS))
  if ! command -v curl >/dev/null 2>&1; then
    echo "WARN: curl not available, skipping readiness wait for ${label}."
    return 0
  fi

  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${label}: ready"
      return 0
    fi
    sleep 2
  done

  echo "WARN: ${label} did not become ready within ${WAIT_SECONDS}s (${url})."
  return 0
}

wait_http "API" "http://127.0.0.1:8000/v1/health"
wait_http "UI" "http://127.0.0.1:8501"
wait_http "ZARR" "http://127.0.0.1:${NIMBUS_ZARR_PORT:-8010}/health"

wait_worker() {
  local url="http://127.0.0.1:8000/v1/worker/status"
  local deadline=$((SECONDS + WAIT_SECONDS))
  if ! command -v curl >/dev/null 2>&1; then
    echo "WARN: curl not available, skipping worker readiness wait."
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: python3 not available, skipping worker readiness wait."
    return 0
  fi

  while [ "${SECONDS}" -lt "${deadline}" ]; do
    payload="$(curl -fsS "${url}" 2>/dev/null || true)"
    if [ -n "${payload}" ] && printf '%s' "${payload}" | python3 -c "import json, sys; data = json.load(sys.stdin); raise SystemExit(0 if int(data.get('workers_alive', 0) or 0) > 0 else 1)"; then
      echo "WORKER: ready"
      return 0
    fi
    sleep 2
  done

  echo "WARN: worker did not report any alive execution node within ${WAIT_SECONDS}s (${url}). Jobs may stay queued."
  return 0
}

wait_worker

echo
echo "Stack started."
echo "API: http://127.0.0.1:8000"
echo "UI:  http://127.0.0.1:8501"
echo "ZARR: http://127.0.0.1:${NIMBUS_ZARR_PORT:-8010}"
