#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
BUILD="${BUILD:-0}"
NO_CACHE="${NO_CACHE:-0}"
WAIT_SECONDS="${WAIT_SECONDS:-45}"
PODMAN_CMD=("${PROJECT_ROOT}/scripts/09_podman_doctor.sh")

if [ ! -x "${PODMAN_CMD[0]}" ]; then
  echo "ERROR: ${PODMAN_CMD[0]} is missing or not executable." >&2
  exit 1
fi

if WAIT_SECONDS="${WAIT_SECONDS}" "${PODMAN_CMD[@]}" compose version >/dev/null 2>&1; then
  COMPOSE_CMD=("${PODMAN_CMD[@]}" compose)
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  echo "ERROR: neither 'podman compose' nor 'podman-compose' is available." >&2
  exit 1
fi

cleanup_containerized_mask_port() {
  local names=(
    "backendnimbus-nimbus-mask-1"
    "backendnimbus_nimbus_mask_1"
    "backendnimbus_nimbus-mask_1"
  )
  local inspected
  local name
  for name in "${names[@]}"; do
    inspected="$(WAIT_SECONDS="${WAIT_SECONDS}" "${PODMAN_CMD[@]}" ps -a --format '{{.Names}}|{{.Ports}}' 2>/dev/null | awk -F'|' -v target="${name}" '$1 == target { print $2; exit }')"
    if [ -n "${inspected}" ] && printf '%s' "${inspected}" | grep -q '8020->8020/tcp'; then
      echo "Removing containerized nimbus-mask to free port 8020 for the native mask service..."
      WAIT_SECONDS="${WAIT_SECONDS}" "${PODMAN_CMD[@]}" rm -f "${name}" >/dev/null 2>&1 || true
    fi
  done
}

EXPECTED_CONTAINERS=(
  "nimbus-mongodb"
  "backendnimbus-nimbus-api-1"
  "backendnimbus-nimbus-worker-1"
  "backendnimbus-nimbus-ui-1"
  "backendnimbus-nimbus-zarr-1"
)

existing_containers=()
stopped_containers=()
missing_containers=()
container_snapshot="$(WAIT_SECONDS="${WAIT_SECONDS}" "${PODMAN_CMD[@]}" ps -a --format '{{.Names}}|{{.Status}}' 2>/dev/null || true)"

container_status() {
  local name="$1"
  local status
  status="$(printf '%s\n' "${container_snapshot}" | awk -F'|' -v target="${name}" '$1 == target { print $2; exit }')"
  if [ -n "${status}" ]; then
    printf '%s' "${status}"
    return 0
  fi

  local alt_name=""
  if printf '%s' "${name}" | grep -q '-'; then
    alt_name="${name//-/_}"
  elif printf '%s' "${name}" | grep -q '_'; then
    alt_name="${name//_/-}"
  fi

  if [ -z "${alt_name}" ]; then
    return 0
  fi
  printf '%s\n' "${container_snapshot}" | awk -F'|' -v target="${alt_name}" '$1 == target { print $2; exit }'
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

cd "${PROJECT_ROOT}"
COMPOSE_ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml -f podman-compose.mask-external.yml)

if [ "${#existing_containers[@]}" -gt 0 ] && [ "${#missing_containers[@]}" -eq 0 ] && [ "${BUILD}" != "1" ]; then
  if [ "${#stopped_containers[@]}" -gt 0 ]; then
    echo "Starting existing containers..."
    WAIT_SECONDS="${WAIT_SECONDS}" "${PODMAN_CMD[@]}" start "${stopped_containers[@]}" >/dev/null
  else
    echo "Stack containers already exist and are running; skipping compose up."
  fi
else
  if [ "${BUILD}" = "1" ] && [ "${NO_CACHE}" = "1" ]; then
    "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" down --remove-orphans || true
    "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" build --no-cache
    "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" up -d --force-recreate --remove-orphans
  elif [ "${BUILD}" = "1" ]; then
    "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" up -d --build --force-recreate --remove-orphans
  else
    "${COMPOSE_CMD[@]}" "${COMPOSE_ARGS[@]}" up -d
  fi
fi

cleanup_containerized_mask_port

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

wait_http "API" "http://127.0.0.1:8000/v1/health"
wait_http "UI" "http://127.0.0.1:8501"
wait_http "ZARR" "http://127.0.0.1:${NIMBUS_ZARR_PORT:-8010}/readiness"
wait_http "MASK(host)" "http://127.0.0.1:${NIMBUS_MASK_PORT:-8020}/health"
wait_worker

echo "External mask mode is active."
echo "Start the host mask service with: ./scripts/12_up_mask_service_native.sh"
echo "Containers will call NIMBUS_MASK_SERVICE_URL=${NIMBUS_MASK_SERVICE_URL:-http://host.containers.internal:8020}"
echo
echo "API:  http://127.0.0.1:8000"
echo "UI:   http://127.0.0.1:8501"
echo "ZARR: http://127.0.0.1:${NIMBUS_ZARR_PORT:-8010}"
echo "MASK: http://127.0.0.1:${NIMBUS_MASK_PORT:-8020}"
