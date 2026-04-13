#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
BUILD="${BUILD:-0}"
NO_CACHE="${NO_CACHE:-0}"
WAIT_SECONDS="${WAIT_SECONDS:-45}"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi

if command -v podman machine >/dev/null 2>&1; then
  podman machine start >/dev/null 2>&1 || true
fi

wait_podman() {
  local deadline=$((SECONDS + WAIT_SECONDS))
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
  echo "ERROR: podman did not become ready within ${WAIT_SECONDS}s." >&2
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

cleanup_containerized_mask_port() {
  local names=(
    "backendnimbus-nimbus-mask-1"
    "backendnimbus_nimbus_mask_1"
    "backendnimbus_nimbus-mask_1"
  )
  local inspected
  local name
  for name in "${names[@]}"; do
    inspected="$(podman ps -a --format '{{.Names}}|{{.Ports}}' 2>/dev/null | awk -F'|' -v target="${name}" '$1 == target { print $2; exit }')"
    if [ -n "${inspected}" ] && printf '%s' "${inspected}" | grep -q '8020->8020/tcp'; then
      echo "Removing containerized nimbus-mask to free port 8020 for the native mask service..."
      podman rm -f "${name}" >/dev/null 2>&1 || true
    fi
  done
}

EXPECTED_CONTAINERS=(
  "nimbus-mongodb"
  "backendnimbus_nimbus-api_1"
  "backendnimbus_nimbus-mask_1"
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

cd "${PROJECT_ROOT}"
COMPOSE_ARGS=(--env-file "${ENV_FILE}" -f podman-compose.yml -f podman-compose.mask-external.yml)

if [ "${#existing_containers[@]}" -gt 0 ] && [ "${#missing_containers[@]}" -eq 0 ] && [ "${BUILD}" != "1" ]; then
  if [ "${#stopped_containers[@]}" -gt 0 ]; then
    echo "Starting existing containers..."
    podman start "${stopped_containers[@]}" >/dev/null
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

echo "External mask mode is active."
echo "Start the host mask service with: ./scripts/12_up_mask_service_native.sh"
echo "Containers will call NIMBUS_MASK_SERVICE_URL=${NIMBUS_MASK_SERVICE_URL:-http://host.containers.internal:8020}"
