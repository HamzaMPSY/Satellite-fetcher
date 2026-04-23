#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_ENV_FILE="${PROJECT_ROOT}/.env"
DEFAULT_ENV_FILE="${PROJECT_ROOT}/.env.oci.remote"
ENV_FILE="${ENV_FILE:-${DEFAULT_ENV_FILE}}"
RESTART="${RESTART:-0}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"
RUNTIME_DIR="${PROJECT_ROOT}/.runtime/oci-remote"
API_PORT="${NIMBUS_API_PORT:-${NIMBUS_OCI_REMOTE_API_PORT:-8000}}"
ZARR_PORT="${NIMBUS_ZARR_PORT:-${NIMBUS_OCI_REMOTE_ZARR_PORT:-8010}}"
MASK_PORT="${NIMBUS_MASK_PORT:-${NIMBUS_OCI_REMOTE_MASK_PORT:-8020}}"

if [ -f "${BASE_ENV_FILE}" ] && [ "${ENV_FILE}" != "${BASE_ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${BASE_ENV_FILE}"
  set +a
fi

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

mkdir -p "${RUNTIME_DIR}"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export PATH="${PROJECT_ROOT}/.venv/bin:${PATH}"
export NIMBUS_MASK_SERVICE_URL="${NIMBUS_MASK_SERVICE_URL:-http://127.0.0.1:${MASK_PORT}}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv/bin/python}"
if [ ! -x "${PYTHON_BIN}" ]; then
  echo "ERROR: ${PYTHON_BIN} is missing." >&2
  exit 1
fi

stop_pidfile() {
  local pidfile="$1"
  if [ ! -f "${pidfile}" ]; then
    return 0
  fi
  local pid
  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" 2>/dev/null || true
  fi
  rm -f "${pidfile}"
}

service_running() {
  local pidfile="$1"
  if [ ! -f "${pidfile}" ]; then
    return 1
  fi
  local pid
  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1
}

start_service() {
  local name="$1"
  local pidfile="$2"
  local logfile="$3"
  shift 3
  if [ "${RESTART}" = "1" ]; then
    stop_pidfile "${pidfile}"
  fi
  if service_running "${pidfile}"; then
    echo "${name}: already running"
    return 0
  fi
  echo "${name}: starting"
  nohup "$@" >"${logfile}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pidfile}"
  sleep 1
  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    echo "ERROR: ${name} exited early. See ${logfile}" >&2
    tail -n 40 "${logfile}" >&2 || true
    exit 1
  fi
}

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
  local url="http://127.0.0.1:${API_PORT}/v1/worker/status"
  local deadline=$((SECONDS + WAIT_SECONDS))
  if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: curl/python3 not available, skipping worker readiness wait."
    return 0
  fi
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    local payload
    payload="$(curl -fsS "${url}" 2>/dev/null || true)"
    if [ -n "${payload}" ] && printf '%s' "${payload}" | python3 -c "import json, sys; data = json.load(sys.stdin); raise SystemExit(0 if int(data.get('workers_alive', 0) or 0) > 0 else 1)"; then
      echo "WORKER: ready"
      return 0
    fi
    sleep 2
  done
  echo "WARN: worker did not report any alive execution node within ${WAIT_SECONDS}s (${url})."
  return 0
}

start_service \
  "ZARR" \
  "${RUNTIME_DIR}/zarr.pid" \
  "${RUNTIME_DIR}/zarr.log" \
  "${PYTHON_BIN}" \
  -m \
  uvicorn \
  nimbuschain_zarr_service.main:app \
  --host \
  0.0.0.0 \
  --port \
  "${ZARR_PORT}"

start_service \
  "MASK" \
  "${RUNTIME_DIR}/mask.pid" \
  "${RUNTIME_DIR}/mask.log" \
  "${PYTHON_BIN}" \
  -m \
  uvicorn \
  nimbuschain_mask_service.main:app \
  --host \
  0.0.0.0 \
  --port \
  "${MASK_PORT}"

start_service \
  "API" \
  "${RUNTIME_DIR}/api.pid" \
  "${RUNTIME_DIR}/api.log" \
  "${PYTHON_BIN}" \
  -m \
  uvicorn \
  nimbuschain_fetch_service.main:app \
  --host \
  0.0.0.0 \
  --port \
  "${API_PORT}"

start_service \
  "WORKER" \
  "${RUNTIME_DIR}/worker.pid" \
  "${RUNTIME_DIR}/worker.log" \
  "${PYTHON_BIN}" \
  -m \
  nimbuschain_fetch.worker

wait_http "ZARR" "http://127.0.0.1:${ZARR_PORT}/readiness"
wait_http "MASK" "http://127.0.0.1:${MASK_PORT}/health"
wait_http "API" "http://127.0.0.1:${API_PORT}/v1/health"
wait_worker

echo
echo "OCI remote runtime started."
echo "API:  http://127.0.0.1:${API_PORT}"
echo "ZARR: http://127.0.0.1:${ZARR_PORT}"
echo "MASK: http://127.0.0.1:${MASK_PORT}"
echo "Logs: ${RUNTIME_DIR}"
