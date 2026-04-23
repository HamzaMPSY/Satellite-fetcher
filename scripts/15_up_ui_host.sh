#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.oci.local-ui}"
WAIT_SECONDS="${WAIT_SECONDS:-45}"
RUNTIME_DIR="${PROJECT_ROOT}/.runtime/oci-ui"

if [ ! -f "${ENV_FILE}" ]; then
  ENV_FILE="${PROJECT_ROOT}/.env"
fi

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

STREAMLIT_BIN="${PROJECT_ROOT}/.venv/bin/streamlit"
if [ ! -x "${STREAMLIT_BIN}" ]; then
  echo "ERROR: ${STREAMLIT_BIN} is missing." >&2
  exit 1
fi

mkdir -p "${RUNTIME_DIR}"
PID_FILE="${RUNTIME_DIR}/ui.pid"
LOG_FILE="${RUNTIME_DIR}/ui.log"
UI_PORT="${NIMBUS_UI_PORT:-8501}"
export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export NIMBUS_SERVICE_URL="${NIMBUS_SERVICE_URL:-http://127.0.0.1:${NIMBUS_OCI_LOCAL_API_PORT:-18000}}"
export STREAMLIT_BROWSER_GATHER_USAGE_STATS="false"

if [ -f "${PID_FILE}" ]; then
  PID="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [ -n "${PID:-}" ] && kill -0 "${PID}" >/dev/null 2>&1; then
    echo "UI(host): already running on http://127.0.0.1:${UI_PORT}"
    exit 0
  fi
  rm -f "${PID_FILE}"
fi

nohup "${STREAMLIT_BIN}" run \
  "${PROJECT_ROOT}/src/nimbuschain_fetch_ui/app.py" \
  --server.address 0.0.0.0 \
  --server.port "${UI_PORT}" >"${LOG_FILE}" 2>&1 &
PID=$!
echo "${PID}" >"${PID_FILE}"

if ! command -v curl >/dev/null 2>&1; then
  echo "UI(host) started. Open http://127.0.0.1:${UI_PORT}"
  exit 0
fi

DEADLINE=$((SECONDS + WAIT_SECONDS))
while [ "${SECONDS}" -lt "${DEADLINE}" ]; do
  if curl -fsS "http://127.0.0.1:${UI_PORT}" >/dev/null 2>&1; then
    echo "UI(host): ready"
    echo "URL: http://127.0.0.1:${UI_PORT}"
    echo "API target: ${NIMBUS_SERVICE_URL}"
    echo "Logs: ${LOG_FILE}"
    exit 0
  fi
  sleep 2
done

echo "WARN: UI(host) did not become ready within ${WAIT_SECONDS}s. Check ${LOG_FILE}" >&2
exit 0
