#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="${PROJECT_ROOT}/.runtime"
API_PID_FILE="${RUNTIME_DIR}/k8s_api_port_forward.pid"
UI_PID_FILE="${RUNTIME_DIR}/k8s_ui_port_forward.pid"
mkdir -p "${RUNTIME_DIR}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

stop_pid() {
  local pid_file="$1"
  if [ -f "${pid_file}" ]; then
    local pid
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${pid_file}"
  fi
}

stop_pid "${API_PID_FILE}"
stop_pid "${UI_PID_FILE}"

nohup kubectl -n nimbuschain port-forward svc/nimbus-api 8000:8000 >"${RUNTIME_DIR}/k8s_api_port_forward.log" 2>&1 &
echo $! > "${API_PID_FILE}"

nohup kubectl -n nimbuschain port-forward svc/nimbus-ui 8501:8501 >"${RUNTIME_DIR}/k8s_ui_port_forward.log" 2>&1 &
echo $! > "${UI_PID_FILE}"

echo "API: http://127.0.0.1:8000"
echo "UI:  http://127.0.0.1:8501"

