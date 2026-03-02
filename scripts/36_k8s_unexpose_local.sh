#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="${PROJECT_ROOT}/.k8s-port-forward"

stop_forward() {
  local name="$1"
  local pid_file="${RUNTIME_DIR}/${name}.pid"

  if [[ ! -f "${pid_file}" ]]; then
    echo "${name} was not exposed."
    return
  fi

  local pid
  pid="$(cat "${pid_file}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${pid_file}"
  echo "${name} tunnel stopped."
}

stop_forward "nimbus-ui"
stop_forward "nimbus-api"
