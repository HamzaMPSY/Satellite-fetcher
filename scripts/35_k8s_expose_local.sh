#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="${PROJECT_ROOT}/.k8s-port-forward"
mkdir -p "${RUNTIME_DIR}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

if ! kubectl config current-context >/dev/null 2>&1; then
  echo "ERROR: kubectl has no current context." >&2
  exit 1
fi

start_forward() {
  local name="$1"
  local local_port="$2"
  local remote_port="$3"
  local pid_file="${RUNTIME_DIR}/${name}.pid"
  local log_file="${RUNTIME_DIR}/${name}.log"

  if [[ -f "${pid_file}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pid_file}")"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
      echo "${name} already exposed on http://127.0.0.1:${local_port}"
      return
    fi
    rm -f "${pid_file}"
  fi

  nohup kubectl -n nimbuschain port-forward "svc/${name}" "${local_port}:${remote_port}" \
    >"${log_file}" 2>&1 &
  local new_pid=$!
  echo "${new_pid}" >"${pid_file}"

  sleep 2
  if ! kill -0 "${new_pid}" >/dev/null 2>&1; then
    echo "ERROR: failed to expose ${name}. See ${log_file}" >&2
    cat "${log_file}" >&2 || true
    rm -f "${pid_file}"
    exit 1
  fi

  echo "${name} exposed on http://127.0.0.1:${local_port}"
}

start_forward "nimbus-api" 8000 8000
start_forward "nimbus-ui" 8501 8501

echo
echo "UI:  http://127.0.0.1:8501"
echo "API: http://127.0.0.1:8000"
echo "Stop with: ./scripts/36_k8s_unexpose_local.sh"
