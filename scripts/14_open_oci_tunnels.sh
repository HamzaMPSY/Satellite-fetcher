#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.oci.local-ui}"
WAIT_SECONDS="${WAIT_SECONDS:-20}"
RUNTIME_DIR="${PROJECT_ROOT}/.runtime/oci-tunnels"

if [ ! -f "${ENV_FILE}" ]; then
  ENV_FILE="${PROJECT_ROOT}/.env"
fi

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

REMOTE_HOST="${NIMBUS_OCI_REMOTE_HOST:-}"
REMOTE_USER="${NIMBUS_OCI_REMOTE_USER:-opc}"
REMOTE_SSH_PORT="${NIMBUS_OCI_REMOTE_PORT:-22}"
SSH_KEY_FILE="${NIMBUS_OCI_SSH_KEY_FILE:-}"
SSH_PROXY_JUMP="${NIMBUS_OCI_SSH_PROXY_JUMP:-}"
SSH_PROXY_COMMAND="${NIMBUS_OCI_SSH_PROXY_COMMAND:-}"
LOCAL_API_PORT="${NIMBUS_OCI_LOCAL_API_PORT:-18000}"
LOCAL_ZARR_PORT="${NIMBUS_OCI_LOCAL_ZARR_PORT:-18010}"
LOCAL_MASK_PORT="${NIMBUS_OCI_LOCAL_MASK_PORT:-18020}"
REMOTE_API_PORT="${NIMBUS_OCI_REMOTE_API_PORT:-8000}"
REMOTE_ZARR_PORT="${NIMBUS_OCI_REMOTE_ZARR_PORT:-8010}"
REMOTE_MASK_PORT="${NIMBUS_OCI_REMOTE_MASK_PORT:-8020}"
TUNNEL_ZARR="${NIMBUS_OCI_TUNNEL_ZARR:-0}"
TUNNEL_MASK="${NIMBUS_OCI_TUNNEL_MASK:-0}"

if [ -z "${REMOTE_HOST}" ]; then
  echo "ERROR: NIMBUS_OCI_REMOTE_HOST is required." >&2
  exit 1
fi

mkdir -p "${RUNTIME_DIR}"

SSH_ARGS=(
  -o BatchMode=yes
  -o ExitOnForwardFailure=yes
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=3
  -p "${REMOTE_SSH_PORT}"
)

if [ -n "${SSH_KEY_FILE}" ]; then
  SSH_ARGS+=(-i "${SSH_KEY_FILE/#\~/${HOME}}")
fi
if [ -n "${SSH_PROXY_JUMP}" ]; then
  SSH_ARGS+=(-J "${SSH_PROXY_JUMP}")
fi
if [ -n "${SSH_PROXY_COMMAND}" ]; then
  SSH_ARGS+=(-o "ProxyCommand=${SSH_PROXY_COMMAND}")
fi

service_running() {
  local pidfile="$1"
  if [ ! -f "${pidfile}" ]; then
    return 1
  fi
  local pid
  pid="$(cat "${pidfile}" 2>/dev/null || true)"
  [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1
}

wait_local_tcp() {
  local label="$1"
  local port="$2"
  local deadline=$((SECONDS + WAIT_SECONDS))
  if ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: python3 not available, skipping local tunnel check for ${label}."
    return 0
  fi
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if python3 - "${port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket()
sock.settimeout(1.0)
try:
    sock.connect(("127.0.0.1", port))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
raise SystemExit(0)
PY
    then
      echo "${label}: tunnel ready on 127.0.0.1:${port}"
      return 0
    fi
    sleep 1
  done
  echo "WARN: ${label} tunnel did not open within ${WAIT_SECONDS}s (127.0.0.1:${port})."
  return 0
}

start_tunnel() {
  local label="$1"
  local local_port="$2"
  local remote_port="$3"
  local pidfile="${RUNTIME_DIR}/${label}.pid"
  local logfile="${RUNTIME_DIR}/${label}.log"

  if service_running "${pidfile}"; then
    echo "${label}: already running"
    return 0
  fi

  echo "${label}: opening 127.0.0.1:${local_port} -> ${REMOTE_HOST}:127.0.0.1:${remote_port}"
  nohup ssh \
    "${SSH_ARGS[@]}" \
    -L "${local_port}:127.0.0.1:${remote_port}" \
    "${REMOTE_USER}@${REMOTE_HOST}" \
    -N >"${logfile}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pidfile}"
  sleep 1
  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    echo "ERROR: ${label} tunnel exited early. See ${logfile}" >&2
    tail -n 40 "${logfile}" >&2 || true
    exit 1
  fi
}

start_tunnel "api" "${LOCAL_API_PORT}" "${REMOTE_API_PORT}"
wait_local_tcp "API" "${LOCAL_API_PORT}"

if [ "${TUNNEL_ZARR}" = "1" ]; then
  start_tunnel "zarr" "${LOCAL_ZARR_PORT}" "${REMOTE_ZARR_PORT}"
  wait_local_tcp "ZARR" "${LOCAL_ZARR_PORT}"
fi

if [ "${TUNNEL_MASK}" = "1" ]; then
  start_tunnel "mask" "${LOCAL_MASK_PORT}" "${REMOTE_MASK_PORT}"
  wait_local_tcp "MASK" "${LOCAL_MASK_PORT}"
fi

echo
echo "OCI tunnels ready."
echo "API:  http://127.0.0.1:${LOCAL_API_PORT}"
if [ "${TUNNEL_ZARR}" = "1" ]; then
  echo "ZARR: http://127.0.0.1:${LOCAL_ZARR_PORT}"
fi
if [ "${TUNNEL_MASK}" = "1" ]; then
  echo "MASK: http://127.0.0.1:${LOCAL_MASK_PORT}"
fi
echo "Logs: ${RUNTIME_DIR}"
