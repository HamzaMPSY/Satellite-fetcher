#!/usr/bin/env bash
set -euo pipefail

WAIT_SECONDS="${WAIT_SECONDS:-45}"
MACHINE_NAME="${PODMAN_MACHINE_NAME:-podman-machine-default}"
SCRIPT_NAME="$(basename "$0")"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi

has_connection() {
  local name="$1"
  podman system connection list --format '{{.Name}}' 2>/dev/null | grep -Fxq "${name}"
}

use_connection() {
  local name="$1"
  if has_connection "${name}"; then
    podman system connection default "${name}" >/dev/null 2>&1 || true
  fi
}

try_ready() {
  podman info >/dev/null 2>&1
}

start_machine() {
  if command -v podman machine >/dev/null 2>&1; then
    podman machine start >/dev/null 2>&1 || true
  fi
}

wait_until_ready() {
  local deadline=$((SECONDS + WAIT_SECONDS))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if try_ready; then
      return 0
    fi
    sleep 2
  done
  return 1
}

print_diagnostics() {
  echo "ERROR: podman is still unavailable after ${WAIT_SECONDS}s." >&2
  echo >&2
  echo "podman machine list:" >&2
  podman machine list >&2 || true
  echo >&2
  echo "podman system connection list:" >&2
  podman system connection list >&2 || true
  local machine_log
  machine_log="$(find /var/folders -path '*/T/podman/podman-machine-default.log' -print -quit 2>/dev/null || true)"
  if [ -n "${machine_log}" ] && [ -f "${machine_log}" ]; then
    echo >&2
    echo "tail -n 40 ${machine_log}:" >&2
    tail -n 40 "${machine_log}" >&2 || true
  fi
}

ensure_podman_ready() {
  if try_ready; then
    return 0
  fi

  use_connection "${MACHINE_NAME}"
  start_machine
  if wait_until_ready; then
    return 0
  fi

  use_connection "podman-local"
  start_machine
  if wait_until_ready; then
    return 0
  fi

  use_connection "${MACHINE_NAME}"
  print_diagnostics
  return 1
}

ensure_podman_ready

if [ "$#" -gt 0 ]; then
  exec podman "$@"
fi

echo "podman is ready."
podman system connection list
