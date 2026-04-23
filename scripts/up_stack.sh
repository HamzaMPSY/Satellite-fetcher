#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${MODE:-local}"
ENV_FILE_OVERRIDE=""
NO_TUNNELS=0
NO_UI=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/up_stack.sh --mode <local|external-mask|gpu-container|oci-full> [options]

Options:
  --mode <value>       Launch mode. Default: local
  --env-file <path>    Optional env file override.
  --no-tunnels         OCI mode only. Skip opening local SSH tunnels.
  --no-ui              OCI mode only. Skip launching the host Streamlit UI.
  --help               Show this help message.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE_OVERRIDE="${2:-}"
      shift 2
      ;;
    --no-tunnels)
      NO_TUNNELS=1
      shift
      ;;
    --no-ui)
      NO_UI=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -n "${ENV_FILE_OVERRIDE}" ]; then
  export ENV_FILE="${ENV_FILE_OVERRIDE}"
fi

case "${MODE}" in
  local)
    exec "${PROJECT_ROOT}/scripts/10_up_stack.sh"
    ;;
  external-mask)
    exec "${PROJECT_ROOT}/scripts/10_up_stack_external_mask.sh"
    ;;
  gpu-container)
    exec "${PROJECT_ROOT}/scripts/10_up_stack_gpu.sh"
    ;;
  oci-full)
    if [ -z "${ENV_FILE_OVERRIDE}" ] && [ -f "${PROJECT_ROOT}/.env.oci.local-ui" ]; then
      export ENV_FILE="${PROJECT_ROOT}/.env.oci.local-ui"
    fi
    if [ "${NO_TUNNELS}" != "1" ]; then
      "${PROJECT_ROOT}/scripts/14_open_oci_tunnels.sh"
    fi
    if [ "${NO_UI}" != "1" ]; then
      "${PROJECT_ROOT}/scripts/15_up_ui_host.sh"
    fi
    echo
    echo "OCI full mode is ready."
    echo "If the remote runtime is not running yet, open a Managed SSH session and execute:"
    echo "  ./scripts/13_up_stack_oci_remote.sh"
    ;;
  *)
    echo "ERROR: unsupported mode '${MODE}'." >&2
    usage >&2
    exit 1
    ;;
esac
