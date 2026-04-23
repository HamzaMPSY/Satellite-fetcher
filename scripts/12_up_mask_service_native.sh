#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
MASK_PORT="${NIMBUS_MASK_PORT:-8020}"
PODMAN_DOCTOR="${PROJECT_ROOT}/scripts/09_podman_doctor.sh"

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

if [ ! -x "${PROJECT_ROOT}/.venv/bin/uvicorn" ]; then
  echo "ERROR: ${PROJECT_ROOT}/.venv/bin/uvicorn is missing." >&2
  exit 1
fi

cleanup_containerized_mask_port() {
  if [ ! -x "${PODMAN_DOCTOR}" ]; then
    return 0
  fi
  if ! WAIT_SECONDS="${PODMAN_WAIT_SECONDS:-8}" "${PODMAN_DOCTOR}" info >/dev/null 2>&1; then
    return 0
  fi
  local names=(
    "backendnimbus-nimbus-mask-1"
    "backendnimbus_nimbus_mask_1"
    "backendnimbus_nimbus-mask_1"
  )
  local name
  local ports
  for name in "${names[@]}"; do
    ports="$(WAIT_SECONDS="${PODMAN_WAIT_SECONDS:-8}" "${PODMAN_DOCTOR}" ps -a --format '{{.Names}}|{{.Ports}}' 2>/dev/null | awk -F'|' -v target="${name}" '$1 == target { print $2; exit }')"
    if [ -n "${ports}" ] && printf '%s' "${ports}" | grep -q "${MASK_PORT}->${MASK_PORT}/tcp"; then
      echo "Removing containerized nimbus-mask that is still publishing port ${MASK_PORT}..."
      WAIT_SECONDS="${PODMAN_WAIT_SECONDS:-8}" "${PODMAN_DOCTOR}" rm -f "${name}" >/dev/null 2>&1 || true
    fi
  done
}

export PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export NIMBUS_RUNTIME_ROLE="${NIMBUS_RUNTIME_ROLE:-api}"
export NIMBUS_HOST_DATA_DIR="${NIMBUS_HOST_DATA_DIR:-${PROJECT_ROOT}/data/downloads}"

case "$(uname -s)" in
  Darwin)
    export NIMBUS_CLOUDMASK_DEVICE="${NIMBUS_CLOUDMASK_DEVICE:-mps}"
    export NIMBUS_WATERMASK_DEVICE="${NIMBUS_WATERMASK_DEVICE:-mps}"
    ;;
  *)
    export NIMBUS_CLOUDMASK_DEVICE="${NIMBUS_CLOUDMASK_DEVICE:-cpu}"
    export NIMBUS_WATERMASK_DEVICE="${NIMBUS_WATERMASK_DEVICE:-cpu}"
    ;;
esac

SKIP_DEVICE_PROBE="${NIMBUS_SKIP_DEVICE_PROBE:-0}"
if [ "$(uname -s)" = "Darwin" ] && [ "${SKIP_DEVICE_PROBE}" != "1" ] && [ "${NIMBUS_CLOUDMASK_DEVICE}" = "mps" -o "${NIMBUS_WATERMASK_DEVICE}" = "mps" ]; then
  echo "Checking PyTorch MPS availability in ${PROJECT_ROOT}/.venv ..."
  echo "The first torch import can take several seconds on macOS. Do not interrupt unless it is clearly stuck."
  MPS_STATUS="$("${PROJECT_ROOT}/.venv/bin/python" - <<'PY'
import json
try:
    import torch
    status = bool(getattr(getattr(torch, "backends", None), "mps", None) and torch.backends.mps.is_available())
except Exception:
    status = False
print(json.dumps({"mps_available": status}))
PY
)"
  if ! printf '%s' "${MPS_STATUS}" | grep -q '"mps_available": true'; then
    echo "WARN: PyTorch MPS is not available in this .venv. The mask service will fall back to CPU." >&2
    echo "WARN: Check 'python -c \"import torch; print(torch.backends.mps.is_available())\"' before expecting Apple GPU acceleration." >&2
    if [ "${NIMBUS_REQUIRE_ACCELERATOR:-0}" = "1" ]; then
      echo "ERROR: NIMBUS_REQUIRE_ACCELERATOR=1 but MPS is unavailable." >&2
      exit 1
    fi
  fi
elif [ "$(uname -s)" = "Darwin" ] && [ "${SKIP_DEVICE_PROBE}" = "1" ] && [ "${NIMBUS_CLOUDMASK_DEVICE}" = "mps" -o "${NIMBUS_WATERMASK_DEVICE}" = "mps" ]; then
  echo "WARN: Skipping the MPS preflight probe because NIMBUS_SKIP_DEVICE_PROBE=1."
  echo "WARN: The runtime may still fall back to CPU if PyTorch cannot use MPS."
fi

cleanup_containerized_mask_port

exec "${PROJECT_ROOT}/.venv/bin/uvicorn" \
  nimbuschain_mask_service.main:app \
  --host 0.0.0.0 \
  --port "${MASK_PORT}"
