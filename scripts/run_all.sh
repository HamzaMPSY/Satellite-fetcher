#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PODMAN_BIN="${PODMAN_BIN:-}"
if [ -z "$PODMAN_BIN" ]; then
  if command -v podman >/dev/null 2>&1; then
    PODMAN_BIN="$(command -v podman)"
  elif [ -x /opt/homebrew/bin/podman ]; then
    PODMAN_BIN="/opt/homebrew/bin/podman"
  else
    echo "podman is required but was not found on PATH."
    exit 1
  fi
fi

BUILD=1
FORCE_RECREATE=0
FOLLOW_LOGS=0
LAUNCH_MODE="${NIMBUS_PIPELINE_LAUNCH_MODE:-mps}"
SERVICES=()
DEFAULT_BUILD_SERVICES=(
  mongodb
  nimbus-zarr
  nimbus-sen2like
  nimbus-mask
  nimbus-api
  nimbus-worker
  nimbus-ui
)

while [ "$#" -gt 0 ]; do
  case "$1" in
    --no-build)
      BUILD=0
      ;;
    --build)
      BUILD=1
      ;;
    --recreate|--force-recreate)
      FORCE_RECREATE=1
      ;;
    --logs)
      FOLLOW_LOGS=1
      ;;
    --launch-mode|--pipeline-mode|--profile)
      if [ "$#" -lt 2 ]; then
        echo "$1 requires a value: mps or oci"
        exit 2
      fi
      LAUNCH_MODE="$2"
      shift
      ;;
    --mps)
      LAUNCH_MODE="mps"
      ;;
    --oci)
      LAUNCH_MODE="oci"
      ;;
    --help|-h)
      echo "Usage: scripts/run_all.sh [--launch-mode mps|oci] [--build|--no-build] [--recreate] [--logs] [service...]"
      exit 0
      ;;
    *)
      SERVICES+=("$1")
      ;;
  esac
  shift
done

case "$(printf '%s' "$LAUNCH_MODE" | tr '[:upper:]' '[:lower:]')" in
  mps|local-mps|local_mps|ui)
    LAUNCH_MODE="mps"
    ;;
  oci|cloud|vm)
    LAUNCH_MODE="oci"
    ;;
  *)
    echo "Invalid launch mode '$LAUNCH_MODE'. Expected: mps or oci."
    exit 2
    ;;
esac

COMPOSE_PROJECT_NAME_VALUE="${COMPOSE_PROJECT_NAME:-${NIMBUS_COMPOSE_PROJECT_NAME:-backendnimbus}}"
COMPOSE_ARGS=(--project-name "$COMPOSE_PROJECT_NAME_VALUE" -f "$ROOT_DIR/deploy/compose/compose.yml")
if [ "$LAUNCH_MODE" = "mps" ]; then
  COMPOSE_ARGS+=(-f "$ROOT_DIR/deploy/compose/compose.mask-external.yml")
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

apply_launch_mode() {
  export NIMBUS_PIPELINE_LAUNCH_MODE="$LAUNCH_MODE"

  if [ "$LAUNCH_MODE" = "mps" ]; then
    local host_mps_port="${NIMBUS_HOST_MPS_MASK_PORT:-18021}"
    export NIMBUS_HOST_MPS_MASK_PORT="$host_mps_port"
    export NIMBUS_HOST_MPS_MASK_URL="${NIMBUS_HOST_MPS_MASK_URL:-http://host.containers.internal:$host_mps_port}"
    export NIMBUS_MASK_SERVICE_URL="$NIMBUS_HOST_MPS_MASK_URL"

    # Conservative local profile: host MPS does masking, while Sen2Like keeps
    # bounded parallelism to avoid killing the Podman VM on full Landsat scenes.
    export NIMBUS_SEN2LIKE_WORKERS="${NIMBUS_SEN2LIKE_WORKERS:-2}"
    export NIMBUS_SEN2LIKE_BAND_WORKERS="${NIMBUS_SEN2LIKE_BAND_WORKERS:-2}"
    export NIMBUS_SEN2LIKE_PREPROCESS_WORKERS="${NIMBUS_SEN2LIKE_PREPROCESS_WORKERS:-1}"
    export NIMBUS_SEN2LIKE_RAW_FALLBACK="${NIMBUS_SEN2LIKE_RAW_FALLBACK:-false}"
    export NIMBUS_SEN2LIKE_SAFE_RETRY="${NIMBUS_SEN2LIKE_SAFE_RETRY:-true}"

    if [ "$(uname -s)" != "Darwin" ]; then
      echo "Launch mode mps is for the local macOS/UI pipeline. Use --launch-mode oci on Linux/cloud."
      exit 2
    fi

    "$ROOT_DIR/scripts/run_host_mps_mask.sh" --daemon
  else
    export NIMBUS_MASK_SERVICE_URL="${NIMBUS_MASK_SERVICE_URL:-http://nimbus-mask:8020}"
    export NIMBUS_ZARR_SERVICE_URL="${NIMBUS_ZARR_SERVICE_URL:-http://nimbus-zarr:8010}"
    export NIMBUS_SEN2LIKE_SERVICE_URL="${NIMBUS_SEN2LIKE_SERVICE_URL:-http://nimbus-sen2like:8030}"
  fi
}

apply_launch_mode

if [ "$LAUNCH_MODE" = "mps" ] && [ "${#SERVICES[@]}" -gt 0 ]; then
  for service in "${SERVICES[@]}"; do
    if [ "$service" = "nimbus-mask" ]; then
      echo "nimbus-mask is not part of the mps launch profile. Use the host MPS mask service or switch to --launch-mode oci."
      exit 2
    fi
  done
fi

ensure_podman_machine_running() {
  local state=""
  state="$("$PODMAN_BIN" machine inspect --format '{{.State}}' 2>/dev/null || true)"
  if [ "$state" = "running" ]; then
    return 0
  fi

  if [ "$(uname -s)" = "Darwin" ] && command -v screen >/dev/null 2>&1; then
    screen -S nimbus-podman -X quit >/dev/null 2>&1 || true
    screen -dmS nimbus-podman /bin/sh -lc "'$PODMAN_BIN' machine start podman-machine-default > /tmp/nimbus-podman-screen.log 2>&1; while true; do sleep 3600; done"
  else
    "$PODMAN_BIN" machine start >/dev/null 2>&1 || true
  fi

  local deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$PODMAN_BIN" ps >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done

  echo "Podman machine did not become ready within 90 seconds."
  if [ -f /tmp/nimbus-podman-screen.log ]; then
    tail -n 40 /tmp/nimbus-podman-screen.log || true
  fi
  return 1
}

if "$PODMAN_BIN" machine inspect >/dev/null 2>&1; then
  PODMAN_MACHINE_STATE="$("$PODMAN_BIN" machine inspect --format '{{.State}}' 2>/dev/null || true)"
  if [ "$PODMAN_MACHINE_STATE" != "running" ]; then
    ensure_podman_machine_running
  fi
  PODMAN_MEMORY_MB="$("$PODMAN_BIN" machine inspect 2>/dev/null | python3 -c 'import json,sys; data=json.load(sys.stdin); print(int((data[0].get("Resources") or {}).get("Memory") or 0))' 2>/dev/null || true)"
  if [ -n "${PODMAN_MEMORY_MB:-}" ] && [ "$PODMAN_MEMORY_MB" -gt 0 ] && [ "$PODMAN_MEMORY_MB" -lt 16384 ]; then
    echo "Warning: Podman machine memory is ${PODMAN_MEMORY_MB} MB. Sen2Like Landsat normalization can be killed under 16 GB."
    echo "         Recommended before Landsat tests: podman machine stop && podman machine set --memory 16384 && podman machine start"
  fi
fi

echo "Launch mode: $LAUNCH_MODE"
echo "Mask service URL: ${NIMBUS_MASK_SERVICE_URL:-}"

echo "Validating compose configuration..."
"$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" config --quiet

if [ "$BUILD" -eq 1 ]; then
  echo "Building NimbusChain services..."
  if [ "${#SERVICES[@]}" -gt 0 ]; then
    "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" build "${SERVICES[@]}"
  else
    for service in "${DEFAULT_BUILD_SERVICES[@]}"; do
      if [ "$LAUNCH_MODE" = "mps" ] && [ "$service" = "nimbus-mask" ]; then
        continue
      fi
      if [ "$service" = "mongodb" ]; then
        if "$PODMAN_BIN" image exists mongo:7 || "$PODMAN_BIN" image exists docker.io/library/mongo:7; then
          echo "Using existing mongo:7 image."
        else
          "$PODMAN_BIN" pull mongo:7
        fi
      else
        "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" build "$service"
      fi
    done
  fi
fi

UP_ARGS=(up -d --no-build)
if [ "$FORCE_RECREATE" -eq 1 ]; then
  UP_ARGS+=(--force-recreate)
fi
if [ "$LAUNCH_MODE" = "mps" ]; then
  UP_ARGS+=(--scale nimbus-mask=0)
fi

echo "Starting NimbusChain stack..."
if [ "${#SERVICES[@]}" -gt 0 ]; then
  "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" "${UP_ARGS[@]}" "${SERVICES[@]}"
else
  "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" "${UP_ARGS[@]}"
fi

echo
echo "Running containers:"
"$PODMAN_BIN" ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

check_url() {
  local label="$1"
  local url="$2"
  local deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if command -v curl >/dev/null 2>&1; then
      if curl -fsS "$url" >/dev/null 2>&1; then
        echo "  OK  $label  $url"
        return 0
      fi
    else
      if python3 - "$url" >/dev/null 2>&1 <<'PY'
import sys
import urllib.request

urllib.request.urlopen(sys.argv[1], timeout=5).read()
PY
      then
        echo "  OK  $label  $url"
        return 0
      fi
    fi
    sleep 3
  done
  echo "  WAIT $label  $url"
  return 1
}

echo
echo "Health endpoints:"
check_url "API" "http://127.0.0.1:8000/v1/health" || true
check_url "UI" "http://127.0.0.1:8501" || true
check_url "Zarr" "http://127.0.0.1:8010/readiness" || true
if [ "$LAUNCH_MODE" = "mps" ]; then
  check_url "Host MPS Mask" "http://127.0.0.1:${NIMBUS_HOST_MPS_MASK_PORT:-18021}/health" || true
  check_url "API Mask Proxy" "http://127.0.0.1:8000/v1/mask/health" || true
else
  check_url "Mask" "http://127.0.0.1:8020/health" || true
fi
check_url "Sen2Like" "http://127.0.0.1:8030/health" || true

echo
echo "UI: http://127.0.0.1:8501"

if [ "$FOLLOW_LOGS" -eq 1 ]; then
  if [ "${#SERVICES[@]}" -gt 0 ]; then
    "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" logs -f "${SERVICES[@]}"
  else
    "$PODMAN_BIN" compose "${COMPOSE_ARGS[@]}" logs -f
  fi
fi
