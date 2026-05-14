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
    --help|-h)
      echo "Usage: scripts/run_all.sh [--build|--no-build] [--recreate] [--logs] [service...]"
      exit 0
      ;;
    *)
      SERVICES+=("$1")
      ;;
  esac
  shift
done

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
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

echo "Validating compose configuration..."
"$PODMAN_BIN" compose config --quiet

if [ "$BUILD" -eq 1 ]; then
  echo "Building NimbusChain services..."
  if [ "${#SERVICES[@]}" -gt 0 ]; then
    "$PODMAN_BIN" compose build "${SERVICES[@]}"
  else
    for service in "${DEFAULT_BUILD_SERVICES[@]}"; do
      if [ "$service" = "mongodb" ]; then
        "$PODMAN_BIN" pull mongo:7
      else
        "$PODMAN_BIN" compose build "$service"
      fi
    done
  fi
fi

UP_ARGS=(up -d)
if [ "$FORCE_RECREATE" -eq 1 ]; then
  UP_ARGS+=(--force-recreate)
fi

echo "Starting NimbusChain stack..."
if [ "${#SERVICES[@]}" -gt 0 ]; then
  "$PODMAN_BIN" compose "${UP_ARGS[@]}" "${SERVICES[@]}"
else
  "$PODMAN_BIN" compose "${UP_ARGS[@]}"
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
check_url "Mask" "http://127.0.0.1:8020/health" || true
check_url "Sen2Like" "http://127.0.0.1:8030/health" || true

echo
echo "UI: http://127.0.0.1:8501"

if [ "$FOLLOW_LOGS" -eq 1 ]; then
  if [ "${#SERVICES[@]}" -gt 0 ]; then
    "$PODMAN_BIN" compose logs -f "${SERVICES[@]}"
  else
    "$PODMAN_BIN" compose logs -f
  fi
fi
