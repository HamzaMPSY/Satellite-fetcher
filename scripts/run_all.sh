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

if "$PODMAN_BIN" machine inspect >/dev/null 2>&1; then
  "$PODMAN_BIN" machine start >/dev/null 2>&1 || true
fi

echo "Validating compose configuration..."
"$PODMAN_BIN" compose config --quiet

if [ "$BUILD" -eq 1 ]; then
  echo "Building NimbusChain services..."
  if [ "${#SERVICES[@]}" -gt 0 ]; then
    "$PODMAN_BIN" compose build "${SERVICES[@]}"
  else
    "$PODMAN_BIN" compose build
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
