#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
PORT="${NIMBUS_HOST_MPS_MASK_PORT:-18021}"
PID_FILE="${NIMBUS_HOST_MPS_MASK_PID_FILE:-$ROOT_DIR/data/downloads/mask-cache/host-mps-mask.pid}"
LOG_FILE="${NIMBUS_HOST_MPS_MASK_LOG_FILE:-$ROOT_DIR/data/downloads/mask-cache/host-mps-mask.log}"
SCREEN_SESSION="${NIMBUS_HOST_MPS_MASK_SCREEN_SESSION:-nimbus-host-mps-mask}"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Python virtualenv not found at $PYTHON_BIN"
  echo "Run: python3 -m venv .venv && .venv/bin/python -m pip install -e '.[mask-service]'"
  exit 1
fi

mkdir -p "$ROOT_DIR/data/downloads/mask-cache"/{xdg,huggingface,torch,matplotlib,osmnx,omniwater-models,tmp}

export PYTHONPATH="${PYTHONPATH:-$ROOT_DIR/src}"
export NIMBUS_DATA_DIR="${NIMBUS_DATA_DIR:-$ROOT_DIR/data/downloads}"
export NIMBUS_HOST_DATA_DIR="${NIMBUS_HOST_DATA_DIR:-$ROOT_DIR/data/downloads}"
export NIMBUS_ZARRMASK_DIR="${NIMBUS_ZARRMASK_DIR:-$ROOT_DIR/data/downloads/zarrmask}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$ROOT_DIR/data/downloads/mask-cache/xdg}"
export HF_HOME="${HF_HOME:-$ROOT_DIR/data/downloads/mask-cache/huggingface}"
export TORCH_HOME="${TORCH_HOME:-$ROOT_DIR/data/downloads/mask-cache/torch}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$ROOT_DIR/data/downloads/mask-cache/matplotlib}"
export NIMBUS_WATERMASK_OSMNX_CACHE_DIR="${NIMBUS_WATERMASK_OSMNX_CACHE_DIR:-$ROOT_DIR/data/downloads/mask-cache/osmnx}"
export NIMBUS_WATERMASK_MODEL_DIR="${NIMBUS_WATERMASK_MODEL_DIR:-$ROOT_DIR/data/downloads/mask-cache/omniwater-models}"
export NIMBUS_WATERMASK_TMP_DIR="${NIMBUS_WATERMASK_TMP_DIR:-$ROOT_DIR/data/downloads/mask-cache/tmp}"

export NIMBUS_CLOUDMASK_DEVICE="${NIMBUS_CLOUDMASK_DEVICE:-mps}"
export NIMBUS_WATERMASK_DEVICE="${NIMBUS_WATERMASK_DEVICE:-mps}"
export NIMBUS_WATERMASK_BATCH_SIZE="${NIMBUS_WATERMASK_BATCH_SIZE:-1}"
export NIMBUS_CLOUDMASK_BATCH_SIZE="${NIMBUS_CLOUDMASK_BATCH_SIZE:-1}"
export NIMBUS_WATERMASK_TILE_WORKERS="${NIMBUS_WATERMASK_TILE_WORKERS:-1}"
export NIMBUS_CLOUDMASK_TILE_WORKERS="${NIMBUS_CLOUDMASK_TILE_WORKERS:-1}"
export NIMBUS_WATERMASK_TILE_SIZE="${NIMBUS_WATERMASK_TILE_SIZE:-512}"
export NIMBUS_CLOUDMASK_MODEL_TILE_SIZE="${NIMBUS_CLOUDMASK_MODEL_TILE_SIZE:-mask}"
export NIMBUS_WATERMASK_MODEL_TILE_SIZE="${NIMBUS_WATERMASK_MODEL_TILE_SIZE:-mask}"
export NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE="${NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE:-512}"
export NIMBUS_WATERMASK_INFERENCE_OVERLAP_SIZE="${NIMBUS_WATERMASK_INFERENCE_OVERLAP_SIZE:-128}"
export NIMBUS_WATERMASK_MPS_SAFE_MODE="${NIMBUS_WATERMASK_MPS_SAFE_MODE:-1}"
export NIMBUS_WATERMASK_MPS_FAIL_OPEN="${NIMBUS_WATERMASK_MPS_FAIL_OPEN:-0}"
export PYTORCH_ENABLE_MPS_FALLBACK="${PYTORCH_ENABLE_MPS_FALLBACK:-1}"

export NIMBUS_WATERMASK_OSMNX_USE_CACHE="${NIMBUS_WATERMASK_OSMNX_USE_CACHE:-true}"
export NIMBUS_WATERMASK_USE_OSM_WATER="${NIMBUS_WATERMASK_USE_OSM_WATER:-false}"
export NIMBUS_WATERMASK_USE_OSM_BUILDING="${NIMBUS_WATERMASK_USE_OSM_BUILDING:-false}"
export NIMBUS_WATERMASK_USE_OSM_ROADS="${NIMBUS_WATERMASK_USE_OSM_ROADS:-false}"
export NIMBUS_WATERMASK_OSM_MAX_SCENE_SPAN_METERS="${NIMBUS_WATERMASK_OSM_MAX_SCENE_SPAN_METERS:-50000}"

check_mps() {
  "$PYTHON_BIN" - <<'PY'
import sys
import torch

if not torch.backends.mps.is_available():
    print("MPS is not available to this process.", file=sys.stderr)
    print(f"mps_built={torch.backends.mps.is_built()}", file=sys.stderr)
    raise SystemExit(2)

x = torch.ones((8, 8), device="mps")
print(f"MPS OK: torch={torch.__version__}, device={x.device}")
PY
}

stop_existing() {
  if command -v screen >/dev/null 2>&1; then
    screen -S "$SCREEN_SESSION" -X quit >/dev/null 2>&1 || true
  fi
  if [ -f "$PID_FILE" ]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
      echo "Stopping existing host MPS mask service pid=$pid"
      kill "$pid"
      local deadline=$((SECONDS + 20))
      while [ "$SECONDS" -lt "$deadline" ] && kill -0 "$pid" >/dev/null 2>&1; do
        sleep 1
      done
    fi
    rm -f "$PID_FILE"
  fi
}

run_server() {
  check_mps
  echo "$$" > "$PID_FILE"
  exec "$PYTHON_BIN" -m uvicorn nimbuschain_mask_service.main:app \
    --host 0.0.0.0 \
    --port "$PORT"
}

case "${1:-}" in
  --check)
    check_mps
    ;;
  --stop)
    stop_existing
    ;;
  --daemon)
    check_mps
    stop_existing
    echo "Starting host MPS mask service on http://127.0.0.1:$PORT"
    if [ "$(uname -s)" = "Darwin" ] && command -v screen >/dev/null 2>&1; then
      screen -dmS "$SCREEN_SESSION" "$ROOT_DIR/scripts/run_host_mps_mask.sh" --foreground >"$LOG_FILE" 2>&1
      echo "screen=$SCREEN_SESSION"
    else
      nohup "$ROOT_DIR/scripts/run_host_mps_mask.sh" --foreground >"$LOG_FILE" 2>&1 &
      echo "$!" > "$PID_FILE"
      echo "pid=$(cat "$PID_FILE")"
    fi
    echo "log=$LOG_FILE"
    ;;
  --foreground)
    run_server >"$LOG_FILE" 2>&1
    ;;
  "")
    run_server
    ;;
  *)
    echo "Usage: $0 [--check|--daemon|--foreground|--stop]"
    exit 2
    ;;
esac
