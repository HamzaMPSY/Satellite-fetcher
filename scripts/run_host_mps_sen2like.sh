#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
PORT="${NIMBUS_HOST_MPS_SEN2LIKE_PORT:-18031}"
PID_FILE="${NIMBUS_HOST_MPS_SEN2LIKE_PID_FILE:-$ROOT_DIR/data/downloads/sen2like-cache/host-mps-sen2like.pid}"
LOG_FILE="${NIMBUS_HOST_MPS_SEN2LIKE_LOG_FILE:-$ROOT_DIR/data/downloads/sen2like-cache/host-mps-sen2like.log}"
SCREEN_SESSION="${NIMBUS_HOST_MPS_SEN2LIKE_SCREEN_SESSION:-nimbus-host-mps-sen2like}"
VENDOR_DIR="$ROOT_DIR/src/nimbuschain_sen2like_service/vendor/Satellite-fetcher-feature-sen2like_reimplementation"
SIXS_BIN_DIR="${NIMBUS_HOST_MPS_SEN2LIKE_SIXS_BIN_DIR:-/private/tmp/nimbus-sen2like-bin}"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Python virtualenv not found at $PYTHON_BIN"
  echo "Run: python3 -m venv .venv && .venv/bin/python -m pip install -e '.[dev,ui,mask-service,sen2like-service]'"
  exit 1
fi

mkdir -p "$ROOT_DIR/data/downloads/sen2like-cache" "$ROOT_DIR/data/downloads/sen2like"
mkdir -p "$SIXS_BIN_DIR"

if SIXS_PATH="$("$PYTHON_BIN" -m sixs_bin --path 1.1 2>/dev/null)"; then
  ln -sf "$SIXS_PATH" "$SIXS_BIN_DIR/sixsV1.1"
  ln -sf "$SIXS_PATH" "$SIXS_BIN_DIR/sixs"
fi

if [ -d /opt/homebrew/opt/openjdk@17 ]; then
  export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
fi

export PATH="$SIXS_BIN_DIR:/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export PYTHONPATH="${PYTHONPATH:-$ROOT_DIR/src:$VENDOR_DIR}"
export NIMBUS_DATA_DIR="${NIMBUS_DATA_DIR:-$ROOT_DIR/data/downloads}"
export NIMBUS_HOST_DATA_DIR="${NIMBUS_HOST_DATA_DIR:-$ROOT_DIR/data/downloads}"
export NIMBUS_SEN2LIKE_WORK_DIR="${NIMBUS_SEN2LIKE_WORK_DIR:-$ROOT_DIR/data/downloads/sen2like}"
export NIMBUS_SEN2LIKE_VENDOR_DIR="${NIMBUS_SEN2LIKE_VENDOR_DIR:-$VENDOR_DIR}"
export SEN2LIKE_VENDOR_DIR="${SEN2LIKE_VENDOR_DIR:-$VENDOR_DIR}"
export LANDSAT_UPSAMPLING_BASE="${LANDSAT_UPSAMPLING_BASE:-$VENDOR_DIR}"
export NIMBUS_SEN2LIKE_RUNTIME_DEVICE="${NIMBUS_SEN2LIKE_RUNTIME_DEVICE:-mps}"

export NIMBUS_SEN2LIKE_WORKERS="${NIMBUS_SEN2LIKE_WORKERS:-2}"
export NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM="${NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM:-true}"
export NIMBUS_SEN2LIKE_BAND_WORKERS="${NIMBUS_SEN2LIKE_BAND_WORKERS:-2}"
export NIMBUS_SEN2LIKE_BATCH_PRODUCTS="${NIMBUS_SEN2LIKE_BATCH_PRODUCTS:-false}"
export NIMBUS_SEN2LIKE_PRODUCT_PARALLEL_REQUESTS="${NIMBUS_SEN2LIKE_PRODUCT_PARALLEL_REQUESTS:-true}"
export NIMBUS_SEN2LIKE_PREPROCESS_WORKERS="${NIMBUS_SEN2LIKE_PREPROCESS_WORKERS:-1}"
export NIMBUS_SEN2LIKE_SAFE_RETRY="${NIMBUS_SEN2LIKE_SAFE_RETRY:-true}"
export NIMBUS_SEN2LIKE_PREPROCESS_TARGET_SHAPE="${NIMBUS_SEN2LIKE_PREPROCESS_TARGET_SHAPE:-native}"
export NIMBUS_SEN2LIKE_DIRECT_ZARR="${NIMBUS_SEN2LIKE_DIRECT_ZARR:-false}"
export NIMBUS_SEN2LIKE_ZARR_DIR="${NIMBUS_SEN2LIKE_ZARR_DIR:-$ROOT_DIR/data/downloads/zarr}"
export NIMBUS_SEN2LIKE_SPARK_DIR="${NIMBUS_SEN2LIKE_SPARK_DIR:-/private/tmp/nimbus-sen2like-spark}"
export NIMBUS_SEN2LIKE_TIMEOUT_SECONDS="${NIMBUS_SEN2LIKE_TIMEOUT_SECONDS:-3600}"
export NIMBUS_SEN2LIKE_SPARK_DRIVER_MEMORY="${NIMBUS_SEN2LIKE_SPARK_DRIVER_MEMORY:-1g}"
export NIMBUS_SEN2LIKE_SPARK_EXECUTOR_MEMORY="${NIMBUS_SEN2LIKE_SPARK_EXECUTOR_MEMORY:-1g}"
export NIMBUS_SEN2LIKE_SPARK_PYTHON_WORKER_MEMORY="${NIMBUS_SEN2LIKE_SPARK_PYTHON_WORKER_MEMORY:-256m}"
export GDAL_CACHEMAX="${GDAL_CACHEMAX:-256}"
export GDAL_NUM_THREADS="${GDAL_NUM_THREADS:-1}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xmx1536m -XX:MaxRAMPercentage=55}"
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

check_runtime() {
  "$PYTHON_BIN" - <<'PY'
import json
import shutil
import subprocess
import sys

missing = []
for module in (
    "cv2",
    "dask.array",
    "mgrs",
    "osgeo",
    "planetary_computer",
    "pysolar",
    "pyspark",
    "pystac_client",
    "rioxarray",
    "scipy",
    "sixs_bin",
    "skimage",
):
    try:
        __import__(module)
    except Exception as exc:
        missing.append(f"{module}: {exc}")

java_path = shutil.which("java")
if java_path is None:
    missing.append("java executable is not on PATH")
else:
    try:
        completed = subprocess.run(
            [java_path, "-version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:
        missing.append(f"java executable failed: {exc}")
    else:
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip().splitlines()
            missing.append(f"java runtime is not usable: {detail[0] if detail else completed.returncode}")

from nimbuschain_sen2like_service.runner import readiness_payload

payload = readiness_payload()
if not payload.get("sixs_executable_exists"):
    missing.append("sixs executable is not available")

print(json.dumps(payload, indent=2))
if missing:
    print("Sen2Like host runtime is not ready:", file=sys.stderr)
    for item in missing:
        print(f"- {item}", file=sys.stderr)
    raise SystemExit(2)
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
      echo "Stopping existing host MPS Sen2Like service pid=$pid"
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
  check_runtime
  echo "$$" > "$PID_FILE"
  exec "$PYTHON_BIN" -m uvicorn nimbuschain_sen2like_service.main:app \
    --host 0.0.0.0 \
    --port "$PORT"
}

case "${1:-}" in
  --check)
    check_runtime
    ;;
  --stop)
    stop_existing
    ;;
  --daemon)
    check_runtime
    stop_existing
    echo "Starting host MPS Sen2Like service on http://127.0.0.1:$PORT"
    if [ "$(uname -s)" = "Darwin" ] && command -v screen >/dev/null 2>&1; then
      screen -dmS "$SCREEN_SESSION" "$ROOT_DIR/scripts/run_host_mps_sen2like.sh" --foreground >"$LOG_FILE" 2>&1
      echo "screen=$SCREEN_SESSION"
    else
      nohup "$ROOT_DIR/scripts/run_host_mps_sen2like.sh" --foreground >"$LOG_FILE" 2>&1 &
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
