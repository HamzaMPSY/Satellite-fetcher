#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-.vmtest-venv}"

cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required but was not found on PATH."
  exit 1
fi

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required but was not found on PATH."
  exit 1
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
  echo "Created virtual environment at $VENV_DIR"
else
  echo "Reusing virtual environment at $VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip
python -m pip install -e ".[dev,ui,mask-service]"

"$ROOT_DIR/scripts/run_all.sh" --launch-mode "${NIMBUS_PIPELINE_LAUNCH_MODE:-mps}" --build

echo
echo "Running containers:"
podman ps
echo
echo "Health endpoints:"
echo "  API:  http://127.0.0.1:8000/v1/health"
echo "  UI:   http://127.0.0.1:8501"
echo "  Zarr: http://127.0.0.1:8010/readiness"
echo "  Mask: http://127.0.0.1:8000/v1/mask/health"
