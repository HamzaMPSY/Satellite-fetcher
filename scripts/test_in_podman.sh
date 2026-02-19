#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE="${PODMAN_PYTHON_IMAGE:-docker.io/library/python:3.11}"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed or not in PATH." >&2
  exit 1
fi

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <pytest-args...>" >&2
  echo "Example: $0 tests/test_models.py" >&2
  exit 1
fi

echo "Running tests in Podman image: ${IMAGE}"
echo "Project root: ${PROJECT_ROOT}"
echo "Pytest args: $*"

PYTEST_ARGS=""
for arg in "$@"; do
  PYTEST_ARGS+=" $(printf "%q" "${arg}")"
done

podman run --rm \
  -v "${PROJECT_ROOT}:/app:Z" \
  -w /app \
  "${IMAGE}" \
  bash -lc "pip install -e .[dev] >/tmp/pip.log && pytest -q${PYTEST_ARGS}"
