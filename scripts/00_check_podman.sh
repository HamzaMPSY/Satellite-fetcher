#!/usr/bin/env bash
set -euo pipefail

echo "Checking Podman..."
podman --version

echo "Starting Podman machine (safe if already running)..."
podman machine start || true

echo "Podman machine list:"
podman machine list

