#!/usr/bin/env bash
set -euo pipefail

API_IMAGE="${API_IMAGE:-ghcr.io/nimbuschain/nimbus-api:latest}"
UI_IMAGE="${UI_IMAGE:-ghcr.io/nimbuschain/nimbus-ui:latest}"

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi
if ! command -v minikube >/dev/null 2>&1; then
  echo "ERROR: minikube is not installed." >&2
  exit 1
fi

if ! minikube status >/dev/null 2>&1; then
  echo "ERROR: minikube cluster is not running." >&2
  echo "Start first: ./scripts/32_k8s_bootstrap_minikube.sh" >&2
  exit 1
fi

if ! podman image exists "${API_IMAGE}"; then
  echo "ERROR: local image not found: ${API_IMAGE}" >&2
  echo "Build first: podman build -f Containerfile -t ${API_IMAGE} ." >&2
  exit 1
fi
if ! podman image exists "${UI_IMAGE}"; then
  echo "ERROR: local image not found: ${UI_IMAGE}" >&2
  echo "Build first: podman build -f ui/Containerfile -t ${UI_IMAGE} ." >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Exporting images to tar archives..."
podman save -o "${TMP_DIR}/nimbus-api.tar" "${API_IMAGE}"
podman save -o "${TMP_DIR}/nimbus-ui.tar" "${UI_IMAGE}"

echo "Loading images into minikube..."
minikube image load "${TMP_DIR}/nimbus-api.tar"
minikube image load "${TMP_DIR}/nimbus-ui.tar"

echo "Images loaded into minikube:"
minikube image ls | grep -E "nimbus-api|nimbus-ui" || true
echo "Done."
