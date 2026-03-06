#!/usr/bin/env bash
set -euo pipefail

MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY_MB="${MINIKUBE_MEMORY_MB:-6144}"
MINIKUBE_K8S_VERSION="${MINIKUBE_K8S_VERSION:-v1.30.10}"

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERROR: minikube is not installed." >&2
  exit 1
fi

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not installed." >&2
  exit 1
fi

podman machine start >/dev/null 2>&1 || true

minikube start \
  --driver=podman \
  --container-runtime=containerd \
  --kubernetes-version="${MINIKUBE_K8S_VERSION}" \
  --cpus="${MINIKUBE_CPUS}" \
  --memory="${MINIKUBE_MEMORY_MB}"

echo "Minikube is ready."

