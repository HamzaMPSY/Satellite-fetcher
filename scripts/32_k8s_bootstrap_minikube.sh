#!/usr/bin/env bash
set -euo pipefail

MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY_MB="${MINIKUBE_MEMORY_MB:-6144}"
MINIKUBE_K8S_VERSION="${MINIKUBE_K8S_VERSION:-v1.30.10}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PODMAN_DOCTOR="${PROJECT_ROOT}/scripts/09_podman_doctor.sh"

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERROR: minikube is not installed." >&2
  exit 1
fi

if [ ! -x "${PODMAN_DOCTOR}" ]; then
  echo "ERROR: ${PODMAN_DOCTOR} is missing or not executable." >&2
  exit 1
fi

WAIT_SECONDS="${WAIT_SECONDS:-45}" "${PODMAN_DOCTOR}" info >/dev/null

minikube start \
  --driver=podman \
  --container-runtime=containerd \
  --kubernetes-version="${MINIKUBE_K8S_VERSION}" \
  --cpus="${MINIKUBE_CPUS}" \
  --memory="${MINIKUBE_MEMORY_MB}"

echo "Minikube is ready."
