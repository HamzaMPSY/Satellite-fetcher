#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OVERLAY_DIR="${PROJECT_ROOT}/k8s-minikube"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

if ! kubectl config current-context >/dev/null 2>&1; then
  echo "ERROR: kubectl has no current context (start minikube first)." >&2
  exit 1
fi

kubectl apply -k "${OVERLAY_DIR}"

echo "Applied Kubernetes minikube overlay from ${OVERLAY_DIR}"
echo "Next: kubectl -n nimbuschain get pods"
