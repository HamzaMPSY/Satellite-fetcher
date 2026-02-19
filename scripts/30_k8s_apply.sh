#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_DIR="${PROJECT_ROOT}/k8s"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

if ! kubectl config current-context >/dev/null 2>&1; then
  echo "ERROR: kubectl has no current context." >&2
  echo "Hint: create/select a cluster first (e.g. minikube)." >&2
  echo "  brew install minikube" >&2
  echo "  MINIKUBE_MEMORY_MB=6144 ./scripts/32_k8s_bootstrap_minikube.sh" >&2
  exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "ERROR: kubectl context is set but cluster is unreachable." >&2
  echo "Check: kubectl config get-contexts" >&2
  exit 1
fi

kubectl apply -k "${K8S_DIR}"

echo "Applied Kubernetes manifests from ${K8S_DIR}"
echo "Next:"
echo "  kubectl -n nimbuschain get pods"
echo "  kubectl -n nimbuschain get svc"
