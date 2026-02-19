#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_DIR="${PROJECT_ROOT}/k8s"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

kubectl delete -k "${K8S_DIR}"

echo "Deleted Kubernetes manifests from ${K8S_DIR}"

