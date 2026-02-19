#!/usr/bin/env bash
set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is required." >&2
  exit 1
fi

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERROR: minikube is not installed." >&2
  echo "Install with: brew install minikube" >&2
  exit 1
fi

podman machine start >/dev/null 2>&1 || true

MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY_MB="${MINIKUBE_MEMORY_MB:-6144}"
MINIKUBE_K8S_VERSION="${MINIKUBE_K8S_VERSION:-v1.30.10}"
MINIKUBE_CONTAINER_RUNTIME="${MINIKUBE_CONTAINER_RUNTIME:-containerd}"
MINIKUBE_CNI="${MINIKUBE_CNI:-bridge}"

if minikube status >/dev/null 2>&1; then
  echo "Minikube is already running."
else
  echo "Starting minikube with podman driver (cpus=${MINIKUBE_CPUS}, memory=${MINIKUBE_MEMORY_MB}MB, k8s=${MINIKUBE_K8S_VERSION}, cni=${MINIKUBE_CNI})..."
  minikube start \
    --driver=podman \
    --container-runtime="${MINIKUBE_CONTAINER_RUNTIME}" \
    --kubernetes-version="${MINIKUBE_K8S_VERSION}" \
    --cni="${MINIKUBE_CNI}" \
    --cpus="${MINIKUBE_CPUS}" \
    --memory="${MINIKUBE_MEMORY_MB}"
fi

kubectl config use-context minikube >/dev/null 2>&1 || true
kubectl cluster-info

NODE_READY="$(kubectl get node minikube -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
if [[ "${NODE_READY}" != "True" ]]; then
  echo "WARNING: minikube node is not Ready yet. Check: kubectl describe node minikube" >&2
fi

echo "Kubernetes local cluster is ready."
