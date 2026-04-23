#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_IMAGE="${API_IMAGE:-ghcr.io/nimbuschain/nimbus-api:latest}"
UI_IMAGE="${UI_IMAGE:-ghcr.io/nimbuschain/nimbus-ui:latest}"
PODMAN_DOCTOR="${PROJECT_ROOT}/scripts/09_podman_doctor.sh"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

if ! command -v minikube >/dev/null 2>&1; then
  echo "ERROR: minikube is not installed." >&2
  exit 1
fi

cd "${PROJECT_ROOT}"

if ! minikube status >/dev/null 2>&1; then
  echo "ERROR: minikube cluster is not running." >&2
  echo "Start it first with ./scripts/32_k8s_bootstrap_minikube.sh" >&2
  exit 1
fi

if [ -x "${PODMAN_DOCTOR}" ] \
  && WAIT_SECONDS="${WAIT_SECONDS:-45}" "${PODMAN_DOCTOR}" image exists "${API_IMAGE}" \
  && WAIT_SECONDS="${WAIT_SECONDS:-45}" "${PODMAN_DOCTOR}" image exists "${UI_IMAGE}"; then
  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "${TMP_DIR}"' EXIT
  WAIT_SECONDS="${WAIT_SECONDS:-45}" "${PODMAN_DOCTOR}" save -o "${TMP_DIR}/nimbus-api.tar" "${API_IMAGE}"
  WAIT_SECONDS="${WAIT_SECONDS:-45}" "${PODMAN_DOCTOR}" save -o "${TMP_DIR}/nimbus-ui.tar" "${UI_IMAGE}"
  minikube image load "${TMP_DIR}/nimbus-api.tar"
  minikube image load "${TMP_DIR}/nimbus-ui.tar"
fi

kubectl apply -k k8s/overlays/minikube

kubectl -n nimbuschain rollout status deploy/nimbus-api --timeout=180s || true
kubectl -n nimbuschain rollout status deploy/nimbus-worker --timeout=180s || true
kubectl -n nimbuschain rollout status deploy/nimbus-ui --timeout=180s || true

echo "Kubernetes manifests applied."
