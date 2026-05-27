# OMK/OKE Runbook

This runbook covers the Kubernetes handoff path for Oracle-managed
environments. The repo keeps the platform overlay at `k8s/overlays/omk/`.

## Build and publish images

Publish these images with the same tag before applying manifests:

- `ghcr.io/nimbuschain/nimbus-api`
- `ghcr.io/nimbuschain/nimbus-ui`
- `ghcr.io/nimbuschain/nimbus-zarr`
- `ghcr.io/nimbuschain/nimbus-mask`
- `ghcr.io/nimbuschain/nimbus-sen2like`

Update image names or tags through Kustomize overlays when the platform registry
differs from the default.

## Validate manifests

```bash
kubectl kustomize k8s/base
kubectl kustomize k8s/overlays/omk
```

## Deploy

```bash
kubectl apply -k k8s/overlays/omk
```

## Smoke checks

```bash
kubectl -n nimbuschain get pods
kubectl -n nimbuschain get svc
kubectl -n nimbuschain logs deploy/nimbus-api
kubectl -n nimbuschain logs deploy/nimbus-worker
kubectl -n nimbuschain logs deploy/nimbus-zarr
kubectl -n nimbuschain logs deploy/nimbus-mask
kubectl -n nimbuschain logs deploy/nimbus-sen2like
```

Expected internal services:

- `nimbus-zarr:8010`
- `nimbus-mask:8020`
- `nimbus-sen2like:8030`

The API and worker read those URLs from `nimbus-config`.
