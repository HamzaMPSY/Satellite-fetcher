# Master Diagram

NimbusChain Fetch exposes one public control plane and delegates heavy work to
internal services.

```text
Browser
  -> nimbus-ui
       -> nimbus-api
            -> MongoDB
            -> nimbus-worker
                 -> provider APIs
                 -> nimbus-sen2like
                 -> nimbus-zarr
                 -> nimbus-mask
                 -> data/downloads
```

Runtime boundaries:

- `nimbus-api` is the only public API surface.
- `nimbus-worker` claims queued jobs through the shared job store.
- `nimbus-sen2like` normalizes Landsat scenes before conversion.
- `nimbus-zarr` writes normalized scene Zarrs and grouped cubes.
- `nimbus-mask` writes cloud and water layers into existing Zarrs.

Deployment layout:

- local compose: `deploy/compose/`
- Kubernetes base: `k8s/base/`
- local Kubernetes overlay: `k8s/overlays/minikube/`
- platform overlay: `k8s/overlays/omk/`
