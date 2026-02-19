# Kubernetes Deployment (Fetcher + Service + UI)

This guide deploys NimbusChain in Kubernetes with separated runtime roles:

- `nimbus-api`: FastAPI control plane
- `nimbus-worker`: asynchronous download execution
- `nimbus-ui`: Streamlit UI
- `nimbus-mongodb`: metadata/job store

Manifests live in:
- `/Users/mehdidinari/Desktop/backend nimbus/k8s`

## 1) Prerequisites

- `kubectl` configured on your cluster
- dynamic storage provisioner enabled
- metrics-server enabled (for HPA)
- container registry reachable from cluster nodes

For local macOS + Podman:

```bash
brew install minikube
cd "/Users/mehdidinari/Desktop/backend nimbus"
MINIKUBE_MEMORY_MB=6144 ./scripts/32_k8s_bootstrap_minikube.sh
```

The bootstrap script uses `--cni=bridge` by default to avoid `kindnet` image pull issues in restricted TLS environments.

## 2) Build and push images

Worker reuses the same image as API.

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"

podman build -f Containerfile -t ghcr.io/nimbuschain/nimbus-api:latest .
podman build -f ui/Containerfile -t ghcr.io/nimbuschain/nimbus-ui:latest .

podman push ghcr.io/nimbuschain/nimbus-api:latest
podman push ghcr.io/nimbuschain/nimbus-ui:latest
```

If you use another registry/tag, update `k8s/kustomization.yaml`.

For local Minikube (without pushing to registry), load local Podman images:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/34_k8s_load_images_minikube.sh
```

## 3) Configure secrets

Edit:
- `/Users/mehdidinari/Desktop/backend nimbus/k8s/secret.yaml`

Set at least:
- `NIMBUS_COPERNICUS_USERNAME`
- `NIMBUS_COPERNICUS_PASSWORD`
- `NIMBUS_USGS_USERNAME`
- `NIMBUS_USGS_TOKEN`

Optional:
- `NIMBUS_API_KEY`

## 4) Apply manifests

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
kubectl apply -k k8s
```

For local Minikube use:

```bash
cd "/Users/mehdidinari/Desktop/backend nimbus"
./scripts/34_k8s_load_images_minikube.sh
./scripts/33_k8s_apply_minikube.sh
```

This overlay adjusts storage/concurrency for single-node local clusters.

Check rollout:

```bash
kubectl -n nimbuschain get pods
kubectl -n nimbuschain rollout status deploy/nimbus-api
kubectl -n nimbuschain rollout status deploy/nimbus-worker
kubectl -n nimbuschain rollout status deploy/nimbus-ui
kubectl -n nimbuschain rollout status statefulset/nimbus-mongodb
```

## 5) Access API and UI

For local checks:

```bash
kubectl -n nimbuschain port-forward svc/nimbus-api 8000:8000
kubectl -n nimbuschain port-forward svc/nimbus-ui 8501:8501
```

Then open:
- API docs: `http://127.0.0.1:8000/docs`
- UI: `http://127.0.0.1:8501`

Health/metrics:

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/metrics | head -n 30
```

## 6) Scale

Manual scale:

```bash
kubectl -n nimbuschain scale deploy/nimbus-worker --replicas=4
kubectl -n nimbuschain scale deploy/nimbus-api --replicas=3
```

HPA status:

```bash
kubectl -n nimbuschain get hpa
```

## 7) Storage note

`k8s/downloads-pvc.yaml` uses `ReadWriteMany` for multi-worker shared downloads.
If your storage class does not support RWX:

1. change access mode to `ReadWriteOnce`
2. keep worker replicas at `1`
3. or replace shared filesystem with object storage in future iterations

## 8) Logs and troubleshooting

```bash
kubectl -n nimbuschain logs -f deploy/nimbus-api
kubectl -n nimbuschain logs -f deploy/nimbus-worker
kubectl -n nimbuschain logs -f deploy/nimbus-ui
kubectl -n nimbuschain logs -f statefulset/nimbus-mongodb
```

If API is healthy but metrics is 404:
- check `NIMBUS_ENABLE_METRICS` in `k8s/configmap.yaml` is `"true"`
- restart API deployment

```bash
kubectl -n nimbuschain rollout restart deploy/nimbus-api
```

If all pods stay `Pending` and `kubectl get nodes` shows `NotReady` with `NetworkPluginNotReady`:

```bash
minikube delete --all --purge
MINIKUBE_MEMORY_MB=6144 ./scripts/32_k8s_bootstrap_minikube.sh
./scripts/34_k8s_load_images_minikube.sh
./scripts/33_k8s_apply_minikube.sh
```

## 9) Remove stack

```bash
kubectl delete -k /Users/mehdidinari/Desktop/backend\ nimbus/k8s
```
