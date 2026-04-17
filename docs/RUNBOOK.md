# Runbook

## 1. Prerequisites

Required:
- Python 3.11
- Podman + `podman-compose` or Podman Compose plugin

Optional:
- Docker-compatible compose
- Minikube + `kubectl`

## 2. First local setup

```bash
cd /path/to/Satellite-fetcher
cp .env.example .env
```

Do not commit `.env`.

At minimum, configure provider credentials before testing downloads.

### Optional: Copernicus account-pool test mode

The default Copernicus path stays on the primary account.

To test the experimental multi-account downloader alongside the stable path:

1. Keep the primary account in:
   - `NIMBUS_COPERNICUS_USERNAME`
   - `NIMBUS_COPERNICUS_PASSWORD`
2. Add extra accounts in either:
   - `NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE`
   - or `NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON`
3. Set the per-account concurrency target with:
   - `NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY=4`

Example file format:

```json
{
  "accounts": [
    {
      "label": "secondary-1",
      "username": "copernicus-user-2@example.com",
      "password": "replace-me"
    },
    {
      "label": "secondary-2",
      "username": "copernicus-user-3@example.com",
      "password": "replace-me"
    }
  ]
}
```

An example file is available at:
- `docs/copernicus_account_pool.example.json`

In the UI, choose:
- `Download execution -> Account pool test`

The backend will then:
- keep the stable single-account mode untouched unless you explicitly select the test mode
- estimate how many accounts are needed from the number of products found
- distribute downloads across the available accounts

## 3. Start locally with Podman

```bash
./scripts/10_up_stack.sh
```

The script:
- starts the Podman machine on macOS when available
- launches the compose stack
- waits briefly for API, UI and Zarr health endpoints

## 3.1 GPU modes

### Linux + NVIDIA GPU in the `nimbus-mask` container

Use Docker Compose with the GPU override:

```bash
./scripts/10_up_stack_gpu.sh
```

This uses:
- `docker-compose.yml`
- `docker-compose.gpu.yml`

Expected runtime:
- `NIMBUS_CLOUDMASK_DEVICE=cuda`
- `NIMBUS_WATERMASK_DEVICE=cuda`

### macOS + Apple Silicon / Metal (`mps`)

`mps` is not available inside the Linux container runtime, so `nimbus-mask` must run on the host.

Start the stack in external-mask mode:

```bash
./scripts/10_up_stack_external_mask.sh
```

Then start the host-native mask service:

```bash
./scripts/12_up_mask_service_native.sh
```

In that mode:
- the containers use `NIMBUS_MASK_SERVICE_URL=http://host.containers.internal:8020`
- `nimbus-mask` does not run as a container
- the host service defaults to:
  - `NIMBUS_CLOUDMASK_DEVICE=mps`
  - `NIMBUS_WATERMASK_DEVICE=mps`

Important:
- a Linux container on macOS cannot see Metal/MPS directly
- if you keep `nimbus-mask` inside the container on macOS, it will run on CPU

## 4. Local URLs

- UI: [http://127.0.0.1:8501](http://127.0.0.1:8501)
- API docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- API health: [http://127.0.0.1:8000/v1/health](http://127.0.0.1:8000/v1/health)
- API readiness: [http://127.0.0.1:8000/v1/readiness](http://127.0.0.1:8000/v1/readiness)
- Worker status: [http://127.0.0.1:8000/v1/worker/status](http://127.0.0.1:8000/v1/worker/status)
- Zarr health: [http://127.0.0.1:8010/health](http://127.0.0.1:8010/health)
- Zarr readiness: [http://127.0.0.1:8010/readiness](http://127.0.0.1:8010/readiness)
- Backend mask health: [http://127.0.0.1:8000/v1/mask/health](http://127.0.0.1:8000/v1/mask/health)
- Backend mask schema: [http://127.0.0.1:8000/v1/mask/schema](http://127.0.0.1:8000/v1/mask/schema)

## 5. Stop locally

```bash
./scripts/11_down_stack.sh
```

## 6. Inspect logs

```bash
podman logs -f backendnimbus_nimbus-api_1
podman logs -f backendnimbus_nimbus-worker_1
podman logs -f backendnimbus_nimbus-ui_1
podman logs -f backendnimbus_nimbus-zarr_1
podman logs -f backendnimbus_nimbus-mask_1
```

## 7. Smoke checks

### API

```bash
curl -s http://127.0.0.1:8000/v1/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/readiness | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/worker/status | python3 -m json.tool
```

### Converter runtime (via backend public API)

```bash
curl -s http://127.0.0.1:8000/v1/converter/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/converter/readiness | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/converter/schema | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/mask/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/v1/mask/schema | python3 -m json.tool
```

### UI

Open the UI and verify:
- map loads
- provider selector works
- status blocks show API, readiness, worker and Zarr state
- recent jobs panel renders without browser refresh loops

## 8. Troubleshooting

### Podman is not reachable on macOS

```bash
podman machine start
podman system connection list
```

### UI shows no jobs or stale jobs

Check:
- API health/readiness
- worker status
- worker logs
- browser hard refresh

### UI Zarr tab does not show raw sources

Check:
- files really exist under `data/downloads/`
- `manifest.json` exists for downloaded jobs when applicable
- `./data:/data` volume is mounted in the UI container

### Zarr conversion fails

Check:
- raw source path exists in `/data/downloads`
- collection and product type match the source
- Zarr `/readiness` endpoint is `ready`
- converter logs for missing bands or unsupported inputs

### Native mask service does not start on macOS

Check:
- `.venv/bin/uvicorn` exists in the repo root
- `.env` is present if you rely on it for credentials or overrides
- port `8020` is free, or let `./scripts/12_up_mask_service_native.sh` remove the conflicting containerized mask service
- `python -c "import torch; print(bool(getattr(getattr(torch,'backends',None),'mps',None) and torch.backends.mps.is_available()))"` reports `True` if you expect `mps`

### Cloud masking is still CPU-only

Check the current mode first:

```bash
podman exec backendnimbus_nimbus-mask_1 python -c "import torch; print(torch.cuda.is_available()); print(bool(getattr(getattr(torch,'backends',None),'mps',None) and torch.backends.mps.is_available()))"
```

Interpretation:
- `False / False` in the container means no accelerator is exposed to `nimbus-mask`
- on macOS, switch to the external host-native mask service if you want `mps`
- on Linux/NVIDIA, use `./scripts/10_up_stack_gpu.sh`

### Port conflict

```bash
lsof -nP -iTCP:8501 -sTCP:LISTEN
lsof -nP -iTCP:8000 -sTCP:LISTEN
lsof -nP -iTCP:8010 -sTCP:LISTEN
```

## 9. Kubernetes / Minikube

```bash
./scripts/32_k8s_bootstrap_minikube.sh
./scripts/33_k8s_apply_minikube.sh
./scripts/35_k8s_expose_local.sh
```

Stop local exposure:

```bash
./scripts/36_k8s_unexpose_local.sh
```

## 10. Onboarding checklist for a new developer

1. Read `README.md`
2. Read `docs/ARCHITECTURE.md`
3. Launch the Podman stack
4. Verify all health and readiness endpoints
5. Open the UI and submit one small job
6. Inspect one finished job through the API and UI
7. Review `docs/ZARR.md` before touching conversion logic
