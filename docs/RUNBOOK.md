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
cd "/Users/mehdidinari/Desktop/backend nimbus"
cp .env.example .env
```

Do not commit `.env`.

At minimum, configure provider credentials before testing downloads.

## 3. Start locally with Podman

```bash
./scripts/10_up_stack.sh
```

The script:
- starts the Podman machine on macOS when available
- launches the compose stack
- waits briefly for API, UI and Zarr health endpoints

## 4. Local URLs

- UI: [http://127.0.0.1:8501](http://127.0.0.1:8501)
- API docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- API health: [http://127.0.0.1:8000/v1/health](http://127.0.0.1:8000/v1/health)
- API readiness: [http://127.0.0.1:8000/v1/readiness](http://127.0.0.1:8000/v1/readiness)
- Worker status: [http://127.0.0.1:8000/v1/worker/status](http://127.0.0.1:8000/v1/worker/status)
- Zarr health: [http://127.0.0.1:8010/health](http://127.0.0.1:8010/health)
- Zarr readiness: [http://127.0.0.1:8010/readiness](http://127.0.0.1:8010/readiness)

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

1. Read `/Users/mehdidinari/Desktop/backend nimbus/README.md`
2. Read `/Users/mehdidinari/Desktop/backend nimbus/docs/ARCHITECTURE.md`
3. Launch the Podman stack
4. Verify all health and readiness endpoints
5. Open the UI and submit one small job
6. Inspect one finished job through the API and UI
7. Review `/Users/mehdidinari/Desktop/backend nimbus/docs/ZARR.md` before touching conversion logic
