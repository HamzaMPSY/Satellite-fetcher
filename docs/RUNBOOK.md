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

### Sen2Like memory profile for Landsat

Landsat jobs require Sen2Like to produce real Sentinel-like outputs before Zarr
conversion. The default local profile is fail-closed:

```bash
NIMBUS_SEN2LIKE_WORKERS=4
NIMBUS_SEN2LIKE_TIMEOUT_SECONDS=3600
NIMBUS_SEN2LIKE_RAW_FALLBACK=false
NIMBUS_SEN2LIKE_MEM_LIMIT=24g
NIMBUS_SEN2LIKE_SHM_SIZE=8g
NIMBUS_SEN2LIKE_GRI_AUTO_FETCH=true
NIMBUS_SEN2LIKE_ALLOW_GEO_SKIP=false
NIMBUS_SEN2LIKE_GDAL_CACHEMAX=256
NIMBUS_SEN2LIKE_GDAL_NUM_THREADS=1
NIMBUS_SEN2LIKE_JAVA_TOOL_OPTIONS=-Xmx1536m -XX:MaxRAMPercentage=55
NIMBUS_SEN2LIKE_MALLOC_ARENA_MAX=2
NIMBUS_SEN2LIKE_OMP_NUM_THREADS=1
NIMBUS_SEN2LIKE_OPENBLAS_NUM_THREADS=1
NIMBUS_SEN2LIKE_MKL_NUM_THREADS=1
```

On macOS with Podman, give the VM enough memory before starting the stack:

```bash
podman machine stop
podman machine set --memory 32768
podman machine set --cpus 8
podman machine start
```

After restart, verify `podman machine inspect` reports `Memory: 32768` and
`CPUs: 8`, and `podman inspect nimbus-sen2like` shows about 24 GB memory limit
plus 8 GB shared memory. Sen2Like also mounts `nimbus_sen2like_lut_cache` at
`/app/sen2like/lut` so generated 6S LUTs survive container recreation. If
Sen2Like is killed or returns no normalized output, the Landsat job should fail
at `sen2like_failed` instead of continuing with raw `.tar` inputs.

`NIMBUS_SEN2LIKE_GRI_AUTO_FETCH=true` makes Sen2Like build a missing
`GRI_<tile>.tif` on demand before geometric co-registration. Geometry is
fail-closed by default: if the GRI cannot be found or built, the job fails
instead of producing a Landsat-grid Sen2Like output. `NIMBUS_SEN2LIKE_ALLOW_GEO_SKIP=true`
is a debug-only escape hatch for intentionally reproducing the old skip behavior.

### Water mask cache profile

OmniWaterMask uses OSMnx for OpenStreetMap priors. OSMnx defaults to a relative
`./cache` directory, which is not writable in the `/app` workdir used by the
Podman container. Keep the mask caches on the shared data volume:

```bash
NIMBUS_WATERMASK_OSMNX_CACHE_DIR=/data/downloads/mask-cache/osmnx
NIMBUS_WATERMASK_OSMNX_USE_CACHE=true
NIMBUS_WATERMASK_USE_OSM_WATER=true
NIMBUS_WATERMASK_USE_OSM_BUILDING=true
NIMBUS_WATERMASK_USE_OSM_ROADS=true
NIMBUS_WATERMASK_MODEL_DIR=/data/downloads/mask-cache/omniwater-models
NIMBUS_MASK_XDG_CACHE_HOME=/data/downloads/mask-cache/xdg
NIMBUS_MASK_HF_HOME=/data/downloads/mask-cache/huggingface
NIMBUS_MASK_TORCH_HOME=/data/downloads/mask-cache/torch
NIMBUS_MASK_MPLCONFIGDIR=/data/downloads/mask-cache/matplotlib
NIMBUS_WATERMASK_BATCH_SIZE=2
NIMBUS_WATERMASK_TILE_SIZE=2048
NIMBUS_WATERMASK_MODEL_PROGRESS_SECONDS=5
```

If mask logs contain `Permission denied: 'cache'`, the running container is still
using OSMnx's default relative cache. Rebuild/recreate `nimbus-mask` after
applying the env above; do not restart it while an active water-mask job is
running unless you are willing to interrupt that request.

For full Sentinel-like scenes on CPU, keep `NIMBUS_WATERMASK_TILE_SIZE=2048` in
the local stack. The library default remains 512 px, but 512 px creates 484
OmniWater model calls for a 10980 x 10980 scene; 2048 px reduces that to 36
larger calls without changing the water-mask backend.

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

## 3. Start locally

```bash
cd /path/to/Satellite-fetcher
cp .env.example .env
```

After that, start the API/worker/UI/runtime services with the container or host-process commands appropriate for your environment. The old helper startup scripts have been removed and will be replaced later.

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

Stop the services using the same container or host-process commands you used to start them.

## 6. Inspect logs

Inspect logs using the runtime you started the services with. For containers, discover the actual container names and follow the relevant service logs. For host-native services, check the terminal session or process manager that launched them.

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

## 7.1 VM shell commands

For a VM deployment where raw inputs already live in OCI Object Storage, use the shell-first commands below.

Start each service directly:

```bash
nimbuschain-api-service
nimbuschain-worker
nimbuschain-zarr-service
nimbuschain-mask-service
```

Inspect or download OCI inputs:

```bash
nimbuschain-oci ls oci://<bucket>@<namespace>/raw/
nimbuschain-oci stat oci://<bucket>@<namespace>/raw/<scene>.SAFE.zip
nimbuschain-oci cp oci://<bucket>@<namespace>/raw/<scene>.SAFE.zip /data/downloads/raw
nimbuschain-oci cp oci://<bucket>@<namespace>/raw/<scene>.SAFE /data/downloads/raw --recursive
```

Run the stages one by one:

```bash
nimbuschain-zarr-convert /data/downloads/raw/<scene>.SAFE.zip \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --output-uri /data/downloads/zarr/<scene>.zarr

nimbuschain-mask-apply /data/downloads/zarr/<scene>.zarr --mask-types water,cloud

nimbuschain-zarr-cube /data/downloads/zarr/*.zarr \
  --group-by-tile \
  --output-dir /data/downloads/zarr/cubes/manual_vm
```

Or run the end-to-end VM test path in one command:

```bash
nimbuschain-vm-pipeline \
  oci://<bucket>@<namespace>/raw/<scene-a>.SAFE.zip \
  oci://<bucket>@<namespace>/raw/<scene-b>.SAFE.zip \
  --provider copernicus \
  --collection SENTINEL-2 \
  --product-type S2MSI2A \
  --mask-types water,cloud \
  --cube-mode after_mask \
  --group-by-tile
```

Important:
- `nimbuschain-mask-apply` currently expects a local Zarr path on the VM, so download or convert locally before masking.
- `nimbuschain-zarr-convert` can read local paths and `oci://` raw URIs directly.
- `nimbuschain-vm-pipeline` stages `oci://` raw inputs into the VM before conversion.

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
- port `8020` is free
- `python -c "import torch; print(bool(getattr(getattr(torch,'backends',None),'mps',None) and torch.backends.mps.is_available()))"` reports `True` if you expect `mps`

### Cloud masking is still CPU-only

Check the current mode first:

```bash
podman exec backendnimbus-nimbus-mask-1 python -c "import torch; print(torch.cuda.is_available()); print(bool(getattr(getattr(torch,'backends',None),'mps',None) and torch.backends.mps.is_available()))"
```

Interpretation:
- `False / False` in the container means no accelerator is exposed to `nimbus-mask`
- on macOS, use a host-native mask service if you want `mps`
- on Linux/NVIDIA, ensure your chosen container runtime exposes the GPU correctly

In external-mask mode on macOS, do not use `podman exec` for `nimbus-mask` because that service is not running in a container. Check the host Python environment directly instead:

```bash
.venv/bin/python -c "import torch; print(torch.cuda.is_available()); print(bool(getattr(getattr(torch,'backends',None),'mps',None) and torch.backends.mps.is_available()))"
```

### Port conflict

```bash
lsof -nP -iTCP:8501 -sTCP:LISTEN
lsof -nP -iTCP:8000 -sTCP:LISTEN
lsof -nP -iTCP:8010 -sTCP:LISTEN
```

## 9. Kubernetes / Minikube

Bootstrap, apply, and expose your Minikube deployment with the `minikube` / `kubectl` commands appropriate for your environment. The old helper shell scripts have been removed and will be replaced later.

## 10. Onboarding checklist for a new developer

1. Read `README.md`
2. Read `docs/ARCHITECTURE.md`
3. Launch the Podman stack
4. Verify all health and readiness endpoints
5. Open the UI and submit one small job
6. Inspect one finished job through the API and UI
7. Review `docs/ZARR.md` before touching conversion logic
