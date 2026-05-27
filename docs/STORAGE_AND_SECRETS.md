# Storage and Secrets

## Storage

The runtime shared data mount is `/data`, with operational files under
`/data/downloads`.

Tracked repository data:

- `data/Landsat-tiles/`
- `data/Sentinel-2-tiles/`

Runtime-only data:

- raw downloads
- Sen2Like work directories
- Zarr stores
- mask caches
- local job databases and logs

Kubernetes uses `nimbus-downloads-pvc` for `/data`. For multiple workers, the
storage class should support `ReadWriteMany`.

## Secrets

Kubernetes loads sensitive values from `nimbus-secrets`.

Expected provider credentials:

- `NIMBUS_COPERNICUS_USERNAME`
- `NIMBUS_COPERNICUS_PASSWORD`
- `NIMBUS_USGS_USERNAME`
- `NIMBUS_USGS_TOKEN`
- `NIMBUS_API_KEY` when API key protection is enabled

Do not commit `.env`, provider credentials, generated Zarr stores, or local
runtime databases.
