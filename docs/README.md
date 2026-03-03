# Documentation Index

Start here if you want to understand the repository quickly.

## Main guides

- `COMPLETE_GUIDE_FR.md`
  - Full French guide from zero: architecture, containers, UI/API workflow, job lifecycle, terminal commands, tests, and troubleshooting.

- `DEPLOYMENT_3_LAYER.md`
  - End-to-end Podman deployment of independent `api`, `worker`, and `ui` services, including scaling and troubleshooting.

- `KUBERNETES_DEPLOYMENT.md`
  - Kubernetes manifests and deployment runbook for `api`, `worker`, `ui`, and MongoDB.

- `REPOSITORY_GUIDE.md`
  - Deep technical walkthrough of architecture, modules, execution flow, settings, security, persistence, tests, and troubleshooting.

- `API_REFERENCE.md`
  - Endpoint-by-endpoint HTTP reference with request/response examples and curl workflows.

- `integration_streamlit.md`
  - How to replace subprocess CLI usage with direct/service client integration in Streamlit.

- `ZARR_MASK_ARCHITECTURE_CONTRACTS.md`
  - Frozen architecture and inter-service contracts for the future `fetcher -> zarr-converter -> mask` pipeline.

- `ZARR_CONVERTER_SERVICE.md`
  - Skeleton service notes for the new `zarr-converter-service`: endpoints, local run, and container build.

- `ZARR_DATA_MODEL.md`
  - Frozen Zarr v1 data model: layout, dimensions, chunking, compression, and metadata contract.

## Recommended reading order

1. `COMPLETE_GUIDE_FR.md`
2. `DEPLOYMENT_3_LAYER.md`
3. `KUBERNETES_DEPLOYMENT.md`
4. `REPOSITORY_GUIDE.md`
5. `API_REFERENCE.md`
6. `integration_streamlit.md`
7. `ZARR_MASK_ARCHITECTURE_CONTRACTS.md`
8. `ZARR_CONVERTER_SERVICE.md`
9. `ZARR_DATA_MODEL.md`
