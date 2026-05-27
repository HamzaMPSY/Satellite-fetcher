# Validation

Use this checklist before handing changes to the platform team.

## Git hygiene

```bash
git status --short
git check-ignore report_work/PFE_REPORT_improved.docx
```

`report_work/` should stay local and ignored.

## Compose

```bash
docker compose -f deploy/compose/compose.yml config
podman compose -f deploy/compose/compose.yml config
```

## Kubernetes

```bash
kubectl kustomize k8s/base
kubectl kustomize k8s/overlays/minikube
kubectl kustomize k8s/overlays/omk
```

## Python tests

Run targeted service-contract coverage:

```bash
python -m pytest \
  tests/test_fetcher_zarr_client.py \
  tests/test_mask_client_modes.py \
  tests/test_sen2like_service_runner.py \
  tests/test_pipeline_stage_core.py \
  tests/test_pipeline_job_unification.py
```

Add full test coverage before release when runtime dependencies are available.
