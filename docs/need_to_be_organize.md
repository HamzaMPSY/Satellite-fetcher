# Need to Be Organized

This file tracks repository cleanup items that should not block the OMK handoff
but should be resolved before a wider release.

- Decide whether older helper scripts should move under `legacy/`.
- Keep generated notebooks, reports and rendered images outside Git.
- Consolidate manual test recipes from ad hoc notes into `docs/MANUAL_CLI_TESTS.md`.
- Keep compose files canonical under `deploy/compose/`.
- Keep Kubernetes environment defaults in `k8s/base/configmap.yaml` and platform
  differences in overlays.
