# Nimbus Fetch Architecture

This package is the fetch/orchestration service for NimbusChain. It owns job submission, worker execution, provider-driven download workflows, pipeline progress tracking, artifact registration, and the glue code that coordinates Zarr conversion and mask execution.

## Why this refactor started

`NimbusFetcher` had grown into a single orchestration class responsible for:

- API-facing use cases such as job submission, listing, resume, and result reads
- worker lifecycle and queue polling
- provider creation and provider-specific capability checks
- Zarr conversion and cube requests
- mask orchestration
- artifact registration
- pipeline timeline/progress updates

That design worked for early iteration because the whole end-to-end workflow was visible in one place. The downside was that unrelated changes started landing in the same class, which made the fetch layer harder to test, harder to reason about, and harder to extend safely.

## What is implemented in this first modularization pass

This first pass keeps the public `NimbusFetcher` API stable while moving the internal architecture toward explicit application services and typed ports.

### 1. Typed records for persisted data

New module:
- [src/nimbuschain_fetch/domain/records.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/domain/records.py:1)

These records are lightweight typed representations of the most common persisted shapes used by the fetch layer:

- `JobRowRecord`
- `JobResultRecord`
- `ArtifactRowRecord`
- `JobEventRecord`
- `WorkerHeartbeatRecord`

They now sit deeper in the persistence boundary than before. The store protocol and concrete stores expose typed read/list helpers for jobs, results, events, artifacts, and worker heartbeats, so application services can work with named records instead of repeatedly re-parsing raw dictionaries.

This pass also starts pushing typing into workflow payloads, not only persistence rows. Shared workflow models such as `MaskWorkflowItem` and `MaskWorkflowSummary` now represent the integrated mask stage internally before the workflow serializes them back into API/store payloads at the boundary.

The fetcher itself also now reads and writes job results through `JobResultRecord`-based helpers in more runtime paths, including manual conversion, resumed pipeline continuation, mask-job finalization, and result reads. That keeps result-shape knowledge closer to the typed record boundary instead of spreading raw payload assembly across multiple service methods.

The same pattern has now started for persisted jobs as well: fetcher-side helpers normalize `JobRowRecord` values before resume, result, cancel, and mask-entry paths consume them, which reduces direct `row.get(...)` access on raw store payloads.

Shared metadata typing has also started to move deeper than simple top-level wrappers. `PipelineMetadataRecord` and `ConversionMetadataRecord` now sit in repeated fetcher paths, and richer nested records such as `MaskStateRecord` and `ConversionItemRecord` are used where the service previously unpacked nested mask/conversion dictionaries by hand.

### 2. Explicit service ports

New module:
- [src/nimbuschain_fetch/ports.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/ports.py:1)

The fetch package now defines explicit protocols for its most important collaborators:

- `ConverterPort`
- `MaskPort`
- `ProviderPort`
- `CoordinatorAwareProviderPort`
- request objects for converter and mask operations (`ZarrConversionRequest`, `GroupedCubeBuildRequest`, `CubeBuildRequest`, `MaskExecutionRequest`)

These ports make the intended contracts visible and make local vs remote implementations easier to align. The goal is to remove hidden runtime coupling such as ad-hoc `Any` usage and implicit knowledge about method signatures.

This pass also replaces the old runtime introspection path for converter and mask calls in the fetcher with explicit request-object calls plus a compatibility fallback. That keeps the runtime stable while making the real adapter contract much clearer.

### 3. Registries for stores, providers, and executors

New module:
- [src/nimbuschain_fetch/registries.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/registries.py:1)

The fetch service now has dedicated registries for:

- `StoreRegistry`
- `ProviderRegistry`
- `ExecutorRegistry`

Architecturally, this is the beginning of a composition-root approach. Instead of hardcoding store/provider/executor creation directly in the fetcher, the fetcher receives or constructs registry objects that know how to build the concrete implementations.

Benefits:

- easier testing with injected fakes
- easier future support for additional backends
- reduced constructor coupling in `NimbusFetcher`

This pass still keeps the default runtime behavior identical:

- MongoDB and SQLite remain the default store options
- `inprocess` remains the default executor
- Copernicus and USGS remain the default providers

### 4. Extracted application services

New modules:
- [src/nimbuschain_fetch/application/pipeline_state.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/application/pipeline_state.py:1)
- [src/nimbuschain_fetch/application/artifact_registry.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/application/artifact_registry.py:1)
- [src/nimbuschain_fetch/application/conversion.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/application/conversion.py:1)
- [src/nimbuschain_fetch/application/job_execution.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/application/job_execution.py:1)
- [src/nimbuschain_fetch/application/workflows.py](/Users/mohammedkssim/Desktop/layersUpdated/Satellite-fetcher/src/nimbuschain_fetch/application/workflows.py:1)

Two cross-cutting concerns were extracted first because they are used from many workflow paths and had clear service boundaries:

#### `PipelineStateService`

Responsibilities:

- merge and persist pipeline metadata
- advance the pipeline timeline
- write pipeline-related events

Why this is its own service:

- pipeline updates are a cross-cutting concern shared by multiple workflows
- timeline semantics should not live inline inside every workflow branch
- this logic is easier to test when isolated from download/conversion/mask behavior

#### `ArtifactRegistryService`

Responsibilities:

- normalize artifact paths
- generate stable artifact ids
- persist typed artifact requests
- register Zarr, cube, masked Zarr, water mask, and cloud mask outputs

Why this is its own service:

- artifact registration is a separate responsibility from workflow execution
- artifact metadata rules are important and should be centralized
- future changes to artifact semantics should not require editing every workflow path

#### `JobExecutionRegistry`

Responsibilities:

- resolve a workflow handler from a `job_type`
- provide a controlled transition away from large `if/elif` execution branches
- let new workflow types plug into the fetch service without expanding the central dispatcher

What is implemented now:

- `mask_existing_zarr` is executed through the registry-backed handler path
- `search_download` and `download_products` now also dispatch through the registry
- the dispatcher supports both synchronous and asynchronous handlers so workflow migrations can happen incrementally

Why this is useful even before the full split:

- it establishes the architectural seam for workflow handlers now
- it lets the team migrate one job family at a time
- it avoids a risky “all workflows move at once” refactor

#### `FetchJobWorkflowService` and `MaskJobWorkflowService`

Responsibilities:

- own the end-to-end execution flow for fetch/download jobs
- own the dedicated execution flow for mask-existing-Zarr jobs
- keep workflow-specific failure handling close to the workflow itself instead of in the central orchestrator

What is implemented now:

- the execution registry resolves `search_download` and `download_products` into `FetchJobWorkflowService`
- the execution registry resolves `mask_existing_zarr` into `MaskJobWorkflowService`
- `NimbusFetcher` now composes these services during startup and delegates execution into them
- the integrated post-conversion mask stage is also executed from the workflow service instead of remaining inline in `NimbusFetcher`
- fetch workflows persist `JobResultRecord` directly instead of assembling ad-hoc result dictionaries in each branch
- integrated mask item/summary payloads are built through typed workflow models before being written to pipeline metadata and conversion metadata
- the manual conversion follow-on path that resumes cube/mask stages after Zarr writing is now also owned by `FetchJobWorkflowService`

#### `ManualConversionService`

Responsibilities:

- own the manual Zarr conversion use case for an existing job
- read persisted job state through `JobRowRecord`
- hand off post-Zarr continuation back into the extracted workflow services when requested

What is implemented now:

- `convert_existing_job()` in `NimbusFetcher` delegates to `ManualConversionService`
- the manual conversion service reads the source job through the typed job-record boundary instead of a raw store row
- result persistence from the manual conversion path continues through `JobResultRecord`

Why this matters:

- workflow orchestration now has an explicit application-service boundary
- adding a new job workflow no longer requires expanding the central registry and orchestration logic in the same place
- this is the first real shift of workflow behavior out of `NimbusFetcher`, not just utility extraction

## How `NimbusFetcher` changed

`NimbusFetcher` is still the public entrypoint and still coordinates the overall lifecycle, but it now delegates to:

- registries for store/provider/executor creation
- `PipelineStateService` for pipeline metadata and event updates
- `ArtifactRegistryService` for artifact persistence
- `FetchJobWorkflowService` and `MaskJobWorkflowService` for workflow execution dispatch
- `FetcherJobSupport` for job submission, result/resume actions, and standalone mask-job lifecycle handling
- `FetcherOperationsSupport` for runtime reset, list/query endpoints, event streaming, and queue-monitoring operations
- `FetcherPathSupport` for backend-path normalization, default output locations, and legacy path compatibility
- `FetcherStoreRecordSupport` for typed job/result store reads and writes
- `FetcherStatusTimelineSupport` for status reconstruction, date parsing, and pipeline timeline rebuilds
- `FetcherCubeSupport` for cube-output orchestration, resume-path rebuilding, and scene parallelism heuristics
- `FetcherArtifactSupport` for artifact-registration facades and mask-quality payload shaping
- `FetcherProgressSupport` for download/Zarr telemetry shaping and progress emission rules
- `FetcherProviderSupport` for provider construction, provider execution bridging, and cancellation transitions
- `FetcherDownloadCoordinatorSupport` for coordinator instantiation, capability routing, and coordinator status aggregation
- `FetcherNormalizationSupport` for historical row normalization and status/timeline facade behavior
- `FetcherMaskPolicySupport` for mask/cube policy normalization and failure-step selection
- `FetcherMaskApiSupport` for API-facing water/cloud mask request adapters
- `FetcherConversionSupport` for conversion facade helpers around single-output conversion and manual conversion entrypoints
- `FetcherConversionPolicySupport` for conversion/masking parallelism and worker-budget heuristics
- `FetcherLifecycleSupport` for fetcher startup, shutdown, cancellation checks, and job dispatch
- `FetcherWorkerRuntimeSupport` for worker heartbeats and worker capacity reporting
- `FetcherZarrContextSupport` for Zarr dataset inspection, context resolution, transform derivation, and CRS inference
- dedicated runtime helpers for provider execution, mask execution, and Zarr conversion orchestration

This is intentionally an incremental refactor. It reduces direct ownership inside `NimbusFetcher` without forcing a large breaking rewrite across the API, worker, and tests.

The API layer is also less coupled to the concrete fetcher now for the business endpoints. FastAPI dependencies for jobs, artifacts, converter operations, events, and metrics resolve narrow application-service protocols instead of exposing `NimbusFetcher` directly to every router.

## Architectural direction

The target architecture for the fetch service is:

- `domain`
  typed records, value objects, enums, and core orchestration concepts
- `application`
  use-case services and workflow handlers
- `infrastructure`
  concrete stores, providers, executors, and remote clients
- `presentation`
  FastAPI routes and other entrypoints

In that direction, `NimbusFetcher` should eventually become either:

- a thin facade that composes application services, or
- a compatibility layer preserved while routers and workers transition to narrower application services

## What is still intentionally not done in this pass

This refactor does not yet:

- remove all `dict[str, Any]` workflow payloads
- delete the legacy workflow bodies that remain inside `NimbusFetcher` as compatibility fallback code during the transition
- remove the remaining concrete `NimbusFetcher` coupling from runtime, worker, and observability endpoints
- fully standardize local/remote adapter parity
- replace all `job_type` branching with handler registries and workflow-specific handlers

Those are planned next steps, but doing them all at once would create a high regression risk.

## Recommended next steps

1. Move fetch, mask, convert, and resume flows into dedicated workflow handlers.
2. Replace more store-facing raw dictionaries with typed records.
3. Add a narrow API service layer so FastAPI depends on use-case services instead of the full fetcher.
4. Formalize a composition root for runtime assembly.
5. Keep extending contract tests around ports and registries.

## Why this approach is safer

This refactor favors controlled extraction over a full rewrite:

- public APIs remain stable
- default runtime behavior remains unchanged
- new boundaries are introduced where they are easiest to verify
- tests can be added around the new services before larger workflow moves happen

That gives the team a safer path toward modularity while still delivering real architectural improvement immediately.
