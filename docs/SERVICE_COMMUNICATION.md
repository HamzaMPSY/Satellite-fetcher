# Service Communication

This diagram shows how the main backend services communicate in the current pipeline architecture.

## High-Level Service Communication

```mermaid
flowchart LR
    User[User / UI / Client]
    API[Nimbus Fetch API\nnimbuschain_fetch_service]
    Fetcher[Nimbus Fetch Runtime\nnimbuschain_fetch]
    Providers[External Providers\nCopernicus / USGS]
    ZarrSvc[Nimbus Zarr Service\nnimbuschain_zarr_service]
    MaskSvc[Nimbus Mask Service\nnimbuschain_mask_service]
    Shared[(Shared Storage / Artifacts)\nraw bundles, zarr stores, masks]
    Contracts[nimbuschain_shared\ncontracts + clients + shared conventions]

    User -->|HTTP| API
    API -->|application services / job orchestration| Fetcher

    Fetcher -->|provider SDK / HTTP / auth| Providers
    Providers -->|raw products| Fetcher
    Fetcher -->|write raw bundle URI / artifact metadata| Shared

    Fetcher -->|HTTP /convert\nConvertRequest| ZarrSvc
    ZarrSvc -->|read raw_uri| Shared
    ZarrSvc -->|write zarr_uri + dataset summary| Shared
    ZarrSvc -->|ConvertResponse| Fetcher

    Fetcher -->|HTTP /apply\nMaskApplyRequest| MaskSvc
    MaskSvc -->|read source_zarr_uri| Shared
    MaskSvc -->|write cloud/water mask layers| Shared
    MaskSvc -->|Mask result payload| Fetcher

    API -. uses .-> Contracts
    Fetcher -. uses .-> Contracts
    ZarrSvc -. uses .-> Contracts
    MaskSvc -. uses .-> Contracts
```

## Request / Artifact Flow

```mermaid
sequenceDiagram
    participant C as Client / UI
    participant A as Fetch API
    participant F as Fetch Runtime
    participant P as Provider
    participant S as Shared Storage
    participant Z as Zarr Service
    participant M as Mask Service

    C->>A: POST job request
    A->>F: submit / execute workflow

    F->>P: search + download scene
    P-->>F: raw product bundle
    F->>S: persist raw bundle

    F->>Z: POST /convert\nprovider, collection, scene_id,\nraw_uri, output_uri
    Z->>S: read raw_uri
    Z->>S: write scene Zarr
    Z-->>F: zarr_uri + dataset_summary

    opt Cloud / Water masking
        F->>M: POST /apply\nsource_zarr_uri, mask_types,\ndataset_summary
        M->>S: read source_zarr_uri
        M->>S: write mask layers into output Zarr
        M-->>F: mask summary + output_zarr_uri
    end

    F-->>A: job state + result payload
    A-->>C: HTTP response / job status
```

## Notes

- Services communicate primarily through **HTTP APIs**.
- They exchange **typed shared contracts** from `nimbuschain_shared`.
- Large data is usually not passed inline between services.
- Instead, services exchange **references** such as:
  - `raw_uri`
  - `output_uri`
  - `zarr_uri`
  - `source_zarr_uri`
  - `job_id`
  - `scene_id`
- Shared storage is the handoff point for raw bundles, converted Zarr stores, and mask outputs.
