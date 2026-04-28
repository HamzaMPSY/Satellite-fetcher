from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from nimbuschain_fetch.download.download_manager import DownloadManager
from nimbuschain_fetch.geometry.aoi import parse_aoi
from nimbuschain_fetch.models import DownloadProductsRequest, JobCreateRequest, PipelineState, SearchDownloadRequest


def run_provider_job(
    rt: Any,
    job_id: str,
    request: JobCreateRequest,
    output_dir: Any,
    progress_callback: Any,
    retry_callback: Any,
    is_cancelled: Any,
    download_manager_cls: type[DownloadManager] = DownloadManager,
) -> dict[str, Any]:
    provider_name = rt._provider_name(request.provider)
    download_strategy = str(getattr(request, "download_strategy", "default") or "default").strip().lower() or "default"
    data_plane_limit = rt.settings.provider_data_plane_limits_map.get(provider_name, 1)

    download_manager_kwargs: dict[str, Any] = dict(
        max_concurrent=data_plane_limit,
        progress_callback=progress_callback,
        cancel_checker=is_cancelled,
        retry_callback=retry_callback,
    )
    if provider_name == "copernicus":
        download_manager_kwargs.update(
            max_concurrent=min(data_plane_limit, 2),
            max_retries=5,
            initial_delay=2.0,
            backoff_factor=1.5,
            connect_timeout=30.0,
            chunk_size=128 * 1024,
            max_connections=50,
            max_connections_per_host=2,
        )
    elif provider_name == "usgs":
        download_manager_kwargs.update(
            max_concurrent=min(data_plane_limit, 2),
            initial_delay=2.0,
            backoff_factor=1.5,
            connect_timeout=30.0,
            chunk_size=128 * 1024,
            max_connections=50,
            max_connections_per_host=2,
        )

    download_manager = download_manager_cls(**download_manager_kwargs)
    provider = rt._build_provider(provider_name, download_manager)
    if provider_name == "copernicus":
        setattr(provider, "download_strategy", download_strategy)

    if isinstance(request, SearchDownloadRequest):
        if is_cancelled():
            raise rt.job_cancelled_error_cls("cancelled")
        rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.searching,
            pipeline_step="searching",
            pipeline_progress=5.0,
            pipeline_metadata={
                "provider": provider_name,
                "collection": request.collection,
                "product_type": request.product_type,
                "tile_id": request.tile_id,
                "products_found": 0,
                "output_dir": str(output_dir),
                "job_type": request.job_type,
                "download_strategy": download_strategy,
            },
            event_type="job.searching",
            event_payload={"provider": provider_name, "collection": request.collection},
        )
        geom = parse_aoi(request.aoi.model_dump())

        product_ids = provider.search_products(
            collection=request.collection,
            product_type=request.product_type,
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            aoi=geom,
            tile_id=request.tile_id,
        )
        rt.store.append_event(
            job_id,
            "job.products_found",
            {"count": len(product_ids)},
        )
        rt._update_pipeline(
            job_id,
            pipeline_state=PipelineState.downloading,
            pipeline_step="downloading",
            pipeline_progress=10.0 if product_ids else 70.0,
            pipeline_metadata={
                "provider": provider_name,
                "collection": request.collection,
                "product_type": request.product_type,
                "tile_id": request.tile_id,
                "products_found": len(product_ids),
                "products_requested": len(product_ids),
                "output_dir": str(output_dir),
                "job_type": request.job_type,
                "download_strategy": download_strategy,
                **(
                    dict(provider.plan_download_metadata(len(product_ids)))
                    if hasattr(provider, "plan_download_metadata")
                    else {}
                ),
            },
            event_type="job.downloading",
            event_payload={"products_found": len(product_ids)},
        )
        if is_cancelled():
            raise rt.job_cancelled_error_cls("cancelled")

        if not product_ids:
            return {
                "paths": [],
                "metadata": {
                    "job_type": request.job_type,
                    "provider": provider_name,
                    "collection": request.collection,
                    "product_type": request.product_type,
                    "products_found": 0,
                    "products_downloaded": 0,
                    "output_dir": str(output_dir),
                },
            }

        if rt._supports_download_coordinator(provider_name, provider):
            coordinator_result = rt._download_with_coordinator(
                job_id=job_id,
                provider_name=provider_name,
                provider=provider,
                collection=request.collection,
                product_ids=product_ids,
                output_dir=Path(output_dir),
                progress_callback=progress_callback,
                retry_callback=retry_callback,
                cancel_checker=is_cancelled,
                download_strategy=download_strategy,
            )
            paths = list(coordinator_result.paths)
            provider_download_metadata = dict(coordinator_result.metadata or {})
        else:
            paths = provider.download_products(product_ids=product_ids, output_dir=str(output_dir))
            provider_download_metadata = dict(getattr(provider, "last_download_metadata", {}) or {})
        return {
            "paths": paths,
            "metadata": {
                "job_type": request.job_type,
                "provider": provider_name,
                "collection": request.collection,
                "product_type": request.product_type,
                "products_found": len(product_ids),
                "products_downloaded": len(paths),
                "output_dir": str(output_dir),
                "download_strategy": download_strategy,
                **provider_download_metadata,
            },
        }

    request = cast(DownloadProductsRequest, request)
    if hasattr(provider, "dataset"):
        setattr(provider, "dataset", request.collection)
    rt._update_pipeline(
        job_id,
        pipeline_state=PipelineState.downloading,
        pipeline_step="downloading",
        pipeline_progress=10.0,
        pipeline_metadata={
            "provider": provider_name,
            "collection": request.collection,
            "products_requested": len(request.product_ids),
            "products_found": len(request.product_ids),
            "output_dir": str(output_dir),
            "job_type": request.job_type,
            "download_strategy": download_strategy,
            **(
                dict(provider.plan_download_metadata(len(request.product_ids)))
                if hasattr(provider, "plan_download_metadata")
                else {}
            ),
        },
        event_type="job.downloading",
        event_payload={"products_requested": len(request.product_ids)},
    )
    if rt._supports_download_coordinator(provider_name, provider):
        coordinator_result = rt._download_with_coordinator(
            job_id=job_id,
            provider_name=provider_name,
            provider=provider,
            collection=request.collection,
            product_ids=list(request.product_ids),
            output_dir=Path(output_dir),
            progress_callback=progress_callback,
            retry_callback=retry_callback,
            cancel_checker=is_cancelled,
            download_strategy=download_strategy,
        )
        paths = list(coordinator_result.paths)
        provider_download_metadata = dict(coordinator_result.metadata or {})
    else:
        paths = provider.download_products(product_ids=request.product_ids, output_dir=str(output_dir))
        provider_download_metadata = dict(getattr(provider, "last_download_metadata", {}) or {})
    return {
        "paths": paths,
        "metadata": {
            "job_type": request.job_type,
            "provider": provider_name,
            "collection": request.collection,
            "products_requested": len(request.product_ids),
            "products_downloaded": len(paths),
            "output_dir": str(output_dir),
            "download_strategy": download_strategy,
            **provider_download_metadata,
        },
    }
