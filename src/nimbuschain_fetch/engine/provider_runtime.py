from __future__ import annotations

from pathlib import Path
from typing import Any, cast

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
    download_manager_cls: type[Any],
) -> dict[str, Any]:
    provider_name = rt._provider_name(request.provider)
    download_strategy = str(getattr(request, "download_strategy", "default") or "default").strip().lower() or "default"
    data_plane_limit = rt.settings.provider_data_plane_limits_map.get(provider_name, 1)
    download_manager = rt._build_provider_download_manager(
        provider_name=provider_name,
        data_plane_limit=data_plane_limit,
        progress_callback=progress_callback,
        cancel_checker=is_cancelled,
        retry_callback=retry_callback,
        requested_download_strategy=download_strategy,
        download_manager_cls=download_manager_cls,
    )
    provider = rt._build_provider(
        provider_name,
        download_manager,
        requested_download_strategy=download_strategy,
    )

    if isinstance(request, SearchDownloadRequest):
        provider.configure_job(
            collection=request.collection,
            product_type=request.product_type,
            download_strategy=download_strategy,
        )
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
                **dict(provider.plan_download_metadata(len(product_ids))),
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

        if rt._supports_download_coordinator(provider):
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
            provider_download_metadata = dict(provider.download_metadata() or {})
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
    provider.configure_job(
        collection=request.collection,
        download_strategy=download_strategy,
    )
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
            **dict(provider.plan_download_metadata(len(request.product_ids))),
        },
        event_type="job.downloading",
        event_payload={"products_requested": len(request.product_ids)},
    )
    if rt._supports_download_coordinator(provider):
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
        provider_download_metadata = dict(provider.download_metadata() or {})
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
