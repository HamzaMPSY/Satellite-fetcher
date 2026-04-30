from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from nimbuschain_shared.contracts.zarr import (
    BuildCubeRequest,
    BuildCubeResponse,
    BuildGroupedCubesRequest,
    BuildGroupedCubesResponse,
    ConvertRequest,
    ConvertResponse,
    InspectDatasetRequest,
    InspectDatasetResponse,
)
from nimbuschain_zarr_service.constants import APP_VERSION, SERVICE_NAME
from nimbuschain_zarr_service.cube import build_grouped_time_cubes, build_time_cube
from nimbuschain_zarr_service.dependencies import get_conversion_service
from nimbuschain_zarr_service.health import health_response, readiness_response, schema_payload
from nimbuschain_zarr_service.inspection import inspect_dataset_summary
from nimbuschain_zarr_service.models import CubeSummaryRecord
from nimbuschain_zarr_service.service import ZarrConversionService


logger = logging.getLogger("nimbus.zarr")
router = APIRouter()


@router.get("/")
def root() -> dict[str, str]:
    return {
        "service": SERVICE_NAME,
        "status": "ok",
        "version": APP_VERSION,
        "docs": "/docs",
    }


@router.get("/health")
def health():
    return health_response()


@router.get("/readiness")
def readiness():
    return readiness_response()


@router.get("/schema")
def schema() -> dict[str, object]:
    return schema_payload()


@router.post("/convert", response_model=ConvertResponse)
def convert(
    payload: ConvertRequest,
    request: Request,
    service: ZarrConversionService = Depends(get_conversion_service),
) -> ConvertResponse:
    from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError
    from nimbuschain_zarr_service.landsat import LandsatDependencyError, LandsatNormalizationError

    request_id = getattr(request.state, "request_id", None)

    logger.info(
        "conversion_requested job_id=%s pipeline_id=%s provider=%s collection=%s scene_id=%s output_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        payload.provider,
        payload.collection,
        payload.scene_id,
        payload.output_uri,
        extra={"request_id": request_id},
    )

    progress_state = {
        "fraction_bucket": -1,
        "stage": "",
        "array_name": "",
        "band_name": "",
    }

    def _progress_logger(progress) -> None:
        payload = progress.to_dict()
        stage = str(payload.get("stage") or "").strip() or "unknown"
        array_name = str(payload.get("array_name") or "").strip()
        band_name = str(payload.get("band_name") or "").strip()
        fraction = float(payload.get("fraction") or 0.0)
        bucket = int(min(100, max(0, fraction * 100)) // 5)

        should_log = (
            bucket > progress_state["fraction_bucket"]
            or stage != progress_state["stage"]
            or array_name != progress_state["array_name"]
            or band_name != progress_state["band_name"]
            or fraction >= 1.0
        )
        if not should_log:
            return

        progress_state["fraction_bucket"] = bucket
        progress_state["stage"] = stage
        progress_state["array_name"] = array_name
        progress_state["band_name"] = band_name

        logger.info(
            "conversion_progress job_id=%s scene_id=%s stage=%s array_name=%s band_name=%s fraction=%.4f blocks_written=%s total_blocks=%s",
            payload.job_id,
            payload.scene_id,
            stage,
            array_name or "-",
            band_name or "-",
            fraction,
            payload.get("blocks_written"),
            payload.get("total_blocks"),
            extra={"request_id": request_id},
        )

    try:
        normalized_collection = payload.collection.strip().lower() if payload.provider == "usgs" else payload.collection.strip().upper()
        normalized_product_type = payload.product_type.strip().upper() if payload.product_type else None
        conversion_result = service.convert_record(
            provider=payload.provider,
            collection=normalized_collection,
            scene_id=payload.scene_id,
            raw_uri=payload.raw_uri,
            output_uri=payload.output_uri,
            product_type=normalized_product_type,
            progress_callback=_progress_logger,
        )
        written_uri = conversion_result.written_uri
        data_family = conversion_result.data_family
        summary = conversion_result.summary
        dataset_summary = conversion_result.dataset_summary
    except (LandsatNormalizationError, ConversionError) as exc:
        logger.warning(
            "conversion_failed job_id=%s scene_id=%s provider=%s reason=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (LandsatDependencyError, ConversionDependencyError) as exc:
        logger.exception(
            "conversion_runtime_error job_id=%s scene_id=%s provider=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "conversion_unhandled_error job_id=%s scene_id=%s provider=%s",
            payload.job_id,
            payload.scene_id,
            payload.provider,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "conversion_completed job_id=%s scene_id=%s provider=%s data_family=%s zarr_uri=%s band_count=%s ancillary_count=%s",
        payload.job_id,
        payload.scene_id,
        payload.provider,
        data_family,
        written_uri,
        len(dataset_summary["band_names"]),
        len(dataset_summary.get("ancillary_layer_names") or []),
        extra={"request_id": request_id},
    )

    return ConvertResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status="written",
        stage="zarr_converting",
        service=SERVICE_NAME,
        message="Raw product converted into an x/y band time Zarr dataset and written successfully.",
        accepted_at=datetime.now(timezone.utc).isoformat(),
        zarr_uri=written_uri,
        data_family=data_family,
        band_names=list(dataset_summary["band_names"]),
        dimensions=list(dataset_summary["dimensions"]),
        ancillary_layer_names=list(dataset_summary.get("ancillary_layer_names") or []),
        ancillary_dimensions=list(dataset_summary.get("ancillary_dimensions") or []),
        normalization_summary={**summary, "zarr_summary": dataset_summary},
    )


@router.post("/cubes/grouped/build", response_model=BuildGroupedCubesResponse)
def build_grouped_cubes(payload: BuildGroupedCubesRequest, request: Request) -> BuildGroupedCubesResponse:
    from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError

    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "grouped_cube_build_requested job_id=%s pipeline_id=%s source_count=%s output_dir=%s stage_label=%s",
        payload.job_id,
        payload.pipeline_id,
        len(payload.source_zarr_uris),
        payload.output_dir,
        payload.stage_label,
        extra={"request_id": request_id},
    )
    try:
        cube_summary = CubeSummaryRecord.from_mapping(build_grouped_time_cubes(
            list(payload.source_zarr_uris),
            payload.output_dir,
            include_ancillary=bool(payload.include_ancillary),
            include_masks=payload.include_masks,
            start_date=payload.start_date,
            end_date=payload.end_date,
            stage_label=payload.stage_label,
        )).to_dict()
    except ConversionError as exc:
        logger.warning(
            "grouped_cube_build_rejected job_id=%s pipeline_id=%s reason=%s",
            payload.job_id,
            payload.pipeline_id,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConversionDependencyError as exc:
        logger.exception(
            "grouped_cube_build_runtime_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "grouped_cube_build_unhandled_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "grouped_cube_build_completed job_id=%s pipeline_id=%s status=%s outputs=%s",
        payload.job_id,
        payload.pipeline_id,
        cube_summary.get("status"),
        len(list(cube_summary.get("cube_outputs") or [])),
        extra={"request_id": request_id},
    )
    return BuildGroupedCubesResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status=str(cube_summary.get("status") or "skipped"),
        service=SERVICE_NAME,
        cube_summary=cube_summary,
    )


@router.post("/cubes/build", response_model=BuildCubeResponse)
def build_cube(payload: BuildCubeRequest, request: Request) -> BuildCubeResponse:
    from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError

    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "cube_build_requested job_id=%s pipeline_id=%s source_count=%s output_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        len(payload.source_zarr_uris),
        payload.output_uri,
        extra={"request_id": request_id},
    )
    try:
        cube_summary = CubeSummaryRecord.from_mapping(build_time_cube(
            list(payload.source_zarr_uris),
            payload.output_uri,
            include_ancillary=bool(payload.include_ancillary),
            include_masks=bool(payload.include_masks),
        )).to_dict()
    except ConversionError as exc:
        logger.warning(
            "cube_build_rejected job_id=%s pipeline_id=%s reason=%s",
            payload.job_id,
            payload.pipeline_id,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConversionDependencyError as exc:
        logger.exception(
            "cube_build_runtime_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "cube_build_unhandled_error job_id=%s pipeline_id=%s",
            payload.job_id,
            payload.pipeline_id,
            extra={"request_id": request_id},
        )
        raise

    logger.info(
        "cube_build_completed job_id=%s pipeline_id=%s zarr_uri=%s",
        payload.job_id,
        payload.pipeline_id,
        cube_summary.get("zarr_uri"),
        extra={"request_id": request_id},
    )
    return BuildCubeResponse(
        job_id=payload.job_id,
        pipeline_id=payload.pipeline_id,
        status="written",
        service=SERVICE_NAME,
        cube_summary=cube_summary,
    )


@router.post("/inspect-dataset", response_model=InspectDatasetResponse)
def inspect_dataset(payload: InspectDatasetRequest, request: Request) -> InspectDatasetResponse:
    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "dataset_inspection_requested zarr_uri=%s",
        payload.zarr_uri,
        extra={"request_id": request_id},
    )
    try:
        dataset_summary = inspect_dataset_summary(payload.zarr_uri)
    except ValueError as exc:
        logger.warning(
            "dataset_inspection_rejected zarr_uri=%s reason=%s",
            payload.zarr_uri,
            str(exc),
            extra={"request_id": request_id},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "dataset_inspection_unhandled_error zarr_uri=%s",
            payload.zarr_uri,
            extra={"request_id": request_id},
        )
        raise

    return InspectDatasetResponse(
        service=SERVICE_NAME,
        zarr_uri=payload.zarr_uri,
        dataset_summary=dataset_summary,
    )
