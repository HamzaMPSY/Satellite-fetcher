from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from nimbuschain_fetch.domain.records import ArtifactRowRecord
from nimbuschain_fetch.jobs.store import JobStore
from nimbuschain_fetch.models import ArtifactRecord, ArtifactType, ArtifactUpsertRequest, ProviderName


class ArtifactRegistryService:
    def __init__(
        self,
        *,
        store: JobStore,
        normalize_backend_path: Callable[[Any], Any],
        normalize_artifact_row: Callable[[dict[str, Any]], dict[str, Any]],
        path_size_bytes: Callable[[str | Path | None], int | None],
        water_mask_quality_fields: Callable[[dict[str, Any]], dict[str, Any]],
        cloud_mask_quality_fields: Callable[[dict[str, Any]], dict[str, Any]],
        mask_quality_fields: Callable[..., dict[str, Any]],
    ):
        self._store = store
        self._normalize_backend_path = normalize_backend_path
        self._normalize_artifact_row = normalize_artifact_row
        self._path_size_bytes = path_size_bytes
        self._water_mask_quality_fields = water_mask_quality_fields
        self._cloud_mask_quality_fields = cloud_mask_quality_fields
        self._mask_quality_fields = mask_quality_fields

    def upsert(self, request: ArtifactUpsertRequest) -> ArtifactRecord:
        normalized_artifact_uri = self._normalize_backend_path(request.artifact_uri)
        normalized_source_uri = self._normalize_backend_path(request.source_uri) if request.source_uri else None
        artifact_id = hashlib.md5(
            str(normalized_artifact_uri).encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()
        row = self._store.upsert_artifact_record(
            ArtifactRowRecord.from_row(
                {
                    **request.model_dump(mode="python"),
                    "artifact_id": artifact_id,
                    "artifact_type": request.artifact_type.value,
                    "provider": request.provider.value if request.provider else None,
                    "artifact_uri": normalized_artifact_uri,
                    "source_uri": normalized_source_uri,
                }
            )
        )
        row = self._normalize_artifact_row(row.to_row())
        return ArtifactRecord.model_validate(row)

    def register_zarr(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        zarr_uri: str,
        data_family: str,
        conversion_summary: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> None:
        self.upsert(
            ArtifactUpsertRequest(
                artifact_type=ArtifactType.zarr,
                artifact_uri=zarr_uri,
                provider=ProviderName(provider_name),
                collection=collection,
                scene_id=scene_id,
                source_uri=raw_uri,
                created_by_job_id=job_id,
                source_job_id=job_id,
                data_family=data_family,
                band_names=list(dataset_summary.get("band_names") or []),
                dimensions=list(dataset_summary.get("dimensions") or []),
                shape=list(dataset_summary.get("shape") or []),
                metadata={
                    "normalization_summary": conversion_summary,
                    "zarr_summary": dataset_summary,
                    "registered_via": "pipeline_job",
                },
            )
        )

    def register_cube(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        cube_stage: str,
        cube_summary: dict[str, Any],
    ) -> None:
        artifact_uri = str(cube_summary.get("zarr_uri") or "").strip()
        if not artifact_uri:
            return
        scene_ids = [str(item) for item in list(cube_summary.get("scene_ids") or []) if str(item).strip()]
        source_uris = [str(item) for item in list(cube_summary.get("source_zarr_uris") or []) if str(item).strip()]
        self.upsert(
            ArtifactUpsertRequest(
                artifact_type=ArtifactType.zarr_cube,
                artifact_uri=artifact_uri,
                provider=ProviderName(provider_name),
                collection=collection,
                scene_id=scene_ids[0] if len(scene_ids) == 1 else None,
                source_uri=source_uris[0] if len(source_uris) == 1 else None,
                created_by_job_id=job_id,
                source_job_id=job_id,
                data_family=str(cube_summary.get("data_family") or "").strip() or None,
                band_names=[str(item) for item in list(cube_summary.get("band_names") or [])],
                dimensions=[str(item) for item in list(cube_summary.get("dimensions") or [])],
                shape=[int(item) for item in list(cube_summary.get("shape") or [])],
                size_bytes=self._path_size_bytes(artifact_uri),
                metadata={
                    "cube_summary": cube_summary,
                    "cube_stage": cube_stage,
                    "group_key": str(cube_summary.get("group_key") or "").strip() or None,
                    "source_scene_ids": scene_ids,
                    "source_zarr_uris": source_uris,
                    "registered_via": "pipeline_job",
                },
            )
        )

    def register_watermask(
        self,
        *,
        job_id: str,
        source_job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        zarr_uri: str,
        water_mask: dict[str, Any],
    ) -> None:
        artifact_uri = str(water_mask.get("artifact_uri") or "").strip()
        if not artifact_uri:
            return
        self.upsert(
            ArtifactUpsertRequest(
                artifact_type=ArtifactType.watermask,
                artifact_uri=artifact_uri,
                provider=ProviderName(provider_name),
                collection=collection,
                scene_id=scene_id,
                source_uri=zarr_uri,
                created_by_job_id=job_id,
                source_job_id=source_job_id,
                data_family="mask",
                dimensions=["time", "y", "x"],
                shape=list(water_mask.get("shape") or []),
                metadata={
                    "water_mask": water_mask,
                    "quality": self._water_mask_quality_fields(water_mask),
                    "mask_contract_version": "v2",
                    "registered_via": "manual_mask_job",
                    "source_job_id": source_job_id,
                },
            )
        )

    def register_masked_zarr(
        self,
        *,
        job_id: str,
        source_job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        source_zarr_uri: str,
        masked_zarr_uri: str,
        mask_payload: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> None:
        artifact_uri = str(masked_zarr_uri or "").strip()
        if not artifact_uri or artifact_uri == str(source_zarr_uri or "").strip():
            return
        self.upsert(
            ArtifactUpsertRequest(
                artifact_type=ArtifactType.zarr_masked,
                artifact_uri=artifact_uri,
                provider=ProviderName(provider_name),
                collection=collection,
                scene_id=scene_id,
                source_uri=source_zarr_uri,
                created_by_job_id=job_id,
                source_job_id=source_job_id,
                data_family="optical",
                band_names=list(dataset_summary.get("band_names") or []),
                dimensions=list(dataset_summary.get("dimensions") or []),
                shape=list(dataset_summary.get("shape") or []),
                metadata={
                    "mask": mask_payload,
                    "quality": self._mask_quality_fields(
                        water_mask=dict(mask_payload.get("water_mask") or {}),
                        cloud_mask=dict(mask_payload.get("cloud_mask") or {}),
                    ),
                    "mask_contract_version": "v2",
                    "registered_via": "manual_mask_job",
                    "source_zarr_uri": source_zarr_uri,
                    "source_job_id": source_job_id,
                },
            )
        )

    def register_cloudmask(
        self,
        *,
        job_id: str,
        source_job_id: str,
        provider_name: str,
        collection: str,
        scene_id: str,
        zarr_uri: str,
        cloud_mask: dict[str, Any],
    ) -> None:
        artifact_uri = str(cloud_mask.get("artifact_uri") or "").strip()
        if not artifact_uri:
            return
        self.upsert(
            ArtifactUpsertRequest(
                artifact_type=ArtifactType.cloudmask,
                artifact_uri=artifact_uri,
                provider=ProviderName(provider_name),
                collection=collection,
                scene_id=scene_id,
                source_uri=zarr_uri,
                created_by_job_id=job_id,
                source_job_id=source_job_id,
                data_family="mask",
                dimensions=["time", "y", "x"],
                shape=list(cloud_mask.get("shape") or []),
                metadata={
                    "cloud_mask": cloud_mask,
                    "quality": self._cloud_mask_quality_fields(cloud_mask),
                    "mask_contract_version": "v2",
                    "registered_via": "manual_mask_job",
                    "source_job_id": source_job_id,
                },
            )
        )
