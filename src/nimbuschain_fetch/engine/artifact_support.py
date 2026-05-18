from __future__ import annotations

from typing import Any


class FetcherArtifactSupport:
    """Artifact-registry facade and mask-quality payload helpers."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def register_zarr_artifact(
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
        self._rt._artifact_registry.register_zarr(
            job_id=job_id,
            provider_name=provider_name,
            collection=collection,
            scene_id=scene_id,
            raw_uri=raw_uri,
            zarr_uri=zarr_uri,
            data_family=data_family,
            conversion_summary=conversion_summary,
            dataset_summary=dataset_summary,
        )

    def register_cube_artifact(
        self,
        *,
        job_id: str,
        provider_name: str,
        collection: str,
        cube_stage: str,
        cube_summary: dict[str, Any],
    ) -> None:
        self._rt._artifact_registry.register_cube(
            job_id=job_id,
            provider_name=provider_name,
            collection=collection,
            cube_stage=cube_stage,
            cube_summary=cube_summary,
        )

    def register_watermask_artifact(
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
        self._rt._artifact_registry.register_watermask(
            job_id=job_id,
            source_job_id=source_job_id,
            provider_name=provider_name,
            collection=collection,
            scene_id=scene_id,
            zarr_uri=zarr_uri,
            water_mask=water_mask,
        )

    def register_masked_zarr_artifact(
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
        self._rt._artifact_registry.register_masked_zarr(
            job_id=job_id,
            source_job_id=source_job_id,
            provider_name=provider_name,
            collection=collection,
            scene_id=scene_id,
            source_zarr_uri=source_zarr_uri,
            masked_zarr_uri=masked_zarr_uri,
            mask_payload=mask_payload,
            dataset_summary=dataset_summary,
        )

    def register_cloudmask_artifact(
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
        self._rt._artifact_registry.register_cloudmask(
            job_id=job_id,
            source_job_id=source_job_id,
            provider_name=provider_name,
            collection=collection,
            scene_id=scene_id,
            zarr_uri=zarr_uri,
            cloud_mask=cloud_mask,
        )

    def job_related_zarr_uris(
        self,
        *,
        job_id: str,
        row: dict[str, Any],
        result: dict[str, Any],
    ) -> list[str]:
        related: list[str] = []
        for value in list(result.get("zarr_outputs") or row.get("zarr_outputs") or []):
            uri = str(value).strip()
            if uri and uri not in related:
                related.append(uri)
        for artifact_type in ("zarr", "zarr_masked"):
            artifacts = self._rt.list_artifacts(
                artifact_type=artifact_type,
                provider=None,
                collection=None,
                scene_id=None,
                job_id=job_id,
                uri_query=None,
                date_from=None,
                date_to=None,
                page=1,
                page_size=500,
            )
            for item in artifacts.items:
                uri = str(item.artifact_uri).strip()
                if uri and uri not in related:
                    related.append(uri)
        return related

    def masked_zarr_outputs_for_job(
        self,
        *,
        job_id: str,
        result: dict[str, Any],
        row: dict[str, Any],
    ) -> list[str]:
        outputs = self._rt._merge_paths([], list(result.get("masked_zarr_outputs") or []))
        if outputs:
            return outputs
        outputs = self._rt._merge_paths([], list(row.get("masked_zarr_outputs") or []))
        if outputs:
            return outputs
        artifacts = self._rt.list_artifacts(
            artifact_type="zarr_masked",
            provider=None,
            collection=None,
            scene_id=None,
            job_id=job_id,
            uri_query=None,
            date_from=None,
            date_to=None,
            page=1,
            page_size=500,
        )
        return [str(item.artifact_uri).strip() for item in artifacts.items if str(item.artifact_uri).strip()]

    @staticmethod
    def collect_watermask_outputs(
        *,
        result: dict[str, Any],
        water_mask: dict[str, Any],
    ) -> list[str]:
        outputs: list[str] = []
        for value in list(result.get("watermask_outputs") or []):
            item = str(value).strip()
            if item and item not in outputs:
                outputs.append(item)
        for key in ("artifact_uri", "status_path"):
            item = str(water_mask.get(key) or "").strip()
            if item and item not in outputs:
                outputs.append(item)
        derived_zarr_uri = str(water_mask.get("output_zarr_uri") or "").strip()
        if derived_zarr_uri and derived_zarr_uri != str(water_mask.get("input_zarr_uri") or "").strip():
            if derived_zarr_uri not in outputs:
                outputs.append(derived_zarr_uri)
        return outputs

    @staticmethod
    def water_mask_quality_fields(water_mask: dict[str, Any]) -> dict[str, Any]:
        if not water_mask:
            return {}
        return {
            "status": str(water_mask.get("status") or "").strip().lower(),
            "runtime_mode": str(water_mask.get("runtime_mode") or "").strip(),
            "threshold_used": water_mask.get("threshold_used"),
            "sensor_recipe": str(water_mask.get("sensor_recipe") or "").strip(),
            "probability_source": str(water_mask.get("probability_source") or "").strip(),
            "water_fraction": float(water_mask.get("water_fraction") or 0.0),
            "cloud_blocked_fraction": float(water_mask.get("cloud_blocked_fraction") or 0.0),
            "input_bands": [str(item) for item in list(water_mask.get("input_bands") or [])],
            "mask_path": str(water_mask.get("mask_path") or "").strip(),
            "probability_path": str(water_mask.get("probability_path") or "").strip(),
        }

    @staticmethod
    def cloud_mask_quality_fields(cloud_mask: dict[str, Any]) -> dict[str, Any]:
        if not cloud_mask:
            return {}
        inference = dict(cloud_mask.get("inference") or {})
        return {
            "status": str(cloud_mask.get("status") or "").strip().lower(),
            "backend": str(cloud_mask.get("backend") or "").strip(),
            "threshold": cloud_mask.get("threshold"),
            "includes_shadows": bool(
                cloud_mask.get("include_shadows", inference.get("includes_shadows", False))
            ),
            "mask_source": str(cloud_mask.get("mask_source") or inference.get("mask_source") or "").strip(),
            "probability_source": str(
                cloud_mask.get("probability_source") or inference.get("probability_source") or ""
            ).strip(),
            "sensor_recipe": str(
                cloud_mask.get("sensor_recipe") or cloud_mask.get("sensor") or inference.get("sensor_recipe") or ""
            ).strip(),
            "cloud_fraction": float(cloud_mask.get("cloud_fraction") or inference.get("cloud_fraction") or 0.0),
            "cloud_only_fraction": float(
                cloud_mask.get("cloud_only_fraction") or inference.get("cloud_only_fraction") or 0.0
            ),
            "shadow_fraction": float(cloud_mask.get("shadow_fraction") or inference.get("shadow_fraction") or 0.0),
            "input_bands": [str(item) for item in list(cloud_mask.get("input_bands") or [])],
            "mask_path": str(cloud_mask.get("mask_path") or "").strip(),
            "probability_path": str(cloud_mask.get("probability_path") or "").strip(),
        }

    @classmethod
    def mask_quality_fields(cls, *, water_mask: dict[str, Any], cloud_mask: dict[str, Any]) -> dict[str, Any]:
        return {
            "water_mask": cls.water_mask_quality_fields(water_mask),
            "cloud_mask": cls.cloud_mask_quality_fields(cloud_mask),
            "water_fraction": float(water_mask.get("water_fraction") or 0.0),
            "cloud_fraction": float(
                cloud_mask.get("cloud_fraction")
                or dict(cloud_mask.get("inference") or {}).get("cloud_fraction")
                or 0.0
            ),
            "cloud_only_fraction": float(
                cloud_mask.get("cloud_only_fraction")
                or dict(cloud_mask.get("inference") or {}).get("cloud_only_fraction")
                or 0.0
            ),
            "shadow_fraction": float(
                cloud_mask.get("shadow_fraction")
                or dict(cloud_mask.get("inference") or {}).get("shadow_fraction")
                or 0.0
            ),
        }
