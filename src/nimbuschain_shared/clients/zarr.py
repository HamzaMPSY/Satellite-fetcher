from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests

from nimbuschain_shared.dto import (
    CubeBuildRequest,
    GroupedCubeBuildRequest,
    ZarrConversionRequest,
)


class ZarrServiceClient:
    """Thin HTTP client for the standalone Zarr conversion service."""

    def __init__(self, *, service_url: str):
        normalized = str(service_url or "").strip().rstrip("/")
        if not normalized:
            raise ValueError("service_url is required for ZarrServiceClient.")
        self.service_url = normalized
        self._session = requests.Session()

    def close(self) -> None:
        self._session.close()

    def health(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/health")

    def readiness(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/readiness")

    def schema(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/schema")

    def convert_request(
        self,
        request: ZarrConversionRequest,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        return self.convert(
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            provider=request.provider,
            collection=request.collection,
            scene_id=request.scene_id,
            raw_uri=request.raw_uri,
            output_uri=request.output_uri,
            product_type=request.product_type,
            progress_callback=request.progress_callback,
        )

    def convert(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
        progress_callback: Any | None = None,
    ) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
        del progress_callback
        response = self._session.post(
            f"{self.service_url}/convert",
            json={
                "job_id": job_id,
                "pipeline_id": pipeline_id,
                "trace_id": trace_id,
                "provider": provider,
                "collection": collection,
                "product_type": product_type,
                "scene_id": scene_id,
                "raw_uri": raw_uri,
                "raw_format": "auto",
                "output_uri": output_uri,
            },
            timeout=(30, None),
        )
        payload = self._response_payload(response)
        if response.status_code >= 500:
            raise RuntimeError(self._error_detail(payload, response, prefix="Zarr service conversion failed"))
        if response.status_code >= 400:
            raise ValueError(self._error_detail(payload, response, prefix="Zarr conversion request was rejected"))
        summary = dict(payload.get("normalization_summary") or {})
        dataset_summary = dict(summary.pop("zarr_summary", {}) or {})
        if not dataset_summary:
            raise RuntimeError("Zarr service conversion response did not include dataset_summary.")
        written_uri = str(payload.get("zarr_uri") or "").strip()
        data_family = str(payload.get("data_family") or "").strip()
        if not written_uri or not data_family:
            raise RuntimeError("Zarr service conversion response was missing zarr_uri or data_family.")
        return written_uri, data_family, summary, dataset_summary

    def build_grouped_cubes(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        source_zarr_uris: list[str],
        output_dir: str,
        include_ancillary: bool = True,
        include_masks: bool | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        stage_label: str | None = None,
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        del progress_callback
        response = self._session.post(
            f"{self.service_url}/cubes/grouped/build",
            json={
                "job_id": job_id,
                "pipeline_id": pipeline_id,
                "trace_id": trace_id,
                "source_zarr_uris": list(source_zarr_uris),
                "output_dir": output_dir,
                "include_ancillary": bool(include_ancillary),
                "include_masks": include_masks,
                "start_date": self._json_value(start_date),
                "end_date": self._json_value(end_date),
                "stage_label": stage_label,
            },
            timeout=(30, None),
        )
        payload = self._response_payload(response)
        if response.status_code >= 500:
            raise RuntimeError(self._error_detail(payload, response, prefix="Zarr service cube build failed"))
        if response.status_code >= 400:
            raise ValueError(self._error_detail(payload, response, prefix="Zarr cube build request was rejected"))
        summary = dict(payload.get("cube_summary") or {})
        if not summary:
            raise RuntimeError("Zarr service cube build response did not include cube_summary.")
        return summary

    def build_grouped_cubes_request(
        self,
        request: GroupedCubeBuildRequest,
    ) -> dict[str, Any]:
        return self.build_grouped_cubes(
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            source_zarr_uris=request.source_zarr_uris,
            output_dir=request.output_dir,
            include_ancillary=request.include_ancillary,
            include_masks=request.include_masks,
            start_date=request.start_date,
            end_date=request.end_date,
            stage_label=request.stage_label,
            progress_callback=request.progress_callback,
        )

    def build_cube(
        self,
        *,
        job_id: str,
        pipeline_id: str,
        trace_id: str,
        source_zarr_uris: list[str],
        output_uri: str,
        include_ancillary: bool = True,
        include_masks: bool = False,
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        del progress_callback
        response = self._session.post(
            f"{self.service_url}/cubes/build",
            json={
                "job_id": job_id,
                "pipeline_id": pipeline_id,
                "trace_id": trace_id,
                "source_zarr_uris": list(source_zarr_uris),
                "output_uri": output_uri,
                "include_ancillary": bool(include_ancillary),
                "include_masks": bool(include_masks),
            },
            timeout=(30, None),
        )
        payload = self._response_payload(response)
        if response.status_code >= 500:
            raise RuntimeError(self._error_detail(payload, response, prefix="Zarr service cube build failed"))
        if response.status_code >= 400:
            raise ValueError(self._error_detail(payload, response, prefix="Zarr cube build request was rejected"))
        summary = dict(payload.get("cube_summary") or {})
        if not summary:
            raise RuntimeError("Zarr service cube build response did not include cube_summary.")
        return summary

    def build_cube_request(
        self,
        request: CubeBuildRequest,
    ) -> dict[str, Any]:
        return self.build_cube(
            job_id=request.job_id,
            pipeline_id=request.pipeline_id,
            trace_id=request.trace_id,
            source_zarr_uris=request.source_zarr_uris,
            output_uri=request.output_uri,
            include_ancillary=request.include_ancillary,
            include_masks=request.include_masks,
            progress_callback=request.progress_callback,
        )

    def inspect_dataset(self, *, zarr_uri: str) -> dict[str, Any]:
        response = self._session.post(
            f"{self.service_url}/inspect-dataset",
            json={"zarr_uri": zarr_uri},
            timeout=30,
        )
        payload = self._response_payload(response)
        if response.status_code >= 500:
            raise RuntimeError(self._error_detail(payload, response, prefix="Zarr service dataset inspection failed"))
        if response.status_code >= 400:
            raise ValueError(
                self._error_detail(payload, response, prefix="Zarr dataset inspection request was rejected")
            )
        dataset_summary = dict(payload.get("dataset_summary") or {})
        if not dataset_summary:
            raise RuntimeError("Zarr service dataset inspection response did not include dataset_summary.")
        return dataset_summary

    def _get_json(self, path: str) -> tuple[int, dict[str, Any]]:
        response = self._session.get(f"{self.service_url}{path}", timeout=30)
        payload = self._response_payload(response)
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Zarr service returned a non-object payload for {path}.")
        return int(response.status_code), dict(payload)

    @staticmethod
    def _json_value(value: Any) -> Any:
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except TypeError:
                return value
        return value

    @staticmethod
    def _response_payload(response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, Mapping):
            return {}
        return dict(payload)

    @staticmethod
    def _error_detail(payload: Mapping[str, Any], response: requests.Response, *, prefix: str) -> str:
        detail = str(payload.get("detail") or "").strip()
        if detail:
            return f"{prefix}: {detail}"
        text = str(getattr(response, "text", "") or "").strip()
        if text:
            return f"{prefix}: {text}"
        return f"{prefix}: HTTP {response.status_code}"
