from __future__ import annotations

from typing import Any

import requests

from nimbuschain_mask_service.contracts import MaskApplyRequest
from nimbuschain_mask_service.service import MaskService, support_status
from nimbuschain_mask_service.schema import default_mask_model


class MaskServiceClient:
    def __init__(self, *, service_url: str | None = None):
        self.service_url = str(service_url or "").strip().rstrip("/")
        self._session: requests.Session | None = None
        self._service: MaskService | None = None
        if self.service_url:
            self._session = requests.Session()
        else:
            self._service = MaskService()

    @property
    def supports_stage_callbacks(self) -> bool:
        return self._service is not None

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    @staticmethod
    def _coerce_apply_request(kwargs: dict[str, Any]) -> tuple[MaskApplyRequest, dict[str, Any]]:
        payload = dict(kwargs or {})
        job_id = payload.pop("job_id", None)
        stage_callback = payload.pop("stage_callback", None)

        source_zarr_uri = payload.pop("source_zarr_uri", None) or payload.pop("zarr_uri", None)
        cloud_payload = dict(payload.pop("cloud", {}) or {})
        water_payload = dict(payload.pop("water", {}) or {})

        if payload.get("backend") is not None:
            cloud_payload.setdefault("backend", payload.pop("backend"))
        if payload.get("threshold") is not None:
            cloud_payload.setdefault("threshold", payload.pop("threshold"))
        if payload.get("overwrite") is not None:
            overwrite_value = payload.pop("overwrite")
            cloud_payload.setdefault("overwrite", overwrite_value)
            water_payload.setdefault("overwrite", overwrite_value)
        if payload.get("inference_device") is not None:
            inference_device = payload.pop("inference_device")
            cloud_payload.setdefault("inference_device", inference_device)
            water_payload.setdefault("inference_device", inference_device)
        if payload.get("include_shadows") is not None:
            cloud_payload.setdefault("include_shadows", payload.pop("include_shadows"))

        if payload.get("water_backend") is not None:
            water_payload.setdefault("backend", payload.pop("water_backend"))
        if payload.get("water_overwrite") is not None:
            water_payload.setdefault("overwrite", payload.pop("water_overwrite"))
        if payload.get("water_inference_device") is not None:
            water_payload.setdefault("inference_device", payload.pop("water_inference_device"))

        request = MaskApplyRequest.model_validate(
            {
                "source_zarr_uri": source_zarr_uri,
                "output_zarr_uri": payload.get("output_zarr_uri"),
                "provider": payload.get("provider"),
                "collection": payload.get("collection"),
                "product_type": payload.get("product_type"),
                "scene_id": payload.get("scene_id"),
                "acquisition_datetime": payload.get("acquisition_datetime"),
                "dataset_summary": dict(payload.get("dataset_summary") or {}),
                "mask_types": list(payload.get("mask_types") or []),
                "fail_on_error": bool(payload.get("fail_on_error", False)),
                "cloud": cloud_payload,
                "water": water_payload,
            }
        )
        extras = {
            "job_id": job_id,
            "stage_callback": stage_callback,
        }
        return request, extras

    def apply_masks_to_zarr(self, **kwargs: Any) -> dict[str, Any]:
        request, extras = self._coerce_apply_request(kwargs)
        if self._service is not None:
            return self._service.apply_masks_to_zarr(
                job_id=extras.get("job_id"),
                zarr_uri=request.source_zarr_uri,
                provider=request.provider,
                collection=request.collection,
                product_type=request.product_type,
                scene_id=request.scene_id,
                acquisition_datetime=request.acquisition_datetime,
                dataset_summary=request.dataset_summary,
                mask_types=request.mask_types,
                output_zarr_uri=request.output_zarr_uri,
                fail_on_error=request.fail_on_error,
                backend=request.cloud.backend,
                threshold=request.cloud.threshold,
                overwrite=request.cloud.overwrite,
                inference_device=request.cloud.inference_device,
                include_shadows=request.cloud.include_shadows,
                water_backend=request.water.backend,
                water_overwrite=request.water.overwrite,
                water_inference_device=request.water.inference_device,
                stage_callback=extras.get("stage_callback"),
            )
        assert self._session is not None
        response = self._session.post(
            f"{self.service_url}/apply",
            json=request.model_dump(mode="json", exclude_none=True),
            timeout=(30, None),
        )
        response.raise_for_status()
        return dict(response.json())

    def health(self) -> dict[str, Any]:
        if self._service is not None:
            return {"status": "ok", "internal_only": True, **support_status()}
        assert self._session is not None
        response = self._session.get(f"{self.service_url}/health", timeout=30)
        response.raise_for_status()
        return dict(response.json())

    def schema(self) -> dict[str, Any]:
        if self._service is not None:
            return {"status": "ok", "internal_only": True, "mask_model": default_mask_model()}
        assert self._session is not None
        response = self._session.get(f"{self.service_url}/schema", timeout=30)
        response.raise_for_status()
        return dict(response.json())
