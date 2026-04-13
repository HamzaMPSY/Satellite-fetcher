from __future__ import annotations

import threading
import time
from typing import Any

import requests

from nimbuschain_mask_service.contracts import MaskApplyRequest
from nimbuschain_mask_service.service import MaskService, support_status
from nimbuschain_mask_service.schema import default_mask_model


class MaskServiceClient:
    REMOTE_PROGRESS_POLL_SECONDS = 2.0
    REMOTE_APPLY_MAX_RETRIES = 1
    REMOTE_APPLY_RESTART_WAIT_SECONDS = 30.0
    REMOTE_APPLY_RESTART_POLL_SECONDS = 1.0

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
        return True

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
        progress_stop = threading.Event()
        progress_thread: threading.Thread | None = None
        remote_job_id = str(extras.get("job_id") or "").strip()
        stage_callback = extras.get("stage_callback")
        if remote_job_id and callable(stage_callback):
            progress_thread = threading.Thread(
                target=self._poll_remote_progress,
                kwargs={
                    "job_id": remote_job_id,
                    "stage_callback": stage_callback,
                    "stop_event": progress_stop,
                },
                name=f"mask-progress-{remote_job_id[:8]}",
                daemon=True,
            )
            progress_thread.start()
        try:
            request_kwargs: dict[str, Any] = {
                "json": request.model_dump(mode="json", exclude_none=True),
                "timeout": (30, None),
            }
            if remote_job_id:
                request_kwargs["params"] = {"job_id": remote_job_id}
            apply_url = f"{self.service_url}/apply"
            for attempt in range(self.REMOTE_APPLY_MAX_RETRIES + 1):
                try:
                    response = self._session.post(
                        apply_url,
                        **request_kwargs,
                    )
                    response.raise_for_status()
                    return dict(response.json())
                except requests.RequestException as exc:
                    should_retry = (
                        attempt < self.REMOTE_APPLY_MAX_RETRIES
                        and self._is_retryable_remote_error(exc)
                        and self._wait_for_remote_health()
                    )
                    if should_retry:
                        continue
                    detail = str(exc).strip() or exc.__class__.__name__
                    raise RuntimeError(
                        "Mask service request failed. The remote mask process may have restarted, exhausted memory, "
                        f"or closed the connection unexpectedly. Original error: {detail}"
                    ) from exc
        except requests.RequestException as exc:
            detail = str(exc).strip() or exc.__class__.__name__
            raise RuntimeError(
                "Mask service request failed. The remote mask process may have restarted, exhausted memory, "
                f"or closed the connection unexpectedly. Original error: {detail}"
            ) from exc
        finally:
            progress_stop.set()
            if progress_thread is not None:
                progress_thread.join(timeout=2.0)

    def health(self) -> dict[str, Any]:
        if self._service is not None:
            return {"status": "ok", "internal_only": True, **support_status()}
        assert self._session is not None
        response = self._session.get(f"{self.service_url}/health", timeout=30)
        response.raise_for_status()
        return dict(response.json())

    def _wait_for_remote_health(self) -> bool:
        assert self._session is not None
        health_get = getattr(self._session, "get", None)
        if not callable(health_get):
            return False
        deadline = time.monotonic() + float(self.REMOTE_APPLY_RESTART_WAIT_SECONDS)
        while time.monotonic() < deadline:
            try:
                response = health_get(
                    f"{self.service_url}/health",
                    timeout=5,
                )
                response.raise_for_status()
                return True
            except requests.RequestException:
                time.sleep(float(self.REMOTE_APPLY_RESTART_POLL_SECONDS))
        return False

    @staticmethod
    def _is_retryable_remote_error(exc: requests.RequestException) -> bool:
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            return True
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in {502, 503, 504}:
            return True
        detail = str(exc).strip().lower()
        return any(
            pattern in detail
            for pattern in (
                "connection aborted",
                "connection reset",
                "remote end closed connection without response",
                "temporarily unavailable",
            )
        )

    def schema(self) -> dict[str, Any]:
        if self._service is not None:
            return {"status": "ok", "internal_only": True, "mask_model": default_mask_model()}
        assert self._session is not None
        response = self._session.get(f"{self.service_url}/schema", timeout=30)
        response.raise_for_status()
        return dict(response.json())

    def _poll_remote_progress(
        self,
        *,
        job_id: str,
        stage_callback: Any,
        stop_event: threading.Event,
    ) -> None:
        assert self._session is not None
        poll_interval = float(self.REMOTE_PROGRESS_POLL_SECONDS)
        last_sequence = -1
        last_stage_name = ""
        last_payload: dict[str, Any] = {}
        while not stop_event.wait(poll_interval):
            try:
                response = self._session.get(
                    f"{self.service_url}/progress/{job_id}",
                    timeout=10,
                )
                if getattr(response, "status_code", None) == 404:
                    continue
                response.raise_for_status()
                progress = dict(response.json() or {})
            except requests.RequestException:
                continue
            stage_name = str(progress.get("stage_name") or "").strip()
            payload = dict(progress.get("payload") or {})
            try:
                sequence = int(progress.get("sequence") or 0)
            except (TypeError, ValueError):
                sequence = 0
            status = str(progress.get("status") or "").strip().lower()

            if stage_name and callable(stage_callback):
                if sequence > last_sequence or stage_name != last_stage_name or payload != last_payload:
                    stage_callback(stage_name, payload)
                    last_sequence = sequence
                    last_stage_name = stage_name
                    last_payload = dict(payload)
                elif last_stage_name:
                    heartbeat_payload = dict(last_payload)
                    heartbeat_payload["heartbeat"] = True
                    stage_callback(last_stage_name, heartbeat_payload)

            if status in {"finished", "failed", "cancelled"}:
                return
