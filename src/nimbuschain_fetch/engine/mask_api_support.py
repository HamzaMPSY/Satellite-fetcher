from __future__ import annotations

from typing import Any

from nimbuschain_fetch.models import (
    JobCloudMaskRequest,
    JobCloudMaskResponse,
    JobMaskRequest,
    JobWaterMaskRequest,
    JobWaterMaskResponse,
)


class FetcherMaskApiSupport:
    """Mask API request/response adapter helpers for the fetcher facade."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def apply_watermask_existing_job(
        self,
        job_id: str,
        request: JobWaterMaskRequest,
    ) -> JobWaterMaskResponse:
        response = self._rt.apply_mask_existing_job(
            job_id,
            JobMaskRequest.model_validate(request.model_dump(mode="python")),
        )
        return JobWaterMaskResponse.model_validate(response.model_dump(mode="python"))

    def apply_cloud_mask_existing_job(
        self,
        job_id: str,
        request: JobCloudMaskRequest,
    ) -> JobCloudMaskResponse:
        payload = request.model_dump(mode="python")
        payload["mask_types"] = ["cloud"]
        response = self._rt.apply_mask_existing_job(
            job_id,
            JobMaskRequest.model_validate(payload),
        )
        return JobCloudMaskResponse.model_validate(response.model_dump(mode="python"))
