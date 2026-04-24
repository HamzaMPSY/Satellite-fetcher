from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.engine.nimbus_fetcher import JobNotFoundError
from nimbuschain_fetch.models import JobResumeResponse, JobStatusResponse, ProviderName
from nimbuschain_fetch_service.api.jobs import router as jobs_router


class _FakeFetcher:
    def __init__(self, *, response: JobResumeResponse | None = None, error: Exception | None = None):
        self._response = response
        self._error = error

    def resume_job(self, job_id: str) -> JobResumeResponse:
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response


def test_resume_job_route_returns_resume_payload() -> None:
    app = FastAPI()
    app.state.fetcher = _FakeFetcher(
        response=JobResumeResponse(
            source_job_id="job-123",
            resumed_job_id="job-123",
            resume_action="resume_pipeline_from_zarr",
            resume_label="Retry Zarr Conversion",
            spawned_new_job=False,
            message="Resumed the existing job and continued the remaining pipeline stages.",
            job=JobStatusResponse(
                job_id="job-123",
                job_type="search_download",
                job_kind="fetch",
                service_name="fetch_service",
                source_job_id=None,
                state="succeeded",
                pipeline_state="zarr_written",
                pipeline_step="zarr_written",
                provider=ProviderName.copernicus,
                collection="SENTINEL-2",
            ),
        )
    )
    app.include_router(jobs_router)

    with TestClient(app) as client:
        response = client.post("/v1/jobs/job-123/resume")

    assert response.status_code == 200
    payload = response.json()
    assert payload["resume_action"] == "resume_pipeline_from_zarr"
    assert payload["job"]["pipeline_state"] == "zarr_written"


def test_resume_job_route_returns_409_for_non_resumable_job() -> None:
    app = FastAPI()
    app.state.fetcher = _FakeFetcher(error=ValueError("This job cannot be resumed from its current pipeline state."))
    app.include_router(jobs_router)

    with TestClient(app) as client:
        response = client.post("/v1/jobs/job-123/resume")

    assert response.status_code == 409
    assert response.json()["detail"] == "This job cannot be resumed from its current pipeline state."


def test_resume_job_route_returns_404_for_missing_job() -> None:
    app = FastAPI()
    app.state.fetcher = _FakeFetcher(error=JobNotFoundError("job-404"))
    app.include_router(jobs_router)

    with TestClient(app) as client:
        response = client.post("/v1/jobs/job-404/resume")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job 'job-404' not found."
