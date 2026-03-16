from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.client import NimbusFetcherClient
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import (
    ArtifactType,
    JobConvertRequest,
    JobState,
    PipelineState,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.settings import get_settings
from nimbuschain_fetch_service.api.converter import router as converter_router


class FakeCopernicusProvider:
    def __init__(self, settings, download_manager):
        self.settings = settings
        self.download_manager = download_manager

    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi,
        tile_id: str | None = None,
    ) -> list[str]:
        return ["FAKE_PRODUCT_1"]

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        raw_path = output_path / "S2A_MSIL2A_FAKE_SCENE.SAFE.zip"
        raw_path.write_bytes(b"fake-raw-scene")
        return [str(raw_path)]


class FakeZarrConverter:
    def convert(
        self,
        *,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
    ) -> tuple[str, str, dict[str, object], dict[str, object]]:
        store_path = Path(output_uri)
        store_path.mkdir(parents=True, exist_ok=True)
        (store_path / "zarr.json").write_text(
            json.dumps(
                {
                    "zarr_format": 3,
                    "node_type": "group",
                    "attributes": {
                        "provider": provider,
                        "collection": collection,
                        "scene_id": scene_id,
                        "source_uri": raw_uri,
                        "product_type": product_type,
                        "band_names": ["B02", "B03", "B04", "B08"],
                    },
                    "consolidated_metadata": {
                        "metadata": {
                            "imagery": {
                                "shape": [1, 4, 16, 16],
                                "dimension_names": ["time", "band", "y", "x"],
                            }
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return (
            str(store_path),
            "optical",
            {
                "status": "written",
                "scene_id": scene_id,
                "raw_uri": raw_uri,
                "provider": provider,
                "collection": collection,
                "product_type": product_type,
            },
            {
                "dimensions": ["time", "band", "y", "x"],
                "shape": [1, 4, 16, 16],
                "band_names": ["B02", "B03", "B04", "B08"],
            },
        )


@pytest.fixture()
def pipeline_runtime(tmp_path: Path):
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"copernicus": FakeCopernicusProvider},
    )
    fetcher._zarr_converter = FakeZarrConverter()
    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        yield {"settings": settings, "fetcher": fetcher, "client": client}


def _wait_for_completion(client: NimbusFetcherClient, job_id: str, timeout_seconds: float = 10.0):
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while time.monotonic() < deadline:
        status = client.get_job(job_id)
        last_status = status
        if status.state in {JobState.succeeded, JobState.failed, JobState.cancelled}:
            return status
        time.sleep(0.1)
    pytest.fail(f"Job {job_id} did not finish in time. Last status: {last_status}")


def _request_payload() -> SearchDownloadRequest:
    return SearchDownloadRequest.model_validate(
        {
            "job_type": "search_download",
            "provider": ProviderName.copernicus,
            "collection": "SENTINEL-2",
            "product_type": "S2MSI2A",
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 1, 2),
            "aoi": {
                "wkt": "POLYGON((2.30 48.80, 2.30 48.90, 2.40 48.90, 2.40 48.80, 2.30 48.80))"
            },
            "output_dir": "integration/unified-pipeline",
        }
    )


def test_single_job_runs_download_and_zarr_in_one_pipeline(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]

    job_id = client.submit_job(_request_payload())
    final_status = _wait_for_completion(client, job_id)
    result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.zarr_written
    assert final_status.pipeline_step == "zarr_written"
    assert final_status.raw_outputs, "Expected raw outputs on the same pipeline job."
    assert final_status.zarr_outputs, "Expected Zarr outputs on the same pipeline job."

    assert result.job_id == job_id
    assert result.raw_outputs == final_status.raw_outputs
    assert result.zarr_outputs == final_status.zarr_outputs
    assert len(result.paths) == 3, "Expected raw file, manifest, and zarr output in result paths."
    assert any(path.endswith("manifest.json") for path in result.paths)

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 1, "Automatic Zarr conversion must not create a second independent job."

    artifacts = client.list_artifacts(
        artifact_type=ArtifactType.zarr.value,
        job_id=job_id,
        page=1,
        page_size=20,
    )
    assert artifacts.total == 1
    artifact = artifacts.items[0]
    assert artifact.created_by_job_id == job_id
    assert artifact.source_job_id == job_id
    assert artifact.artifact_uri == final_status.zarr_outputs[0]

    event_types = {row["type"] for row in fetcher.store.list_events(job_id, None, 200)}
    assert "job.searching" in event_types
    assert "job.downloaded" in event_types
    assert "job.zarr_converting" in event_types
    assert "job.zarr_written" in event_types


def test_manual_conversion_route_reuses_existing_job_lineage(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, job_id)
    assert status.state == JobState.succeeded
    assert status.raw_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    manual_output = settings.nimbus_data_dir / "zarr" / "manual-conversion.zarr"
    request = {
        "raw_uri": status.raw_outputs[0],
        "output_uri": str(manual_output),
        "scene_id": "manual-conversion",
        "product_type": "S2MSI2A",
    }
    with TestClient(app) as test_client:
        response = test_client.post(f"/v1/jobs/{job_id}/convert", json=request)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["job_id"] == job_id
    assert body["pipeline_state"] == PipelineState.zarr_written.value
    assert body["zarr_outputs"] == [str(manual_output)]

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 1, "Manual conversion must update the same pipeline job, not create a new one."

    result = client.get_result(job_id)
    assert result.zarr_outputs == [str(manual_output)]
    assert result.raw_outputs == status.raw_outputs

    artifacts = client.list_artifacts(
        artifact_type=ArtifactType.zarr.value,
        job_id=job_id,
        page=1,
        page_size=20,
    )
    uris = {item.artifact_uri for item in artifacts.items}
    assert str(manual_output) in uris
    assert status.zarr_outputs[0] in uris
    assert all(item.created_by_job_id == job_id for item in artifacts.items)
    assert all(item.source_job_id == job_id for item in artifacts.items)
