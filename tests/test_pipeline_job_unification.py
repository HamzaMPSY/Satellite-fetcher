from __future__ import annotations

import json
import shutil
import time
from datetime import date
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.client import NimbusFetcherClient
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import (
    ArtifactUpsertRequest,
    ArtifactType,
    JobConvertRequest,
    JobState,
    PipelineState,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.settings import get_settings
from nimbuschain_fetch_service.api.artifacts import router as artifacts_router
from nimbuschain_fetch_service.api.converter import router as converter_router


MASKED_ZARR_ARTIFACT_TYPE = "zarr_masked"


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


class FakeMaskService:
    def __init__(self, root: Path):
        self.root = root

    def apply_omniwater_to_zarr(
        self,
        *,
        job_id: str | None = None,
        zarr_uri: str,
        provider: str,
        collection: str,
        product_type: str | None,
        scene_id: str,
        acquisition_datetime: str | None,
        dataset_summary: dict[str, object],
        fail_on_error: bool = False,
        stage_callback=None,
    ) -> dict[str, object]:
        scene_root = self.root / scene_id
        scene_root.mkdir(parents=True, exist_ok=True)
        artifact_uri = scene_root / "water_mask.tif"
        status_path = scene_root / "water_mask_status.json"
        source_path = Path(zarr_uri)
        masked_zarr_uri = scene_root / f"{source_path.name}.masked.zarr"
        if masked_zarr_uri.exists():
            if masked_zarr_uri.is_dir():
                shutil.rmtree(masked_zarr_uri)
            else:
                masked_zarr_uri.unlink()
        if source_path.is_dir():
            shutil.copytree(source_path, masked_zarr_uri)
            (masked_zarr_uri / "masks").mkdir(parents=True, exist_ok=True)
            (masked_zarr_uri / "masks" / "water.txt").write_text("fake-water-mask", encoding="utf-8")
        artifact_uri.write_bytes(b"fake-water-mask")
        status_path.write_text(json.dumps({"status": "written"}), encoding="utf-8")
        if stage_callback is not None:
            stage_callback(
                "water_masking_started",
                {"job_id": job_id, "zarr_uri": zarr_uri, "scene_id": scene_id},
            )
            stage_callback(
                "water_masking_finished",
                {
                    "job_id": job_id,
                    "zarr_uri": zarr_uri,
                    "scene_id": scene_id,
                    "water_mask": {
                        "status": "written",
                        "input_zarr_uri": zarr_uri,
                        "output_zarr_uri": str(masked_zarr_uri),
                        "storage_mode": "derived_masked_zarr_copy",
                    },
                },
            )
        return {
            "status": "written",
            "reason": None,
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": str(masked_zarr_uri),
            "storage_mode": "derived_masked_zarr_copy",
            "input_bands": ["B04", "B03", "B02", "B08"],
            "artifact_uri": str(artifact_uri),
            "status_path": str(status_path),
            "work_dir": str(scene_root),
            "shape": [1, 16, 16],
            "dtype": "uint8",
            "classes": {"0": "non-water", "1": "water"},
            "model_name": "omniwatermask",
            "model_version": "test",
            "written_at": "2026-01-01T00:00:00+00:00",
            "provider": provider,
            "collection": collection,
            "product_type": product_type,
            "scene_id": scene_id,
            "acquisition_datetime": acquisition_datetime,
            "dataset_summary": dataset_summary,
            "fail_on_error": fail_on_error,
        }


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


def _write_local_zarr_store(
    root: Path,
    *,
    store_name: str,
    provider: str = "copernicus",
    collection: str = "SENTINEL-2",
    scene_id: str = "S2A_MSIL2A_20260322T104741_N0512_R051_T31UDQ_20260322T164209",
) -> Path:
    store_path = root / "zarr" / f"{store_name}.zarr"
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
                    "source_uri": f"/downloads/raw/{store_name}.SAFE.zip",
                    "product_type": "S2MSI2A",
                    "band_names": ["B02", "B03", "B04", "B08"],
                    "data_family": "optical",
                    "crs": "EPSG:32631",
                    "transform": [10.0, 0.0, 399960.0, 0.0, -10.0, 5300040.0],
                },
                "consolidated_metadata": {
                    "metadata": {
                        "imagery": {
                            "shape": [1, 4, 8, 8],
                            "dimension_names": ["time", "band", "y", "x"],
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return store_path


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

    request = {
        "raw_uri": status.raw_outputs[0],
        "output_uri": str(settings.nimbus_data_dir / "zarr" / "manual-conversion.zarr"),
        "scene_id": "manual-conversion",
        "product_type": "S2MSI2A",
    }
    with TestClient(app) as test_client:
        response = test_client.post(f"/v1/jobs/{job_id}/convert", json=request)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["job_id"] == job_id
    assert body["pipeline_state"] == PipelineState.zarr_written.value
    assert body["zarr_outputs"]

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 1, "Manual conversion must update the same pipeline job, not create a new one."

    result = client.get_result(job_id)
    assert result.zarr_outputs == body["zarr_outputs"]
    assert result.raw_outputs == status.raw_outputs

    artifacts = client.list_artifacts(
        artifact_type=ArtifactType.zarr.value,
        job_id=job_id,
        page=1,
        page_size=20,
    )
    uris = {item.artifact_uri for item in artifacts.items}
    assert status.zarr_outputs[0] in uris
    assert all(item.created_by_job_id == job_id for item in artifacts.items)
    assert all(item.source_job_id == job_id for item in artifacts.items)


def test_manual_watermask_route_reuses_existing_job_lineage(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    fetcher._mask_service = FakeMaskService(settings.nimbus_data_dir.parent / "watermask")

    job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{job_id}/water-mask",
            json={"zarr_uri": status.zarr_outputs[0]},
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["job_id"] == job_id
    assert body["zarr_uri"] == status.zarr_outputs[0]
    assert body["water_mask"]["status"] == "written"
    assert body["water_mask"]["input_zarr_uri"] == status.zarr_outputs[0]
    assert body["water_mask"]["output_zarr_uri"] != status.zarr_outputs[0]
    assert body["job"]["job_id"] == job_id
    assert body["watermask_outputs"]
    assert body["water_mask"]["output_zarr_uri"] in body["watermask_outputs"]

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 1, "Manual watermask must update the same pipeline job, not create a new one."

    result = client.get_result(job_id)
    assert result.zarr_outputs == status.zarr_outputs
    assert result.watermask_outputs == body["watermask_outputs"]

    masked_artifacts = client.list_artifacts(
        artifact_type=MASKED_ZARR_ARTIFACT_TYPE,
        job_id=job_id,
        page=1,
        page_size=20,
    )
    assert masked_artifacts.total == 1
    assert masked_artifacts.items[0].artifact_uri == body["water_mask"]["output_zarr_uri"]
    assert masked_artifacts.items[0].created_by_job_id == job_id
    assert masked_artifacts.items[0].source_job_id == job_id

    artifacts = client.list_artifacts(
        artifact_type=ArtifactType.watermask.value,
        job_id=job_id,
        page=1,
        page_size=20,
    )
    assert artifacts.total == 1
    assert artifacts.items[0].artifact_uri == body["water_mask"]["artifact_uri"]
    assert artifacts.items[0].created_by_job_id == job_id
    assert artifacts.items[0].source_job_id == job_id


def test_include_local_artifacts_route_discovers_existing_zarr_store(tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "api",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    fetcher = NimbusFetcher(settings=settings)
    _write_local_zarr_store(settings.nimbus_data_dir, store_name="local-s2-store")

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(artifacts_router)

    with TestClient(app) as test_client:
        response = test_client.get("/v1/artifacts", params={"include_local": "true", "artifact_type": "zarr"})

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["artifact_type"] == ArtifactType.zarr.value
    assert body["items"][0]["artifact_uri"].endswith("local-s2-store.zarr")
    assert body["items"][0]["metadata"]["discovered_local"] is True


def test_include_local_artifacts_route_coalesces_app_data_downloads_aliases(tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "api",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    fetcher = NimbusFetcher(settings=settings)
    local_store = _write_local_zarr_store(settings.nimbus_data_dir, store_name="aliased-store")
    fetcher.upsert_artifact(
        ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri="/app/data/downloads/zarr/aliased-store.zarr",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            scene_id="aliased-scene",
            source_uri="/app/data/downloads/raw/aliased-scene.SAFE.zip",
            metadata={},
        )
    )

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(artifacts_router)

    with TestClient(app) as test_client:
        response = test_client.get("/v1/artifacts", params={"include_local": "true", "artifact_type": "zarr"})

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["artifact_uri"] == str(local_store)
    assert body["items"][0]["metadata"]["runtime_exists"] is True


def test_include_local_artifacts_route_filters_stale_registered_local_store(tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "api",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    fetcher = NimbusFetcher(settings=settings)
    fetcher.upsert_artifact(
        ArtifactUpsertRequest(
            artifact_type=ArtifactType.zarr,
            artifact_uri="/app/data/downloads/zarr/missing-store.zarr",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            scene_id="missing-scene",
            source_uri="/app/data/downloads/raw/missing-scene.SAFE.zip",
            metadata={},
        )
    )

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(artifacts_router)

    with TestClient(app) as test_client:
        response = test_client.get("/v1/artifacts", params={"include_local": "true", "artifact_type": "zarr"})

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 0
    assert body["items"] == []


def test_manual_watermask_route_returns_structured_500_on_runtime_error(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    class ExplodingMaskService:
        def apply_omniwater_to_zarr(self, **_kwargs):
            raise RuntimeError("simulated watermask crash")

    fetcher._mask_service = ExplodingMaskService()

    job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{job_id}/water-mask",
            json={"zarr_uri": status.zarr_outputs[0]},
        )

    assert response.status_code == 500, response.text
    assert "simulated watermask crash" in response.text

    current = client.get_job(job_id)
    assert current.pipeline_state == PipelineState.zarr_written
    assert current.zarr_outputs == status.zarr_outputs
    result = client.get_result(job_id)
    assert result.watermask_outputs == []
