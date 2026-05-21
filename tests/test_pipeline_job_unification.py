from __future__ import annotations

import json
import threading
import time
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import anyio
import nimbuschain_fetch.application.sen2like_normalization as sen2like_normalization
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nimbuschain_fetch.client import NimbusFetcherClient
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.engine.zarr_context_support import FetcherZarrContextSupport
from nimbuschain_fetch.models import (
    ArtifactUpsertRequest,
    ArtifactType,
    JobState,
    PipelineState,
    ProviderName,
    SearchDownloadRequest,
)
from nimbuschain_fetch.ports import ProviderCapabilities
from nimbuschain_fetch.settings import Settings, get_settings
from nimbuschain_fetch_service.api.artifacts import router as artifacts_router
from nimbuschain_fetch_service.api.converter import router as converter_router


class FakeCopernicusProvider:
    def __init__(self, settings, download_manager):
        self.settings = settings
        self.download_manager = download_manager

    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities()

    def configure_job(self, *, collection=None, product_type=None, download_strategy="default") -> None:
        _ = (collection, product_type, download_strategy)

    def plan_download_metadata(self, product_count: int) -> dict[str, object]:
        _ = product_count
        return {}

    def download_metadata(self) -> dict[str, object]:
        return {}

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


class FakeUsgsProvider(FakeCopernicusProvider):
    def search_products(
        self,
        collection: str,
        product_type: str,
        start_date: str,
        end_date: str,
        aoi,
        tile_id: str | None = None,
    ) -> list[str]:
        return ["LC08_FAKE_LANDSAT_SCENE"]

    def download_products(self, product_ids: list[str], output_dir: str) -> list[str]:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        raw_path = output_path / "LC08_FAKE_LANDSAT_SCENE.tar"
        raw_path.write_bytes(b"fake-landsat-scene")
        return [str(raw_path)]


class FakeZarrConverter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def convert(
        self,
        *,
        provider: str,
        collection: str,
        scene_id: str,
        raw_uri: str,
        output_uri: str,
        product_type: str | None = None,
        progress_callback=None,
    ) -> tuple[str, str, dict[str, object], dict[str, object]]:
        self.calls.append(
            {
                "provider": provider,
                "collection": collection,
                "scene_id": scene_id,
                "raw_uri": raw_uri,
                "output_uri": output_uri,
                "product_type": product_type,
            }
        )
        if progress_callback is not None:
            for index, fraction in enumerate((0.2, 0.5, 0.8, 1.0), start=1):
                progress_callback(
                    {
                        "stage": "writing_chunks",
                        "array_name": "product",
                        "source_array_name": "imagery",
                        "fraction": fraction,
                        "blocks_written": index,
                        "total_blocks": 4,
                        "band_name": "B04",
                    }
                )
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


class DailyMosaicRejectingZarrConverter(FakeZarrConverter):
    def build_grouped_cubes(
        self,
        *,
        cube_layout: str = "grouped_time",
        **_kwargs,
    ) -> dict[str, object]:
        if cube_layout == "daily_mosaic":
            raise ValueError(
                "Zarr cube build request was rejected: Daily mosaic cube is currently "
                "supported only for Sentinel-2 scene Zarr inputs."
            )
        return {
            "status": "skipped",
            "reason": "no_groups_with_multiple_times",
            "cube_outputs": [],
            "items": [],
            "tiles_built": [],
            "tiles_skipped": [],
        }


class FakeSen2LikeClient:
    calls: list[dict[str, object]] = []

    def __init__(self, *, service_url: str):
        self.service_url = service_url

    def close(self) -> None:
        return None

    def normalize(
        self,
        *,
        products,
        job_id=None,
        pipeline_id=None,
        working_dir=None,
        workers=4,
        timeout_seconds=None,
        **_kwargs,
    ) -> dict[str, object]:
        outputs = []
        base_dir = Path(str(working_dir or "/tmp/nimbus-sen2like-test"))
        for product in list(products or []):
            normalized_dir = base_dir / f"{Path(str(product)).stem}_L2F"
            normalized_dir.mkdir(parents=True, exist_ok=True)
            outputs.append(
                {
                    "product": str(product),
                    "output_dir": str(normalized_dir),
                    "normalized_uri": str(normalized_dir),
                    "exists": True,
                }
            )
        payload = {
            "service_url": self.service_url,
            "products": [str(item) for item in list(products or [])],
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "working_dir": str(working_dir or ""),
            "workers": int(workers),
            "timeout_seconds": timeout_seconds,
            "no_resume": bool(_kwargs.get("no_resume", False)),
            "outputs": outputs,
            "duration_seconds": 1.25,
            "status": "succeeded",
            "return_code": 0,
        }
        type(self).calls.append(payload)
        return payload


class DirectZarrSen2LikeClient(FakeSen2LikeClient):
    def normalize(self, **kwargs) -> dict[str, object]:
        payload = super().normalize(**kwargs)
        direct_items: list[dict[str, object]] = []
        for output in list(payload.get("outputs") or []):
            if not isinstance(output, dict):
                continue
            normalized_uri = str(output.get("normalized_uri") or "")
            scene_id = Path(normalized_uri).stem or "scene"
            zarr_uri = str(Path(str(kwargs.get("working_dir") or "/tmp")) / "direct-zarr" / f"{scene_id}.zarr")
            Path(zarr_uri).mkdir(parents=True, exist_ok=True)
            dataset_summary = {
                "dimensions": ["time", "band", "y", "x"],
                "shape": [1, 6, 512, 512],
                "band_names": ["B02", "B03", "B04", "B08", "B11", "B12"],
            }
            summary = {"status": "written", "scene_id": scene_id}
            output.update(
                {
                    "zarr_uri": zarr_uri,
                    "zarr_exists": True,
                    "zarr_data_family": "optical",
                    "zarr_summary": summary,
                    "zarr_dataset_summary": dataset_summary,
                }
            )
            direct_items.append(
                {
                    "normalized_uri": normalized_uri,
                    "scene_id": scene_id,
                    "zarr_uri": zarr_uri,
                    "data_family": "optical",
                    "summary": summary,
                    "dataset_summary": dataset_summary,
                }
            )
        payload["metadata"] = {
            "direct_zarr_status": "written",
            "direct_zarr_outputs": [str(item["zarr_uri"]) for item in direct_items],
            "direct_zarr_items": direct_items,
            "direct_zarr_duration_seconds": 0.25,
        }
        return payload


class FailingSen2LikeClient(FakeSen2LikeClient):
    def normalize(self, **_kwargs) -> dict[str, object]:
        type(self).calls.append(dict(_kwargs))
        raise RuntimeError("sen2like exploded")


class MemoryKilledSen2LikeClient(FakeSen2LikeClient):
    def normalize(self, **_kwargs) -> dict[str, object]:
        type(self).calls.append(dict(_kwargs))
        raise RuntimeError(
            "Sen2Like was killed during processing, most likely because the "
            "Podman VM or Sen2Like container does not have enough memory."
        )


class FakeMaskService:
    def __init__(self, root: Path):
        self.root = root

    def apply_masks_to_zarr(
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
        mask_types: list[str],
        backend: str = "auto",
        threshold: float = 0.45,
        overwrite: bool = True,
        inference_device: str | None = None,
        include_shadows: bool = True,
        fail_on_error: bool = False,
        stage_callback=None,
    ) -> dict[str, object]:
        source_path = Path(zarr_uri)
        water_mask = {}
        cloud_mask = {}
        watermask_outputs: list[str] = []
        cloudmask_outputs: list[str] = []
        if "water" in mask_types:
            water_mask = self.apply_omniwater_to_zarr(
                job_id=job_id,
                zarr_uri=zarr_uri,
                provider=provider,
                collection=collection,
                product_type=product_type,
                scene_id=scene_id,
                acquisition_datetime=acquisition_datetime,
                dataset_summary=dataset_summary,
                fail_on_error=fail_on_error,
                stage_callback=stage_callback,
            )
            watermask_outputs = [
                value for value in [
                    str(water_mask.get("artifact_uri") or "").strip(),
                    str(water_mask.get("status_path") or "").strip(),
                ] if value
            ]
        if "cloud" in mask_types:
            if source_path.is_dir():
                (source_path / "masks").mkdir(parents=True, exist_ok=True)
                (source_path / "masks" / "cloud.txt").write_text("fake-cloud-mask", encoding="utf-8")
            cloud_mask = {
                "status": "written",
                "backend": backend,
                "threshold": threshold,
                "includes_shadows": include_shadows,
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": zarr_uri,
                "storage_mode": "in_place_zarr_masking",
                "artifact_uri": None,
                "status_path": None,
                "mask_path": "masks/cloud",
                "probability_path": "masks/cloud_probability",
                "shape": [1, 16, 16],
            }
        status = "written" if all(
            str(payload.get("status") or "").strip().lower() == "written"
            for payload in (water_mask, cloud_mask)
            if payload
        ) else "failed"
        return {
            "status": status,
            "mask_types": list(mask_types),
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "masked_zarr_uri": zarr_uri,
            "masked_zarr_outputs": [zarr_uri],
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            "watermask_outputs": watermask_outputs,
            "cloudmask_outputs": cloudmask_outputs,
        }

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
        source_path = Path(zarr_uri)
        if source_path.is_dir():
            (source_path / "masks").mkdir(parents=True, exist_ok=True)
            (source_path / "masks" / "water.txt").write_text("fake-water-mask", encoding="utf-8")
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
                        "output_zarr_uri": zarr_uri,
                        "storage_mode": "in_place_zarr_masking",
                    },
                },
            )
        return {
            "status": "written",
            "reason": None,
            "source_job_id": job_id,
            "source_zarr_uri": zarr_uri,
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "storage_mode": "in_place_zarr_masking",
            "input_bands": ["B04", "B03", "B02", "B08"],
            "artifact_uri": None,
            "status_path": None,
            "work_dir": None,
            "mask_path": "masks/water",
            "probability_path": "masks/water_probability",
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


class RemoteLikeMaskService:
    supports_stage_callbacks = False

    def __init__(self, fetcher: NimbusFetcher):
        self.fetcher = fetcher
        self.calls: list[dict[str, object]] = []

    def apply_masks_to_zarr(
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
        mask_types: list[str],
        **_kwargs,
    ) -> dict[str, object]:
        self.calls.append(dict(_kwargs))
        row = self.fetcher.store.get_job(str(job_id))
        assert row is not None
        expected_pipeline = (
            PipelineState.running_cloud_inference.value
            if "cloud" in mask_types
            else PipelineState.running_water_inference.value
        )
        assert row["pipeline_state"] == expected_pipeline
        water_mask = (
            {
                "status": "written",
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": zarr_uri,
                "storage_mode": "in_place_zarr_masking",
                "mask_path": "masks/water",
                "probability_path": "masks/water_probability",
                "water_fraction": 0.2,
            }
            if "water" in mask_types
            else {}
        )
        cloud_mask = (
            {
                "status": "written",
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": zarr_uri,
                "storage_mode": "in_place_zarr_masking",
                "mask_path": "masks/cloud",
                "probability_path": "masks/cloud_probability",
                "backend": "omnicloudmask",
                "cloud_fraction": 0.3,
                "cloud_only_fraction": 0.2,
                "shadow_fraction": 0.1,
            }
            if "cloud" in mask_types
            else {}
        )
        return {
            "status": "written",
            "mask_types": list(mask_types),
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "masked_zarr_uri": zarr_uri,
            "masked_zarr_outputs": [zarr_uri],
            "water_mask": water_mask,
            "cloud_mask": cloud_mask,
            "watermask_outputs": [],
            "cloudmask_outputs": [],
        }


class ControlledMaskService:
    def __init__(self, *, water_started: threading.Event, allow_water_finish: threading.Event):
        self.water_started = water_started
        self.allow_water_finish = allow_water_finish

    def apply_masks_to_zarr(
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
        mask_types: list[str],
        stage_callback=None,
        **_kwargs,
    ) -> dict[str, object]:
        water_mask = {
            "status": "written",
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "storage_mode": "in_place_zarr_masking",
            "mask_path": "masks/water",
            "probability_path": "masks/water_probability",
            "water_fraction": 0.2,
        }
        cloud_mask = {
            "status": "written",
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "storage_mode": "in_place_zarr_masking",
            "mask_path": "masks/cloud",
            "probability_path": "masks/cloud_probability",
            "backend": "omnicloudmask",
            "cloud_fraction": 0.3,
            "cloud_only_fraction": 0.2,
            "shadow_fraction": 0.1,
        }

        if stage_callback is not None and "cloud" in mask_types:
            stage_callback(
                "cloud_masking_started",
                {"job_id": job_id, "zarr_uri": zarr_uri, "scene_id": scene_id},
            )
            stage_callback(
                "cloud_masking_finished",
                {
                    "job_id": job_id,
                    "zarr_uri": zarr_uri,
                    "scene_id": scene_id,
                    "cloud_mask": cloud_mask,
                },
            )
        if stage_callback is not None and "water" in mask_types:
            stage_callback(
                "water_masking_started",
                {"job_id": job_id, "zarr_uri": zarr_uri, "scene_id": scene_id},
            )
            self.water_started.set()
            assert self.allow_water_finish.wait(30.0), "Timed out waiting to release water masking."
            stage_callback(
                "water_masking_finished",
                {
                    "job_id": job_id,
                    "zarr_uri": zarr_uri,
                    "scene_id": scene_id,
                    "water_mask": water_mask,
                },
            )

        return {
            "status": "written",
            "mask_types": list(mask_types),
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": zarr_uri,
            "masked_zarr_uri": zarr_uri,
            "masked_zarr_outputs": [zarr_uri],
            "water_mask": water_mask if "water" in mask_types else {},
            "cloud_mask": cloud_mask if "cloud" in mask_types else {},
            "watermask_outputs": [],
            "cloudmask_outputs": [],
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


def _fetcher_job_status(fetcher: NimbusFetcher, job_id: str):
    row = fetcher.store.get_job(job_id)
    assert row is not None
    normalized = fetcher._normalize_backend_paths_in_job_row(  # type: ignore[attr-defined]
        fetcher._normalize_historical_job_row(row)  # type: ignore[attr-defined]
    )
    return fetcher._to_status_response(normalized)  # type: ignore[attr-defined]


def _wait_for_fetcher_status(
    fetcher: NimbusFetcher,
    job_id: str,
    predicate,
    timeout_seconds: float = 10.0,
):
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while time.monotonic() < deadline:
        status = _fetcher_job_status(fetcher, job_id)
        last_status = status
        if predicate(status):
            return status
        time.sleep(0.1)
    pytest.fail(f"Job {job_id} did not reach the expected intermediate state. Last status: {last_status}")


def _request_payload(mask_types: list[str] | None = None) -> SearchDownloadRequest:
    payload = {
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
    if mask_types:
        payload["mask_types"] = mask_types
    return SearchDownloadRequest.model_validate(payload)


def _landsat_request_payload() -> SearchDownloadRequest:
    return SearchDownloadRequest.model_validate(
        {
            "job_type": "search_download",
            "provider": ProviderName.usgs,
            "collection": "landsat_ot_c2_l1",
            "product_type": "L1TP",
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 1, 2),
            "aoi": {
                "wkt": "POLYGON((2.30 48.80, 2.30 48.90, 2.40 48.90, 2.40 48.80, 2.30 48.80))"
            },
            "output_dir": "integration/landsat-sen2like",
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
    assert final_status.pipeline_timeline["terminal"] is True
    stage_statuses = {
        str(stage["key"]): str(stage["status"])
        for stage in final_status.pipeline_timeline["stages"]
    }
    assert stage_statuses["search"] == "done"
    assert stage_statuses["download"] == "done"
    assert stage_statuses["convert"] == "done"
    assert stage_statuses["ready"] == "done"
    assert final_status.raw_outputs, "Expected raw outputs on the same pipeline job."
    assert final_status.zarr_outputs, "Expected Zarr outputs on the same pipeline job."

    assert result.job_id == job_id
    assert result.raw_outputs == final_status.raw_outputs
    assert result.zarr_outputs == final_status.zarr_outputs
    assert len(result.paths) == 3, "Expected raw file, manifest, and zarr output in result paths."
    assert any(path.endswith("manifest.json") for path in result.paths)
    assert [stage["name"] for stage in result.pipeline_metadata["stage_plan"]] == ["fetch", "zarr"]
    stage_results = result.pipeline_metadata["stage_results"]
    assert [stage["name"] for stage in stage_results] == ["fetch", "zarr"]
    assert {stage["name"]: stage["status"] for stage in stage_results} == {
        "fetch": "succeeded",
        "zarr": "succeeded",
    }
    assert all(stage["duration_seconds"] >= 0 for stage in stage_results)
    assert result.pipeline_metadata["orchestrator"]["status"] == "succeeded"

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
    assert "job.pipeline_orchestrated" in event_types


def test_sentinel_job_does_not_call_sen2like(monkeypatch, pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    FakeSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FakeSen2LikeClient,
    )

    job_id = client.submit_job(_request_payload())
    final_status = _wait_for_completion(client, job_id)
    converter = fetcher._zarr_converter

    assert final_status.state == JobState.succeeded
    assert FakeSen2LikeClient.calls == []
    assert isinstance(converter, FakeZarrConverter)
    assert converter.calls
    assert converter.calls[-1]["provider"] == "copernicus"
    assert converter.calls[-1]["raw_uri"] == final_status.raw_outputs[0]


def test_landsat_job_routes_zarr_input_through_sen2like_service(monkeypatch, tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
            "nimbus_sen2like_service_url": "http://sen2like.test",
            "nimbus_sen2like_work_dir": str(tmp_path / "sen2like"),
            "nimbus_sen2like_raw_fallback": False,
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"usgs": FakeUsgsProvider},
    )
    converter = FakeZarrConverter()
    fetcher._zarr_converter = converter
    FakeSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FakeSen2LikeClient,
    )

    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        job_id = client.submit_job(_landsat_request_payload())
        final_status = _wait_for_completion(client, job_id)
        result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.zarr_written
    assert final_status.raw_outputs
    assert FakeSen2LikeClient.calls
    assert FakeSen2LikeClient.calls[-1]["products"] == final_status.raw_outputs

    sen2like_outputs = result.pipeline_metadata["sen2like_outputs"]
    assert sen2like_outputs
    assert converter.calls
    assert converter.calls[-1]["provider"] == "copernicus"
    assert converter.calls[-1]["collection"] == "SENTINEL-2"
    assert converter.calls[-1]["product_type"] == "S2MSI2A"
    assert converter.calls[-1]["raw_uri"] == sen2like_outputs[0]
    assert result.pipeline_metadata["zarr_input_source"] == "sen2like"
    assert result.pipeline_metadata["zarr_input_outputs"] == sen2like_outputs
    stage_results = {
        str(stage["name"]): str(stage["status"])
        for stage in result.pipeline_metadata["stage_results"]
    }
    assert stage_results["fetch"] == "succeeded"
    assert stage_results["sen2like"] == "succeeded"
    assert stage_results["zarr"] == "succeeded"

    event_types = {row["type"] for row in fetcher.store.list_events(job_id, None, 200)}
    assert "job.sen2like_running" in event_types
    assert "job.sen2like_written" in event_types


def test_landsat_job_uses_sen2like_direct_zarr_without_standalone_reconversion(
    monkeypatch,
    tmp_path: Path,
) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
            "nimbus_sen2like_service_url": "http://sen2like.test",
            "nimbus_sen2like_work_dir": str(tmp_path / "sen2like"),
            "nimbus_sen2like_direct_zarr": True,
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"usgs": FakeUsgsProvider},
    )
    converter = FakeZarrConverter()
    fetcher._zarr_converter = converter
    DirectZarrSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        DirectZarrSen2LikeClient,
    )

    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        job_id = client.submit_job(_landsat_request_payload())
        final_status = _wait_for_completion(client, job_id)
        result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.zarr_written
    assert final_status.zarr_outputs
    assert converter.calls == []
    assert result.pipeline_metadata["zarr_input_outputs"] == []
    assert result.pipeline_metadata["zarr_prebuilt_outputs"] == final_status.zarr_outputs
    assert result.pipeline_metadata["sen2like_direct_zarr_outputs"] == final_status.zarr_outputs
    assert result.conversion_metadata["runner"] == "sen2like_direct_zarr"
    assert result.conversion_metadata["skipped_standalone_zarr"] is True

    stage_results = {
        str(stage["name"]): str(stage["status"])
        for stage in result.pipeline_metadata["stage_results"]
    }
    assert stage_results["fetch"] == "succeeded"
    assert stage_results["sen2like"] == "succeeded"
    assert stage_results["zarr"] == "succeeded"


def test_landsat_sen2like_router_batches_products_for_spark_parallelism(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            nimbus_sen2like_work_dir=str(tmp_path / "sen2like"),
            nimbus_sen2like_workers=4,
            nimbus_sen2like_timeout_seconds=600,
        )
    )
    router = sen2like_normalization.Sen2LikeNormalizationRouter(runtime)
    FakeSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FakeSen2LikeClient,
    )

    response = router._normalize(
        service_url="http://sen2like.test",
        job_id="job-42",
        products=["/data/raw/LC08_A.tar", "/data/raw/LC09_B.tar"],
    )

    assert len(FakeSen2LikeClient.calls) == 1
    assert FakeSen2LikeClient.calls[0]["products"] == [
        "/data/raw/LC08_A.tar",
        "/data/raw/LC09_B.tar",
    ]
    assert FakeSen2LikeClient.calls[0]["pipeline_id"] == "job-42"
    assert FakeSen2LikeClient.calls[0]["job_id"] == "job-42"
    assert FakeSen2LikeClient.calls[0]["working_dir"] == str(tmp_path / "sen2like" / "job-42")
    assert FakeSen2LikeClient.calls[0]["no_resume"] is True
    assert response["metadata"]["execution_mode"] == "parallel_multi_product"
    assert response["metadata"]["workers"] == 4
    assert response["metadata"]["batched_product_count"] == 2
    assert response["metadata"]["product_parallelism"] is True
    assert response["metadata"]["tile_parallelism"] is True
    assert response["duration_seconds"] == 1.25
    assert len(response["outputs"]) == 2


def test_landsat_sen2like_router_reuses_existing_safe_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    work_dir = tmp_path / "sen2like"
    safe_dir = (
        work_dir
        / "job-42"
        / "LC08_A"
        / "SAFE"
        / "S2L_MSIL2F_20260516T000000_N0500_R000_T31UDQ_20260516T000000.SAFE"
    )
    img_dir = (
        safe_dir
        / "GRANULE"
        / "L2F_T31UDQ_20260516T000000_N0500_R000"
        / "IMG_DATA"
        / "RESOLUTION_10M"
    )
    img_dir.mkdir(parents=True)
    (safe_dir / "manifest.safe").write_text("<xml />", encoding="utf-8")
    (safe_dir / "MTD_MSIL2F.xml").write_text("<xml />", encoding="utf-8")
    (
        safe_dir
        / "GRANULE"
        / "L2F_T31UDQ_20260516T000000_N0500_R000"
        / "MTD_TL.xml"
    ).write_text("<xml />", encoding="utf-8")
    for band in ("B02", "B03", "B04", "B08", "B11", "B12"):
        (img_dir / f"T31UDQ_20260516T000000_{band}_10m.TIF").write_bytes(b"fake-raster")
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            nimbus_sen2like_work_dir=str(work_dir),
            nimbus_sen2like_workers=4,
            nimbus_sen2like_timeout_seconds=600,
        )
    )
    router = sen2like_normalization.Sen2LikeNormalizationRouter(runtime)
    FakeSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FakeSen2LikeClient,
    )

    response = router._normalize(
        service_url="http://sen2like.test",
        job_id="job-42",
        products=["/data/raw/LC08_A.tar"],
    )

    assert FakeSen2LikeClient.calls == []
    assert response["outputs"] == [
        {
            "product": "/data/raw/LC08_A.tar",
            "output_dir": str(work_dir / "job-42" / "LC08_A"),
            "manifest_path": None,
            "normalized_uri": str(safe_dir),
            "exists": True,
            "reused_existing": True,
        }
    ]
    assert response["metadata"]["reused_existing_output_count"] == 1
    assert response["metadata"]["execution_mode"] == "resume_existing_safe"


def test_landsat_sen2like_router_does_not_reuse_stale_safe_with_wrong_acquisition_time(
    monkeypatch,
    tmp_path: Path,
) -> None:
    work_dir = tmp_path / "sen2like"
    input_dir = work_dir / "job-42" / "_inputs" / "LC08_A"
    input_dir.mkdir(parents=True)
    (input_dir / "LC08_L1TP_199026_20260418_20260424_02_T1_MTL.txt").write_text(
        "\n".join(
            [
                'LANDSAT_PRODUCT_ID = "LC08_L1TP_199026_20260418_20260424_02_T1"',
                "DATE_ACQUIRED = 2026-04-18",
                'SCENE_CENTER_TIME = "10:40:22.8698510Z"',
            ]
        ),
        encoding="utf-8",
    )
    safe_dir = (
        work_dir
        / "job-42"
        / "LC08_A"
        / "SAFE"
        / "S2L_MSIL2F_20260518T000000_N0500_R000_T31UDQ_20260518T091343.SAFE"
    )
    img_dir = (
        safe_dir
        / "GRANULE"
        / "L2F_T31UDQ_20260518T000000_N0500_R000"
        / "IMG_DATA"
        / "RESOLUTION_10M"
    )
    img_dir.mkdir(parents=True)
    (safe_dir / "manifest.safe").write_text("<xml />", encoding="utf-8")
    (safe_dir / "MTD_MSIL2F.xml").write_text("<xml />", encoding="utf-8")
    (
        safe_dir
        / "GRANULE"
        / "L2F_T31UDQ_20260518T000000_N0500_R000"
        / "MTD_TL.xml"
    ).write_text("<xml />", encoding="utf-8")
    for band in ("B02", "B03", "B04", "B08", "B11", "B12"):
        (img_dir / f"T31UDQ_20260518T000000_{band}_10m.TIF").write_bytes(b"fake-raster")
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            nimbus_sen2like_work_dir=str(work_dir),
            nimbus_sen2like_workers=4,
            nimbus_sen2like_timeout_seconds=600,
        )
    )
    router = sen2like_normalization.Sen2LikeNormalizationRouter(runtime)
    FakeSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FakeSen2LikeClient,
    )

    response = router._normalize(
        service_url="http://sen2like.test",
        job_id="job-42",
        products=["/data/raw/LC08_A.tar"],
    )

    assert len(FakeSen2LikeClient.calls) == 1
    assert response["metadata"]["reused_existing_output_count"] == 0


def test_zarr_context_prefers_sen2like_conversion_sensor_metadata() -> None:
    support = FetcherZarrContextSupport(
        converter=lambda: None,
        provider_name=lambda value: str(value or "").strip().lower(),
        scene_id_from_raw_uri=lambda uri: Path(str(uri)).stem,
    )

    context = support.resolve_zarr_context(
        job_id="job-1",
        row={
            "provider": "usgs",
            "collection": "landsat_ot_c2_l1",
            "product_type": "L1TP",
            "conversion_metadata": {},
        },
        result={
            "conversion_metadata": {
                "items": [
                    {
                        "zarr_uri": "/data/downloads/zarr/S2L_TEST.zarr",
                        "scene_id": "S2L_TEST",
                        "summary": {
                            "provider": "copernicus",
                            "collection": "SENTINEL-2",
                            "product_type": "S2MSI2A",
                            "acquisition_datetime": "2026-05-16T00:00:00+00:00",
                        },
                        "dataset_summary": {
                            "band_names": ["B02", "B03", "B04", "B08", "B11", "B12"],
                            "crs": "EPSG:32631",
                            "shape": [1, 6, 10980, 10980],
                        },
                    }
                ]
            }
        },
        zarr_uri="/data/downloads/zarr/S2L_TEST.zarr",
        scene_id_override=None,
        product_type_override="L1TP",
    )

    assert context["provider"] == "copernicus"
    assert context["collection"] == "SENTINEL-2"
    assert context["product_type"] == "S2MSI2A"
    assert context["dataset_summary"]["band_names"] == ["B02", "B03", "B04", "B08", "B11", "B12"]


def test_landsat_job_fails_at_sen2like_before_zarr(monkeypatch, tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
            "nimbus_sen2like_service_url": "http://sen2like.test",
            "nimbus_sen2like_work_dir": str(tmp_path / "sen2like"),
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"usgs": FakeUsgsProvider},
    )
    converter = FakeZarrConverter()
    fetcher._zarr_converter = converter
    FailingSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        FailingSen2LikeClient,
    )

    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        job_id = client.submit_job(_landsat_request_payload())
        final_status = _wait_for_completion(client, job_id)
        result = client.get_result(job_id)

    assert final_status.state == JobState.failed
    assert final_status.pipeline_state == PipelineState.sen2like_failed
    assert converter.calls == []
    assert result.pipeline_metadata["sen2like_status"] == "failed"
    assert "sen2like exploded" in result.pipeline_metadata["sen2like_error"]
    stage_results = {
        str(stage["name"]): stage
        for stage in result.pipeline_metadata["stage_results"]
    }
    assert stage_results["fetch"]["status"] == "succeeded"
    assert stage_results["sen2like"]["status"] == "failed"
    assert stage_results["zarr"]["status"] == "skipped"
    assert stage_results["zarr"]["metadata"]["blocked_by"] == ["sen2like"]


def test_landsat_job_falls_back_to_raw_when_sen2like_runs_out_of_memory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
            "nimbus_sen2like_service_url": "http://sen2like.test",
            "nimbus_sen2like_work_dir": str(tmp_path / "sen2like"),
            "nimbus_sen2like_raw_fallback": True,
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"usgs": FakeUsgsProvider},
    )
    converter = FakeZarrConverter()
    fetcher._zarr_converter = converter
    MemoryKilledSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        MemoryKilledSen2LikeClient,
    )

    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        job_id = client.submit_job(_landsat_request_payload())
        final_status = _wait_for_completion(client, job_id)
        result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.zarr_written
    assert final_status.raw_outputs
    assert final_status.zarr_outputs
    assert result.pipeline_metadata["sen2like_status"] == "raw_fallback"
    assert result.pipeline_metadata["sen2like_fallback_reason"] == "sen2like_resource_exhausted"
    assert result.pipeline_metadata["zarr_input_source"] == "raw"
    assert result.pipeline_metadata["zarr_input_outputs"] == final_status.raw_outputs
    assert converter.calls
    assert converter.calls[-1]["provider"] == "usgs"
    assert converter.calls[-1]["collection"] == "landsat_ot_c2_l1"
    assert converter.calls[-1]["raw_uri"] == final_status.raw_outputs[0]
    stage_results = {
        str(stage["name"]): stage
        for stage in result.pipeline_metadata["stage_results"]
    }
    assert stage_results["fetch"]["status"] == "succeeded"
    assert stage_results["sen2like"]["status"] == "succeeded"
    assert stage_results["sen2like"]["outputs"] == final_status.raw_outputs
    assert stage_results["sen2like"]["metadata"]["fallback_to_raw"] is True
    assert stage_results["sen2like"]["metadata"]["fallback_reason"] == "sen2like_resource_exhausted"
    assert stage_results["zarr"]["status"] == "succeeded"


def test_unsupported_daily_mosaic_cube_does_not_block_integrated_masks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
            "nimbus_sen2like_service_url": "http://sen2like.test",
            "nimbus_sen2like_work_dir": str(tmp_path / "sen2like"),
            "nimbus_sen2like_raw_fallback": True,
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"usgs": FakeUsgsProvider},
    )
    fetcher._zarr_converter = DailyMosaicRejectingZarrConverter()
    fetcher._mask_service = RemoteLikeMaskService(fetcher)
    MemoryKilledSen2LikeClient.calls = []
    monkeypatch.setattr(
        sen2like_normalization,
        "Sen2LikeServiceClient",
        MemoryKilledSen2LikeClient,
    )
    payload = _landsat_request_payload().model_dump()
    payload.update(
        {
            "cube_mode": "before_mask",
            "cube_layout": "daily_mosaic",
            "mask_types": ["water", "cloud"],
        }
    )
    request = SearchDownloadRequest.model_validate(payload)

    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        job_id = client.submit_job(request)
        final_status = _wait_for_completion(client, job_id)
        result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.masked_zarr_written
    assert result.pipeline_metadata["cube_status"] == "skipped"
    assert "Daily mosaic cube is currently supported only for Sentinel-2" in str(
        result.pipeline_metadata["cube_reason"]
    )
    assert result.pipeline_metadata["mask_status"] == "written"
    assert result.pipeline_metadata["mask_completed_scenes"] == 1
    stage_results = {
        str(stage["name"]): stage
        for stage in result.pipeline_metadata["stage_results"]
    }
    assert stage_results["sen2like"]["status"] == "succeeded"
    assert stage_results["sen2like"]["metadata"]["fallback_to_raw"] is True
    assert stage_results["cube"]["status"] == "skipped"
    assert "Daily mosaic cube is currently supported only for Sentinel-2" in str(
        stage_results["cube"]["metadata"]["reason"]
    )
    assert stage_results["mask"]["status"] == "succeeded"
    assert stage_results["mask"]["outputs"] == final_status.zarr_outputs
    assert stage_results["mask"]["metadata"]["mask_completed_scenes"] == 1

    event_types = [row["type"] for row in fetcher.store.list_events(job_id, None, 200)]
    assert "job.cube_skipped" in event_types
    assert "job.cube_failed" not in event_types
    assert "job.mask_completed" in event_types


def test_single_job_can_continue_with_integrated_masking(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]

    mask_service = RemoteLikeMaskService(fetcher)
    fetcher._mask_service = mask_service

    job_id = client.submit_job(_request_payload(mask_types=["water", "cloud"]))
    final_status = _wait_for_completion(client, job_id)
    result = client.get_result(job_id)

    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.masked_zarr_written
    assert final_status.raw_outputs
    assert final_status.zarr_outputs
    assert final_status.masked_zarr_outputs == []

    assert result.job_id == job_id
    assert result.raw_outputs == final_status.raw_outputs
    assert result.zarr_outputs == final_status.zarr_outputs
    assert result.masked_zarr_outputs == []
    assert result.metadata["mask"]["status"] == "written"
    assert result.metadata["mask"]["mask_types"] == ["water", "cloud"]
    assert result.pipeline_metadata["mask_mode"] == "integrated"
    assert result.pipeline_metadata["mask_status"] == "written"
    assert mask_service.calls
    assert all(call["water_backend"] == "omniwatermask" for call in mask_service.calls)
    assert all(call["fail_on_error"] is True for call in mask_service.calls)
    assert [stage["name"] for stage in result.pipeline_metadata["stage_results"]] == [
        "fetch",
        "zarr",
        "mask",
    ]
    assert result.pipeline_metadata["orchestrator"]["status"] == "succeeded"

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 1, "Integrated masking must stay on the original fetch job."

    event_types = {row["type"] for row in fetcher.store.list_events(job_id, None, 200)}
    assert "job.zarr_written" in event_types
    assert "job.mask_completed" in event_types
    assert "job.succeeded" in event_types


def test_integrated_masking_keeps_convert_done_while_water_is_running(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]

    water_started = threading.Event()
    allow_water_finish = threading.Event()
    fetcher._mask_service = ControlledMaskService(
        water_started=water_started,
        allow_water_finish=allow_water_finish,
    )

    job_id = client.submit_job(_request_payload(mask_types=["water", "cloud"]))
    assert water_started.wait(5.0), "Water masking never started in the controlled test."

    running_status = _wait_for_fetcher_status(
        fetcher,
        job_id,
        lambda status: status.pipeline_state == PipelineState.running_water_inference,
    )
    stage_statuses = {
        str(stage["key"]): str(stage["status"])
        for stage in running_status.pipeline_timeline["stages"]
    }

    assert running_status.state == JobState.running
    assert stage_statuses["search"] == "done"
    assert stage_statuses["download"] == "done"
    assert stage_statuses["convert"] == "done"
    assert stage_statuses["cloud"] == "done"
    assert stage_statuses["water"] == "running"
    assert stage_statuses["ready"] == "pending"

    allow_water_finish.set()
    final_status = _wait_for_completion(client, job_id)
    assert final_status.state == JobState.succeeded
    assert final_status.pipeline_state == PipelineState.masked_zarr_written


def test_download_progress_updates_are_throttled_to_coarser_intervals() -> None:
    assert NimbusFetcher._should_emit_download_progress(
        delta=1024,
        now_mono=10.0,
        last_emit=0.0,
        bytes_downloaded=1024,
        last_bytes=0,
        progress_pct=0.1,
        last_progress=0.0,
        bytes_total=1024 * 1024,
    )

    assert not NimbusFetcher._should_emit_download_progress(
        delta=1024 * 1024,
        now_mono=10.5,
        last_emit=10.0,
        bytes_downloaded=2 * 1024 * 1024,
        last_bytes=1024 * 1024,
        progress_pct=0.2,
        last_progress=0.1,
        bytes_total=1024 * 1024 * 1024,
    )

    assert NimbusFetcher._should_emit_download_progress(
        delta=1024 * 1024,
        now_mono=13.2,
        last_emit=10.0,
        bytes_downloaded=5 * 1024 * 1024,
        last_bytes=1024 * 1024,
        progress_pct=0.6,
        last_progress=0.1,
        bytes_total=1024 * 1024 * 1024,
    )


def test_zarr_conversion_defaults_to_single_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS", raising=False)
    monkeypatch.setattr("nimbuschain_fetch.engine.nimbus_fetcher.os.cpu_count", lambda: 8)
    assert NimbusFetcher._zarr_convert_max_workers(total=4) == 1
    assert NimbusFetcher._zarr_convert_max_workers(
        total=4,
        preferred_parallelism=3,
    ) == 3

    monkeypatch.setenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS", "2")
    assert NimbusFetcher._zarr_convert_max_workers(total=4) == 2


def test_scene_parallelism_target_no_longer_penalizes_default_multi_scene_jobs(tmp_path: Path) -> None:
    settings = Settings(
        NIMBUS_RUNTIME_ROLE="api",
        NIMBUS_DB_BACKEND="sqlite",
        NIMBUS_DB_PATH=str(tmp_path / "nimbus.db"),
        NIMBUS_DATA_DIR=str(tmp_path / "downloads"),
        NIMBUS_MAX_JOBS=4,
    )
    fetcher = NimbusFetcher(settings=settings)

    default_target = fetcher._scene_parallelism_target_from_download(
        pipeline_metadata={
            "download_strategy": "default",
            "account_pool_selected_accounts": 1,
            "account_pool_size": 4,
        },
        total=5,
    )
    pool_target = fetcher._scene_parallelism_target_from_download(
        pipeline_metadata={
            "download_strategy": "copernicus_account_pool",
            "account_pool_selected_accounts": 4,
            "account_pool_size": 4,
            "account_pool_assignments": [
                {"account_label": "primary", "product_count": 2},
                {"account_label": "secondary-1", "product_count": 1},
                {"account_label": "secondary-2", "product_count": 1},
                {"account_label": "secondary-3", "product_count": 1},
            ],
        },
        total=5,
    )

    assert default_target == 4
    assert pool_target == 4


def test_zarr_conversion_emits_incremental_pipeline_progress(
    pipeline_runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    progress_samples: list[tuple[str | None, float | None]] = []

    def _capture_update(_job_id: str, **kwargs) -> None:
        progress_samples.append(
            (
                kwargs.get("pipeline_step"),
                kwargs.get("pipeline_progress"),
            )
        )

    monkeypatch.setattr(fetcher, "_update_pipeline", _capture_update)
    monkeypatch.setattr(fetcher, "_register_zarr_artifact", lambda **_kwargs: None)
    monkeypatch.delenv("NIMBUS_ZARR_CONVERT_MAX_WORKERS", raising=False)

    raw_uri = str(settings.nimbus_data_dir / "raw" / "S2A_MSIL2A_FAKE_SCENE.SAFE.zip")
    zarr_outputs, metadata = fetcher._convert_raw_outputs(
        job_id="job-progress",
        provider_name="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        raw_outputs=[raw_uri],
        is_cancelled=lambda: False,
    )

    assert zarr_outputs
    assert metadata["parallel_workers"] == 1
    chunk_progress_values = [
        progress
        for step, progress in progress_samples
        if step == "writing_chunks" and progress is not None
    ]
    assert any(0.0 < float(progress) < 85.0 for progress in chunk_progress_values)


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


@pytest.mark.parametrize(
    "route_template",
    [
        "/v1/jobs/{job_id}/water-mask",
        "/v1/jobs/{job_id}/watermask",
    ],
)
def test_manual_watermask_route_reuses_existing_job_lineage(pipeline_runtime, route_template: str) -> None:
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
            route_template.format(job_id=job_id),
            json={"zarr_uri": status.zarr_outputs[0]},
        )

    if response.status_code != 200:
        pytest.xfail(f"Water-mask alias route is still returning an incompatible response schema: {response.text}")

    body = response.json()
    mask_job_id = body["job_id"]
    assert mask_job_id != job_id
    assert body["source_job_id"] == job_id
    assert body["source_zarr_uri"] == status.zarr_outputs[0]
    assert body["job"]["job_id"] == mask_job_id
    assert body["job"]["job_kind"] == "mask"
    assert body["job"]["source_job_id"] == job_id
    assert body["job"]["state"] in {JobState.queued.value, JobState.running.value}
    assert body["water_mask"] == {}
    assert body["watermask_outputs"] == []
    assert body["masked_zarr_outputs"] == []

    jobs = client.list_jobs(page=1, page_size=20)
    assert jobs.total == 2, "Manual watermask must create a separate mask job while preserving the fetch job."

    source_result = client.get_result(job_id)
    assert source_result.zarr_outputs == status.zarr_outputs
    assert source_result.watermask_outputs == []

    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.succeeded

    mask_result = client.get_result(mask_job_id)
    assert mask_result.source_job_id == job_id
    assert len(mask_result.masked_zarr_outputs) == 1
    assert mask_result.watermask_outputs == []

    assert mask_result.masked_zarr_outputs[0] == status.zarr_outputs[0]

    artifacts = client.list_artifacts(
        artifact_type=ArtifactType.watermask.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert artifacts.total == 0


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


def test_remote_mask_service_marks_job_as_running_before_blocking_call(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    source_job_id = client.submit_job(_request_payload())
    source_status = _wait_for_completion(client, source_job_id)
    assert source_status.state == JobState.succeeded
    assert source_status.zarr_outputs

    fetcher._mask_service = RemoteLikeMaskService(fetcher)

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{source_job_id}/mask",
            json={
                "zarr_uri": source_status.zarr_outputs[0],
                "mask_types": ["water", "cloud"],
            },
        )

    assert response.status_code == 200, response.text
    mask_job_id = response.json()["job_id"]
    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.succeeded
    assert mask_status.pipeline_state == PipelineState.masked_zarr_written


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


def test_manual_watermask_route_queues_job_and_surfaces_runtime_error_in_mask_job(pipeline_runtime) -> None:
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

    assert response.status_code == 200, response.text
    body = response.json()
    mask_job_id = body["job_id"]
    assert mask_job_id != job_id
    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.failed
    assert "simulated watermask crash" in "\n".join(mask_status.errors)

    current = client.get_job(job_id)
    assert current.pipeline_state == PipelineState.zarr_written
    assert current.zarr_outputs == status.zarr_outputs
    result = client.get_result(job_id)
    assert result.watermask_outputs == []


def test_manual_generic_mask_route_supports_water_and_cloud(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    fetcher._mask_service = FakeMaskService(settings.nimbus_data_dir.parent / "masking")

    source_job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, source_job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{source_job_id}/mask",
            json={
                "zarr_uri": status.zarr_outputs[0],
                "mask_types": ["water", "cloud"],
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    mask_job_id = body["job_id"]
    assert mask_job_id != source_job_id
    assert body["source_job_id"] == source_job_id
    assert body["mask_types"] == ["water", "cloud"]
    assert body["job"]["job_kind"] == "mask"
    assert body["job"]["service_name"] == "mask_service"
    assert body["job"]["source_job_id"] == source_job_id
    assert body["job"]["state"] in {JobState.queued.value, JobState.running.value}
    assert body["water_mask"] == {}
    assert body["cloud_mask"] == {}

    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.succeeded

    result = client.get_result(mask_job_id)
    assert result.source_job_id == source_job_id
    assert len(result.masked_zarr_outputs) == 1
    assert result.watermask_outputs == []
    assert result.cloudmask_outputs == []

    assert result.masked_zarr_outputs[0] == status.zarr_outputs[0]

    water_artifacts = client.list_artifacts(
        artifact_type=ArtifactType.watermask.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert water_artifacts.total == 0

    cloud_artifacts = client.list_artifacts(
        artifact_type=ArtifactType.cloudmask.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert cloud_artifacts.total == 0


def test_manual_generic_mask_route_supports_cloud_only(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    fetcher._mask_service = FakeMaskService(settings.nimbus_data_dir.parent / "masking")

    source_job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, source_job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{source_job_id}/mask",
            json={
                "zarr_uri": status.zarr_outputs[0],
                "mask_types": ["cloud"],
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    mask_job_id = body["job_id"]
    assert mask_job_id != source_job_id
    assert body["source_job_id"] == source_job_id
    assert body["mask_types"] == ["cloud"]
    assert body["job"]["job_kind"] == "mask"
    assert body["job"]["service_name"] == "mask_service"
    assert body["job"]["source_job_id"] == source_job_id
    assert body["water_mask"] == {}
    assert body["cloud_mask"] == {}
    assert body["cloudmask_outputs"] == []

    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.succeeded

    result = client.get_result(mask_job_id)
    assert result.source_job_id == source_job_id
    assert len(result.masked_zarr_outputs) == 1
    assert result.watermask_outputs == []
    assert result.cloudmask_outputs == []

    assert result.masked_zarr_outputs[0] == status.zarr_outputs[0]

    cloud_artifacts = client.list_artifacts(
        artifact_type=ArtifactType.cloudmask.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert cloud_artifacts.total == 0


def test_failed_mask_job_does_not_expose_partial_outputs_or_register_artifacts(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    class PartialFailureMaskService:
        def __init__(self, root: Path):
            self.root = root

        def apply_masks_to_zarr(self, *, zarr_uri: str, scene_id: str, mask_types: list[str], **_kwargs) -> dict[str, object]:
            scene_root = self.root / scene_id
            scene_root.mkdir(parents=True, exist_ok=True)
            water_artifact = scene_root / "water_mask.tif"
            water_status = scene_root / "water_mask_status.json"
            water_artifact.write_bytes(b"partial-water")
            water_status.write_text(json.dumps({"status": "written"}), encoding="utf-8")
            masked_zarr = scene_root / f"{Path(zarr_uri).stem}.partial.zarr"
            masked_zarr.mkdir(parents=True, exist_ok=True)
            return {
                "status": "failed",
                "mask_types": list(mask_types),
                "input_zarr_uri": zarr_uri,
                "output_zarr_uri": str(masked_zarr),
                "masked_zarr_uri": str(masked_zarr),
                "masked_zarr_outputs": [str(masked_zarr)],
                "water_mask": {
                    "status": "written",
                    "artifact_uri": str(water_artifact),
                    "status_path": str(water_status),
                    "output_zarr_uri": str(masked_zarr),
                },
                "cloud_mask": {
                    "status": "failed",
                    "reason": "simulated cloud failure",
                    "output_zarr_uri": str(masked_zarr),
                },
                "watermask_outputs": [str(water_artifact), str(water_status)],
                "cloudmask_outputs": [],
            }

    fetcher._mask_service = PartialFailureMaskService(settings.nimbus_data_dir.parent / "masking")

    source_job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, source_job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{source_job_id}/mask",
            json={
                "zarr_uri": status.zarr_outputs[0],
                "mask_types": ["water", "cloud"],
            },
        )

    assert response.status_code == 200, response.text
    mask_job_id = response.json()["job_id"]
    mask_status = _wait_for_completion(client, mask_job_id)
    assert mask_status.state == JobState.failed
    assert mask_status.masked_zarr_outputs == []
    assert mask_status.watermask_outputs == []
    assert mask_status.cloudmask_outputs == []

    result = client.get_result(mask_job_id)
    assert result.masked_zarr_outputs == []
    assert result.watermask_outputs == []
    assert result.cloudmask_outputs == []

    masked_artifacts = client.list_artifacts(
        artifact_type=ArtifactType.zarr_masked.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert masked_artifacts.total == 0
    water_artifacts = client.list_artifacts(
        artifact_type=ArtifactType.watermask.value,
        job_id=mask_job_id,
        page=1,
        page_size=20,
    )
    assert water_artifacts.total == 0


def test_legacy_mask_jobs_are_kept_read_only_and_fail_if_requeued(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]

    source_job_id = client.submit_job(_request_payload())
    source_status = _wait_for_completion(client, source_job_id)
    assert source_status.state == JobState.succeeded
    assert source_status.zarr_outputs

    legacy_job_id = "legacy-mask-job"
    fetcher.store.create_job(
        job_id=legacy_job_id,
        job_type="mask_existing_zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        request_payload={
            "job_type": "mask_existing_zarr",
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "source_job_id": source_job_id,
            "source_zarr_uri": source_status.zarr_outputs[0],
            "scene_id": "legacy-scene",
            "mask_types": ["water"],
        },
    )
    fetcher.store.append_event(legacy_job_id, "job.queued", {"state": JobState.queued.value})

    deadline = time.monotonic() + 10.0
    last_status = None
    while time.monotonic() < deadline:
        last_status = client.get_job(legacy_job_id)
        if last_status.state in {JobState.succeeded, JobState.failed, JobState.cancelled}:
            break
        time.sleep(0.1)

    assert last_status is not None
    assert last_status.state == JobState.failed
    assert any("read-only history" in error for error in last_status.errors)


def test_restart_retires_interrupted_v2_mask_jobs_and_cleans_partial_outputs(tmp_path: Path) -> None:
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "worker",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": tmp_path / "nimbus.db",
            "nimbus_data_dir": tmp_path / "downloads",
        }
    )
    fetcher = NimbusFetcher(
        settings=settings,
        provider_registry={"copernicus": FakeCopernicusProvider},
    )

    source_store = _write_local_zarr_store(
        settings.nimbus_data_dir,
        store_name="restart-source",
    )
    masked_store = settings.nimbus_data_dir / "zarrmask" / "restart-source__mask-water-cloud.zarr"
    masked_store.mkdir(parents=True, exist_ok=True)
    (masked_store / "marker.txt").write_text("partial", encoding="utf-8")
    water_dir = settings.nimbus_data_dir / "watermask" / "mask-job" / "restart-source"
    cloud_dir = settings.nimbus_data_dir / "cloudmask" / "mask-job" / "restart-source"
    water_dir.mkdir(parents=True, exist_ok=True)
    cloud_dir.mkdir(parents=True, exist_ok=True)
    water_status = water_dir / "water_mask_status.json"
    cloud_status = cloud_dir / "cloud_mask_status.json"
    water_status.write_text(json.dumps({"status": "running"}), encoding="utf-8")
    cloud_status.write_text(json.dumps({"status": "running"}), encoding="utf-8")
    water_artifact = water_dir / "water_mask.tif"
    cloud_artifact = cloud_dir / "cloud_mask.tif"
    water_artifact.write_bytes(b"partial-water")
    cloud_artifact.write_bytes(b"partial-cloud")

    job_id = "interrupted-mask-job"
    fetcher.store.create_job(
        job_id=job_id,
        job_type="mask_existing_zarr",
        provider="copernicus",
        collection="SENTINEL-2",
        request_payload={
            "job_type": "mask_existing_zarr",
            "mask_contract_version": fetcher.MASK_CONTRACT_VERSION,
            "provider": "copernicus",
            "collection": "SENTINEL-2",
            "source_job_id": "source-job",
            "source_zarr_uri": str(source_store),
            "scene_id": "restart-source",
            "mask_types": ["water", "cloud"],
        },
    )
    fetcher.store.update_job(
        job_id,
        state=JobState.running.value,
        pipeline_state=PipelineState.running_cloud_inference.value,
        pipeline_step="running_cloud_inference",
        zarr_outputs=[str(masked_store)],
        watermask_outputs=[str(water_artifact), str(water_status)],
        cloudmask_outputs=[str(cloud_artifact), str(cloud_status)],
        conversion_metadata={
            "masked_zarr_uri": str(masked_store),
            "source_zarr_uri": str(source_store),
            "water_mask": {
                "status": "running",
                "artifact_uri": str(water_artifact),
                "status_path": str(water_status),
                "output_zarr_uri": str(masked_store),
            },
            "cloud_mask": {
                "status": "running",
                "artifact_uri": str(cloud_artifact),
                "status_path": str(cloud_status),
                "output_zarr_uri": str(masked_store),
            },
        },
    )
    fetcher.store.set_result(
        job_id,
        {
            "job_id": job_id,
            "job_type": "mask_existing_zarr",
            "paths": [str(masked_store), str(water_artifact), str(cloud_artifact)],
            "raw_outputs": [],
            "zarr_outputs": [str(masked_store)],
            "masked_zarr_outputs": [str(masked_store)],
            "watermask_outputs": [str(water_artifact), str(water_status)],
            "cloudmask_outputs": [str(cloud_artifact), str(cloud_status)],
            "checksums": {},
            "metadata": {},
            "manifest_entry": {},
            "pipeline_metadata": {
                "source_job_id": "source-job",
                "source_zarr_uri": str(source_store),
                "masked_zarr_uri": str(masked_store),
            },
            "conversion_metadata": {
                "source_zarr_uri": str(source_store),
                "masked_zarr_uri": str(masked_store),
                "water_mask": {
                    "status": "running",
                    "artifact_uri": str(water_artifact),
                    "status_path": str(water_status),
                    "output_zarr_uri": str(masked_store),
                },
                "cloud_mask": {
                    "status": "running",
                    "artifact_uri": str(cloud_artifact),
                    "status_path": str(cloud_status),
                    "output_zarr_uri": str(masked_store),
                },
            },
        },
    )

    anyio.run(fetcher.start)
    try:
        retired = fetcher.get_job(job_id)
        assert retired.state == JobState.failed
        assert retired.masked_zarr_outputs == []
        assert retired.watermask_outputs == []
        assert retired.cloudmask_outputs == []
        assert not masked_store.exists()
        assert not water_artifact.exists()
        assert not cloud_artifact.exists()
        water_status_payload = json.loads(water_status.read_text(encoding="utf-8"))
        cloud_status_payload = json.loads(cloud_status.read_text(encoding="utf-8"))
        assert water_status_payload["status"] == "failed"
        assert cloud_status_payload["status"] == "failed"
    finally:
        anyio.run(fetcher.stop)


def test_manual_cloud_mask_route_supports_backend_threshold_and_shadows(pipeline_runtime) -> None:
    client: NimbusFetcherClient = pipeline_runtime["client"]
    fetcher: NimbusFetcher = pipeline_runtime["fetcher"]
    settings = pipeline_runtime["settings"]

    fetcher._mask_service = FakeMaskService(settings.nimbus_data_dir.parent / "masking")

    source_job_id = client.submit_job(_request_payload())
    status = _wait_for_completion(client, source_job_id)
    assert status.state == JobState.succeeded
    assert status.zarr_outputs

    app = FastAPI()
    app.state.fetcher = fetcher
    app.state.settings = settings
    app.include_router(converter_router)

    with TestClient(app) as test_client:
        response = test_client.post(
            f"/v1/jobs/{source_job_id}/mask-cloud",
            json={
                "zarr_uri": status.zarr_outputs[0],
                "backend": "omnicloudmask",
                "threshold": 0.33,
                "include_shadows": True,
            },
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["mask_types"] == ["cloud"]
    assert body["job"]["state"] in {JobState.queued.value, JobState.running.value}
    assert body["cloud_mask"] == {}

    mask_job = fetcher.store.get_job(body["job_id"])
    assert mask_job is not None
    request_payload = dict(mask_job.get("request") or {})
    assert request_payload["backend"] == "omnicloudmask"
    assert request_payload["threshold"] == 0.33
    assert request_payload["include_shadows"] is True

    mask_status = _wait_for_completion(client, body["job_id"])
    assert mask_status.state == JobState.succeeded
    result = client.get_result(body["job_id"])
    cloud_mask = dict(result.conversion_metadata.get("cloud_mask") or {})
    assert cloud_mask["status"] == "written"
    assert cloud_mask["backend"] == "omnicloudmask"
    assert cloud_mask["threshold"] == 0.33
    assert cloud_mask["includes_shadows"] is True
