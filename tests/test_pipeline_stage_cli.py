from __future__ import annotations

import json

import nimbuschain_fetch.pipeline.runners as pipeline_runners
import nimbuschain_fetch.pipeline.sen2like as sen2like_module
from nimbuschain_fetch.stage_cli import main


def test_stage_cli_plan_outputs_json(capsys) -> None:
    code = main(
        [
            "plan",
            "--job-id",
            "job-1",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "planned"
    assert payload["launch_mode"] == "mps"
    assert [stage["name"] for stage in payload["stages"]] == ["fetch", "zarr"]


def test_stage_cli_plan_includes_sen2like_only_for_landsat(capsys) -> None:
    code = main(
        [
            "plan",
            "--provider",
            "usgs",
            "--collection",
            "landsat_ot_c2_l2",
            "--product-type",
            "L2SP",
            "--target-stage",
            "zarr",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "planned"
    assert [stage["name"] for stage in payload["stages"]] == ["fetch", "sen2like", "zarr"]
    assert payload["stages"][-1]["depends_on"] == ["sen2like"]


def test_stage_cli_run_stage_outputs_timed_results(capsys) -> None:
    code = main(
        [
            "run-stage",
            "--job-id",
            "job-2",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--stage",
            "zarr",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "completed"
    assert [result["name"] for result in payload["results"]] == ["fetch", "zarr"]
    assert all(result["duration_seconds"] >= 0 for result in payload["results"])


def test_stage_cli_plans_mask_only_when_mask_types_are_present(capsys) -> None:
    code = main(
        [
            "plan",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--mask-types",
            "water",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert [stage["name"] for stage in payload["stages"]] == ["fetch", "zarr", "mask"]


def test_stage_cli_plans_cube_without_mask_when_cube_mode_is_after_mask(capsys) -> None:
    code = main(
        [
            "plan",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--cube-mode",
            "after_mask",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert [stage["name"] for stage in payload["stages"]] == ["fetch", "zarr", "cube"]
    assert payload["stages"][-1]["depends_on"] == ["zarr"]


def test_stage_cli_run_stage_executes_real_zarr_runner(monkeypatch, tmp_path, capsys) -> None:
    captured: dict[str, object] = {"convert_calls": []}

    class FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["service_url"] = service_url

        def convert(self, **kwargs):
            captured["convert_calls"].append(kwargs)
            return (
                kwargs["output_uri"],
                "optical",
                {"scene_id": kwargs["scene_id"]},
                {"acquisition_datetime": "2026-05-01T10:00:00Z"},
            )

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(pipeline_runners, "ZarrServiceClient", FakeZarrClient)

    code = main(
        [
            "run-stage",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--raw-uri",
            "/data/raw/S2A_MSIL2A_TEST.SAFE.zip",
            "--zarr-service-url",
            "http://nimbus-zarr:8010",
            "--zarr-output-dir",
            str(tmp_path),
            "--stage",
            "zarr",
            "--execute",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["execution_mode"] == "runtime"
    assert [result["name"] for result in payload["results"]] == ["fetch", "zarr"]
    assert payload["results"][-1]["status"] == "succeeded"
    assert payload["results"][-1]["outputs"] == [str(tmp_path / "S2A_MSIL2A_TEST.zarr")]
    assert captured["service_url"] == "http://nimbus-zarr:8010"
    assert captured["closed"] is True
    assert captured["convert_calls"][0]["provider"] == "copernicus"


def test_stage_cli_run_stage_chains_existing_zarr_mask_and_cube(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {"masked": [], "cube_sources": []}

    class FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["zarr_service_url"] = service_url

        def build_grouped_cubes(self, **kwargs):
            captured["cube_sources"] = list(kwargs["source_zarr_uris"])
            captured["cube_layout"] = kwargs["cube_layout"]
            captured["target_crs"] = kwargs["target_crs"]
            captured["target_resolution_m"] = kwargs["target_resolution_m"]
            captured["overlap_policy"] = kwargs["overlap_policy"]
            return {
                "status": "written",
                "cube_outputs": ["/data/cubes/T31UDQ.zarr"],
                "source_zarr_uris": list(kwargs["source_zarr_uris"]),
            }

        def close(self) -> None:
            captured["zarr_closed"] = True

    class FakeMaskClient:
        def __init__(self, *, service_url: str | None = None):
            captured["mask_service_url"] = service_url

        def apply_masks_to_zarr(self, **kwargs):
            captured["masked"].append(kwargs)
            return {
                "status": "written",
                "masked_zarr_uri": "/data/zarr/SCENE_masked.zarr",
                "masked_zarr_outputs": ["/data/zarr/SCENE_masked.zarr"],
            }

        def close(self) -> None:
            captured["mask_closed"] = True

    monkeypatch.setattr(pipeline_runners, "ZarrServiceClient", FakeZarrClient)
    monkeypatch.setattr(pipeline_runners, "MaskServiceClient", FakeMaskClient)

    code = main(
        [
            "run-stage",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--source-zarr-uri",
            "/data/zarr/SCENE.zarr",
            "--mask-types",
            "water,cloud",
            "--cube-mode",
            "after_mask",
            "--cube-layout",
            "daily_mosaic",
            "--cube-target-crs",
            "EPSG:32631",
            "--cube-target-resolution-m",
            "20",
            "--cube-overlap-policy",
            "latest",
            "--zarr-service-url",
            "http://nimbus-zarr:8010",
            "--mask-service-url",
            "http://nimbus-mask:8020",
            "--stage",
            "cube",
            "--execute",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    statuses = {result["name"]: result["status"] for result in payload["results"]}

    assert code == 0
    assert statuses == {
        "fetch": "succeeded",
        "zarr": "succeeded",
        "mask": "succeeded",
        "cube": "succeeded",
    }
    assert captured["masked"][0]["zarr_uri"] == "/data/zarr/SCENE.zarr"
    assert captured["cube_sources"] == ["/data/zarr/SCENE_masked.zarr"]
    assert captured["cube_layout"] == "daily_mosaic"
    assert captured["target_crs"] == "EPSG:32631"
    assert captured["target_resolution_m"] == 20
    assert captured["overlap_policy"] == "latest"
    assert payload["results"][-1]["outputs"] == ["/data/cubes/T31UDQ.zarr"]


def test_stage_cli_landsat_runtime_fails_when_sen2like_fails_by_default(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    captured: dict[str, object] = {"convert_calls": []}

    class FakeSen2LikeClient:
        def __init__(self, *, service_url: str):
            captured["sen2like_service_url"] = service_url

        def normalize(self, **kwargs):
            captured["sen2like_products"] = list(kwargs["products"])
            raise RuntimeError("Sen2Like was killed")

        def close(self) -> None:
            captured["sen2like_closed"] = True

    class FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["zarr_service_url"] = service_url

        def convert(self, **kwargs):
            captured["convert_calls"].append(kwargs)
            return (
                kwargs["output_uri"],
                "optical",
                {"scene_id": kwargs["scene_id"]},
                {"acquisition_datetime": "2026-05-01T10:00:00Z"},
            )

        def close(self) -> None:
            captured["zarr_closed"] = True

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeSen2LikeClient)
    monkeypatch.setattr(pipeline_runners, "ZarrServiceClient", FakeZarrClient)

    code = main(
        [
            "run-stage",
            "--provider",
            "usgs",
            "--collection",
            "landsat_ot_c2_l1",
            "--product-type",
            "L1TP",
            "--raw-uri",
            "/data/raw/LC09_L1TP_TEST",
            "--sen2like-service-url",
            "http://nimbus-sen2like:8030",
            "--zarr-service-url",
            "http://nimbus-zarr:8010",
            "--zarr-output-dir",
            str(tmp_path),
            "--stage",
            "zarr",
            "--execute",
        ]
    )

    captured_output = capsys.readouterr()
    payload = json.loads(captured_output.err)
    statuses = {result["name"]: result["status"] for result in payload["results"]}
    sen2like_result = payload["results"][1]

    assert code == 1
    assert statuses == {"fetch": "succeeded", "sen2like": "failed", "zarr": "skipped"}
    assert captured["sen2like_products"] == ["/data/raw/LC09_L1TP_TEST"]
    assert captured["sen2like_closed"] is True
    assert sen2like_result["metadata"]["fallback_to_raw"] is False
    assert sen2like_result["metadata"]["failure_reason"] == "sen2like_service_failed"
    assert captured["convert_calls"] == []


def test_stage_cli_landsat_runtime_allows_raw_fallback_when_flag_is_set(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    captured: dict[str, object] = {"convert_calls": []}

    class FakeSen2LikeClient:
        def __init__(self, *, service_url: str):
            captured["sen2like_service_url"] = service_url

        def normalize(self, **kwargs):
            captured["sen2like_products"] = list(kwargs["products"])
            raise RuntimeError("Sen2Like was killed")

        def close(self) -> None:
            captured["sen2like_closed"] = True

    class FakeZarrClient:
        def __init__(self, *, service_url: str):
            captured["zarr_service_url"] = service_url

        def convert(self, **kwargs):
            captured["convert_calls"].append(kwargs)
            return (
                kwargs["output_uri"],
                "optical",
                {"scene_id": kwargs["scene_id"]},
                {"acquisition_datetime": "2026-05-01T10:00:00Z"},
            )

        def close(self) -> None:
            captured["zarr_closed"] = True

    monkeypatch.setattr(sen2like_module, "Sen2LikeServiceClient", FakeSen2LikeClient)
    monkeypatch.setattr(pipeline_runners, "ZarrServiceClient", FakeZarrClient)

    code = main(
        [
            "run-stage",
            "--provider",
            "usgs",
            "--collection",
            "landsat_ot_c2_l1",
            "--product-type",
            "L1TP",
            "--raw-uri",
            "/data/raw/LC09_L1TP_TEST",
            "--sen2like-service-url",
            "http://nimbus-sen2like:8030",
            "--allow-sen2like-raw-fallback",
            "--zarr-service-url",
            "http://nimbus-zarr:8010",
            "--zarr-output-dir",
            str(tmp_path),
            "--stage",
            "zarr",
            "--execute",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    statuses = {result["name"]: result["status"] for result in payload["results"]}
    sen2like_result = payload["results"][1]
    zarr_call = captured["convert_calls"][0]

    assert code == 0
    assert statuses == {"fetch": "succeeded", "sen2like": "succeeded", "zarr": "succeeded"}
    assert captured["sen2like_products"] == ["/data/raw/LC09_L1TP_TEST"]
    assert captured["sen2like_closed"] is True
    assert sen2like_result["metadata"]["fallback_to_raw"] is True
    assert zarr_call["provider"] == "usgs"
    assert zarr_call["collection"] == "landsat_ot_c2_l1"
    assert zarr_call["product_type"] == "L1TP"
    assert zarr_call["raw_uri"] == "/data/raw/LC09_L1TP_TEST"
