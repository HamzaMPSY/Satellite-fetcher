from __future__ import annotations

import json

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
