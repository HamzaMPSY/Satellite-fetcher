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
    assert [stage["name"] for stage in payload["stages"]] == [
        "fetch",
        "sen2like",
        "zarr",
        "mask",
        "cube",
    ]


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


def test_stage_cli_skips_optional_mask_when_mask_types_are_empty(capsys) -> None:
    code = main(
        [
            "run-stage",
            "--provider",
            "usgs",
            "--collection",
            "landsat_ot_c2_l2",
            "--product-type",
            "L2SP",
            "--stage",
            "mask",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["results"][-1]["name"] == "mask"
    assert payload["results"][-1]["status"] == "skipped"
    assert payload["results"][-1]["metadata"]["reason"] == "mask_types_empty"
