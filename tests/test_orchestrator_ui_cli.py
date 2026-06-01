from __future__ import annotations

from nimbuschain_fetch_ui.orchestrator_cli import build_stage_cli_command, parse_stage_cli_payload


def test_visual_orchestrator_builds_stage_cli_command() -> None:
    command = build_stage_cli_command(
        action="run-stage",
        provider="usgs",
        collection="landsat_ot_c2_l2",
        product_type="L2SP",
        job_id="job-1",
        mask_types=["water", "cloud"],
        cube_mode="before_mask",
        launch_mode="mps",
        run_stage="mask",
        raw_uri="/data/downloads/raw/LC08_SCENE",
        sen2like_service_url="http://nimbus-sen2like:8030",
        sen2like_working_dir="/data/downloads/sen2like",
        sen2like_workers=8,
        python_executable="python",
    )

    assert command[:4] == ["python", "-m", "nimbuschain_fetch.stage_cli", "run-stage"]
    assert command[command.index("--provider") + 1] == "usgs"
    assert command[command.index("--launch-mode") + 1] == "mps"
    assert command[command.index("--mask-types") + 1] == "water,cloud"
    assert command[command.index("--stage") + 1] == "mask"
    assert command[command.index("--raw-uri") + 1] == "/data/downloads/raw/LC08_SCENE"
    assert command[command.index("--sen2like-workers") + 1] == "8"


def test_visual_orchestrator_omits_sen2like_options_when_not_needed() -> None:
    command = build_stage_cli_command(
        action="plan",
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        job_id="job-2",
        mask_types=[],
        cube_mode="none",
        target_stage="zarr",
        python_executable="python",
    )

    assert "--sen2like-service-url" not in command
    assert "--sen2like-working-dir" not in command
    assert "--sen2like-workers" not in command
    assert "--raw-uri" not in command


def test_visual_orchestrator_parses_last_json_line() -> None:
    payload, error = parse_stage_cli_payload(
        "log line\n{\"status\":\"planned\",\"stages\":[]}\n",
        "",
    )

    assert error is None
    assert payload == {"status": "planned", "stages": []}
