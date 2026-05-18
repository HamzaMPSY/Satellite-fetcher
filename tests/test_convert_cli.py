from __future__ import annotations

import json

from nimbuschain_zarr_service import convert_cli


def test_convert_cli_defaults_scene_id_from_raw_uri(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class FakeService:
        def convert(self, **kwargs):
            captured.update(kwargs)
            return (
                "/tmp/output.zarr",
                "optical",
                {"scene_id": kwargs["scene_id"]},
                {"dimensions": ["time", "band", "y", "x"]},
            )

    monkeypatch.setattr(convert_cli, "ZarrConversionService", FakeService)
    args = convert_cli.build_parser().parse_args(
        [
            "/tmp/S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820.SAFE.zip",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--output-uri",
            "/tmp/output.zarr",
        ]
    )

    assert convert_cli.run(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert captured["scene_id"] == "S2A_MSIL2A_20260410T080021_N0512_R035_T37RDP_20260410T134820"
    assert payload["scene_id"] == captured["scene_id"]
    assert payload["zarr_uri"] == "/tmp/output.zarr"
