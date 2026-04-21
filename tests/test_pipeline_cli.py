from __future__ import annotations

import json

from nimbuschain_fetch import pipeline_cli


def test_vm_pipeline_cli_chains_convert_mask_and_grouped_cube(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {"converted": [], "masked": []}

    monkeypatch.setattr(
        pipeline_cli,
        "_materialize_raw_source",
        lambda raw_uri, stage_dir: f"/staged/{raw_uri.split('/')[-1]}",
    )

    class FakeConverter:
        def convert(self, **kwargs):
            captured["converted"].append(kwargs)
            scene_id = kwargs["scene_id"]
            zarr_uri = kwargs["output_uri"]
            return (
                zarr_uri,
                "optical",
                {"scene_id": scene_id},
                {"acquisition_datetime": "2026-04-10T08:00:21Z"},
            )

    class FakeMaskClient:
        def __init__(self, *, service_url: str | None = None):
            captured["mask_service_url"] = service_url

        def apply_masks_to_zarr(self, **kwargs):
            captured["masked"].append(kwargs)
            return {
                "status": "written",
                "masked_zarr_uri": str(kwargs["zarr_uri"]).replace(".zarr", "_masked.zarr"),
            }

        def close(self) -> None:
            captured["mask_closed"] = True

    monkeypatch.setattr(pipeline_cli, "ZarrConversionService", FakeConverter)
    monkeypatch.setattr(pipeline_cli, "MaskServiceClient", FakeMaskClient)
    monkeypatch.setattr(
        pipeline_cli,
        "build_grouped_time_cubes",
        lambda sources, output_dir, include_ancillary, include_masks: {
            "status": "written",
            "source_zarr_uris": list(sources),
            "output_dir": output_dir,
            "include_masks": include_masks,
        },
    )

    args = pipeline_cli.build_parser().parse_args(
        [
            "oci://bucket@namespace/raw/A.SAFE.zip",
            "oci://bucket@namespace/raw/B.SAFE.zip",
            "--provider",
            "copernicus",
            "--collection",
            "SENTINEL-2",
            "--product-type",
            "S2MSI2A",
            "--mask-types",
            "water",
            "--cube-mode",
            "grouped",
            "--include-masks-in-cube",
        ]
    )

    assert pipeline_cli.run(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(captured["converted"]) == 2
    assert len(captured["masked"]) == 2
    assert captured["mask_closed"] is True
    assert payload["cube_result"]["status"] == "written"
    assert all(item.endswith("_masked.zarr") for item in payload["final_scene_uris"])
