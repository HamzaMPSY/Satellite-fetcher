from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


VENDOR_ROOT = (
    Path(__file__).resolve().parents[1]
    / "vendor"
    / "src"
    / "nimbuschain_sen2like_service"
    / "vendor"
    / "Satellite-fetcher-feature-sen2like_reimplementation"
)
ZARR_PIPELINE = VENDOR_ROOT / "zarr" / "Pipeline_with_zarr.py"


def _pipeline_module(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(VENDOR_ROOT))
    module = importlib.import_module("Pipeline")
    monkeypatch.setattr(module, "_BASE", tmp_path, raising=False)
    return module


def _zarr_pipeline_module(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(VENDOR_ROOT))
    module_name = "Pipeline_with_zarr_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        ZARR_PIPELINE,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_BASE", tmp_path, raising=False)
    return module


def test_detect_spacecraft_reads_landsat_mtl_txt_when_json_is_missing(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    scene = tmp_path / "LC91990262026116LGN00"
    scene.mkdir()
    (scene / "LC09_L1TP_199026_20260426_20260426_02_T1_MTL.txt").write_text(
        'GROUP = IMAGE_ATTRIBUTES\n    SPACECRAFT_ID = "LANDSAT_9"\nEND_GROUP = IMAGE_ATTRIBUTES\n',
        encoding="utf-8",
    )

    assert pipeline._detect_spacecraft(scene) == "LANDSAT_9"


def test_product_config_uses_each_product_spacecraft_in_mixed_batch(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    l8_scene = tmp_path / "LC81990262026108LGN00"
    l9_scene = tmp_path / "LC91990262026116LGN00"
    l8_scene.mkdir()
    l9_scene.mkdir()
    (l8_scene / "LC08_L1TP_199026_20260418_20260424_02_T1_MTL.txt").write_text(
        'SPACECRAFT_ID = "LANDSAT_8"\n',
        encoding="utf-8",
    )
    (l9_scene / "LC09_L1TP_199026_20260426_20260426_02_T1_MTL.txt").write_text(
        'SPACECRAFT_ID = "LANDSAT_9"\n',
        encoding="utf-8",
    )
    base_config = {
        "lut_path": str(tmp_path / "lut" / "lut_6s_L8.json"),
        "atm_sensor_tag": "L8",
        "sbaf": {"mission": "LANDSAT_8", "s2_target": "Sentinel-2A"},
    }

    l8_config = pipeline._config_for_landsat_product(base_config, l8_scene)
    l9_config = pipeline._config_for_landsat_product(base_config, l9_scene)

    assert l8_config["atm_sensor_tag"] == "L8"
    assert l8_config["sbaf"]["mission"] == "LANDSAT_8"
    assert l8_config["lut_path"].endswith("lut_6s_L8.json")
    assert l9_config["atm_sensor_tag"] == "L9"
    assert l9_config["sbaf"]["mission"] == "LANDSAT_9"
    assert l9_config["lut_path"].endswith("lut_6s_L9.json")


def test_zarr_pipeline_product_config_uses_each_product_spacecraft_in_mixed_batch(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _zarr_pipeline_module(monkeypatch, tmp_path)
    l8_scene = tmp_path / "LC81990262026108LGN00"
    l9_scene = tmp_path / "LC91990262026116LGN00"
    l8_scene.mkdir()
    l9_scene.mkdir()
    (l8_scene / "LC08_L1TP_199026_20260418_20260424_02_T1_MTL.txt").write_text(
        'SPACECRAFT_ID = "LANDSAT_8"\n',
        encoding="utf-8",
    )
    (l9_scene / "LC09_L1TP_199026_20260426_20260426_02_T1_MTL.txt").write_text(
        'SPACECRAFT_ID = "LANDSAT_9"\n',
        encoding="utf-8",
    )
    base_config = {
        "lut_path": str(tmp_path / "lut" / "lut_6s_L8.json"),
        "atm_sensor_tag": "L8",
        "sbaf": {"mission": "LANDSAT_8", "s2_target": "Sentinel-2A"},
    }

    l8_config = pipeline._config_for_landsat_product(base_config, l8_scene)
    l9_config = pipeline._config_for_landsat_product(base_config, l9_scene)

    assert l8_config["atm_sensor_tag"] == "L8"
    assert l8_config["sbaf"]["mission"] == "LANDSAT_8"
    assert l8_config["lut_path"].endswith("lut_6s_L8.json")
    assert l9_config["atm_sensor_tag"] == "L9"
    assert l9_config["sbaf"]["mission"] == "LANDSAT_9"
    assert l9_config["lut_path"].endswith("lut_6s_L9.json")


def test_geometric_gri_lookup_does_not_fetch_when_auto_fetch_is_disabled(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    fake_gri_fetch = ModuleType("Geometric_Processing.gri_fetch")
    fake_gri_fetch.derive_mgrs_tile = lambda _scene: "T31UDQ"

    def fail_fetch(*_args, **_kwargs):
        raise AssertionError("get_or_fetch_gri must not run when auto-fetch is disabled")

    fake_gri_fetch.get_or_fetch_gri = fail_fetch
    monkeypatch.setitem(sys.modules, "Geometric_Processing.gri_fetch", fake_gri_fetch)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_GRI_AUTO_FETCH", "false")

    step = pipeline.GeometricProcessingStep({})
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={
            "landsat_path": str(tmp_path / "LC09_SCENE"),
            "gri_cache_dir": str(tmp_path / "gri-cache"),
            "gri_path": str(tmp_path / "gri-cache" / "GRI_T31UDQ.tif"),
        },
    )

    try:
        step._resolve_gri_path(ctx)
    except FileNotFoundError as exc:
        assert "auto-fetch is disabled" in str(exc)
        assert "T31UDQ" in str(exc)
    else:
        raise AssertionError("Expected missing local GRI to fail via FileNotFoundError.")


def test_geometric_gri_auto_fetch_receives_landsat_scene_path(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    scene_path = tmp_path / "LC91990262026100LGN00"
    scene_path.mkdir()
    fetched_gri = tmp_path / "gri-cache" / "GRI_T31UDQ.tif"
    calls = {}
    fake_gri_fetch = ModuleType("Geometric_Processing.gri_fetch")
    fake_gri_fetch.derive_mgrs_tile = lambda _scene: "T31UDQ"

    def fake_fetch(mgrs_tile, cache_dir, *, landsat_scene=None):
        calls["mgrs_tile"] = mgrs_tile
        calls["cache_dir"] = cache_dir
        calls["landsat_scene"] = landsat_scene
        return fetched_gri

    fake_gri_fetch.get_or_fetch_gri = fake_fetch
    monkeypatch.setitem(sys.modules, "Geometric_Processing.gri_fetch", fake_gri_fetch)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_GRI_AUTO_FETCH", "true")

    step = pipeline.GeometricProcessingStep({})
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={
            "landsat_path": str(scene_path),
            "gri_cache_dir": str(tmp_path / "gri-cache"),
            "gri_path": str(tmp_path / "gri-cache" / "GRI_T31UDQ.tif"),
        },
    )

    assert step._resolve_gri_path(ctx) == fetched_gri
    assert calls["mgrs_tile"] == "T31UDQ"
    assert calls["landsat_scene"] == str(scene_path)


def test_gri_fetch_derives_landsat_date_from_mtl(monkeypatch, tmp_path) -> None:
    monkeypatch.syspath_prepend(str(VENDOR_ROOT))
    gri_fetch = importlib.import_module("Geometric_Processing.gri_fetch")
    scene_path = tmp_path / "LC09_SCENE"
    scene_path.mkdir()
    (scene_path / "LC09_L2SP_199026_20260410_20260411_02_T1_MTL.txt").write_text(
        'DATE_ACQUIRED = "2026-04-10"\n',
        encoding="utf-8",
    )

    assert gri_fetch.derive_landsat_date(scene_path).strftime("%Y%m%d") == "20260410"


def test_gri_fetch_derives_landsat_date_from_compact_scene_name(
    monkeypatch,
) -> None:
    monkeypatch.syspath_prepend(str(VENDOR_ROOT))
    gri_fetch = importlib.import_module("Geometric_Processing.gri_fetch")

    assert gri_fetch.derive_landsat_date("LC81990262026108LGN00").strftime("%Y%m%d") == "20260418"


def test_geometric_processing_fails_instead_of_skipping_by_default(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    monkeypatch.delenv("NIMBUS_SEN2LIKE_ALLOW_GEO_SKIP", raising=False)

    step = pipeline.GeometricProcessingStep({})

    def fail_processing(_ctx):
        raise FileNotFoundError("No local GRI available for T31UDQ")

    monkeypatch.setattr(step, "_do_geometric_processing", fail_processing)
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={"landsat_path": str(tmp_path / "LC09_SCENE")},
    )

    with pytest.raises(RuntimeError) as exc_info:
        step.run(ctx)

    message = str(exc_info.value)
    assert "geometry_required_failed" in message
    assert "No local GRI available for T31UDQ" in message


def test_geometric_processing_skip_requires_explicit_debug_flag(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_ALLOW_GEO_SKIP", "true")

    step = pipeline.GeometricProcessingStep({})

    def fail_processing(_ctx):
        raise FileNotFoundError("No local GRI available for T31UDQ")

    monkeypatch.setattr(step, "_do_geometric_processing", fail_processing)
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={"landsat_path": str(tmp_path / "LC09_SCENE")},
    )

    result = step.run(ctx)

    assert result["geo_skipped"] is True
    assert "No local GRI available for T31UDQ" in result["geo_skip_reason"]


def test_geometric_gri_lookup_skips_unusable_cached_gri_without_fetch(
    monkeypatch,
    tmp_path,
) -> None:
    pipeline = _pipeline_module(monkeypatch, tmp_path)
    fake_gri_fetch = ModuleType("Geometric_Processing.gri_fetch")
    fake_gri_fetch.derive_mgrs_tile = lambda _scene: "T31UDQ"

    def fail_fetch(*_args, **_kwargs):
        raise AssertionError("get_or_fetch_gri must not run for an unusable cached GRI")

    fake_gri_fetch.get_or_fetch_gri = fail_fetch
    monkeypatch.setitem(sys.modules, "Geometric_Processing.gri_fetch", fake_gri_fetch)
    monkeypatch.setenv("NIMBUS_SEN2LIKE_GRI_AUTO_FETCH", "false")

    cache_dir = tmp_path / "gri-cache"
    cache_dir.mkdir()
    cached_gri = cache_dir / "GRI_T31UDQ.tif"
    cached_gri.write_text("not a geotiff", encoding="utf-8")

    step = pipeline.GeometricProcessingStep({})
    ctx = pipeline.Context(
        product_id="LC09_SCENE",
        working_dir=tmp_path / "work",
        config={
            "landsat_path": str(tmp_path / "LC09_SCENE"),
            "gri_cache_dir": str(cache_dir),
            "gri_path": str(cached_gri),
        },
    )

    try:
        step._resolve_gri_path(ctx)
    except FileNotFoundError as exc:
        assert "auto-fetch is disabled" in str(exc)
        assert "not usable" in str(exc)
    else:
        raise AssertionError("Expected unusable local GRI to fail via FileNotFoundError.")
