from __future__ import annotations

import datetime as dt

from nimbuschain_fetch_ui.job_api_runtime import build_job_payload


def test_build_job_payload_omits_mask_types_when_pipeline_stops_at_zarr() -> None:
    payload = build_job_payload(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 2),
        aoi_wkt="POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    )

    assert "mask_types" not in payload


def test_build_job_payload_includes_integrated_mask_types() -> None:
    payload = build_job_payload(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 2),
        aoi_wkt="POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
        mask_types=["water", "cloud"],
    )

    assert payload["mask_types"] == ["water", "cloud"]


def test_build_job_payload_includes_copernicus_account_pool_strategy() -> None:
    payload = build_job_payload(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 2),
        aoi_wkt="POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
        download_strategy="copernicus_account_pool",
    )

    assert payload["download_strategy"] == "copernicus_account_pool"


def test_build_job_payload_includes_daily_mosaic_cube_options() -> None:
    payload = build_job_payload(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 5),
        aoi_wkt="POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
        mask_types=["cloud"],
        cube_mode="after_mask",
        cube_start_date=dt.date(2026, 1, 2),
        cube_end_date=dt.date(2026, 1, 4),
        cube_layout="daily_mosaic",
        cube_target_crs="EPSG:32631",
        cube_target_resolution_m=20,
        cube_overlap_policy="latest",
    )

    assert payload["cube_mode"] == "after_mask"
    assert payload["cube_start_date"] == "2026-01-02"
    assert payload["cube_end_date"] == "2026-01-04"
    assert payload["cube_layout"] == "daily_mosaic"
    assert payload["cube_target_crs"] == "EPSG:32631"
    assert payload["cube_target_resolution_m"] == 20
    assert payload["cube_overlap_policy"] == "latest"
