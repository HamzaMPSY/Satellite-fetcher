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
