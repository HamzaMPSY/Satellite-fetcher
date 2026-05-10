from __future__ import annotations

from datetime import date

import pytest

from nimbuschain_fetch.models import AOIInput, ProviderName, SearchDownloadRequest
from nimbuschain_fetch.engine.mask_policy_support import FetcherMaskPolicySupport


def test_search_download_request_defaults_cube_dates_to_job_dates() -> None:
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
        cube_mode="before_mask",
    )

    assert request.cube_start_date == date(2026, 1, 1)
    assert request.cube_end_date == date(2026, 1, 5)


def test_search_download_request_rejects_after_mask_without_masks() -> None:
    with pytest.raises(ValueError, match="cube_mode='after_mask' requires at least one mask_type"):
        SearchDownloadRequest(
            job_type="search_download",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 5),
            aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
            cube_mode="after_mask",
        )


def test_search_download_request_rejects_download_only_with_masks() -> None:
    with pytest.raises(ValueError, match="download_only cannot be combined with mask_types"):
        SearchDownloadRequest(
            job_type="search_download",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 5),
            aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
            download_only=True,
            mask_types=["water"],
        )


def test_search_download_request_rejects_download_only_with_cube_mode() -> None:
    with pytest.raises(ValueError, match="download_only requires cube_mode='none'"):
        SearchDownloadRequest(
            job_type="search_download",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 5),
            aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
            download_only=True,
            cube_mode="before_mask",
        )


def test_search_download_request_accepts_daily_mosaic_cube_options() -> None:
    request = SearchDownloadRequest(
        job_type="search_download",
        provider=ProviderName.copernicus,
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
        mask_types=["cloud"],
        cube_mode="after_mask",
        cube_layout="daily_mosaic",
        cube_target_crs="EPSG:32631",
        cube_target_resolution_m=20,
        cube_overlap_policy="latest",
    )

    cube_config = FetcherMaskPolicySupport.cube_config_from_request(request)

    assert request.cube_layout == "daily_mosaic"
    assert cube_config == {
        "mode": "after_mask",
        "start_date": date(2026, 1, 1),
        "end_date": date(2026, 1, 5),
        "layout": "daily_mosaic",
        "target_crs": "EPSG:32631",
        "target_resolution_m": 20,
        "overlap_policy": "latest",
    }


def test_search_download_request_rejects_cube_options_without_cube_mode() -> None:
    with pytest.raises(ValueError, match="cube layout options require cube_mode != 'none'"):
        SearchDownloadRequest(
            job_type="search_download",
            provider=ProviderName.copernicus,
            collection="SENTINEL-2",
            product_type="S2MSI2A",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 5),
            aoi=AOIInput(wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
            cube_layout="daily_mosaic",
        )
