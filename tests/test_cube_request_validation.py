from __future__ import annotations

from datetime import date

import pytest

from nimbuschain_fetch.models import AOIInput, ProviderName, SearchDownloadRequest


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
