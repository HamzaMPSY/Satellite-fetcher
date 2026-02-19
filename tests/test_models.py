from __future__ import annotations

import pytest
from pydantic import ValidationError

from nimbuschain_fetch.models import DownloadProductsRequest, SearchDownloadRequest


def test_search_download_rejects_invalid_dates() -> None:
    with pytest.raises(ValidationError):
        SearchDownloadRequest.model_validate(
            {
                "job_type": "search_download",
                "provider": "copernicus",
                "collection": "SENTINEL-2",
                "product_type": "S2MSI2A",
                "start_date": "2026-01-10",
                "end_date": "2026-01-01",
                "aoi": {"wkt": "POLYGON((0 0,0 1,1 1,1 0,0 0))"},
            }
        )


def test_search_download_rejects_traversal_output_dir() -> None:
    with pytest.raises(ValidationError):
        SearchDownloadRequest.model_validate(
            {
                "job_type": "search_download",
                "provider": "copernicus",
                "collection": "SENTINEL-2",
                "product_type": "S2MSI2A",
                "start_date": "2026-01-01",
                "end_date": "2026-01-02",
                "aoi": {"wkt": "POLYGON((0 0,0 1,1 1,1 0,0 0))"},
                "output_dir": "../../etc",
            }
        )


def test_download_products_requires_non_empty_ids() -> None:
    with pytest.raises(ValidationError):
        DownloadProductsRequest.model_validate(
            {
                "job_type": "download_products",
                "provider": "usgs",
                "collection": "landsat_ot_c2_l2",
                "product_ids": [],
            }
        )
