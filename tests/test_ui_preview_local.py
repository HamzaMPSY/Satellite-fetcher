from __future__ import annotations

from nimbuschain_fetch_ui.preview_local import (
    build_copernicus_filter,
    parse_copernicus_products,
    parse_usgs_scenes,
    preview_products_local,
)


def test_build_copernicus_filter_includes_all_constraints() -> None:
    query = build_copernicus_filter(
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date="2025-01-01",
        end_date="2025-01-02",
        aoi_wkt="POLYGON((0 0,0 1,1 1,1 0,0 0))",
        tile_id="31TFJ",
    )

    assert "Collection/Name eq 'SENTINEL-2'" in query
    assert "productType" in query
    assert "tileId" in query
    assert "Intersects" in query


def test_parse_copernicus_products_payload() -> None:
    payload = {
        "@odata.count": 2,
        "value": [
            {
                "Id": "id-1",
                "Name": "S2A_TEST_1",
                "ContentLength": 1048576,
                "ContentDate": {"Start": "2025-01-01T10:00:00Z"},
                "Attributes": [
                    {"Name": "tileId", "Value": "31TFJ"},
                    {"Name": "productType", "Value": "S2MSI2A"},
                ],
            },
            {
                "Id": "id-2",
                "Name": "S2A_TEST_2",
                "ContentLength": 2097152,
                "ContentDate": {"Start": "2025-01-01T11:00:00Z"},
                "Attributes": [{"Name": "tileId", "Value": "31TFK"}],
            },
        ],
    }
    parsed = parse_copernicus_products(payload, max_items=5)

    assert parsed["total"] == 2
    assert len(parsed["items"]) == 2
    assert parsed["items"][0]["tile_id"] == "31TFJ"
    assert parsed["items"][0]["size_mb"] == 1.0


def test_parse_usgs_scenes_filters_on_product_type() -> None:
    payload = {
        "data": {
            "results": [
                {
                    "entityId": "E1",
                    "displayId": "LC09_L2SP_199032_20250101_20250102_02_T1",
                    "temporalCoverage": {"startDate": "2025-01-01"},
                },
                {
                    "entityId": "E2",
                    "displayId": "LC09_L1TP_199032_20250101_20250102_02_T1",
                    "temporalCoverage": {"startDate": "2025-01-01"},
                },
            ]
        }
    }

    parsed = parse_usgs_scenes(payload, max_items=10, product_type="L2SP")
    assert parsed["total"] == 1
    assert parsed["items"][0]["id"] == "E1"


def test_preview_products_local_handles_missing_aoi_and_unknown_provider() -> None:
    missing_aoi = preview_products_local(
        provider="copernicus",
        collection="SENTINEL-2",
        product_type="S2MSI2A",
        start_date="2025-01-01",
        end_date="2025-01-02",
        aoi_wkt="",
    )
    assert missing_aoi["items"] == []
    assert "AOI is empty" in missing_aoi["error"]

    unsupported = preview_products_local(
        provider="other",
        collection="x",
        product_type="y",
        start_date="2025-01-01",
        end_date="2025-01-02",
        aoi_wkt="POLYGON((0 0,0 1,1 1,1 0,0 0))",
    )
    assert unsupported["items"] == []
    assert "unsupported" in unsupported["error"].lower()
