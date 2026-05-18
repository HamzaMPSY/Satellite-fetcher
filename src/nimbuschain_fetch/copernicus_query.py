from __future__ import annotations

import re
from typing import Any

from shapely import wkt as shapely_wkt
from shapely.geometry.base import BaseGeometry


_COPERNICUS_WKT_PRECISION = 6


def _odata_string(value: Any) -> str:
    return "'" + str(value or "").strip().replace("'", "''") + "'"


def _normalize_aoi_geometry(aoi: BaseGeometry) -> BaseGeometry:
    if getattr(aoi, "is_empty", True):
        raise ValueError("AOI geometry is empty.")
    if aoi.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError("AOI must be a Polygon or MultiPolygon.")
    if not aoi.is_valid:
        repaired = aoi.buffer(0)
        if getattr(repaired, "is_empty", True):
            raise ValueError("AOI geometry is empty after repair.")
        if repaired.geom_type not in {"Polygon", "MultiPolygon"} or not repaired.is_valid:
            raise ValueError("AOI geometry is invalid.")
        return repaired
    return aoi


def serialize_copernicus_aoi(aoi: BaseGeometry | None) -> str:
    if aoi is None:
        return ""
    normalized = _normalize_aoi_geometry(aoi)
    return re.sub(
        r"\s+",
        " ",
        shapely_wkt.dumps(
            normalized,
            rounding_precision=_COPERNICUS_WKT_PRECISION,
            trim=True,
        ),
    ).strip()


def build_copernicus_filter(
    *,
    collection: str,
    product_type: str,
    start_date: str,
    end_date: str,
    aoi: BaseGeometry | None,
    tile_id: str | None = None,
) -> str:
    query = (
        f"Collection/Name eq {_odata_string(collection)} "
        f"and ContentDate/Start gt {_odata_string(f'{start_date}T00:00:00Z')} "
        f"and ContentDate/Start lt {_odata_string(f'{end_date}T23:59:59Z')}"
    )

    if product_type:
        query += (
            " and Attributes/OData.CSC.StringAttribute/any("
            "att:att/Name eq 'productType' and "
            f"att/OData.CSC.StringAttribute/Value eq {_odata_string(product_type)})"
        )

    if tile_id:
        query += (
            " and Attributes/OData.CSC.StringAttribute/any("
            "att:att/Name eq 'tileId' and "
            f"att/OData.CSC.StringAttribute/Value eq {_odata_string(tile_id)})"
        )

    aoi_wkt = serialize_copernicus_aoi(aoi)
    if aoi_wkt:
        query += f" and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}')"

    return query
