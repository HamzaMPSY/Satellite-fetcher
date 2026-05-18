from __future__ import annotations

from typing import Any, Mapping

from shapely import wkt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


def parse_aoi(value: Any) -> BaseGeometry:
    """Parse AOI from {wkt|geojson} payload into a Shapely geometry."""

    if isinstance(value, Mapping):
        wkt_value = value.get("wkt")
        geojson_value = value.get("geojson")
    elif isinstance(value, str):
        wkt_value = value
        geojson_value = None
    else:
        raise ValueError("AOI must be an object containing 'wkt' or 'geojson'.")

    if bool(wkt_value) == bool(geojson_value):
        raise ValueError("AOI must contain exactly one of 'wkt' or 'geojson'.")

    if wkt_value:
        geom = wkt.loads(str(wkt_value))
    else:
        geom = shape(geojson_value)

    if geom.is_empty:
        raise ValueError("AOI geometry is empty.")

    if geom.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError("AOI must be a Polygon or MultiPolygon.")

    if not geom.is_valid:
        raise ValueError("AOI geometry is invalid.")

    return geom


def validate_aoi_payload(value: Any) -> None:
    parse_aoi(value)
