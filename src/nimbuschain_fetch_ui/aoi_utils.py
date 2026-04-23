from __future__ import annotations

import json
from typing import Iterable

import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


def safe_union(geoms: Iterable[BaseGeometry]) -> BaseGeometry | None:
    items = [geom for geom in geoms if geom is not None and not getattr(geom, "is_empty", True)]
    if not items:
        return None

    try:
        union_all = getattr(shapely, "union_all", None)
        if callable(union_all):
            return union_all(items)
    except Exception:
        pass

    try:
        return unary_union(items)
    except Exception:
        pass

    merged = items[0]
    for geom in items[1:]:
        try:
            merged = merged.union(geom)
        except Exception:
            continue
    return merged


def parse_aoi_text(text: str) -> BaseGeometry | None:
    if not text or not text.strip():
        return None

    raw = text.strip()
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
        except Exception:
            return None

        try:
            obj_type = str(obj.get("type", "")).strip()
            if obj_type == "Feature":
                geometry = obj.get("geometry")
                return shape(geometry) if geometry else None
            if obj_type == "FeatureCollection":
                geoms = []
                for feature in obj.get("features", []) or []:
                    geometry = feature.get("geometry") if isinstance(feature, dict) else None
                    if geometry:
                        geoms.append(shape(geometry))
                return safe_union(geoms)
            return shape(obj)
        except Exception:
            return None

    try:
        return shapely_wkt.loads(raw)
    except Exception:
        return None
