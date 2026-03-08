"""Geospatial helpers used across the UI."""

from __future__ import annotations

import math
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import shapely
from shapely.geometry import Polygon, mapping
from shapely import wkt as shapely_wkt
from shapely.ops import unary_union

from nimbuschain_fetch_ui.aoi_utils import parse_aoi_text


def ensure_4326(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    return gdf.set_crs(epsg=4326) if gdf.crs is None else gdf.to_crs(epsg=4326)


def get_name_col(gdf: Optional[gpd.GeoDataFrame], system: str) -> Optional[str]:
    if gdf is None or gdf.empty:
        return None
    candidates = (
        ["PR", "PATH_ROW", "WRSPR", "PATH", "name", "Name"]
        if system == "landsat"
        else ["Name", "name", "TILE_ID", "tile_id", "MGRS_TILE", "mgrs"]
    )
    for c in candidates:
        if c in gdf.columns:
            return c
    for c in gdf.columns:
        if c != "geometry" and gdf[c].dtype == object:
            return c
    return None


def safe_union(geoms):
    """Robust union across Shapely versions and environments."""
    if not geoms:
        return None
    try:
        ua = getattr(shapely, "union_all", None)
        if callable(ua):
            return ua(geoms)
    except Exception:
        pass
    try:
        return unary_union(geoms)
    except Exception:
        pass
    u = geoms[0]
    for g in geoms[1:]:
        try:
            u = u.union(g)
        except Exception:
            continue
    return u


def parse_geometry(text: str):
    return parse_aoi_text(text)


def make_square_wkt(lat, lng, km):
    half = km / 2.0
    dlat = half / 111.0
    dlon = half / (111.0 * max(0.05, abs(math.cos(math.radians(lat)))))
    p = Polygon([
        (lng - dlon, lat - dlat), (lng + dlon, lat - dlat),
        (lng + dlon, lat + dlat), (lng - dlon, lat + dlat),
        (lng - dlon, lat - dlat),
    ])
    return shapely_wkt.dumps(p, rounding_precision=6)


def zoom_for_bounds(bounds: Tuple[float, float, float, float]) -> int:
    """Compute a practical Leaflet zoom level from lon/lat bounds span."""
    try:
        minx, miny, maxx, maxy = bounds
        span = max(abs(float(maxx) - float(minx)), abs(float(maxy) - float(miny)))
    except Exception:
        return 10

    if span > 120:
        return 2
    if span > 60:
        return 3
    if span > 30:
        return 4
    if span > 15:
        return 5
    if span > 8:
        return 6
    if span > 4:
        return 7
    if span > 2:
        return 8
    if span > 1:
        return 9
    if span > 0.5:
        return 10
    if span > 0.2:
        return 11
    if span > 0.1:
        return 12
    if span > 0.05:
        return 13
    return 14


def compute_intersections(polys, gdf, ncol):
    """Return intersecting tile names and intersecting subset GeoDataFrame."""
    if gdf is None or gdf.empty or not polys or not ncol:
        return [], None
    try:
        au = safe_union(polys)
        if au is None or getattr(au, "is_empty", True):
            return [], gdf.iloc[0:0]
        try:
            sindex = gdf.sindex
            possible_idx = list(sindex.intersection(au.bounds))
            candidates = gdf.iloc[possible_idx]
        except Exception:
            candidates = gdf
        c = candidates[candidates.intersects(au)].copy()
        if c.empty:
            return [], c
        c = c[[ncol, "geometry"]].copy()
        return sorted(c[ncol].astype(str).unique().tolist()), c
    except Exception:
        return [], None


def find_tiles(gdf, col, query, limit=50):
    q = (query or "").strip()
    if not q:
        return gdf.iloc[0:0]
    s = gdf[col].astype(str)
    exact = gdf[s.str.upper() == q.upper()]
    if not exact.empty:
        return exact[[col, "geometry"]].copy()
    return gdf[s.str.contains(q, case=False, na=False)][[col, "geometry"]].iloc[:limit].copy()


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:12]


def _compact_rings(geom, simplify_tol: float = 0.0, precision: int = 4) -> List[List[List[float]]]:
    if geom is None or getattr(geom, "is_empty", True):
        return []

    g = geom
    if simplify_tol > 0:
        try:
            g = g.simplify(simplify_tol, preserve_topology=True)
        except Exception:
            g = geom

    polys = []
    gtype = getattr(g, "geom_type", "")
    if gtype == "Polygon":
        polys = [g]
    elif gtype == "MultiPolygon":
        polys = [p for p in g.geoms if p is not None and not p.is_empty]
    else:
        return []

    rings: List[List[List[float]]] = []
    for p in polys:
        try:
            coords = [[round(float(x), precision), round(float(y), precision)] for x, y in p.exterior.coords]
        except Exception:
            continue
        if len(coords) < 4:
            continue
        if coords[0] == coords[-1]:
            coords = coords[:-1]
        if len(coords) >= 3:
            rings.append(coords)
    return rings


def selected_tiles_to_wkt(gdf, ncol, selected_tiles) -> str:
    if gdf is None or gdf.empty or not ncol or not selected_tiles:
        return ""

    sel = {str(t).strip() for t in selected_tiles if str(t).strip()}
    if not sel:
        return ""

    try:
        subset = gdf[gdf[ncol].astype(str).isin(sel)]
    except Exception:
        return ""

    wkts: List[str] = []
    for geom in subset.geometry:
        if geom is None or getattr(geom, "is_empty", True):
            continue
        gtype = getattr(geom, "geom_type", "")
        if gtype == "Polygon":
            wkts.append(geom.wkt)
        elif gtype == "MultiPolygon":
            wkts.extend([p.wkt for p in geom.geoms if p is not None and not p.is_empty])
    return "\n".join(wkts)


def selected_tiles_to_geometry(gdf, ncol, selected_tiles):
    if gdf is None or gdf.empty or not ncol or not selected_tiles:
        return None
    sel = {str(t).strip() for t in selected_tiles if str(t).strip()}
    if not sel:
        return None
    try:
        subset = gdf[gdf[ncol].astype(str).isin(sel)]
    except Exception:
        return None
    geoms = [g for g in subset.geometry if g is not None and not getattr(g, "is_empty", True)]
    return safe_union(geoms) if geoms else None


__all__ = [
    "ensure_4326",
    "get_name_col",
    "safe_union",
    "parse_geometry",
    "make_square_wkt",
    "zoom_for_bounds",
    "compute_intersections",
    "find_tiles",
    "_md5",
    "_compact_rings",
    "selected_tiles_to_wkt",
    "selected_tiles_to_geometry",
    "mapping",
]