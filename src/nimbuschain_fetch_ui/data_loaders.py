"""Data loading helpers for tile grids and derived structures."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import streamlit as st
from loguru import logger

from nimbuschain_fetch_ui.constants import PATHS
from nimbuschain_fetch_ui.geo_utils import (
    ensure_4326,
    get_name_col,
    _md5,
    _compact_rings,
)


@st.cache_data(show_spinner="Loading Sentinel-2 grid…")
def load_s2():
    tiles, nocov = None, None
    for p in [PATHS.S2_GEOJSON, PATHS.S2_GEOJSON_FALLBACK]:
        if tiles is not None:
            break
        try:
            if Path(p).exists():
                tiles = ensure_4326(gpd.read_file(p))
                logger.info(f"Loaded Sentinel-2 grid from {p} ({len(tiles)} tiles)")
        except Exception as e:
            logger.warning(f"Failed to read Sentinel-2 grid '{p}': {e}")
    try:
        for p in [PATHS.S2_NOCOV, PATHS.S2_NOCOV_FALLBACK]:
            if Path(p).exists():
                nocov = ensure_4326(gpd.read_file(p))
                break
    except Exception as e:
        logger.warning(f"Failed to read Sentinel-2 no-coverage '{PATHS.S2_NOCOV}': {e}")
        nocov = None
    if tiles is not None and not tiles.empty:
        try:
            _ = tiles.sindex
        except Exception:
            pass
    return tiles, nocov


@st.cache_data(show_spinner="Loading Landsat WRS-2 grid…")
def load_landsat():
    for p in [PATHS.LANDSAT_GEOJSON, PATHS.LANDSAT_GEOJSON_FALLBACK]:
        try:
            if Path(p).exists():
                gdf = ensure_4326(gpd.read_file(p))
                if gdf is not None and not gdf.empty:
                    logger.info(f"Loaded Landsat grid from {p} ({len(gdf)} tiles)")
                    try:
                        _ = gdf.sindex
                    except Exception:
                        pass
                return gdf
        except Exception as e:
            logger.warning(f"Failed to read Landsat grid '{p}': {e}")
    return None


def load_tiles():
    s2, s2n = load_s2()
    ls = load_landsat()
    return {"sentinel-2": {"tiles": s2, "nocov": s2n}, "landsat": {"tiles": ls, "nocov": None}}


@st.cache_data(show_spinner="Preparing grid for display…")
def prepare_compact_grid(_gdf_id: str, ncol: str, system: str) -> Tuple[str, str]:
    gdf = st.session_state.get(f"_raw_gdf_{system}")
    if gdf is None or gdf.empty or not ncol:
        return "[]", ""
    names = gdf[ncol].astype(str).tolist()
    geoms = gdf.geometry.tolist()
    bdf = gdf.geometry.bounds.round(4)
    include_geom = (system == "landsat")
    simplify_tol = 0.02 if include_geom else 0.0
    features = []
    for name, geom, minx, miny, maxx, maxy in zip(
        names, geoms, bdf["minx"], bdf["miny"], bdf["maxx"], bdf["maxy"]
    ):
        item = {"n": name, "b": [float(minx), float(miny), float(maxx), float(maxy)]}
        if include_geom:
            rings = _compact_rings(geom, simplify_tol=simplify_tol, precision=4)
            if rings:
                item["g"] = rings
        features.append(item)
    js = json.dumps(features, separators=(",", ":"))
    return js, _md5(js)


@st.cache_data(show_spinner=False)
def prepare_nocov_geojson(_nocov_id: str) -> Tuple[str, str]:
    gdf = st.session_state.get("_raw_nocov")
    if gdf is None or gdf.empty:
        return "null", ""
    simplified = gdf.copy()
    simplified["geometry"] = simplified.geometry.simplify(0.01, preserve_topology=True)
    js = simplified[["geometry"]].to_json()
    return js, _md5(js)


__all__ = [
    "load_tiles",
    "load_s2",
    "load_landsat",
    "prepare_compact_grid",
    "prepare_nocov_geojson",
]
