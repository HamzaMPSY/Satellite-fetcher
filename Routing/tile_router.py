from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np

log = logging.getLogger("nimbus.tile_router")

# ---------------------------------------------------------------------------
# Public constants — thresholds live here so callers can override them
# ---------------------------------------------------------------------------

# Cloud masking
CLOUD_SKIP_THRESHOLD   = 0.90   # ≥90 % cloud → SKIP immediately
CLOUD_PARTIAL          = 0.30   # ≥30 % cloud → treat as MIXED (less reliable)

# Index thresholds
NDVI_WATER_MAX         = 0.00   # NDVI ≤ 0   AND NDWI ≥ 0.2 → WATER
NDWI_WATER_MIN         = 0.20
NDVI_VEG_MIN           = 0.45   # NDVI ≥ 0.45 → DENSE_VEGETATION
NDVI_SOIL_MAX          = 0.15   # NDVI ≤ 0.15 AND NDWI < 0.05 → BARE_SOIL
NDWI_SOIL_MAX          = 0.05
BRIGHTNESS_URBAN_MIN   = 0.08   # brightness variance ≥ 0.08 → candidate URBAN
NDVI_URBAN_MAX         = 0.30   # NDVI < 0.30 (not strongly vegetated)

# Sen2Like step names (mirrors Pipeline.steps order)
ALL_STEPS = [
    "geometric_processing",
    "atmospheric_correction",
    "sbaf",
    "valid_pixel_mask",
    "brdf_adjustment",
    "data_fusion",
    "packaging",
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class TileClass(str, Enum):
    SKIP              = "SKIP"
    WATER             = "WATER"
    DENSE_VEGETATION  = "DENSE_VEGETATION"
    BARE_SOIL         = "BARE_SOIL"
    URBAN             = "URBAN"
    MIXED             = "MIXED"


@dataclass
class TileProfile:
    """Quick statistics computed from raw tile bands."""
    tile_id:           str
    sensor:            str                  # e.g. "LANDSAT_8", "SENTINEL_2A"
    cloud_fraction:    float                # 0.0 – 1.0
    ndvi:              float                # Normalised Difference Vegetation Index
    ndwi:              float                # Normalised Difference Water Index
    brightness_var:    float                # spatial variance of visible brightness
    nodata_fraction:   float = 0.0
    extra_stats:       dict  = field(default_factory=dict)


@dataclass
class RoutedTile:
    tile_id:    str
    tile_class: TileClass
    steps:      list[str]
    profile:    TileProfile
    metadata:   dict = field(default_factory=dict)


@dataclass
class TileResult:
    tile_id:    str
    tile_class: TileClass
    steps_run:  list[str]
    outputs:    dict      = field(default_factory=dict)
    skipped:    bool      = False
    error:      str       = ""


# ---------------------------------------------------------------------------
# Step 1 — Tile Profiling
# ---------------------------------------------------------------------------

SENSOR_BAND_MAP: dict[str, dict[str, int]] = {
    "LANDSAT_8": {
        "blue":  1,   # B2
        "green": 2,   # B3
        "red":   3,   # B4
        "nir":   4,   # B5
        "swir1": 5,   # B6
        "swir2": 6,   # B7
    },
    "LANDSAT_9": {   # same layout as L8
        "blue":  1,
        "green": 2,
        "red":   3,
        "nir":   4,
        "swir1": 5,
        "swir2": 6,
    },
    "SENTINEL_2A": {
        "blue":  0,   # B2
        "green": 1,   # B3
        "red":   2,   # B4
        "nir":   3,   # B8
        "swir1": 4,   # B11
        "swir2": 5,   # B12
    },
    "SENTINEL_2B": {
        "blue":  0,
        "green": 1,
        "red":   2,
        "nir":   3,
        "swir1": 4,
        "swir2": 5,
    },
}

FALLBACK_LOGICAL_NAMES = {
    # Landsat numeric
    "B2": "blue", "B3": "green", "B4": "red",
    "B5": "nir",  "B6": "swir1", "B7": "swir2",
    # Sentinel-2 numeric
    "B02": "blue", "B03": "green", "B04": "red",
    "B08": "nir",  "B11": "swir1", "B12": "swir2",
    # Color-name labels (compact routing tiles from _load_tile_array_for_routing)
    "BLUE": "blue", "GREEN": "green", "RED": "red",
    "NIR":  "nir",  "SWIR1": "swir1", "SWIR2": "swir2",
}


def _band_index(
    logical: str,
    sensor: str,
    band_names: list[str] | None,
    n_bands: int,
) -> int | None:
    if band_names:
        for i, name in enumerate(band_names):
            mapped = FALLBACK_LOGICAL_NAMES.get(name.upper())
            if mapped == logical and i < n_bands:
                return i

    sensor_key = sensor.upper().replace("-", "_").replace(" ", "_")
    if sensor_key in SENSOR_BAND_MAP:
        idx = SENSOR_BAND_MAP[sensor_key].get(logical)
        if idx is not None and idx < n_bands:
            return idx

    return None


def profile_tile(tile: np.ndarray, metadata: dict) -> TileProfile:
    
    tile_id    = metadata.get("tile_id", "unknown")
    sensor     = metadata.get("sensor",  "UNKNOWN")
    band_names = metadata.get("band_names", None)
    nodata_val = float(metadata.get("nodata", -9999.0))
    n_bands, rows, cols = tile.shape
    n_pixels = rows * cols

    # ------------------------------------------------------------------
    # Mask construction
    # ------------------------------------------------------------------
    if np.isnan(tile).any():
        nodata_mask = np.isnan(tile).any(axis=0)
    else:
        nodata_mask = (tile == nodata_val).any(axis=0)

    nodata_fraction = float(nodata_mask.sum()) / n_pixels if n_pixels > 0 else 1.0

    # Cloud fraction — prefer supplied cloud mask, otherwise approximate
    cloud_mask_raw = metadata.get("cloud_mask")
    if cloud_mask_raw is not None:
        cm = np.asarray(cloud_mask_raw, dtype=bool)
        # Exclude nodata pixels from cloud fraction denominator
        valid_count = max(int((~nodata_mask).sum()), 1)
        cloud_fraction = float((cm & ~nodata_mask).sum()) / valid_count
    else:
        # Approximate: bright, low-NDVI pixels in visible bands
        cloud_fraction = _estimate_cloud_fraction(tile, sensor, band_names, nodata_mask)

    # ------------------------------------------------------------------
    # Index computation helpers
    # ------------------------------------------------------------------
    def _get_band(logical: str) -> np.ndarray | None:
        idx = _band_index(logical, sensor, band_names, n_bands)
        if idx is None:
            return None
        b = tile[idx].astype(np.float32)
        b[nodata_mask] = np.nan
        return b

    red_band  = _get_band("red")
    nir_band  = _get_band("nir")
    green_band = _get_band("green")
    swir1_band = _get_band("swir1")

    # ------------------------------------------------------------------
    # NDVI  — (NIR - Red) / (NIR + Red)
    # ------------------------------------------------------------------
    ndvi = _safe_index(nir_band, red_band, default=0.0)

    # ------------------------------------------------------------------
    # NDWI  — (Green - NIR) / (Green + NIR)   [McFeeters 1996]
    # ------------------------------------------------------------------
    ndwi = _safe_index(green_band, nir_band, default=0.0)

    # ------------------------------------------------------------------
    # Brightness variance — spatial variance of mean visible brightness
    # ------------------------------------------------------------------
    brightness_var = _brightness_variance(tile, sensor, band_names, nodata_mask)

    # ------------------------------------------------------------------
    # Extra stats (non-blocking)
    # ------------------------------------------------------------------
    extra: dict[str, Any] = {}
    if swir1_band is not None and nir_band is not None:
        extra["ndsi"] = float(_safe_index(green_band, swir1_band, default=0.0))
    if red_band is not None:
        valid_red = red_band[~np.isnan(red_band)]
        extra["mean_red"] = float(valid_red.mean()) if valid_red.size > 0 else 0.0

    return TileProfile(
        tile_id        = tile_id,
        sensor         = sensor,
        cloud_fraction = cloud_fraction,
        ndvi           = ndvi,
        ndwi           = ndwi,
        brightness_var = brightness_var,
        nodata_fraction= nodata_fraction,
        extra_stats    = extra,
    )


def _safe_index(
    a: np.ndarray | None,
    b: np.ndarray | None,
    *,
    default: float = 0.0,
) -> float:
    """
    Compute nanmean( (a - b) / (a + b) ).
    Returns ``default`` if inputs are None or all-NaN.
    """
    if a is None or b is None:
        return default
    denom = a + b
    denom[denom == 0] = np.nan
    idx_map = (a - b) / denom
    valid = idx_map[~np.isnan(idx_map)]
    return float(valid.mean()) if valid.size > 0 else default


def _estimate_cloud_fraction(
    tile: np.ndarray,
    sensor: str,
    band_names: list[str] | None,
    nodata_mask: np.ndarray,
) -> float:

    n_bands = tile.shape[0]
    blue_idx = _band_index("blue", sensor, band_names, n_bands)
    nir_idx  = _band_index("nir",  sensor, band_names, n_bands)

    if blue_idx is None and nir_idx is None:
        return 0.0

    valid_mask = ~nodata_mask
    valid_count = int(valid_mask.sum())
    if valid_count == 0:
        return 0.0

    cloud_candidate = np.zeros(tile.shape[1:], dtype=bool)
    if blue_idx is not None:
        cloud_candidate |= (tile[blue_idx] > 0.20) & valid_mask
    if nir_idx is not None:
        cloud_candidate |= (tile[nir_idx]  > 0.25) & valid_mask

    return float(cloud_candidate.sum()) / valid_count


def _brightness_variance(
    tile: np.ndarray,
    sensor: str,
    band_names: list[str] | None,
    nodata_mask: np.ndarray,
) -> float:
    n_bands = tile.shape[0]
    visible_bands = []
    for logical in ("blue", "green", "red"):
        idx = _band_index(logical, sensor, band_names, n_bands)
        if idx is not None:
            b = tile[idx].astype(np.float32)
            b[nodata_mask] = np.nan
            visible_bands.append(b)

    if not visible_bands:
        # Fall back to first 3 bands
        for i in range(min(3, n_bands)):
            b = tile[i].astype(np.float32)
            b[nodata_mask] = np.nan
            visible_bands.append(b)

    if not visible_bands:
        return 0.0

    brightness = np.nanmean(np.stack(visible_bands, axis=0), axis=0)
    valid = brightness[~np.isnan(brightness)]
    return float(np.var(valid)) if valid.size > 0 else 0.0


# ---------------------------------------------------------------------------
# Step 2 — Classification
# ---------------------------------------------------------------------------

def classify_tile(
    profile: TileProfile,
    *,
    cloud_skip: float      = CLOUD_SKIP_THRESHOLD,
    cloud_partial: float   = CLOUD_PARTIAL,
    ndvi_water_max: float  = NDVI_WATER_MAX,
    ndwi_water_min: float  = NDWI_WATER_MIN,
    ndvi_veg_min: float    = NDVI_VEG_MIN,
    ndvi_soil_max: float   = NDVI_SOIL_MAX,
    ndwi_soil_max: float   = NDWI_SOIL_MAX,
    brightness_urban: float= BRIGHTNESS_URBAN_MIN,
    ndvi_urban_max: float  = NDVI_URBAN_MAX,
) -> TileClass:

    cf  = profile.cloud_fraction
    ndf = profile.nodata_fraction

    # 1. Skip worthless tiles immediately
    if cf >= cloud_skip or ndf >= 0.95:
        return TileClass.SKIP


    partially_cloudy = cf >= cloud_partial

    # 2. Water
    if profile.ndvi <= ndvi_water_max and profile.ndwi >= ndwi_water_min:
        return TileClass.WATER

    # 3. Dense vegetation
    if profile.ndvi >= ndvi_veg_min and not partially_cloudy:
        return TileClass.DENSE_VEGETATION

    # 4. Bare soil / arid
    if (
        profile.ndvi <= ndvi_soil_max
        and profile.ndwi <= ndwi_soil_max
        and not partially_cloudy
    ):
        return TileClass.BARE_SOIL

    # 5. Urban — spectrally heterogeneous, low vegetation
    if (
        profile.brightness_var >= brightness_urban
        and profile.ndvi <= ndvi_urban_max
        and not partially_cloudy
    ):
        return TileClass.URBAN

    # 6. Mixed / partially cloudy / ambiguous
    return TileClass.MIXED


# ---------------------------------------------------------------------------
# Step 3 — Routing
# ---------------------------------------------------------------------------

_ROUTING_TABLE: dict[TileClass, list[str]] = {
    TileClass.SKIP: [],

    TileClass.WATER: [
        "geometric_processing",
        "atmospheric_correction",
        "valid_pixel_mask",
        "data_fusion",
        "packaging",
    ],

    TileClass.DENSE_VEGETATION: [
        "geometric_processing",
        "atmospheric_correction",
        "sbaf",
        "valid_pixel_mask",
        "brdf_adjustment",
        "data_fusion",
        "packaging",
    ],

    TileClass.BARE_SOIL: [
        "geometric_processing",
        "atmospheric_correction",
        "sbaf",
        "valid_pixel_mask",
        "brdf_adjustment",
        "data_fusion",
        "packaging",
    ],

    TileClass.URBAN: [
        "geometric_processing",
        "atmospheric_correction",
        "sbaf",           # spectral adjustment critical in urban
        "valid_pixel_mask",
        "brdf_adjustment",
        "data_fusion",
        "packaging",
    ],

    TileClass.MIXED: [   # safest: full pipeline
        "geometric_processing",
        "atmospheric_correction",
        "sbaf",
        "valid_pixel_mask",
        "brdf_adjustment",
        "data_fusion",
        "packaging",
    ],
}


def route_tile(tile_class: TileClass) -> list[str]:

    steps = _ROUTING_TABLE.get(tile_class, list(ALL_STEPS))
    return list(steps)  # defensive copy


# ---------------------------------------------------------------------------
# Step 4 — Spark Pipeline
# ---------------------------------------------------------------------------

def build_pipeline(sc, tile_rdd, *, profile_kwargs=None, classify_kwargs=None):
 
    profile_kwargs  = profile_kwargs  or {}
    classify_kwargs = classify_kwargs or {}

    # Broadcast config dicts so workers don't re-serialise them for every tile
    bc_pk = sc.broadcast(profile_kwargs)
    bc_ck = sc.broadcast(classify_kwargs)

    def _process(tile_and_meta):
        import numpy as _np   # noqa: F401 — ensure numpy available in executor
        from Routing.tile_router import (  # re-import in executor
            profile_tile   as _profile,
            classify_tile  as _classify,
            route_tile     as _route,
            TileResult     as _TileResult,
            TileClass      as _TileClass,
        )

        tile, meta = tile_and_meta
        tile_id    = meta.get("tile_id", "unknown")

        try:
            profile    = _profile(tile, meta, **bc_pk.value)
            tile_class = _classify(profile, **bc_ck.value)
            steps      = _route(tile_class)

            if tile_class == _TileClass.SKIP:
                return _TileResult(
                    tile_id    = tile_id,
                    tile_class = tile_class,
                    steps_run  = [],
                    skipped    = True,
                )

            # ---- Execute the routed steps --------------------------------
            outputs = {}
            for step_name in steps:
                step_fn = _get_step_fn(step_name)
                if step_fn is not None:
                    outputs.update(step_fn(tile, meta, profile, outputs) or {})

            return _TileResult(
                tile_id    = tile_id,
                tile_class = tile_class,
                steps_run  = steps,
                outputs    = outputs,
            )

        except Exception as exc:
            import traceback as _tb
            return _TileResult(
                tile_id    = tile_id,
                tile_class = TileClass.MIXED,
                steps_run  = [],
                error      = f"{exc}\n{_tb.format_exc()}",
            )

    return tile_rdd.map(_process)


# ---------------------------------------------------------------------------
# Step registry — maps step name → callable
# ---------------------------------------------------------------------------
_STEP_REGISTRY: dict[str, Any] = {}


def register_step(name: str, fn) -> None:
    if name not in ALL_STEPS:
        log.warning(
            "Registering unknown step '%s'; not in ALL_STEPS=%s", name, ALL_STEPS
        )
    _STEP_REGISTRY[name] = fn


def _get_step_fn(name: str):
    fn = _STEP_REGISTRY.get(name)
    if fn is None:
        log.debug("[router] No implementation registered for step '%s' — using stub", name)
    return fn


# ---------------------------------------------------------------------------
# DataFrame API (alternative entry point)
# ---------------------------------------------------------------------------

def build_pipeline_df(spark, tile_df, *, profile_kwargs=None, classify_kwargs=None):

    import io
    import json
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, BooleanType, ArrayType
    )

    profile_kwargs  = profile_kwargs  or {}
    classify_kwargs = classify_kwargs or {}

    RESULT_SCHEMA = StructType([
        StructField("tile_id",    StringType(),  True),
        StructField("tile_class", StringType(),  True),
        StructField("steps_run",  ArrayType(StringType()), True),
        StructField("skipped",    BooleanType(), True),
        StructField("error",      StringType(),  True),
    ])

    def _process_row(row):
        import io as _io
        import json as _json
        import numpy as _np
        from Routing.tile_router import profile_tile, classify_tile, route_tile, TileClass

        tile_id = row["tile_id"]
        try:
            tile_array = _np.load(_io.BytesIO(row["tile_data"]))
            meta       = _json.loads(row["metadata"])
            meta.setdefault("tile_id", tile_id)

            profile    = profile_tile(tile_array, meta, **profile_kwargs)
            tile_class = classify_tile(profile, **classify_kwargs)
            steps      = route_tile(tile_class)

            return (
                tile_id,
                tile_class.value,
                steps,
                tile_class == TileClass.SKIP,
                "",
            )
        except Exception as exc:
            return (tile_id, "MIXED", [], False, str(exc))

    result_rdd = tile_df.rdd.map(_process_row)
    return spark.createDataFrame(result_rdd, schema=RESULT_SCHEMA)


# ---------------------------------------------------------------------------
# Convenience wrapper for single-tile testing / unit tests
# ---------------------------------------------------------------------------

class TileRouter:


    def __init__(self, profile_kwargs=None, classify_kwargs=None):
        self.profile_kwargs  = profile_kwargs  or {}
        self.classify_kwargs = classify_kwargs or {}

    def process_single(
        self,
        tile: np.ndarray,
        metadata: dict,
    ) -> tuple[TileProfile, TileClass, list[str]]:
       
        profile    = profile_tile(tile, metadata, **self.profile_kwargs)
        tile_class = classify_tile(profile, **self.classify_kwargs)
        steps      = route_tile(tile_class)
        return profile, tile_class, steps

    def summary(self, tile: np.ndarray, metadata: dict) -> str:
        profile, tile_class, steps = self.process_single(tile, metadata)
        lines = [
            f"Tile      : {profile.tile_id}",
            f"Sensor    : {profile.sensor}",
            f"Class     : {tile_class.value}",
            f"Cloud %   : {profile.cloud_fraction * 100:.1f}",
            f"NDVI      : {profile.ndvi:+.3f}",
            f"NDWI      : {profile.ndwi:+.3f}",
            f"Bright Var: {profile.brightness_var:.5f}",
            f"Steps     : {' → '.join(steps) if steps else '(none — SKIP)'}",
        ]
        return "\n".join(lines)