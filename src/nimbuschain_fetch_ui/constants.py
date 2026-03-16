"""Shared constants and configuration for the Streamlit UI."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

# Prefer the mounted source tree inside the container over the installed wheel.
SRC_ROOT = Path(__file__).resolve().parents[1]

# Streamlit can be launched from any working directory (VSCode, terminal, etc.).
# Resolve everything against the project root (folder containing the repo).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOWNLOADS_DIR = Path(os.getenv("NIMBUS_UI_DATA_DIR", "/data/downloads"))
ZARR_STORES_DIR = DOWNLOADS_DIR / "zarr"
NOHUP_PATH = PROJECT_ROOT / "nohup.out"
PID_PATH = PROJECT_ROOT / "job_pid"
DEFAULT_API_URL = os.getenv("NIMBUS_SERVICE_URL", "http://nimbus-api:8000")
DEFAULT_API_KEY = os.getenv("NIMBUS_API_KEY", "")
RECENT_JOBS_WINDOW_HOURS = int(os.getenv("NIMBUS_UI_RECENT_JOBS_HOURS", "72"))
RECENT_JOB_CATEGORY_MINUTES = int(os.getenv("NIMBUS_UI_RECENT_JOB_MINUTES", "60"))
PROVIDER_ISSUE_WINDOW_MINUTES = int(os.getenv("NIMBUS_UI_PROVIDER_ISSUE_MINUTES", "15"))
RECENT_JOBS_LIMIT = int(os.getenv("NIMBUS_UI_RECENT_JOBS_LIMIT", "80"))
RECENT_JOBS_FETCH_LIMIT = max(100, RECENT_JOBS_LIMIT * 3)
JOB_MONITOR_REFRESH_EVERY = os.getenv("NIMBUS_UI_JOB_REFRESH_EVERY", "5s")
FINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}
ACTIVE_JOB_STATES = {"queued", "running", "cancel_requested"}


@dataclass(frozen=True)
class TilePaths:
    # Prefer mounted container paths, then workspace fallback.
    S2_GEOJSON: str = "/data/Sentinel-2-tiles/sentinel-2_grids.geojson"
    S2_NOCOV: str = "/data/Sentinel-2-tiles/sentinel-2_no_coverage.geojson"
    S2_GEOJSON_FALLBACK: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_grids.geojson")
    S2_NOCOV_FALLBACK: str = str(PROJECT_ROOT / "data" / "Sentinel-2-tiles" / "sentinel-2_no_coverage.geojson")
    LANDSAT_GEOJSON: str = "/data/Landsat-tiles/wrs2_descending.geojson"
    LANDSAT_GEOJSON_FALLBACK: str = str(PROJECT_ROOT / "data" / "Landsat-tiles" / "wrs2_descending.geojson")


@dataclass(frozen=True)
class MapConfig:
    MIN_GRID_ZOOM: int = 5
    DEFAULT_CENTER: Tuple[float, float] = (48.8566, 2.3522)
    DEFAULT_ZOOM: int = 8
    MAP_HEIGHT: int = 700


PATHS = TilePaths()
MCFG = MapConfig()

PROVIDERS: Dict[str, List[str]] = {
    "Copernicus": ["SENTINEL-1", "SENTINEL-2", "SENTINEL-3", "SENTINEL-5P"],
    "USGS": ["landsat_ot_c2_l1", "landsat_ot_c2_l2"],
}

PRODUCT_TYPES: Dict[str, List[str]] = {
    "SENTINEL-1": ["RAW", "GRD", "SLC", "IW_SLC__1S"],
    "SENTINEL-2": ["S2MSI1C", "S2MSI2A"],
    "SENTINEL-3": [
        "S3OL1EFR", "S3OL1ERR", "S3SL1RBT", "S3OL2WFR", "S3OL2WRR",
        "S3OL2LFR", "S3OL2LRR", "S3SL2LST", "S3SL2FRP", "S3SR2LAN",
        "S3SY2SYN", "S3SY2VGP", "S3SY2VG1", "S3SY2V10", "S3SY2AOD",
    ],
    "SENTINEL-5P": [
        "L2__NO2___", "L2__CH4___", "L2__CO____",
        "L2__O3____", "L2__SO2___", "L2__HCHO__",
    ],
    "landsat_ot_c2_l1": ["L1TP", "L1GT", "L1GS"],
    "landsat_ot_c2_l2": ["L2SP", "L2SR"],
}

# Explicit mapping from UI provider name to CLI/API value
PROVIDER_CLI_MAP: Dict[str, str] = {
    "Copernicus": "copernicus",
    "USGS": "usgs",
}


__all__ = [
    "SRC_ROOT",
    "PROJECT_ROOT",
    "DOWNLOADS_DIR",
    "ZARR_STORES_DIR",
    "NOHUP_PATH",
    "PID_PATH",
    "DEFAULT_API_URL",
    "DEFAULT_API_KEY",
    "RECENT_JOBS_WINDOW_HOURS",
    "RECENT_JOB_CATEGORY_MINUTES",
    "PROVIDER_ISSUE_WINDOW_MINUTES",
    "RECENT_JOBS_LIMIT",
    "RECENT_JOBS_FETCH_LIMIT",
    "JOB_MONITOR_REFRESH_EVERY",
    "FINAL_JOB_STATES",
    "ACTIVE_JOB_STATES",
    "TilePaths",
    "MapConfig",
    "PATHS",
    "MCFG",
    "PROVIDERS",
    "PRODUCT_TYPES",
    "PROVIDER_CLI_MAP",
]
