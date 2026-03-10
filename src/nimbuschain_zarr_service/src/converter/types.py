from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import numpy as np


@dataclass
class SatelliteData:
    """Container for satellite imagery bands, masks, and metadata."""

    bands: np.ndarray

    masks: np.ndarray
    metadata: Dict[str, Any]
    product_id: str
    band_names: List[str]
    timestamps: List[datetime]
    crs: str
    transform: Tuple[float, ...]

    def __post_init__(self):
        if self.bands.ndim == 3:
            self.bands = self.bands[np.newaxis, ...]
        if self.masks.ndim == 2:
            self.masks = self.masks[np.newaxis, ...]


SentinelData = SatelliteData
LandsatData = SatelliteData
