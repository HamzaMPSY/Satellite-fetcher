from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import yaml


@dataclass
class BandConfig:
    name: str
    patterns: list[str]
    categorical: bool = False


@dataclass
class CollectionConfig:
    collection: str
    bands: list[BandConfig]
    reference_band_order: list[str]
    subdir_filter: str | None = None
    extensions: set[str] | None = None


class ConfigLoader:
    """Loads band configuration from a YAML file."""

    def __init__(self, yaml_path: str | Path):
        self.yaml_path = Path(yaml_path)
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Band config YAML not found: {self.yaml_path}")

    def load(self) -> dict[str, CollectionConfig]:
        raw = yaml.safe_load(self.yaml_path.read_text()) or {}
        collections: dict[str, CollectionConfig] = {}
        for key, value in raw.items():
            bands_cfg = []
            for band in value.get("bands", []):
                bands_cfg.append(
                    BandConfig(
                        name=band["name"],
                        patterns=list(band.get("patterns", [])),
                        categorical=bool(band.get("categorical", False)),
                    )
                )
            collections[key] = CollectionConfig(
                collection=key,
                bands=bands_cfg,
                reference_band_order=list(value.get("reference_band_order", [])),
                subdir_filter=value.get("subdir_filter"),
                extensions=set(value.get("extensions", [])) or None,
            )
        return collections


def find_first(iterable: Iterable[Any], predicate) -> Any | None:
    for item in iterable:
        if predicate(item):
            return item
    return None