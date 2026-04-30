from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from nimbuschain_shared.resolution import target_pixel_size_for as shared_target_pixel_size_for
from nimbuschain_zarr_service.models import CollectionProductSpecRecord, ConverterConfigRecord


def _config_candidates() -> list[Path]:
    here = Path(__file__).resolve().parent
    return [
        here / "config" / "config.yaml",
        Path.cwd() / "src" / "nimbuschain_zarr_service" / "config" / "config.yaml",
    ]


@lru_cache(maxsize=1)
def load_converter_config() -> dict[str, Any]:
    for candidate in _config_candidates():
        if candidate.exists():
            return yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
    raise FileNotFoundError("nimbuschain_zarr_service config.yaml not found")


def load_converter_config_record() -> ConverterConfigRecord:
    return ConverterConfigRecord.from_mapping(load_converter_config())


def resolution_policy() -> dict[str, Any]:
    return dict(load_converter_config().get("resolution_policy", {}))


def collection_resolution_policy(provider: str, collection: str) -> dict[str, Any]:
    policy = resolution_policy()
    collection_map = dict(policy.get("collections") or {})
    normalized_collection = (
        collection.strip().upper()
        if provider.strip().lower() == "copernicus"
        else collection.strip().lower()
    )
    entry = collection_map.get(normalized_collection) or collection_map.get(collection)
    return dict(entry or {})


def target_pixel_size_for(provider: str, collection: str) -> float | None:
    return shared_target_pixel_size_for(provider, collection)


def supported_collections() -> dict[str, list[str]]:
    config = load_converter_config()
    return {
        "copernicus": list((config.get("copernicus") or {}).keys()),
        "usgs": list((config.get("usgs") or {}).keys()),
    }


def supported_product_types() -> dict[str, list[str]]:
    config = load_converter_config()
    product_types: dict[str, list[str]] = {}
    for collection, entries in (config.get("copernicus") or {}).items():
        product_types[collection] = list((entries or {}).keys())
    for collection, entries in (config.get("usgs") or {}).items():
        product_types[collection] = list((entries or {}).keys())
    return product_types


def get_copernicus_product_spec(collection: str, product_type: str) -> dict[str, Any]:
    config = load_converter_config().get("copernicus", {})
    return dict((((config.get(collection) or {}).get(product_type)) or {}))


def get_landsat_product_spec(collection: str, product_type: str) -> dict[str, Any]:
    config = load_converter_config().get("usgs", {})
    return dict((((config.get(collection) or {}).get(product_type)) or {}))


def get_copernicus_product_spec_record(collection: str, product_type: str) -> CollectionProductSpecRecord:
    return CollectionProductSpecRecord(
        provider="copernicus",
        collection=collection,
        product_type=product_type,
        payload=get_copernicus_product_spec(collection, product_type),
    )


def get_landsat_product_spec_record(collection: str, product_type: str) -> CollectionProductSpecRecord:
    return CollectionProductSpecRecord(
        provider="usgs",
        collection=collection,
        product_type=product_type,
        payload=get_landsat_product_spec(collection, product_type),
    )
