from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlparse

from nimbuschain_shared.zarr import ConversionError


_CONTAINER_DATA_PREFIXES = (
    "/data/downloads",
    "/app/data/downloads",
    "/download",
    "/downloads",
    "/app/download",
    "/app/downloads",
)


def _project_data_root() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "downloads"


def _resolve_candidate(path: Path) -> Path:
    try:
        return path.expanduser().resolve(strict=False)
    except TypeError:  # pragma: no cover - compatibility guard
        return path.expanduser().resolve()


def _host_data_root_candidates() -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for raw in (
        os.getenv("NIMBUS_HOST_DATA_DIR"),
        os.getenv("NIMBUS_DATA_DIR"),
        str(_project_data_root()),
    ):
        value = str(raw or "").strip()
        if not value:
            continue
        candidate = Path(value).expanduser()
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)
    return candidates


def _map_container_data_uri(raw_value: str, *, allow_missing: bool) -> Path | None:
    for prefix in _CONTAINER_DATA_PREFIXES:
        if raw_value == prefix or raw_value.startswith(prefix + "/"):
            suffix = raw_value[len(prefix) :].lstrip("/")
            fallback: Path | None = None
            for root in _host_data_root_candidates():
                mapped = root / suffix if suffix else root
                if mapped.exists():
                    return _resolve_candidate(mapped)
                if allow_missing and fallback is None:
                    fallback = _resolve_candidate(mapped)
            return fallback
    return None


def _map_data_downloads_suffix(candidate: Path, *, allow_missing: bool) -> Path | None:
    parts = list(candidate.parts)
    for idx in range(len(parts) - 1):
        if parts[idx] == "data" and parts[idx + 1] == "downloads":
            suffix = parts[idx + 2 :]
            fallback: Path | None = None
            for root in _host_data_root_candidates():
                mapped = root.joinpath(*suffix)
                if mapped.exists():
                    return _resolve_candidate(mapped)
                if allow_missing and fallback is None:
                    fallback = _resolve_candidate(mapped)
            return fallback
    return None


def local_path_for_uri(uri: str, *, allow_missing: bool = True) -> Path:
    raw_value = str(uri or "").strip()
    if not raw_value:
        return _resolve_candidate(Path("."))

    parsed = urlparse(raw_value)
    if parsed.scheme and parsed.scheme.lower() not in {"", "file"}:
        raise ConversionError(f"Unsupported local path URI for masking: {uri}")

    if parsed.scheme.lower() == "file":
        raw_value = unquote(parsed.path)

    mapped = _map_container_data_uri(raw_value, allow_missing=allow_missing)
    if mapped is not None:
        return mapped

    candidate = Path(raw_value).expanduser()
    if candidate.exists():
        return candidate.resolve()

    mapped = _map_data_downloads_suffix(candidate, allow_missing=allow_missing)
    if mapped is not None:
        return mapped

    return _resolve_candidate(candidate)


__all__ = ["local_path_for_uri"]
