from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


ZARR_FORMAT_VERSION = 1


@dataclass(frozen=True)
class ChunkShape:
    time: int = 1
    band: int = 1
    y: int = 1024
    x: int = 1024


class ConversionError(ValueError):
    """Raised when a product or dataset cannot be normalized safely."""


class ConversionDependencyError(RuntimeError):
    """Raised when a required runtime dependency is unavailable."""


def _coerce_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
