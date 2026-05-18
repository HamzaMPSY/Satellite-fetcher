"""Centralized Loguru setup for the Streamlit UI."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from loguru import logger


def _resolve_log_sink(log_path: Path) -> Path | None:
    """Return a writable log path, falling back to a temp location if needed."""
    candidates = [
        log_path,
        Path(tempfile.gettempdir()) / "nimbuschain_fetch_ui" / log_path.name,
    ]
    for candidate in candidates:
        try:
            candidate.parent.mkdir(parents=True, exist_ok=True)
            with candidate.open("a", encoding="utf-8"):
                pass
            return candidate
        except OSError:
            continue
    return None


def configure_logging(log_path: Path) -> None:
    """Configure loguru sinks for console and file output."""
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    resolved_log_path = _resolve_log_sink(log_path)
    if resolved_log_path is None:
        logger.warning("File logging disabled: no writable log path for {}", log_path)
        return
    if resolved_log_path != log_path:
        logger.warning("File logging redirected from {} to {}", log_path, resolved_log_path)
    logger.add(
        str(resolved_log_path),
        level="DEBUG",
        rotation="5 MB",
        retention="3 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
    )


__all__ = ["configure_logging", "logger"]
