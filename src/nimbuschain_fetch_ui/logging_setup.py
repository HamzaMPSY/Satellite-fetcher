"""Centralized Loguru setup for the Streamlit UI."""
from __future__ import annotations

import sys
from pathlib import Path
from loguru import logger


def configure_logging(log_path: Path) -> None:
    """Configure loguru sinks for console and file output."""
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    logger.add(
        str(log_path),
        level="DEBUG",
        rotation="5 MB",
        retention="3 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
    )


__all__ = ["configure_logging", "logger"]
