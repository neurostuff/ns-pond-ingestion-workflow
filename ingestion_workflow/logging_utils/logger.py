"""Lightweight logging configuration utilities."""
from __future__ import annotations

import logging
import sys
from typing import Optional


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger for CLI applications."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger instance, ensuring configure_logging has run."""
    if not logging.getLogger().handlers:
        configure_logging()
    return logging.getLogger(name or "ingestion_workflow")
