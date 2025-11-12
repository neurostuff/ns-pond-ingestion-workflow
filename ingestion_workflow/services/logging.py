"""Minimal logging utilities for the ingestion workflow."""

from __future__ import annotations

import logging
from typing import Optional


_ROOT_LOGGER_NAME = "ingestion_workflow"


def _ensure_root_logger() -> logging.Logger:
    logger = logging.getLogger(_ROOT_LOGGER_NAME)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a namespaced logger (stub implementation)."""

    _ensure_root_logger()
    if not name:
        return logging.getLogger(_ROOT_LOGGER_NAME)
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")
