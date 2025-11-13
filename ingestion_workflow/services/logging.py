"""Logging utilities for the ingestion workflow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


_ROOT_LOGGER_NAME = "ingestion_workflow"
_CONSOLE_FILTER_FLAG = "to_console"


class _ConsoleFilter(logging.Filter):
    """Allow only records flagged for console emission."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - simple predicate
        return bool(getattr(record, _CONSOLE_FILTER_FLAG, False))


def configure_logging(
    *,
    log_to_file: bool,
    log_file: Optional[Path],
    log_to_console: bool,
) -> None:
    """Configure logging sinks for this run."""

    root_logger = logging.getLogger(_ROOT_LOGGER_NAME)
    root_logger.setLevel(logging.INFO)
    root_logger.propagate = False

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if log_to_file and log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        console_handler.addFilter(_ConsoleFilter())
        root_logger.addHandler(console_handler)

    if not root_logger.handlers:  # pragma: no cover - fallback
        fallback_handler = logging.StreamHandler()
        fallback_handler.setFormatter(formatter)
        root_logger.addHandler(fallback_handler)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a namespaced logger rooted under ``ingestion_workflow``."""

    logger_name = _ROOT_LOGGER_NAME if not name else f"{_ROOT_LOGGER_NAME}.{name}"
    return logging.getLogger(logger_name)


def console_kwargs() -> dict[str, bool]:
    """Helper to flag log records for console emission."""

    return {_CONSOLE_FILTER_FLAG: True}
