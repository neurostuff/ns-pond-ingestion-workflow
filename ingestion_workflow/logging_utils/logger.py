"""Lightweight logging configuration utilities."""
from __future__ import annotations

import logging
import sys
from typing import Optional

_STANDARD_LOG_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
}


class ExtraFormatter(logging.Formatter):
    """Formatter that appends log-record extras to the output."""

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_LOG_ATTRS and not key.startswith("_")
        }
        if extras:
            formatted = f"{formatted} | {extras}"
        return formatted


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger for CLI applications."""
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ExtraFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger instance, ensuring configure_logging has run."""
    if not logging.getLogger().handlers:
        configure_logging()
    return logging.getLogger(name or "ingestion_workflow")
