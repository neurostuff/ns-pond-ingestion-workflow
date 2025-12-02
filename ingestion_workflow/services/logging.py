"""Logging utilities for the ingestion workflow."""

from __future__ import annotations

import logging
import queue
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Optional


_ROOT_LOGGER_NAME = "ingestion_workflow"
_CONSOLE_FILTER_FLAG = "to_console"
_queue_listener: Optional[QueueListener] = None


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

    global _queue_listener
    if _queue_listener is not None:
        try:
            _queue_listener.stop()
        except Exception:  # pragma: no cover - best-effort shutdown
            pass
        _queue_listener = None

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.propagate = False

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handlers: list[logging.Handler] = []
    if log_to_file and log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    ingestion_logger = logging.getLogger(_ROOT_LOGGER_NAME)
    ingestion_logger.setLevel(logging.DEBUG)
    ingestion_logger.propagate = True

    for handler in list(ingestion_logger.handlers):
        ingestion_logger.removeHandler(handler)

    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        console_handler.addFilter(_ConsoleFilter())
        handlers.append(console_handler)

    if not handlers:
        # Fallback to console output when no other handlers exist.
        fallback_handler = logging.StreamHandler()
        fallback_handler.setFormatter(formatter)
        handlers.append(fallback_handler)

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)
    _queue_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    _queue_listener.start()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a namespaced logger rooted under ``ingestion_workflow``."""

    logger_name = _ROOT_LOGGER_NAME if not name else f"{_ROOT_LOGGER_NAME}.{name}"
    return logging.getLogger(logger_name)


def console_kwargs() -> dict[str, bool]:
    """Helper to flag log records for console emission."""

    return {_CONSOLE_FILTER_FLAG: True}
