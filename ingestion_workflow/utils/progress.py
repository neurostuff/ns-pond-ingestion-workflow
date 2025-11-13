"""Utility helpers for consistent progress reporting."""

from __future__ import annotations

import logging
from typing import Callable

from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def emit_progress(
    hook: Callable[[int], None] | None,
    step: int = 1,
) -> None:
    """Invoke a progress hook while ignoring consumer failures."""
    if hook is None or step <= 0:
        return
    try:
        hook(step)
    except Exception:  # pragma: no cover - diagnostic only
        logger.debug("Progress hook raised an exception.", exc_info=True)


def progress_callback(progress: tqdm | None) -> Callable[[int], None] | None:
    """Create a hook that updates the provided tqdm progress bar."""
    if progress is None:
        return None

    def _hook(increment: int = 1) -> None:
        if increment <= 0:
            return
        progress.update(increment)

    return _hook


__all__ = ["emit_progress", "progress_callback"]
