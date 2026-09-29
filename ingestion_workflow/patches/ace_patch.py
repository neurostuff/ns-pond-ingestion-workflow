"""Compatibility patches for ACE."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from ace import sources as ace_sources

logger = logging.getLogger(__name__)

_PATCH_APPLIED = False

#: Set to skip tables ACE would fetch from a separate URL.
#:
#: An environment variable rather than a module flag, because extraction runs in
#: a process pool: a flag set in the parent does not reach a spawned worker,
#: while the environment does.
SKIP_REMOTE_ENV = "NSPOND_ACE_SKIP_REMOTE_TABLES"


def set_skip_remote_tables(skip: bool) -> None:
    """Turn the skip on or off for this process and any it spawns."""
    if skip:
        os.environ[SKIP_REMOTE_ENV] = "1"
    else:
        os.environ.pop(SKIP_REMOTE_ENV, None)


def skipping_remote_tables() -> bool:
    return os.environ.get(SKIP_REMOTE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.debug("Failed to ensure directory %s: %s", path.parent, exc)


def _patched_download_table(self, url: str):
    """Source._download_table, writing text safely and optionally offline.

    A cached table is always used. Only the fetch is skipped, so a run with the
    skip on still returns every table downloaded by an earlier run.
    """

    table_html: Optional[str] = None
    table_dir = getattr(self, "table_dir", None)
    skip = skipping_remote_tables()

    if table_dir is not None:
        filename = Path(table_dir) / url.replace("/", "_")
        _ensure_parent_dir(filename)
        if filename.exists():
            table_html = filename.read_text(encoding="utf-8")
        elif skip:
            logger.debug("Skipping remote table %s", url)
            return None
        else:
            table_html = ace_sources.scrape.get_url(url)
            if table_html:
                filename.write_text(table_html, encoding="utf-8")
    elif skip:
        logger.debug("Skipping remote table %s", url)
        return None
    else:
        table_html = ace_sources.scrape.get_url(url)

    if table_html:
        table_html = self.decode_html_entities(table_html)
        return ace_sources.BeautifulSoup(table_html, "lxml")

    return None


def apply_patch() -> None:
    """Apply the ACE patches exactly once."""

    global _PATCH_APPLIED
    if _PATCH_APPLIED:
        return

    original = getattr(ace_sources.Source, "_download_table", None)
    if original is None:  # pragma: no cover - defensive guard
        logger.warning("ACE Source._download_table is missing; skipping download patch.")
        return

    ace_sources.Source._download_table = _patched_download_table
    _PATCH_APPLIED = True
    logger.debug("Patched ACE Source._download_table to use text writes.")


# Ensure the patch is active as soon as the module is imported.
apply_patch()
