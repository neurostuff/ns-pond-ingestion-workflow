"""Compatibility patches for ACE."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from ace import sources as ace_sources

logger = logging.getLogger(__name__)

_PATCH_APPLIED = False


def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.debug("Failed to ensure directory %s: %s", path.parent, exc)


def _patched_download_table(self, url: str):
    """Patched version of Source._download_table that writes text safely."""

    table_html: Optional[str] = None
    table_dir = getattr(self, "table_dir", None)

    if table_dir is not None:
        filename = Path(table_dir) / url.replace("/", "_")
        _ensure_parent_dir(filename)
        if filename.exists():
            table_html = filename.read_text(encoding="utf-8")
        else:
            table_html = ace_sources.scrape.get_url(url)
            if table_html:
                filename.write_text(table_html, encoding="utf-8")
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
        logger.warning(
            "ACE Source._download_table is missing; skipping download patch."
        )
        return

    ace_sources.Source._download_table = _patched_download_table
    _PATCH_APPLIED = True
    logger.debug("Patched ACE Source._download_table to use text writes.")


# Ensure the patch is active as soon as the module is imported.
apply_patch()
