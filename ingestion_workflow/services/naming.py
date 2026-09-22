"""Filesystem-safe names for extracted tables.

Its own module because `services.nspond` needs only this, and importing it from
`services.create_analyses` used to drag in the OpenAI SDK -- half a second on
every `ingest` invocation, including `--help`.
"""

from __future__ import annotations

import re


def sanitize_table_id(table_id: str | None, index: int) -> str:
    """Sanitize table identifiers for filesystem-safe usage.

    The `table-{index + 1}` fallback is part of the ns-pond layout pondie reads;
    `extractors.utils.sanitize_table_id` zero-pads and is deliberately not this.
    """
    if table_id:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", table_id).strip("-")
        if normalized:
            return normalized.lower()
    return f"table-{index + 1}"


__all__ = ["sanitize_table_id"]
