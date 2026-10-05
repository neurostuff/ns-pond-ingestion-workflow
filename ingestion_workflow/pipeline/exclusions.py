"""How a person's "not coordinates" verdict reaches upload and sync.

The verdicts live in the catalog's `exclusions` table (see `Catalog.exclude`),
not in any artifact, so rerunning a stage can never overwrite one. Both stages
read them the same way through these helpers: excluded tables are left out of
what is written, and an article with nothing left is retracted.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Mapping, Optional

WHOLE_ARTICLE = "*"


def kept(payload: Optional[Mapping[str, Any]], excluded: Mapping[str, Any]) -> Dict[str, Any]:
    """The analyses payload without the excluded tables."""
    if not payload:
        return {}
    if WHOLE_ARTICLE in excluded:
        return {}
    return {table_id: blob for table_id, blob in payload.items() if str(table_id) not in excluded}


def digest(excluded: Mapping[str, Any]) -> str:
    """A stable token for the set of excluded tables, for a fingerprint.

    Empty for an article with no verdicts, so adding the mechanism leaves every
    existing fingerprint as it was and nothing goes stale on its account.
    """
    if not excluded:
        return ""
    return "excluded:" + hashlib.blake2b(
        "\n".join(sorted(str(t) for t in excluded)).encode("utf-8"), digest_size=8
    ).hexdigest()
