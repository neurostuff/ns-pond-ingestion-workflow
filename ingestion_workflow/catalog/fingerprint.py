"""Fingerprints decide reuse: an artifact is stale when its inputs changed."""

from __future__ import annotations

import hashlib
from typing import Any, Optional


def fingerprint(*parts: Any, upstream: Optional[str] = None) -> str:
    """Hash the values an output depends on, plus the upstream artifact's hash."""
    digest = hashlib.blake2b(digest_size=16)
    for part in parts:
        digest.update(repr(part).encode("utf-8"))
        digest.update(b"\x1f")
    if upstream:
        digest.update(upstream.encode("utf-8"))
    return digest.hexdigest()
