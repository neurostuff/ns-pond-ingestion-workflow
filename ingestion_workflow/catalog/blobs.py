"""Content-addressed store for payloads too large to sit in a catalog row."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional


class BlobStore:
    """Gzipped JSON keyed by the sha256 of its uncompressed bytes."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, digest: str) -> Path:
        return self.root / digest[:2] / f"{digest}.json.gz"

    def put(self, payload: Any) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        target = self._path(digest)
        if target.exists():
            return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        # Write to a sibling temp file and rename, so a killed run never leaves a
        # half-written blob that a later run would trust on the strength of its name.
        fd, tmp = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(gzip.compress(raw, compresslevel=6))
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return digest

    def get(self, digest: Optional[str]) -> Optional[Any]:
        if not digest:
            return None
        path = self._path(digest)
        if not path.exists():
            return None
        return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))

    def exists(self, digest: Optional[str]) -> bool:
        return bool(digest) and self._path(digest).exists()
