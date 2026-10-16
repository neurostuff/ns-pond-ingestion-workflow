"""A consistent copy of the catalog database."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path


def backup(conn: sqlite3.Connection, target: Path) -> Path:
    """Copy the database `conn` is open on to `target`, a path not yet taken.

    The sqlite backup API copies a consistent snapshot, WAL included. The blob
    store needs no copy: blobs are addressed by content and never rewritten.
    """
    target = Path(target)
    if target.exists():
        raise FileExistsError(f"{target} exists; give a new path for the backup")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with closing(sqlite3.connect(target)) as out:
            conn.backup(out)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return target
