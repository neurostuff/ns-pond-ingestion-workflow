"""Record the copy each stored download holds, in the summary of its catalog row.

The download stage records a download's copy (route, version, access, url) when it
fetches it; this does the same, offline, for the downloads stored before it did, by
the same rule (`extractors.copies.copy_of`), reading each download's payload and files:

    python scripts/backfill_download_copies.py <catalog root>                  # dry run
    python scripts/backfill_download_copies.py <catalog root> --apply --backup <path>

Run it where the pipeline runs: a download's files may be stored relative to there.
A dry run opens the catalog read-only and prints what it would record, counted by
source, route, version and access. --apply first copies the catalog to `--backup`, a
path not yet taken, then changes only the summaries: no fingerprint, status or
`updated_at` moves, so nothing goes stale. The copy replaces the `article_version` that
the pdf, wiley and springer sources recorded. A row written meanwhile is left alone, and so
is one whose payload or files are gone; each is printed with why. A second run finds
nothing left to record.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from lxml import etree

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.extractors.copies import copy_of  # noqa: E402
from ingestion_workflow.models import DownloadResult  # noqa: E402

CHUNK = 500

Row = Tuple[str, str, Optional[str], str]


def _copy(blobs: Path, row: Row) -> Tuple[str, str, str, Optional[dict], Optional[str]]:
    """(article, source, summary read, the copy or None, why not)."""
    article_id, source, blob, summary = row
    payload = BlobStore(blobs).get(blob)
    if payload is None:
        return article_id, source, summary, None, "payload blob missing"
    try:
        copy = copy_of(DownloadResult.from_dict(payload)).to_dict()
    except (OSError, ValueError, KeyError, IndexError, etree.LxmlError) as exc:
        return article_id, source, summary, None, f"{type(exc).__name__}: {exc}"
    return article_id, source, summary, copy, None


def _with_copy(summary: str, copy: dict) -> dict:
    """The summary with its copy, which replaces the version some sources recorded."""
    kept = {k: v for k, v in json.loads(summary).items() if k != "article_version"}
    return {**kept, "copy": copy}


def _chunk(args: Tuple[Path, Sequence[Row]]) -> List[tuple]:
    blobs, rows = args
    return [_copy(blobs, row) for row in rows]


def run(root: Path, *, apply: bool = False, backup: Optional[Path] = None,
        workers: int = 1, out=sys.stdout) -> Counter:
    path = Path(root) / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    conn = (sqlite3.connect(path, isolation_level=None) if apply
            else sqlite3.connect(f"file:{path}?mode=ro", uri=True))
    counts: Counter = Counter()
    try:
        rows = conn.execute(
            "SELECT article_id, source, blob, summary FROM artifacts "
            "WHERE stage='download' AND status='ok' ORDER BY article_id, source").fetchall()
        todo = [r for r in rows if "copy" not in json.loads(r[3])]
        counts["already recorded"] = len(rows) - len(todo)
        if apply:
            # Not Catalog.open, which would migrate an older catalog first.
            catalog = Catalog(conn, Path(root))
            print(f"catalog backed up to {catalog.backup(backup)}", file=out)
        chunks = [(Path(root) / "blobs", todo[i:i + CHUNK]) for i in range(0, len(todo), CHUNK)]
        with ProcessPoolExecutor(workers) as pool:
            results = [r for done in pool.map(_chunk, chunks) for r in done]
        with catalog._write() if apply else nullcontext(conn):
            for article_id, source, summary, copy, why in results:
                if copy is None:
                    counts["left: " + why.split(":")[0]] += 1
                    print(f"left\t{article_id}\t{source}\t{why}", file=out)
                    continue
                key = (source, copy["route"], copy.get("version", "-"), copy["access"])
                counts["\t".join(key)] += 1
                if apply:
                    written = conn.execute(
                        "UPDATE artifacts SET summary=? WHERE article_id=? AND stage='download' "
                        "AND source=? AND status='ok' AND summary=?",
                        (json.dumps(_with_copy(summary, copy), separators=(",", ":")),
                         article_id, source, summary)).rowcount
                    if not written:
                        counts["left: written meanwhile"] += 1
                        print(f"left\t{article_id}\t{source}\twritten meanwhile", file=out)
    finally:
        conn.close()
    print("source\troute\tversion\taccess\tdownloads", file=out)
    for key, n in sorted(counts.items()):
        print(f"{key}\t{n}", file=out)
    print("applied" if apply else "dry run, nothing written", file=out)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog", type=Path, help="catalog directory (holding catalog.sqlite)")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    parser.add_argument("--backup", type=Path, help="where --apply copies the catalog first")
    parser.add_argument("--workers", type=int, default=1, help="processes reading downloads")
    args = parser.parse_args(argv)
    run(args.catalog, apply=args.apply, backup=args.backup, workers=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
