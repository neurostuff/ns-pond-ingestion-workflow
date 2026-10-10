#!/usr/bin/env python3
"""Turn the passages already in the catalog into spans of the text they will index.

A one-off migration: before 2026-10-09 the passages stage stored each passage's own
text, read from the download, and nothing tied it to `text.txt`. This finds each stored
passage in the text the passages stage now reads (the extraction triage judges, or for
an article without one the text passages kept) and, where every passage and every hit of
an article is found exactly once -- verbatim, or by its letters and digits alone --
records the article's passages as spans of that text. An article with a passage found
twice (ambiguous) or not at all (unmatched) is not guessed at: it is left as it is, its
fingerprint no longer current, so the passages stage reads it again.

Every realigned article's passages fingerprint changes too, and prose chains on it: the
report counts the prose results either outcome invalidates.

Read-only unless --write. The catalog is opened read-only for a dry run.

Usage:
    python scripts/realign_passages.py --catalog-root .catalog --out /tmp/realign [--write]
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from array import array
from collections import Counter
from pathlib import Path
from typing import List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion_workflow.catalog import Catalog, Outcome, Status  # noqa: E402
from ingestion_workflow.config import Settings  # noqa: E402
from ingestion_workflow.pipeline import Context  # noqa: E402
from ingestion_workflow.pipeline.stages.passages import (  # noqa: E402
    PassagesStage,
    _choose,
    text_of,
)
from ingestion_workflow.pipeline.stages.triage import judged_extraction  # noqa: E402
from ingestion_workflow.services.offsets import sha256  # noqa: E402
from ingestion_workflow.services.prose_passages import HEADING, MINUS  # noqa: E402

ALNUM = re.compile(r"[^\W_]+")
FOLD = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹", "0123456789")


def squash(text: str) -> Tuple[str, array]:
    """`text`'s letters and digits only, lower case, and where each one sits in `text`."""
    runs, where = [], array("l")
    for m in ALNUM.finditer(text.translate(FOLD)):
        run = m.group().lower()
        if len(run) != m.end() - m.start():
            run = m.group()
        runs.append(run)
        where.extend(range(m.start(), m.end()))
    return "".join(runs), where


def _all(haystack: str, needle: str, limit: int = 2) -> List[int]:
    found, at = [], haystack.find(needle)
    while at >= 0 and len(found) < limit:
        found.append(at)
        at = haystack.find(needle, at + 1)
    return found


class Text:
    def __init__(self, text: str) -> None:
        self.text = text
        self._squashed: Optional[Tuple[str, array]] = None

    def place(self, needle: str) -> Tuple[str, Optional[Tuple[int, int]]]:
        """`exact`, `normalized`, `ambiguous` or `unmatched`, and the span when placed."""
        exact = _all(self.text, needle)
        if len(exact) == 1:
            return "exact", (exact[0], exact[0] + len(needle))
        if len(exact) > 1:
            return "ambiguous", None
        wanted, _ = squash(needle)
        if not wanted:
            return "unmatched", None
        if self._squashed is None:
            self._squashed = squash(self.text)
        squashed, where = self._squashed
        found = _all(squashed, wanted)
        if len(found) == 1:
            return "normalized", (where[found[0]], where[found[0] + len(wanted) - 1] + 1)
        return ("ambiguous" if found else "unmatched"), None


def _number(v: float) -> str:
    digits = re.escape(f"{abs(v):g}") + (r"(?:\.0+)?" if float(v).is_integer() else r"\d*")
    return rf"[{MINUS}-]\s?{digits}" if v < 0 else rf"(?<![{MINUS}\d.-])\+?{digits}"


def _hit(text: str, span: Tuple[int, int], hit: dict) -> Optional[List[int]]:
    """The one place inside the passage that prints the hit's x, y and z."""
    rx = re.compile(r"[^\d\n]{1,12}?".join(_number(hit[k]) for k in "xyz") + r"(?![\d.])")
    found = [m.span() for m in rx.finditer(text, *span)]
    return list(found[0]) if len(found) == 1 else None


def _near(text: Text, needle: str, span: Tuple[int, int], before: bool) -> Optional[List[int]]:
    """A context sentence, kept only where it sits beside the passage."""
    if not needle:
        return None
    _, got = text.place(needle)
    if got is None or (got[1] > span[0] if before else got[0] < span[1]):
        return None
    return list(got)


def realign(text: str, old: dict) -> Tuple[str, Counter, Optional[list]]:
    """The article's outcome, its passages' outcomes, and its passages as spans when realigned."""
    t, counts, out = Text(text), Counter(), []
    headings = [(m.start(1), m.end(1), m.group(1).strip()) for m in HEADING.finditer(text)]
    for p in old.get("passages", []):
        how, span = t.place(p["text"])
        counts[how] += 1
        if span is None:
            continue
        hits = [_hit(text, span, h) for h in p.get("hits", [])]
        if any(h is None for h in hits):
            counts["hit_unplaced"] += 1
            continue
        heading = next(([a, b] for a, b, name in reversed(headings) if b <= span[0] and name == p.get("heading")),
                       None)
        out.append({"span": list(span), "before": _near(t, p.get("before", ""), span, True),
                    "after": _near(t, p.get("after", ""), span, False), "heading": heading,
                    "space": p.get("space"),
                    "hits": [{**h, "span": s} for h, s in zip(p.get("hits", []), hits)]})
    n = len(old.get("passages", []))
    if len(out) == n:
        return "realigned", counts, out
    return ("ambiguous" if counts["ambiguous"] else "unmatched"), counts, None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--catalog-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--out", type=Path, required=True, help="Directory for summary.json and articles.jsonl")
    parser.add_argument("--write", action="store_true", help="Record the realigned passages (default: dry run)")
    args = parser.parse_args()

    if args.write:
        catalog = Catalog.open(args.catalog_root)
    else:
        conn = sqlite3.connect(f"file:{args.catalog_root / 'catalog.sqlite'}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        catalog = Catalog(conn, args.catalog_root)
    ctx = Context(Settings(data_root=args.data_root, catalog_root=args.catalog_root), catalog)
    args.out.mkdir(parents=True, exist_ok=True)
    articles, passages, prose = Counter(), Counter(), Counter()
    with (args.out / "articles.jsonl").open("w") as log:
        ids = [r[0] for r in catalog._conn.execute(
            "SELECT article_id FROM artifacts WHERE stage='passages' AND status=?", (Status.OK.value,))]
        for start in range(0, len(ids), 900):
            chunk = ids[start:start + 900]
            found = catalog.artifacts(chunk, "passages")
            extractions = catalog.artifacts(chunk, "extract")
            downloads = catalog.artifacts(chunk, "download")
            reads = catalog.artifacts(chunk, "prose")
            rows: List[Outcome] = []
            for article_id in chunk:
                artifact = found[article_id][""]
                if not artifact.summary.get("passages"):
                    articles["no passages"] += 1
                    continue
                old = catalog.payload(artifact) or {}
                if old.get("text_sha256"):
                    articles["already spans"] += 1
                    continue
                extraction = judged_extraction(ctx, extractions.get(article_id, {}), downloads.get(article_id, {}))
                path, sha = text_of(ctx, extraction)
                if path is None:  # no extraction text: the text passages kept is the article's
                    path, extraction = old.get("full_text_path"), None
                if not path or not Path(path).is_file():
                    outcome, counts, new = "no text", Counter(), None
                else:
                    text = Path(path).read_bytes().decode("utf-8")
                    sha = sha256(text)
                    outcome, counts, new = realign(text, old)
                articles[outcome] += 1
                passages.update(counts)
                read = reads.get(article_id, {}).get("")
                if read is not None and read.status is Status.OK:
                    prose["prose results invalidated"] += 1
                    prose["passages read again"] += read.summary.get("passages", 0)
                log.write(json.dumps({"article_id": article_id, "outcome": outcome, "text": path,
                                      "from": "extract" if extraction else "passages", **counts}) + "\n")
                if args.write and new is not None:
                    fp = (PassagesStage.fingerprint_for(None, extraction, sha) if extraction is not None
                          else PassagesStage.fingerprint_for(_choose(downloads.get(article_id, {}))))
                    payload = {**old, "text_from": "extract" if extraction else "download",
                               "full_text_path": path, "text_sha256": sha, "passages": new}
                    rows.append(Outcome(article_id=article_id, stage="passages", source="", status=Status.OK,
                                        fingerprint=fp, payload=payload, summary=artifact.summary))
            if rows:
                catalog.record(rows)
    summary = {"articles": dict(articles), "passages": dict(passages), "prose": dict(prose),
               "written": bool(args.write)}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
