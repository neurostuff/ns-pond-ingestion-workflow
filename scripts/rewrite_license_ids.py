"""Respell the licence ids the catalog's metadata stored before study_schema 0.11.0.

study_schema paper-parse 0.11.0 spells each `LicenseId` as its member name (`cc_by`,
`all_rights_reserved`); the metadata stored before it holds the former spelling
(`cc-by`, `all-rights-reserved`), which no longer validates, so sync cannot read it.
This rewrites, in each metadata artifact, the ids of the payload's `license`,
`copy_licenses` and `reported_copies[].license`, and the summary's `license` and
`copy_licenses`. A source's own wording (`stated_as`, the providers' raw records, the
download summaries' listed `license`) is left as the source wrote it.

    python scripts/rewrite_license_ids.py <catalog root>                  # dry run
    python scripts/rewrite_license_ids.py <catalog root> --apply --backup <path>

Run it once, before the pipeline runs with study_schema 0.11.0. Nothing is fetched.

A dry run opens the catalog read-only and prints what it would rewrite, counted by
former id. Rows are read, checked and written `BATCH` at a time, so memory stays flat
however large the catalog. --apply first copies the catalog to `--backup`, a path not
yet taken, then stores each rewritten payload as a new blob and changes only the
metadata rows' `blob` and `summary`: no fingerprint, status or `updated_at` moves. Sync,
which fingerprints the metadata's blob (and `PAPER_PARSE_VERSION`), then writes the
parse files again. A row written meanwhile is left alone, and so is one whose payload is
gone or, respelled, still does not load as sync reads it (every payload is checked,
respelled or not); each is printed with why. A second run finds nothing left to rewrite.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.models.metadata import ArticleMetadata  # noqa: E402
from study_schema.models.paper_parse import LicenseId  # noqa: E402

#: How many metadata rows are read, checked and written together.
BATCH = 500

#: Each former spelling, and the id it is now.
FORMER = {member.value.replace("_", "-"): member.value
          for member in LicenseId if "_" in member.value}


def _respell(holder: Optional[Dict[str, Any]], key: str, seen: Counter) -> None:
    """Respell `holder[key]` when it is a former spelling, counting it in `seen`."""
    if isinstance(holder, dict) and holder.get(key) in FORMER:
        seen[holder[key]] += 1
        holder[key] = FORMER[holder[key]]


def respelled_payload(payload: Dict[str, Any], seen: Counter) -> Dict[str, Any]:
    """The metadata payload with its licences' former ids respelled."""
    out = json.loads(json.dumps(payload))
    _respell(out.get("license"), "license_id", seen)
    for lic in (out.get("copy_licenses") or {}).values():
        _respell(lic, "license_id", seen)
    for copy in out.get("reported_copies") or []:
        _respell(copy.get("license"), "license_id", seen)
    return out


def respelled_summary(summary: Dict[str, Any], seen: Counter) -> Dict[str, Any]:
    """The metadata summary with the ids it lists for the report respelled."""
    out = json.loads(json.dumps(summary))
    _respell(out, "license", seen)
    copies = out.get("copy_licenses")
    for source in list(copies or {}):
        _respell(copies, source, seen)
    return out


def _checked(rows, blobs: BlobStore, counts: Counter, out) -> List[tuple]:
    """Of one batch of metadata rows, those with a former id, respelled and loading as
    sync reads them; each row left is counted and printed with why."""
    todo: List[tuple] = []
    for article_id, source, blob, summary in rows:
        seen: Counter = Counter()
        payload = blobs.get(blob) if blob else None
        if blob and payload is None:
            counts["left: payload blob missing"] += 1
            print(f"left\t{article_id}\tpayload blob missing", file=out)
            continue
        new_payload = respelled_payload(payload, seen) if payload is not None else None
        new_summary = respelled_summary(json.loads(summary), seen)
        if new_payload is not None:
            # As sync reads it: a payload that would still not load is left, and shown.
            try:
                ArticleMetadata.from_dict(new_payload)
            except (ValidationError, ValueError, KeyError) as exc:
                why = f"{type(exc).__name__}: {' '.join(str(exc).split())[:200]}"
                counts["left: does not validate"] += 1
                print(f"left\t{article_id}\t{why}", file=out)
                continue
        if not seen:
            counts["already current"] += 1
            continue
        todo.append((article_id, source, blob, summary, new_payload, new_summary, seen))
    return todo


def run(root: Path, *, apply: bool = False, backup: Optional[Path] = None,
        out=sys.stdout) -> Counter:
    path = Path(root) / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    conn = (sqlite3.connect(path, isolation_level=None) if apply
            else sqlite3.connect(f"file:{path}?mode=ro", uri=True))
    blobs = BlobStore(Path(root) / "blobs")
    counts: Counter = Counter()
    respelled: Counter = Counter()
    try:
        if apply:
            # Not Catalog.open, which would migrate an older catalog first.
            catalog = Catalog(conn, Path(root))
            print(f"catalog backed up to {catalog.backup(backup)}", file=out)
        after = ("", "")
        while True:
            rows = conn.execute(
                "SELECT article_id, source, blob, summary FROM artifacts WHERE stage='metadata' "
                "AND (article_id, source) > (?, ?) ORDER BY article_id, source LIMIT ?",
                (*after, BATCH)).fetchall()
            if not rows:
                break
            after = rows[-1][:2]
            todo = _checked(rows, blobs, counts, out)
            with catalog._write() if apply else nullcontext(conn):
                for article_id, source, blob, summary, new_payload, new_summary, seen in todo:
                    if apply and not conn.execute(
                            "UPDATE artifacts SET blob=?, summary=? WHERE article_id=? "
                            "AND stage='metadata' AND source=? AND blob IS ? AND summary=?",
                            (blobs.put(new_payload) if new_payload is not None else blob,
                             json.dumps(new_summary, separators=(",", ":")),
                             article_id, source, blob, summary)).rowcount:
                        counts["left: written meanwhile"] += 1
                        print(f"left\t{article_id}\twritten meanwhile", file=out)
                        continue
                    counts["respelled"] += 1
                    respelled.update(seen)
    finally:
        conn.close()
    for key, n in sorted(counts.items()):
        print(f"{key}\t{n}", file=out)
    for former, n in sorted(respelled.items()):
        print(f"{former} -> {FORMER[former]}\t{n}", file=out)
    print("applied" if apply else "dry run, nothing written", file=out)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog", type=Path, help="catalog directory (holding catalog.sqlite)")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    parser.add_argument("--backup", type=Path, help="where --apply copies the catalog first")
    args = parser.parse_args(argv)
    run(args.catalog, apply=args.apply, backup=args.backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
