"""List the articles whose stored pdf extraction gives two tables one id.

A table continued over pages repeats its caption's label, and before 7e3ac063 both
pieces got the same id, so each piece's files overwrote the last and analyses read one.
Re-extracting numbers them on (`table-ii`, `table-ii-2`), but stored extractions keep
their fingerprints, so this finds them for a targeted refresh:

    python scripts/repeated_table_ids_manifest.py <catalog root> <manifest.jsonl> \
        [--exclusions <exclusions.tsv>]
    ingest run -m <manifest.jsonl> --stage extract --refresh extract:pdf
    ingest run -m <manifest.jsonl> --refresh triage --refresh analyses

A person's exclusion of a repeated id covered every piece; after the refresh only the
first piece has that id. `--exclusions` writes each such exclusion again for the other
pieces (`article_id, table_id, reason, note`), to record with `Catalog.exclude` once
the refresh has run. The catalog is opened read-only.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog, Status  # noqa: E402
from ingestion_workflow.models.ids import Identifiers  # noqa: E402


def repeated_ids(catalog: Catalog) -> Dict[str, Dict[str, int]]:
    """article_id -> {table id: how many tables hold it} for every OK pdf extraction
    that repeats one."""
    ids = list(catalog.all_article_ids())
    found = {}
    for article_id, by_source in catalog.artifacts(ids, "extract").items():
        extraction = by_source.get("pdf")
        if extraction is None or extraction.status is not Status.OK:
            continue
        counts = Counter(t.get("table_id") for t in (catalog.payload(extraction) or {})
                         .get("tables") or [])
        repeated = {table_id: n for table_id, n in counts.items() if n > 1}
        if repeated:
            found[article_id] = repeated
    return found


def widened_exclusions(catalog: Catalog, found: Dict[str, Dict[str, int]]
                       ) -> List[Tuple[str, str, str, str]]:
    """Each exclusion of a repeated id, again for the pieces numbered on."""
    rows = []
    for article_id, by_table in catalog.exclusions(list(found)).items():
        for table_id, verdict in by_table.items():
            for n in range(2, found[article_id].get(table_id, 0) + 1):
                rows.append((article_id, f"{table_id}-{n}", verdict["reason"], verdict["note"]))
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog_root", type=Path, help="Directory holding catalog.sqlite.")
    parser.add_argument("manifest", type=Path, help="JSONL identifiers manifest to write.")
    parser.add_argument("--exclusions", type=Path,
                        help="TSV of the exclusions to record for the pieces numbered on.")
    args = parser.parse_args(argv)
    root = args.catalog_root.resolve()
    conn = sqlite3.connect((root / "catalog.sqlite").as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        catalog = Catalog(conn, root)
        found = repeated_ids(catalog)
        Identifiers(list(catalog.identifiers(list(found)).values())).save(args.manifest)
        widened = widened_exclusions(catalog, found)
    finally:
        conn.close()
    print(f"{len(found)} articles -> {args.manifest}; "
          f"{len(widened)} exclusions to record for the pieces numbered on")
    if args.exclusions:
        args.exclusions.write_text("".join("\t".join(row) + "\n" for row in widened))
    return 0


if __name__ == "__main__":
    sys.exit(main())
