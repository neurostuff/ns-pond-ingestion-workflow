"""Settle the DOIs that two catalog articles hold in different case.

Run once, before the catalog is first opened by code at schema version 3
(which matches DOIs in lower case), with every pipeline process stopped:

    python scripts/repair_doi_collisions.py <catalog root>            # dry run
    python scripts/repair_doi_collisions.py <catalog root> --apply

Each row of `doi_collisions.tsv`, beside this script, is one DOI and the
decision the PubMed and Crossref evidence in it supports:

* `drop`: the DOI is `other`'s; `article` holds it as a foreign second DOI, and
  loses it, in whatever spelling it holds it;
* `merge`: the two articles are one paper; they are merged as the catalog
  merges, and only if the merge would not leave one article with two PMIDs or
  PMCIDs;
* `keep`: two papers that share one DOI; nothing changes. After the version-3
  migration the DOI resolves to the article holding it in lower case, the
  other keeps its spelling (logged by the migration) and is found by its PMID.

--apply copies the catalog to `catalog.pre-doi-repair.sqlite` beside it first.
A second run finds every row done.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import Counter
from contextlib import nullcontext
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.backup import backup  # noqa: E402
from ingestion_workflow.utils.doi import normalize_doi  # noqa: E402

TABLE = Path(__file__).with_name("doi_collisions.tsv")
#: The schema version whose migration lower-cases doi aliases.
LOWER_CASE_VERSION = 3


def _dois(conn: sqlite3.Connection, article_id: str, doi: str) -> list:
    """The spellings of `doi` that `article_id` holds."""
    return [row[0] for row in conn.execute(
        "SELECT value FROM aliases WHERE kind='doi' AND article_id=?", (article_id,))
        if normalize_doi(row[0]) == normalize_doi(doi)]


def run(root: Path, *, apply: bool = False, table: Path = TABLE, out=sys.stdout) -> Counter:
    path = Path(root) / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    conn = (sqlite3.connect(path, isolation_level=None) if apply
            else sqlite3.connect(f"file:{path}?mode=ro", uri=True))
    conn.row_factory = sqlite3.Row
    counts: Counter = Counter()
    try:
        version = int(conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'").fetchone()[0])
        if version >= LOWER_CASE_VERSION:
            raise SystemExit(f"{path} is already at schema version {version}; this repair "
                             f"runs before the version-{LOWER_CASE_VERSION} migration")
        # Not Catalog.open, which would migrate first.
        catalog = Catalog(conn, Path(root))
        if apply:
            copy = backup(conn, path.with_name("catalog.pre-doi-repair.sqlite"))
            print(f"catalog backed up to {copy}", file=out)
        with open(table, newline="", encoding="utf-8") as fh:
            decisions = list(csv.DictReader(fh, delimiter="\t"))
        with catalog._write() if apply else nullcontext(conn):
            for d in decisions:
                doi, action, article, other = d["doi"], d["action"], d["article"], d["other"]
                if action == "drop":
                    held = _dois(conn, article, doi)
                    if not held:
                        kind, outcome = "done", "done"
                    elif not _dois(conn, catalog._follow_merge(other), doi):
                        kind, outcome = "skipped", f"skipped: {other} does not hold it"
                    else:
                        kind, outcome = "dropped", f"drop {', '.join(held)} from {article}"
                        if apply:
                            conn.executemany(
                                "DELETE FROM aliases "
                                "WHERE kind='doi' AND value=? AND article_id=?",
                                [(value, article) for value in held])
                elif action == "merge":
                    a, b = catalog._follow_merge(article), catalog._follow_merge(other)
                    if a == b:
                        kind, outcome = "done", "done"
                    elif not catalog._compatible(conn, [a, b], {}):
                        kind = "merge refused"
                        outcome = "merge refused: the two hold different PMIDs or PMCIDs"
                    else:
                        kind, outcome = "merged", f"merge {a} and {b}"
                        if apply:
                            catalog._merge(conn, [a, b])
                elif action == "keep":
                    kind, outcome = "kept", "kept: two papers sharing one DOI"
                else:
                    raise SystemExit(f"{table}: unknown action {action!r} for {doi}")
                counts[kind] += 1
                print(f"{doi}\t{outcome}", file=out)
    finally:
        conn.close()
    summary = ", ".join(f"{n} {k}" for k, n in sorted(counts.items()))
    print(("applied: " if apply else "dry run, nothing written: ") + summary, file=out)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog", type=Path, help="catalog directory (holding catalog.sqlite)")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    args = parser.parse_args(argv)
    run(args.catalog, apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
