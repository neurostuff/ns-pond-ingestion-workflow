"""Hand exclusions left on retired articles to the articles that replaced them.

Code before the fix to `Catalog._merge` did not move a person's table exclusions
when it merged two articles, so a merged article lost them. Run once after the
fixed code is deployed, with every pipeline process stopped:

    python scripts/repair_exclusions.py <catalog root>            # count only
    python scripts/repair_exclusions.py <catalog root> --apply

The count opens the database read-only. --apply copies the catalog to
`catalog.pre-exclusions-repair.sqlite` beside it first, then moves the rows
with the rule a merge uses (`Catalog._move_exclusions`): a table excluded on
both sides keeps the survivor's row. A second run reports 0.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.backup import backup  # noqa: E402


def _stranded(conn: sqlite3.Connection) -> dict:
    """{retired article id: its live survivor} for each retired article holding exclusions."""
    merged = dict(
        conn.execute("SELECT id, merged_into FROM articles WHERE merged_into IS NOT NULL")
    )
    out = {}
    for (article_id,) in conn.execute("SELECT DISTINCT article_id FROM exclusions"):
        if article_id not in merged:
            continue
        seen = {article_id}
        survivor = merged[article_id]
        while survivor in merged and survivor not in seen:
            seen.add(survivor)
            survivor = merged[survivor]
        out[article_id] = survivor
    return out


def run(root: Path, *, apply: bool = False, out=sys.stdout) -> int:
    path = Path(root) / "catalog.sqlite"
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as reader:
        reader.row_factory = None
        stranded = _stranded(reader)
        rows = sum(
            reader.execute(
                "SELECT COUNT(*) FROM exclusions WHERE article_id=?", (a,)
            ).fetchone()[0]
            for a in stranded
        )
        print(f"{rows:,} exclusions on {len(stranded):,} retired articles", file=out)
        if not apply or not stranded:
            return rows
        copy = backup(reader, Path(root) / "catalog.pre-exclusions-repair.sqlite")
    print(f"catalog backed up to {copy}", file=out)
    with closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        for loser, survivor in stranded.items():
            Catalog._move_exclusions(conn, loser, survivor)
        conn.execute("COMMIT")
    print(f"moved {rows:,} exclusions", file=out)
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog", type=Path, help="catalog directory (holding catalog.sqlite)")
    parser.add_argument("--apply", action="store_true", help="write; the default only counts")
    args = parser.parse_args(argv)
    run(args.catalog, apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
