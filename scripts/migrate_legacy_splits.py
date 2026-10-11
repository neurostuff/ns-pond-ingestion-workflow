"""Declare the sign splits of payloads stored before `split{}` existed.

Run once, after the change that made sync refuse such payloads, and before
the next sync:

    python scripts/migrate_legacy_splits.py <catalog root>                  # dry run
    python scripts/migrate_legacy_splits.py <catalog root> --apply --backup <dir>

A payload stored before the declaration marks a split only by name: the
inverse half was named `X (negative)` (later `X (inverse)`) and followed its
original `X` in the same table. Each such payload is rewritten into the form
the analyses stage writes now: the halves are paired by `metadata["split"]`,
the inverse half is named and its signed values negated as the stage does
(`inverse_name`, `statistics.inverted`), and the collection gains
`split_declared`. A suffixed
name the stage cannot have written -- one with a point that is not negative --
is the paper's own, and is kept as printed.

Every stage that stores the analyses stage's collections is rewritten, because
sync writes `space`'s, which carries what `analyses` stored through `resolve`
and `roles`. Each payload goes to a new blob and `artifacts.blob` is repointed;
the old blob is left where it was, and no fingerprint, status or timestamp
moves, so nothing is re-run by it. A second run finds nothing to do.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.models.statistics import inverted, side  # noqa: E402
from ingestion_workflow.services.coordinate_flags import inverse_name  # noqa: E402

#: The stages whose payload is `{table_id: collection}`.
STAGES = ("analyses", "resolve", "roles", "space")
#: How the stage marked the inverse half before it declared it, in either spelling.
LEGACY_SUFFIXES = (" (inverse)", " (negative)")
#: The collection `resolve` adds for the prose; the stage never split prose sets.
PROSE = "prose"


def declare(collection: Dict[str, Any]) -> Tuple[Dict[str, Any], Counter]:
    """The collection with its name-marked splits declared, and what was found.

    Pairs are found before any leftover is declared, and from the end: a
    paper's own "Load (negative)" that was split is stored as "Load (negative)"
    then "Load (negative) (negative)", and its first half must pair with the
    second, not be taken as the inverse of an earlier "Load". A suffixed
    analysis left with no partner is declared an inverse half with no original
    rather than read as an ordinary analysis.

    The stage only ever put points with a negative value in an inverse half, so a
    suffixed analysis holding any other point is the paper's own name: it is
    neither paired nor declared, and counted as kept.
    """
    analyses = [dict(a, metadata=dict(a.get("metadata") or {})) for a in collection["analyses"]]

    def undeclared(analysis):
        return not analysis["metadata"].get("split")

    def invert(analysis, original_name):
        analysis["name"] = inverse_name(original_name)
        coordinates = []
        for c in analysis.get("coordinates") or []:
            c = dict(c, statistic_value=inverted(c.get("statistic_value"), c.get("statistic_type")))
            if "sign" in c:
                c["sign"] = side(c["statistic_value"], c.get("statistic_type")) or "unsigned"
            coordinates.append(c)
        analysis["coordinates"] = coordinates

    def all_negative(analysis):
        values = [c.get("statistic_value") for c in analysis.get("coordinates") or []]
        return bool(values) and all(isinstance(v, (int, float)) and v < 0 for v in values)

    pairs = leftovers = kept = 0
    for i in range(len(analyses) - 1, 0, -1):
        prev, analysis = analyses[i - 1], analyses[i]
        if (
            undeclared(prev)
            and undeclared(analysis)
            and all_negative(analysis)
            and prev.get("table_id") == analysis.get("table_id")
            and analysis.get("name") in (prev.get("name", "") + s for s in LEGACY_SUFFIXES)
        ):
            prev["metadata"]["split"] = {"half": "original", "index": i - 1}
            analysis["metadata"]["split"] = {"half": "inverse", "original_index": i - 1}
            invert(analysis, prev["name"])
            pairs += 1
    for analysis in analyses:
        name = analysis.get("name") or ""
        suffix = next((s for s in LEGACY_SUFFIXES if name.endswith(s)), None)
        if not (undeclared(analysis) and suffix):
            continue
        if not all_negative(analysis):
            kept += 1
            continue
        analysis["metadata"]["split"] = {"half": "inverse", "original_index": None}
        invert(analysis, name[: -len(suffix)])
        leftovers += 1
    return {**collection, "analyses": analyses, "split_declared": True}, Counter(
        pairs=pairs, leftovers=leftovers, kept=kept
    )


def migrate_payload(payload: Any) -> Tuple[Optional[Dict[str, Any]], Counter, Optional[str]]:
    """`(new payload or None when already declared, counts, refusal reason)`."""
    counts: Counter = Counter()
    if not isinstance(payload, dict):
        return None, counts, "payload is not a mapping of tables"
    out, changed = {}, False
    for table_id, collection in payload.items():
        if not isinstance(collection, dict) or not isinstance(collection.get("analyses"), list):
            return None, Counter(), f"table {table_id} has no analyses list"
        if collection.get("split_declared"):
            out[table_id] = collection
            continue
        changed = True
        if table_id == PROSE:
            out[table_id] = {**collection, "split_declared": True}
            continue
        out[table_id], found = declare(collection)
        counts.update(found)
    return (out if changed else None), counts, None


def _open(catalog: Path, writable: bool) -> sqlite3.Connection:
    path = catalog / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if writable:
        conn = sqlite3.connect(path, isolation_level=None)
    else:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _backup(conn: sqlite3.Connection, backup: Path) -> Path:
    backup.mkdir(parents=True, exist_ok=True)
    target = backup / "catalog.sqlite"
    if target.exists():
        raise SystemExit(f"{target} exists; choose an empty backup directory")
    # The sqlite backup API copies a consistent snapshot, WAL included.
    with sqlite3.connect(target) as out:
        conn.backup(out)
    return target


def run(catalog: Path, *, apply: bool = False, backup: Optional[Path] = None,
        stages=STAGES, out=sys.stdout) -> Counter:
    catalog = Path(catalog)
    if apply and backup is None:
        raise SystemExit("--apply needs --backup <dir>: the catalog is copied there first")
    conn = _open(catalog, writable=apply)
    blobs = BlobStore(catalog / "blobs")
    counts: Counter = Counter()
    refusals: Counter = Counter()
    try:
        if apply:
            print(f"catalog backed up to {_backup(conn, Path(backup))}", file=out)
        rows = conn.execute(
            "SELECT article_id, stage, source, blob FROM artifacts "
            f"WHERE stage IN ({','.join('?' * len(stages))}) AND status='ok' "
            "AND blob IS NOT NULL ORDER BY stage, article_id, source",
            tuple(stages),
        ).fetchall()
        repoint: List[Tuple[str, str, str, str, str]] = []
        for row in rows:
            counts["payloads seen"] += 1
            payload = blobs.get(row["blob"])
            if payload is None:
                reason = "blob missing from the blob store"
                new = None
            else:
                new, found, reason = migrate_payload(payload)
            if reason:
                refusals[reason] += 1
                print(f"refused {row['stage']} {row['article_id']}: {reason}", file=out)
                continue
            if new is None:
                counts["payloads unchanged (declared or empty)"] += 1
                continue
            counts["payloads rewritten"] += 1
            counts[f"payloads rewritten ({row['stage']})"] += 1
            counts["splits paired"] += found["pairs"]
            counts["leftovers declared"] += found["leftovers"]
            counts["suffixed names kept as printed (a point not negative)"] += found["kept"]
            if apply:
                digest = blobs.put(new)
                repoint.append(
                    (digest, row["article_id"], row["stage"], row["source"], row["blob"])
                )
        if apply and repoint:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Matched on the old blob too, so a stage that wrote meanwhile is not overwritten.
                conn.executemany(
                    "UPDATE artifacts SET blob=? WHERE article_id=? AND stage=? AND source=? "
                    "AND blob=?",
                    repoint,
                )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
    finally:
        conn.close()
    counts["payloads refused"] = sum(refusals.values())
    print("applied" if apply else "dry run: nothing written", file=out)
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}", file=out)
    for reason, n in refusals.most_common():
        print(f"  refused, {reason}: {n}", file=out)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "catalog", type=Path, help="catalog root (holds catalog.sqlite and blobs/)"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="count only (the default)")
    mode.add_argument("--apply", action="store_true", help="write the rewritten payloads")
    parser.add_argument(
        "--backup", type=Path, help="directory the catalog is copied to before --apply"
    )
    args = parser.parse_args(argv)
    run(args.catalog, apply=args.apply, backup=args.backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
