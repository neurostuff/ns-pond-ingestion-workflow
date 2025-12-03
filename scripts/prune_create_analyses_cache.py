"""Prune create_analyses cache entries based on manifest diffs.

This script:
1. Loads the union of manifest_n1000.jsonl + manifest_n10.jsonl + manifest_n50.jsonl.
2. Loads the union of split_manifests/manifest_n32290_{00..03}.jsonl.
3. Computes (union1 - union2) based on an identifier key (neurostore > doi > pmid > pmcid).
4. Deletes matching rows from the create_analyses SQLite index.
5. Deletes corresponding artifact files whose names begin with the cached slug
   (with any ``::`` in the slug replaced by ``-``).

Use --dry-run to see what would be removed without touching disk/db.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


BASE_DIR = Path(__file__).resolve().parent.parent

# Defaults per user request
DEFAULT_MANIFESTS = [
    BASE_DIR / "manifest_n1000.jsonl",
    BASE_DIR / "manifest_n10.jsonl",
    BASE_DIR / "manifest_n50.jsonl",
]

DEFAULT_SUBTRACT_MANIFESTS = [
    BASE_DIR / "split_manifests" / "manifest_n32290" / f"manifest_n32290_{i:02d}.jsonl"
    for i in range(4)
]

DEFAULT_INDEX = Path("/data/alejandro/projects/ns-pond/ingestion-cache-indices/create_analyses/index.sqlite")
DEFAULT_ARTIFACT_DIR = Path(
    "/data/alejandro/projects/ns-pond/ingestion-cache-indices/create_analyses/artifacts"
)


ManifestRecord = Dict[str, Optional[str]]
Key = Tuple[str, str]


def load_manifest(path: Path) -> List[ManifestRecord]:
    rows: List[ManifestRecord] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def identifier_key(record: ManifestRecord) -> Key:
    for field in ("neurostore", "doi", "pmid", "pmcid"):
        value = record.get(field)
        if value:
            return (field, value)
    # fallback to a stable blob if nothing else is available
    return ("blob", json.dumps(record, sort_keys=True))


def build_union(manifest_paths: Iterable[Path]) -> Dict[Key, ManifestRecord]:
    union: Dict[Key, ManifestRecord] = {}
    for path in manifest_paths:
        for row in load_manifest(path):
            union[identifier_key(row)] = row
    return union


def query_slugs_for_record(cur: sqlite3.Cursor, record: ManifestRecord) -> Set[str]:
    clauses: List[str] = []
    params: List[str] = []
    for field in ("doi", "pmid", "pmcid"):
        value = record.get(field)
        if value:
            clauses.append(f"{field}=?")
            params.append(value)
    if not clauses:
        return set()
    query = f"SELECT slug FROM create_analyses WHERE {' OR '.join(clauses)}"
    rows = cur.execute(query, params).fetchall()
    return {row[0] for row in rows}


def delete_slugs(cur: sqlite3.Cursor, slugs: Set[str]) -> int:
    if not slugs:
        return 0
    deleted = 0
    # SQLite has a limit on bound vars; chunk to be safe.
    slug_list = list(slugs)
    chunk_size = 500
    for i in range(0, len(slug_list), chunk_size):
        chunk = slug_list[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        deleted += cur.execute(
            f"DELETE FROM create_analyses WHERE slug IN ({placeholders})",
            chunk,
        ).rowcount
    return deleted


def remove_artifacts(artifact_dir: Path, slugs: Set[str]) -> List[str]:
    removed: List[str] = []
    for slug in slugs:
        safe_prefix = slug.replace("::", "-")
        pattern = str(artifact_dir / f"{safe_prefix}*.jsonl")
        for path in glob.glob(pattern):
            os.remove(path)
            removed.append(path)
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually delete rows/files. Default is dry-run.")
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX, help="Path to create_analyses index.sqlite")
    parser.add_argument("--artifacts", type=Path, default=DEFAULT_ARTIFACT_DIR, help="Path to artifacts directory")
    parser.add_argument(
        "--manifests",
        type=Path,
        nargs="*",
        default=DEFAULT_MANIFESTS,
        help="Manifests to include in the union",
    )
    parser.add_argument(
        "--subtract-manifests",
        type=Path,
        nargs="*",
        default=DEFAULT_SUBTRACT_MANIFESTS,
        help="Manifests to subtract from the union",
    )
    args = parser.parse_args()

    include_union = build_union(args.manifests)
    subtract_union = build_union(args.subtract_manifests)

    remaining = {
        key: record for key, record in include_union.items() if key not in subtract_union
    }
    print(f"Loaded include union: {len(include_union)} records")
    print(f"Loaded subtract union: {len(subtract_union)} records")
    print(f"Remaining after subtract: {len(remaining)} records")

    if not remaining:
        print("Nothing to remove; exiting.")
        return

    conn = sqlite3.connect(args.index)
    cur = conn.cursor()

    slugs_to_delete: Set[str] = set()
    for record in remaining.values():
        slugs_to_delete.update(query_slugs_for_record(cur, record))

    print(f"Matched {len(slugs_to_delete)} slugs in SQLite index for deletion.")

    removed_artifacts: List[str] = []
    deleted_rows = 0
    if args.apply:
        deleted_rows = delete_slugs(cur, slugs_to_delete)
        conn.commit()
        removed_artifacts = remove_artifacts(args.artifacts, slugs_to_delete)
    else:
        print("Dry-run: no database rows or files were removed. Use --apply to delete.")

    print(f"Rows deleted: {deleted_rows}")
    print(f"Artifacts removed: {len(removed_artifacts)}")
    if removed_artifacts:
        print("\nRemoved artifact paths:")
        for path in removed_artifacts:
            print(path)


if __name__ == "__main__":
    main()
