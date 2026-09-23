"""Import pre-refactor sqlite caches into the catalog.

The old caches are opened `mode=ro` and never written, so a migration cannot
lose downloads that took months to gather. Re-running is safe: article ids are
derived from identifiers, so the same row always lands on the same article.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier

logger = logging.getLogger(__name__)

BATCH = 2000

#: (old namespace dir, table, new stage). Sources come from the subdirectory.
LAYOUTS: Tuple[Tuple[str, str, str, bool], ...] = (
    ("download", "downloads", "download", True),
    ("extract", "extractions", "extract", True),
    ("metadata", "metadata_entries", "metadata", False),
    ("create_analyses", "create_analyses", "analyses", False),
    ("upload", "upload_entries", "upload", False),
)


@dataclass
class MigrationReport:
    imported: Dict[str, int] = field(default_factory=dict)
    articles_before: int = 0
    articles_after: int = 0
    skipped: Dict[str, int] = field(default_factory=dict)
    dry_run: bool = False

    def render(self) -> str:
        verb = "would import" if self.dry_run else "imported"
        lines = [f"{verb} from legacy caches:"]
        for stage, count in sorted(self.imported.items()):
            skipped = self.skipped.get(stage, 0)
            note = f"   ({skipped:,} skipped, no usable identifier)" if skipped else ""
            lines.append(f"  {stage:<12} {count:>10,}{note}")
        lines.append(
            f"\ncatalog articles: {self.articles_before:,} -> {self.articles_after:,}"
        )
        return "\n".join(lines)


def migrate_caches(
    old_root: Path,
    catalog: Catalog,
    *,
    stages: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> MigrationReport:
    old_root = Path(old_root)
    wanted = {name.lower() for name in stages} if stages else None
    report = MigrationReport(dry_run=dry_run, articles_before=catalog.count_articles())

    for namespace, table, stage, per_source in LAYOUTS:
        if wanted and stage not in wanted:
            continue
        for source, db_path in _databases(old_root / namespace, per_source):
            if not db_path.exists():
                continue
            logger.info("migrating %s (%s) from %s", stage, source or "-", db_path)
            count, skipped = _import_table(
                catalog, db_path, table, stage, source, dry_run=dry_run
            )
            report.imported[stage] = report.imported.get(stage, 0) + count
            report.skipped[stage] = report.skipped.get(stage, 0) + skipped

    report.articles_after = catalog.count_articles()
    return report


def _databases(namespace_root: Path, per_source: bool) -> Iterator[Tuple[str, Path]]:
    if not namespace_root.exists():
        return
    if per_source:
        for child in sorted(namespace_root.iterdir()):
            if child.is_dir() and (child / "index.sqlite").exists():
                yield child.name, child / "index.sqlite"
    else:
        yield "", namespace_root / "index.sqlite"


def _open_readonly(path: Path) -> sqlite3.Connection:
    """Open a legacy cache with no possibility of writing to it.

    `immutable=1` takes no locks and creates no `-shm`/`-wal` sidecars, so it
    works on read-only media and cannot modify the source. It also ignores any
    un-checkpointed WAL, so when one is present we fall back to `mode=ro`,
    which still cannot write the database but does need to map a `-shm` file.
    """
    wal = path.with_name(path.name + "-wal")
    if wal.exists() and wal.stat().st_size > 0:
        logger.warning(
            "%s has an un-checkpointed WAL; opening mode=ro so its contents are seen", path
        )
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _import_table(
    catalog: Catalog,
    db_path: Path,
    table: str,
    stage: str,
    source: str,
    *,
    dry_run: bool,
) -> Tuple[int, int]:
    conn = _open_readonly(db_path)
    try:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not columns:
            return 0, 0
        imported = skipped = 0
        pending: List[Tuple[Identifier, dict]] = []
        for row in conn.execute(f"SELECT * FROM {table}"):
            identifier = _identifier(row, columns)
            if identifier is None:
                skipped += 1
                continue
            pending.append((identifier, _payload(row)))
            if len(pending) >= BATCH:
                imported += _flush(catalog, pending, stage, source, dry_run)
                pending.clear()
        imported += _flush(catalog, pending, stage, source, dry_run)
        return imported, skipped
    finally:
        conn.close()


def _identifier(row: sqlite3.Row, columns: set) -> Optional[Identifier]:
    """Prefer the indexed identifier columns; fall back to the payload, then the slug."""
    fields = {
        kind: row[kind]
        for kind in ("pmid", "pmcid", "doi")
        if kind in columns and row[kind]
    }
    if not fields:
        fields = _identifier_from_payload(row)
    if not fields and "slug" in columns:
        fields = _identifier_from_slug(row["slug"])
    if not fields:
        return None
    return Identifier(**fields)


#: Legacy slugs are `slugify(f"{pmid}-{doi}-{pmcid}")`. The pmid is leading
#: digits and the pmcid a trailing `pmc…`; whatever sits between them is the
#: DOI with its punctuation flattened, which cannot be reversed — so only the
#: two recoverable parts are used, which is enough to name the article.
_SLUG_PMID = re.compile(r"^(\d{4,9})(?:-|$)")
_SLUG_PMCID = re.compile(r"(?:^|-)(pmc\d+)$", re.IGNORECASE)


def _identifier_from_slug(slug: Optional[str]) -> Dict[str, str]:
    if not slug:
        return {}
    # An analyses cache key is "<article slug>::<table>"; keep the article half.
    slug = str(slug).split("::", 1)[0]
    fields: Dict[str, str] = {}
    pmid = _SLUG_PMID.match(slug)
    if pmid:
        fields["pmid"] = pmid.group(1)
    pmcid = _SLUG_PMCID.search(slug)
    if pmcid:
        fields["pmcid"] = pmcid.group(1).upper()
    return fields


def _identifier_from_payload(row: sqlite3.Row) -> Dict[str, str]:
    for column in ("payload_json", "metadata_json"):
        try:
            blob = row[column]
        except (IndexError, KeyError):
            continue
        if not blob:
            continue
        try:
            data = json.loads(blob)
        except (TypeError, ValueError):
            continue
        for candidate in _identifier_candidates(data):
            fields = {
                kind: candidate[kind]
                for kind in ("pmid", "pmcid", "doi")
                if candidate.get(kind)
            }
            if fields:
                return fields
    return {}


def _identifier_candidates(data) -> Iterator[dict]:
    if isinstance(data, dict):
        if any(key in data for key in ("pmid", "pmcid", "doi")):
            yield data
        for key in ("identifier", "seed_identifier", "analysis_collection", "article_data"):
            value = data.get(key)
            if isinstance(value, dict):
                yield from _identifier_candidates(value)


def _payload(row: sqlite3.Row) -> dict:
    try:
        return json.loads(row["payload_json"]) if row["payload_json"] else {}
    except (TypeError, ValueError):
        return {}


def _flush(
    catalog: Catalog,
    pending: Sequence[Tuple[Identifier, dict]],
    stage: str,
    source: str,
    dry_run: bool,
) -> int:
    if not pending:
        return 0
    if dry_run:
        return len(pending)
    refs = catalog.register_many([identifier for identifier, _ in pending])
    # No fingerprint: the legacy cache recorded nothing about the prompt, model
    # or extractor version behind a row, so there is nothing to compare against.
    # An empty fingerprint reads as "provenance unknown, accept it", which keeps
    # a migration from re-downloading the whole corpus. Force a recompute with
    # `ingest run --refresh <stage>`.
    outcomes = [
        Outcome(
            article_id=ref.id,
            stage=stage,
            source=source,
            status=Status.OK,
            fingerprint="",
            payload=payload or None,
            summary={"migrated": True},
        )
        for ref, (_, payload) in zip(refs, pending)
    ]
    catalog.record(outcomes)
    return len(outcomes)


__all__ = ["MigrationReport", "migrate_caches"]
