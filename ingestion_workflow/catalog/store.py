"""The catalog: article identity, stage artifacts, and attempt history.

One sqlite database, one connection per process. Every "is this already done?"
question in the pipeline is answered here and nowhere else.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import shortuuid

from ingestion_workflow.models.ids import Identifier

from .blobs import BlobStore
from .models import ALIAS_KINDS, NO_SOURCE, ArticleRef, Artifact, Outcome, Status, utcnow
from .schema import DDL, PRAGMAS, SCHEMA_VERSION

_UUID_NAMESPACE = "https://neurostore.org/ingestion/article/"


def _article_id(seed: str) -> str:
    """Derive an article id from its strongest identifier.

    Deterministic so that re-registering the same article, or re-running a
    migration, yields the same id instead of a duplicate.
    """
    return shortuuid.uuid(name=_UUID_NAMESPACE + seed)[:12]


def _alias_pairs(identifier: Identifier) -> List[Tuple[str, str]]:
    pairs = []
    for kind in ALIAS_KINDS:
        value = getattr(identifier, kind, None)
        if value:
            pairs.append((kind, str(value)))
    return pairs


class Catalog:
    """Read/write access to one catalog database."""

    def __init__(self, connection: sqlite3.Connection, root: Path) -> None:
        self._conn = connection
        self.root = Path(root)
        self.blobs = BlobStore(self.root / "blobs")

    # -- lifecycle -----------------------------------------------------------

    @classmethod
    def open(cls, root: Path | str) -> "Catalog":
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(root / "catalog.sqlite", isolation_level=None)
        conn.row_factory = sqlite3.Row
        for pragma in PRAGMAS:
            conn.execute(pragma)
        conn.executescript(DDL)
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO NOTHING",
            (str(SCHEMA_VERSION),),
        )
        return cls(conn, root)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Catalog":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    # -- identity ------------------------------------------------------------

    def resolve(self, identifier: Identifier) -> Optional[ArticleRef]:
        """Find an existing article by any of its identifiers."""
        for kind, value in _alias_pairs(identifier):
            row = self._conn.execute(
                "SELECT article_id FROM aliases WHERE kind=? AND value=?", (kind, value)
            ).fetchone()
            if row:
                return self.ref(self._follow_merge(row["article_id"]))
        return None

    def register_many(self, identifiers: Sequence[Identifier]) -> List[ArticleRef]:
        """Register articles, adding aliases to existing ones. Idempotent."""
        if not identifiers:
            return []
        refs: List[ArticleRef] = []
        now = utcnow()
        with self._write() as conn:
            for identifier in identifiers:
                pairs = _alias_pairs(identifier)
                if not pairs:
                    continue
                found = {
                    self._follow_merge(row["article_id"])
                    for kind, value in pairs
                    if (
                        row := conn.execute(
                            "SELECT article_id FROM aliases WHERE kind=? AND value=?",
                            (kind, value),
                        ).fetchone()
                    )
                }
                if not found:
                    article_id = _article_id(f"{pairs[0][0]}:{pairs[0][1]}")
                    conn.execute(
                        "INSERT INTO articles(id, created_at) VALUES(?, ?) "
                        "ON CONFLICT(id) DO NOTHING",
                        (article_id, now),
                    )
                else:
                    article_id = self._merge(conn, sorted(found))
                conn.executemany(
                    "INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?) "
                    "ON CONFLICT(kind, value) DO UPDATE SET article_id=excluded.article_id",
                    [(kind, value, article_id) for kind, value in pairs],
                )
                refs.append(article_id)
        return [self.ref(article_id) for article_id in refs]

    def register(self, identifier: Identifier) -> ArticleRef:
        refs = self.register_many([identifier])
        if not refs:
            raise ValueError("Identifier carries no pmid, pmcid, doi or neurostore id")
        return refs[0]

    def _merge(self, conn: sqlite3.Connection, ids: Sequence[str]) -> str:
        """Point every id at the oldest one. Nothing is deleted."""
        survivor = ids[0]
        for other in ids[1:]:
            conn.execute("UPDATE articles SET merged_into=? WHERE id=?", (survivor, other))
            conn.execute("UPDATE aliases SET article_id=? WHERE article_id=?", (survivor, other))
            conn.execute(
                "UPDATE OR IGNORE artifacts SET article_id=? WHERE article_id=?",
                (survivor, other),
            )
        return survivor

    def _follow_merge(self, article_id: str) -> str:
        seen = set()
        while article_id not in seen:
            seen.add(article_id)
            row = self._conn.execute(
                "SELECT merged_into FROM articles WHERE id=?", (article_id,)
            ).fetchone()
            if row is None or not row["merged_into"]:
                return article_id
            article_id = row["merged_into"]
        return article_id

    def ref(self, article_id: str) -> ArticleRef:
        return ArticleRef(id=article_id, identifier=self.identifier(article_id))

    def identifier(self, article_id: str) -> Identifier:
        rows = self._conn.execute(
            "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)
        ).fetchall()
        fields = {row["kind"]: row["value"] for row in rows}
        return Identifier(**{kind: fields.get(kind) for kind in ALIAS_KINDS})

    def identifiers(self, article_ids: Sequence[str]) -> Dict[str, Identifier]:
        """Batch form of `identifier`, one query per chunk instead of per article."""
        out: Dict[str, Dict[str, str]] = {aid: {} for aid in article_ids}
        for chunk in _chunks(article_ids, 900):
            marks = ",".join("?" * len(chunk))
            for row in self._conn.execute(
                f"SELECT article_id, kind, value FROM aliases WHERE article_id IN ({marks})",
                tuple(chunk),
            ):
                out[row["article_id"]][row["kind"]] = row["value"]
        return {
            aid: Identifier(**{kind: fields.get(kind) for kind in ALIAS_KINDS})
            for aid, fields in out.items()
        }

    def count_articles(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM articles WHERE merged_into IS NULL"
        ).fetchone()
        return int(row["n"])

    def all_article_ids(self) -> Iterator[str]:
        for row in self._conn.execute(
            "SELECT id FROM articles WHERE merged_into IS NULL ORDER BY created_at, id"
        ):
            yield row["id"]

    # -- artifacts -----------------------------------------------------------

    def artifact(
        self, article_id: str, stage: str, source: str = NO_SOURCE
    ) -> Optional[Artifact]:
        row = self._conn.execute(
            "SELECT * FROM artifacts WHERE article_id=? AND stage=? AND source=?",
            (article_id, stage, source),
        ).fetchone()
        return _artifact(row) if row else None

    def artifacts(
        self, article_ids: Sequence[str], stage: str
    ) -> Dict[str, Dict[str, Artifact]]:
        """All artifacts for a stage, as {article_id: {source: Artifact}}."""
        out: Dict[str, Dict[str, Artifact]] = {}
        for chunk in _chunks(article_ids, 900):
            marks = ",".join("?" * len(chunk))
            for row in self._conn.execute(
                f"SELECT * FROM artifacts WHERE stage=? AND article_id IN ({marks})",
                (stage, *chunk),
            ):
                out.setdefault(row["article_id"], {})[row["source"]] = _artifact(row)
        return out

    def artifacts_for_article(self, article_id: str) -> List[Artifact]:
        return [
            _artifact(row)
            for row in self._conn.execute(
                "SELECT * FROM artifacts WHERE article_id=? ORDER BY stage, source",
                (article_id,),
            )
        ]

    def payload(self, artifact: Optional[Artifact]) -> Optional[Any]:
        """The full payload behind an artifact, read from the blob store."""
        return self.blobs.get(artifact.blob) if artifact else None

    def record(self, outcomes: Sequence[Outcome]) -> None:
        """Persist stage results and their attempt history in one transaction."""
        if not outcomes:
            return
        now = utcnow()
        rows = []
        attempts = []
        for outcome in outcomes:
            blob = self.blobs.put(outcome.payload) if outcome.payload is not None else None
            rows.append(
                (
                    outcome.article_id,
                    outcome.stage,
                    outcome.source,
                    outcome.status.value,
                    outcome.fingerprint,
                    blob,
                    json.dumps(outcome.summary, separators=(",", ":")),
                    outcome.error,
                    now,
                )
            )
            attempts.append(
                (
                    outcome.article_id,
                    outcome.stage,
                    outcome.source,
                    now,
                    1 if outcome.status is Status.OK else 0,
                    outcome.error,
                )
            )
        with self._write() as conn:
            conn.executemany(
                """
                INSERT INTO artifacts
                    (article_id, stage, source, status, fingerprint, blob,
                     summary, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(article_id, stage, source) DO UPDATE SET
                    status=excluded.status, fingerprint=excluded.fingerprint,
                    blob=excluded.blob, summary=excluded.summary,
                    error=excluded.error, updated_at=excluded.updated_at
                """,
                rows,
            )
            conn.executemany(
                "INSERT INTO attempts(article_id, stage, source, attempted_at, ok, error) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                attempts,
            )

    def attempt_counts(
        self, article_ids: Sequence[str], stage: str, source: str
    ) -> Dict[str, Tuple[int, Optional[str]]]:
        """{article_id: (failed_attempts, last_attempt_at)} for a stage/source."""
        out: Dict[str, Tuple[int, Optional[str]]] = {}
        for chunk in _chunks(article_ids, 900):
            marks = ",".join("?" * len(chunk))
            for row in self._conn.execute(
                f"""
                SELECT article_id, COUNT(*) AS n, MAX(attempted_at) AS last
                FROM attempts
                WHERE stage=? AND source=? AND ok=0 AND article_id IN ({marks})
                GROUP BY article_id
                """,
                (stage, source, *chunk),
            ):
                out[row["article_id"]] = (int(row["n"]), row["last"])
        return out

    # -- reporting -----------------------------------------------------------

    def status_counts(self) -> Dict[str, Dict[str, int]]:
        """{stage: {status: count}} across the whole catalog."""
        out: Dict[str, Dict[str, int]] = {}
        for row in self._conn.execute(
            "SELECT stage, status, COUNT(*) AS n FROM artifacts GROUP BY stage, status"
        ):
            out.setdefault(row["stage"], {})[row["status"]] = int(row["n"])
        return out

    def vacuum(self) -> None:
        self._conn.execute("VACUUM")


def _artifact(row: sqlite3.Row) -> Artifact:
    return Artifact(
        article_id=row["article_id"],
        stage=row["stage"],
        source=row["source"],
        status=Status(row["status"]),
        fingerprint=row["fingerprint"],
        blob=row["blob"],
        summary=json.loads(row["summary"]) if row["summary"] else {},
        error=row["error"],
        updated_at=row["updated_at"],
    )


def _chunks(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def is_retryable(
    artifact: Optional[Artifact],
    attempts: int,
    last_attempt: Optional[str],
    *,
    max_attempts: int,
    backoff: timedelta,
) -> bool:
    """Whether a failed artifact has earned another try."""
    if artifact is None:
        return True
    if artifact.status is not Status.FAILED:
        return False
    if attempts >= max_attempts:
        return False
    if last_attempt is None:
        return True
    try:
        when = datetime.fromisoformat(last_attempt)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - when >= backoff
