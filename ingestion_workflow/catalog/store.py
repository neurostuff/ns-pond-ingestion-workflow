"""The catalog: article identity, stage artifacts, and attempt history.

One sqlite database, one connection per process. Every "is this already done?"
question in the pipeline is answered here and nowhere else.
"""

from __future__ import annotations

import base64
import logging
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from ingestion_workflow.models.ids import Identifier

from .blobs import BlobStore
from .models import ALIAS_KINDS, NO_SOURCE, ArticleRef, Artifact, Outcome, Status, utcnow
from .schema import DDL, PRAGMAS, SCHEMA_VERSION

logger = logging.getLogger(__name__)

#: Article ids are 12 lowercase base32 characters. Deliberately not the
#: mixed-case shortuuid-12 that Neurostore uses for base_study_id, so the two
#: cannot be mistaken for each other where both appear.
ID_LENGTH = 12


def _article_id(seed: str) -> str:
    """A stable, opaque key for an article.

    `seed` is one string, `"<kind>:<value>"`, built from the strongest
    identifier the article had when it was first registered -- pmcid, else
    pmid, else doi, else neurostore. Nothing else contributes: not the other
    identifiers, not the source, not the time.

    So the id is reproducible for a given first-observation, and two catalogs
    agree only if they saw the same identifier first. That is the case for a
    repeated migration over the same caches, which is what the reproducibility
    is for; it is not a canonical name for the article, and nothing depends on
    it being one. The id also never changes once assigned -- a later pmcid on a
    doi-seeded article becomes an alias -- so a random id would be equally
    correct here, as `test_a_random_id_would_also_be_correct` shows.

    This is not the Neurostore base_study_id. That is assigned by Neurostore
    during upload, does not exist until then, and is recorded as a `neurostore`
    alias once known.
    """
    digest = hashlib.blake2b(seed.encode("utf-8"), digest_size=10).digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()[:ID_LENGTH]


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

    #: Kinds an article may hold only one of. Two PMIDs on one article means two
    #: papers were fused; the same for two PMCIDs. DOIs are not on the list --
    #: one paper legitimately has several (a preprint, an erratum, case variants).
    SINGULAR = ("pmid", "pmcid")

    def register_many(self, identifiers: Sequence[Identifier]) -> List[ArticleRef]:
        """Register articles, adding aliases to existing ones. Idempotent.

        Returns one ref per identifier that carries any id, in order.

        A known id is evidence that a record is an existing article, not proof.
        It used to be treated as proof: every article any of a record's ids
        pointed to was merged into one, and the record's other ids were moved
        onto it. One cache had stamped the same PMCID on about 1,300 different
        papers, and migrating it fused all of them into a single article that
        held 1,322 PMIDs. So now:

        * the record anchors on the article holding its PMID, else its PMCID,
          else a DOI, else its neurostore id;
        * an anchor that holds a different PMID is not this paper: a new
          article is made instead;
        * other matched articles are merged in only if the result would still
          hold at most one PMID and one PMCID;
        * an id another article owns is never taken from it.

        Every refusal is logged and counted on `self.conflicts`.
        """
        if not identifiers:
            return []
        refs: List[str] = []
        now = utcnow()
        with self._write() as conn:
            for identifier in identifiers:
                pairs = _alias_pairs(identifier)
                if not pairs:
                    continue
                owners: Dict[Tuple[str, str], str] = {}
                for kind, value in pairs:
                    row = conn.execute(
                        "SELECT article_id FROM aliases WHERE kind=? AND value=?", (kind, value)
                    ).fetchone()
                    if row:
                        owners[(kind, value)] = self._follow_merge(row["article_id"])
                incoming = {k: v for k, v in pairs if k in self.SINGULAR}

                anchor = None
                for kind in ("pmid", "pmcid", "doi", "neurostore"):
                    hit = next((owners[p] for p in pairs if p[0] == kind and p in owners), None)
                    if hit:
                        anchor = hit
                        break
                if anchor is not None and not self._compatible(conn, [anchor], incoming):
                    self._conflict(identifier, f"{anchor} holds a different "
                                   f"{'/'.join(self.SINGULAR)}; registering a new article")
                    anchor = None
                if anchor is None:
                    seed = next(((k, v) for k, v in pairs if (k, v) not in owners), pairs[0])
                    anchor = _article_id(f"{seed[0]}:{seed[1]}")
                    if self._follow_merge(anchor) != anchor or conn.execute(
                        "SELECT 1 FROM aliases WHERE article_id=? LIMIT 1", (anchor,)
                    ).fetchone():
                        # The seed's natural id is taken by another paper.
                        anchor = _article_id(f"{seed[0]}:{seed[1]}:{now}:{len(refs)}")
                    conn.execute(
                        "INSERT INTO articles(id, created_at) VALUES(?, ?) "
                        "ON CONFLICT(id) DO NOTHING",
                        (anchor, now),
                    )

                group = [anchor]
                for other in dict.fromkeys(owners.values()):
                    if other in group:
                        continue
                    if self._compatible(conn, group + [other], incoming):
                        group.append(other)
                    else:
                        self._conflict(identifier, f"not merging {other} into {anchor}: "
                                       "they would hold two PMIDs or PMCIDs")
                article_id = self._merge(conn, group) if len(group) > 1 else anchor

                for kind, value in pairs:
                    owner = owners.get((kind, value))
                    owner = self._follow_merge(owner) if owner else None
                    if owner == article_id:
                        continue
                    if owner is not None:
                        self._conflict(identifier, f"{kind} {value} belongs to {owner}; left there")
                        continue
                    if kind in self.SINGULAR and self._held(conn, article_id, kind) - {value}:
                        self._conflict(identifier, f"{article_id} already has a {kind}; "
                                       f"{value} not attached")
                        continue
                    conn.execute(
                        "INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?) "
                        "ON CONFLICT(kind, value) DO NOTHING",
                        (kind, value, article_id),
                    )
                refs.append(article_id)
        return [self.ref(article_id) for article_id in refs]

    #: Refusals since this catalog was opened, for callers that report them.
    conflicts: int = 0

    def _conflict(self, identifier: Identifier, why: str) -> None:
        self.conflicts += 1
        logger.warning("catalog: %s -- %s", identifier.slug, why)

    @staticmethod
    def _held(conn: sqlite3.Connection, article_id: str, kind: str) -> set:
        return {row[0] for row in conn.execute(
            "SELECT value FROM aliases WHERE article_id=? AND kind=?", (article_id, kind))}

    def _compatible(self, conn, article_ids: Sequence[str], incoming: Mapping[str, str]) -> bool:
        """Whether fusing these articles with the record would bring two papers together.

        Judged pairwise, by what each side holds: two sides clash on a kind when
        both hold values of it and neither holds all of the other's. An article
        that is already wrong -- one holding many PMIDs from before this guard --
        is not made worse by a record that brings nothing new, so it still
        resolves; otherwise every lookup of it would make an empty new article.
        """
        sides = [{kind: self._held(conn, a, kind) for kind in self.SINGULAR} for a in article_ids]
        sides.append({kind: ({incoming[kind]} if incoming.get(kind) else set()) for kind in self.SINGULAR})
        for kind in self.SINGULAR:
            seen: set = set()
            for side in sides:
                values = side[kind]
                if values and seen and not (values <= seen or seen <= values):
                    return False
                seen |= values
        return True

    def register(self, identifier: Identifier) -> ArticleRef:
        refs = self.register_many([identifier])
        if not refs:
            raise ValueError("Identifier carries no pmid, pmcid, doi or neurostore id")
        return refs[0]

    def add_aliases(self, pairs: Sequence[Tuple[str, str, str]]) -> None:
        """Attach identifiers to articles that already exist.

        Used for ids that are only learned later -- a Neurostore base_study_id
        is not known until the upload stage has run.
        """
        rows = [
            (kind, value, article_id)
            for article_id, kind, value in pairs
            if article_id and kind in ALIAS_KINDS and value
        ]
        if not rows:
            return
        with self._write() as conn:
            for kind, value, article_id in rows:
                row = conn.execute(
                    "SELECT article_id FROM aliases WHERE kind=? AND value=?", (kind, value)
                ).fetchone()
                if row and self._follow_merge(row["article_id"]) != self._follow_merge(article_id):
                    # Taking it would leave the other article unreachable by an id it
                    # was registered under; the clash is a fault to report, not resolve.
                    self.conflicts += 1
                    logger.warning("catalog: %s %s belongs to %s, not %s; left there",
                                   kind, value, row["article_id"], article_id)
                    continue
                conn.execute(
                    "INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?) "
                    "ON CONFLICT(kind, value) DO NOTHING",
                    (kind, value, article_id),
                )

    def _merge(self, conn: sqlite3.Connection, ids: Sequence[str]) -> str:
        """Point every id at the oldest article. Nothing is deleted.

        Oldest rather than lowest-hash: it has had longer to accumulate
        artifacts, so keeping it is the smaller rewrite. `created_at` has
        second resolution and a batch shares one timestamp, so the id breaks
        ties to keep the choice deterministic.
        """
        survivor = self._oldest(conn, ids)
        for other in [i for i in ids if i != survivor]:
            conn.execute("UPDATE articles SET merged_into=? WHERE id=?", (survivor, other))
            conn.execute("UPDATE aliases SET article_id=? WHERE article_id=?", (survivor, other))
            self._move_artifacts(conn, other, survivor)
        return survivor

    @staticmethod
    def _move_artifacts(conn: sqlite3.Connection, loser: str, survivor: str) -> None:
        """Carry the loser's artifacts over, keeping the better of any clash.

        `UPDATE OR IGNORE` alone silently leaves a clashing artifact attached to
        an article that no longer exists, where nothing can reach it again.
        """
        conn.execute(
            """
            DELETE FROM artifacts WHERE article_id = ?
              AND EXISTS (
                SELECT 1 FROM artifacts keep
                WHERE keep.article_id = ?
                  AND keep.stage = artifacts.stage
                  AND keep.source = artifacts.source
                  AND (
                    (keep.status = 'ok' AND artifacts.status != 'ok')
                    OR (
                      (keep.status = 'ok') = (artifacts.status = 'ok')
                      AND keep.updated_at >= artifacts.updated_at
                    )
                  )
              )
            """,
            (loser, survivor),
        )
        # Whatever is left is either unclashed or the better of the pair.
        conn.execute(
            """
            DELETE FROM artifacts WHERE article_id = ?
              AND EXISTS (
                SELECT 1 FROM artifacts loser
                WHERE loser.article_id = ?
                  AND loser.stage = artifacts.stage
                  AND loser.source = artifacts.source
              )
            """,
            (survivor, loser),
        )
        conn.execute(
            "UPDATE artifacts SET article_id = ? WHERE article_id = ?", (survivor, loser)
        )
        conn.execute("UPDATE attempts SET article_id = ? WHERE article_id = ?", (survivor, loser))

    @staticmethod
    def _oldest(conn: sqlite3.Connection, ids: Sequence[str]) -> str:
        marks = ",".join("?" * len(ids))
        row = conn.execute(
            f"SELECT id FROM articles WHERE id IN ({marks}) ORDER BY created_at, id LIMIT 1",
            tuple(ids),
        ).fetchone()
        return row["id"] if row else sorted(ids)[0]

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

    def article_ids_where_summary(self, stage: str, key: str) -> set:
        """Articles whose finished `stage` artifact has a truthy `key`.

        One query over the `(stage, status)` index rather than a walk: asking
        it of 485,126 triage rows returns the 48,390 with a passing table in
        half a second. The alternative was enumerating every article and
        deciding one at a time, which for `analyses` meant planning 477,625
        that had nothing to do.

        `key` names a field this codebase writes, never anything a user typed.
        """
        rows = self._conn.execute(
            "SELECT article_id FROM artifacts "
            "WHERE stage = ? AND status = 'ok' "
            "AND json_extract(summary, '$." + key + "') > 0",
            (stage,),
        )
        return {row["article_id"] for row in rows}

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

    # -- exclusions ----------------------------------------------------------

    #: The table id that stands for every table of an article.
    WHOLE_ARTICLE = "*"

    def exclude(self, rows: Sequence[Tuple[str, str, str, str]]) -> int:
        """Record (article_id, table_id, reason, note) verdicts. Idempotent:
        marking a table again replaces its reason and note."""
        now = utcnow()
        with self._write() as conn:
            conn.executemany(
                "INSERT INTO exclusions(article_id, table_id, reason, note, created_at) "
                "VALUES(?, ?, ?, ?, ?) ON CONFLICT(article_id, table_id) DO UPDATE SET "
                "reason=excluded.reason, note=excluded.note",
                [(a, str(t), r, n or "", now) for a, t, r, n in rows],
            )
        return len(rows)

    def unexclude(self, article_id: str, table_id: Optional[str] = None) -> int:
        """Withdraw a verdict, or every verdict on the article when no table is given."""
        with self._write() as conn:
            if table_id is None:
                cur = conn.execute("DELETE FROM exclusions WHERE article_id=?", (article_id,))
            else:
                cur = conn.execute(
                    "DELETE FROM exclusions WHERE article_id=? AND table_id=?",
                    (article_id, str(table_id)),
                )
        return cur.rowcount

    def exclusions(self, article_ids: Optional[Sequence[str]] = None) -> Dict[str, Dict[str, Dict[str, str]]]:
        """article_id -> table_id -> {reason, note, created_at}; every article when none are given."""
        out: Dict[str, Dict[str, Dict[str, str]]] = {}
        if article_ids is None:
            rows = self._conn.execute(
                "SELECT article_id, table_id, reason, note, created_at FROM exclusions"
            ).fetchall()
        else:
            ids = list(dict.fromkeys(article_ids))
            rows = []
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                rows += self._conn.execute(
                    "SELECT article_id, table_id, reason, note, created_at FROM exclusions "
                    "WHERE article_id IN (%s)" % ",".join("?" * len(chunk)),
                    chunk,
                ).fetchall()
        for row in rows:
            out.setdefault(row["article_id"], {})[row["table_id"]] = {
                "reason": row["reason"], "note": row["note"], "created_at": row["created_at"],
            }
        return out

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
        """{stage: {status: articles}} across the whole catalog.

        Counts articles, not artifacts, so the columns are comparable with
        `ready_counts` and with the catalog total. A stage that runs per source
        can hold several artifacts for one article; that article's state is the
        best any of its sources reached, so the columns stay mutually
        exclusive.
        """
        out: Dict[str, Dict[str, int]] = {}
        for row in self._conn.execute(
            """
            SELECT stage, state, COUNT(*) AS n FROM (
                SELECT stage, article_id,
                    CASE
                        WHEN MAX(status = 'ok') THEN 'ok'
                        WHEN MAX(status = 'failed') THEN 'failed'
                        WHEN MAX(status = 'permanent') THEN 'permanent'
                        ELSE 'skipped'
                    END AS state
                FROM artifacts GROUP BY stage, article_id
            ) GROUP BY stage, state
            """
        ):
            out.setdefault(row["stage"], {})[row["state"]] = int(row["n"])
        return out

    def ready_counts(self, requirements: Mapping[str, Optional[str]]) -> Dict[str, int]:
        """How many articles each stage could process right now.

        Ready means the upstream stage succeeded and this stage has no entry
        yet, so it counts the queue standing at each stage independently. A
        run's plan cascades instead -- a downstream stage reports `blocked`
        because its upstream has not run *in that pass* -- which answers a
        different question.
        """
        counts: Dict[str, int] = {}
        for stage, upstream in requirements.items():
            if upstream is None:
                row = self._conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM articles ar
                    WHERE ar.merged_into IS NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM artifacts a
                        WHERE a.article_id = ar.id AND a.stage = ?
                      )
                    """,
                    (stage,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    """
                    SELECT COUNT(DISTINCT up.article_id) AS n FROM artifacts up
                    WHERE up.stage = ? AND up.status = 'ok'
                      AND NOT EXISTS (
                        SELECT 1 FROM artifacts a
                        WHERE a.article_id = up.article_id AND a.stage = ?
                      )
                    """,
                    (upstream, stage),
                ).fetchone()
            counts[stage] = int(row["n"])
        return counts

    def stranded_artifacts(self) -> int:
        """Artifacts attached to an article that a merge retired."""
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS n FROM artifacts a
            JOIN articles ar ON ar.id = a.article_id
            WHERE ar.merged_into IS NOT NULL
            """
        ).fetchone()
        return int(row["n"])

    def repair_merges(self) -> int:
        """Re-run artifact hand-over for merges that stranded something."""
        rows = self._conn.execute(
            """
            SELECT DISTINCT a.article_id AS loser, ar.merged_into AS survivor
            FROM artifacts a JOIN articles ar ON ar.id = a.article_id
            WHERE ar.merged_into IS NOT NULL
            """
        ).fetchall()
        if not rows:
            return 0
        with self._write() as conn:
            for row in rows:
                self._move_artifacts(
                    conn, row["loser"], self._follow_merge(row["survivor"])
                )
        return len(rows)

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
