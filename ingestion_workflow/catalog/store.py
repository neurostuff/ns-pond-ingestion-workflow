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
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from ingestion_workflow.models.ids import DUPLICATE_PUBMED_RECORDS, Identifier
from ingestion_workflow.utils.doi import normalize_doi

from .backup import backup
from .blobs import BlobStore
from .merges import Merges, follow
from .models import (
    ALIAS_KINDS,
    NO_SOURCE,
    WHOLE_ARTICLE,
    ArticleRef,
    Artifact,
    Outcome,
    Status,
    utcnow,
)
from .schema import DDL, PRAGMAS, SCHEMA_VERSION

logger = logging.getLogger(__name__)

#: Article ids are 12 lowercase base32 characters. Deliberately not the
#: mixed-case shortuuid-12 that Neurostore uses for base_study_id, so the two
#: cannot be mistaken for each other where both appear.
ID_LENGTH = 12

#: Stages whose last OK answer outlives a later failed attempt. A refresh that
#: fails (PubMed down) is recorded as an attempt only; what downstream stages
#: read stays the last answer that worked.
KEEPS_LAST_OK = ("notices",)


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


def _presented(aliases: Sequence[Tuple[str, str]]) -> Identifier:
    """The Identifier an article presents, from its alias rows.

    An article holds several PMIDs when two PubMed records of one paper were
    merged on purpose, or from before the registration guard. It presents the
    lowest, PubMed's earlier record, as `pmid`, so every stage asks about the
    same one, and the rest under `other_ids[DUPLICATE_PUBMED_RECORDS]`. For
    other kinds the last row read wins.
    """
    fields: Dict[str, str] = {}
    pmids: List[str] = []
    for kind, value in aliases:
        if kind == "pmid":
            pmids.append(value)
        else:
            fields[kind] = value
    pmids.sort(key=lambda p: (len(p), p))
    return Identifier(
        **{kind: fields.get(kind) for kind in ALIAS_KINDS if kind != "pmid"},
        pmid=pmids[0] if pmids else None,
        other_ids={DUPLICATE_PUBMED_RECORDS: ",".join(pmids[1:])} if len(pmids) > 1 else None,
    )


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
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        # a catalog with no version row was just created
        version = int(row["value"]) if row else SCHEMA_VERSION
        if version > SCHEMA_VERSION:
            conn.close()
            raise RuntimeError(
                f"{root / 'catalog.sqlite'} is schema version {version}; this code reads "
                f"version {SCHEMA_VERSION}. Update the code instead of opening it with this one."
            )
        catalog = cls(conn, root)
        if version < SCHEMA_VERSION:
            try:
                catalog._migrate()
            except BaseException:
                conn.close()
                raise
        elif row is None:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)", (str(SCHEMA_VERSION),)
            )
        return catalog

    def backup(self, target: Path | str) -> Path:
        """Copy this catalog's database to `target`, a path not yet taken.

        A second connection reads the copy: sqlite cannot back up from one that
        holds the write lock, and a reader sees a consistent snapshot of what
        the others have committed.
        """
        with closing(sqlite3.connect(self.root / "catalog.sqlite")) as reader:
            return backup(reader, Path(target))

    def _migrate(self) -> None:
        """Bring the catalog up to `SCHEMA_VERSION`.

        Under the write lock, the version is read again: a process that opened
        the catalog while another migrated it waits here, then finds it current.
        A copy is taken next, beside the catalog, because the rewrite drops the
        spellings it replaces; without the copy nothing is migrated. The data
        and the new version commit together, so a failed migration leaves the
        catalog as it was.
        """
        with self._write() as conn:
            version = int(conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'").fetchone()["value"])
            if version >= SCHEMA_VERSION:
                return
            # `backup` reads through a second connection, as sqlite cannot back up
            # from the one holding the write lock. That lock keeps any write out
            # between the copy and the rewrite.
            copy = self.backup(self.root / f"catalog.pre-v{version}.sqlite")
            logger.warning("catalog: schema %d -> %d; backed up to %s",
                           version, SCHEMA_VERSION, copy)
            self._canonical_doi_aliases(conn)
            conn.execute(
                "UPDATE meta SET value=? WHERE key='schema_version'", (str(SCHEMA_VERSION),)
            )

    def _canonical_doi_aliases(self, conn: sqlite3.Connection) -> Dict[str, int]:
        """Rewrite doi aliases to the canonical DOI that lookups use: bare, and
        lower case.

        A DOI held in another spelling (a resolver URL, upper case) would not be
        found, and its article would be registered a second time. Where the
        canonical DOI is already an alias: of the same article, the other
        spelling is dropped; of another article that could be this paper, the
        two are merged; of a different paper (another PMID or PMCID), the
        spelling is left, and logged, for a person to settle.
        """
        counts = dict.fromkeys(("rewritten", "dropped", "merged", "left"), 0)
        rows = conn.execute("SELECT value, article_id FROM aliases WHERE kind='doi'").fetchall()
        for row in rows:
            value, canonical = row["value"], normalize_doi(row["value"])
            if canonical == value:
                continue
            other = conn.execute(
                "SELECT article_id FROM aliases WHERE kind='doi' AND value=?", (canonical,)
            ).fetchone()
            if other is None:
                conn.execute("UPDATE aliases SET value=? WHERE kind='doi' AND value=?",
                             (canonical, value))
                counts["rewritten"] += 1
                continue
            # An earlier row of this pass may have merged this article away.
            mine = self._follow_merge(row["article_id"])
            theirs = self._follow_merge(other["article_id"])
            if mine != theirs and self._compatible(conn, [theirs, mine], {}):
                mine = theirs = self._merge(conn, [theirs, mine])
                counts["merged"] += 1
            if mine == theirs:
                conn.execute("DELETE FROM aliases WHERE kind='doi' AND value=?", (value,))
                counts["dropped"] += 1
            else:
                logger.warning(
                    "catalog: doi %s of %s is also %s of %s, another paper; left as it is",
                    value, mine, canonical, theirs,
                )
                counts["left"] += 1
        logger.warning(
            "catalog: canonical doi aliases: %(rewritten)d rewritten, %(dropped)d other "
            "spellings dropped, %(merged)d articles merged, %(left)d left", counts,
        )
        return counts

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
    #: one paper legitimately has several (a preprint, an erratum).
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
            (alias_kind, alias_value, article_id)
            for article_id, kind, value in pairs
            if article_id and kind in ALIAS_KINDS
            for alias_kind, alias_value in _alias_pairs(Identifier(**{kind: value}))
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
            self._move_exclusions(conn, other, survivor)
        return survivor

    @staticmethod
    def _move_exclusions(
        conn: sqlite3.Connection, loser: str, survivor: str
    ) -> List[Tuple[str, str, str, str]]:
        """Carry a person's table exclusions to the survivor.

        Both sides excluding one table is the same verdict twice: the survivor's
        row stays and the loser's goes. Those rows are returned as (article_id,
        table_id, reason, note) and logged, so what a clash drops is on record.
        """
        dropped = [
            tuple(row)
            for row in conn.execute(
                """
                SELECT article_id, table_id, reason, note FROM exclusions
                WHERE article_id = ? AND table_id IN (
                    SELECT table_id FROM exclusions WHERE article_id = ?
                )
                """,
                (loser, survivor),
            )
        ]
        for row in dropped:
            logger.warning("catalog: exclusion %s %s (%r, %r) dropped; %s holds the same table",
                           *row, survivor)
        conn.execute(
            "UPDATE OR IGNORE exclusions SET article_id = ? WHERE article_id = ?",
            (survivor, loser),
        )
        conn.execute("DELETE FROM exclusions WHERE article_id = ?", (loser,))
        return dropped

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
        """Read from the database at each step: a write path merges as it goes."""
        def merged_into(article_id: str) -> Optional[str]:
            row = self._conn.execute(
                "SELECT merged_into FROM articles WHERE id=?", (article_id,)
            ).fetchone()
            return row["merged_into"] if row else None

        return follow(article_id, merged_into)

    def _merges(self) -> Merges:
        return Merges({
            row["id"]: row["merged_into"]
            for row in self._conn.execute(
                "SELECT id, merged_into FROM articles WHERE merged_into IS NOT NULL"
            )
        })

    def current_id(self, article_id: str) -> str:
        """The article `article_id` is now: itself, or the one it was merged into."""
        return self._follow_merge(article_id)

    def ref(self, article_id: str) -> ArticleRef:
        return ArticleRef(id=article_id, identifier=self.identifier(article_id))

    def identifier(self, article_id: str) -> Identifier:
        rows = self._conn.execute(
            "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)
        ).fetchall()
        return _presented([(row["kind"], row["value"]) for row in rows])

    def identifiers(self, article_ids: Sequence[str]) -> Dict[str, Identifier]:
        """Batch form of `identifier`, one query per chunk instead of per article."""
        out: Dict[str, List[Tuple[str, str]]] = {aid: [] for aid in article_ids}
        for chunk in _chunks(article_ids, 900):
            marks = ",".join("?" * len(chunk))
            for row in self._conn.execute(
                f"SELECT article_id, kind, value FROM aliases WHERE article_id IN ({marks})",
                tuple(chunk),
            ):
                out[row["article_id"]].append((row["kind"], row["value"]))
        return {aid: _presented(aliases) for aid, aliases in out.items()}

    def aliases(self, article_id: str, kind: str) -> List[str]:
        """Every `kind` alias of the article, sorted; `identifier` presents only one."""
        rows = self._conn.execute(
            "SELECT value FROM aliases WHERE article_id=? AND kind=? ORDER BY value",
            (article_id, kind),
        )
        return [row["value"] for row in rows]

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

    def articles_naming_base_studies(
        self, base_study_ids: Sequence[str], stages: Sequence[str], skipped_for: str
    ) -> Dict[str, set]:
        """{base study id: the articles whose artifact at one of `stages` names it as
        `base_study_id`, when that artifact is ok or was skipped for `skipped_for`}. One
        query, whatever the number of ids."""
        wanted = json.dumps(sorted(set(base_study_ids)))
        rows = self._conn.execute(
            "SELECT json_extract(summary, '$.base_study_id') AS base, article_id "
            "FROM artifacts WHERE stage IN (SELECT value FROM json_each(?)) AND source = '' "
            "AND (status = 'ok' "
            "OR (status = 'skipped' AND json_extract(summary, '$.reason') = ?)) "
            "AND json_extract(summary, '$.base_study_id') IN (SELECT value FROM json_each(?))",
            (json.dumps(list(stages)), skipped_for, wanted),
        )
        out: Dict[str, set] = {}
        for row in rows:
            out.setdefault(row["base"], set()).add(row["article_id"])
        return out

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

    # -- searches ------------------------------------------------------------

    def record_search(
        self,
        query: str,
        years: str,
        rendered: Mapping[str, str],
        resolved: Mapping[str, Sequence[Optional[str]]],
    ) -> int:
        """Keep one search run: its query, years, each provider's query and hits.

        `resolved` is, per provider, the article each of its records is now (see
        `resolve_hits`), None for a record with no DOI, PMID or PMCID. The run's
        id is returned.
        """
        providers = {
            name: {
                "query": rendered[name],
                "records": len(ids),
                "without_id": sum(1 for a in ids if a is None),
            }
            for name, ids in resolved.items()
        }
        with self._write() as conn:
            search_id = conn.execute(
                "INSERT INTO searches(query, years, searched_at, providers) VALUES(?, ?, ?, ?)",
                (query, years, utcnow(), json.dumps(providers)),
            ).lastrowid
            conn.executemany(
                "INSERT OR IGNORE INTO search_hits(search_id, provider, article_id) "
                "VALUES(?, ?, ?)",
                [
                    (search_id, name, article_id)
                    for name, ids in resolved.items()
                    for article_id in set(ids) - {None}
                ],
            )
        return search_id

    def _search_hits(self, search_id: int) -> Dict[str, set]:
        """Per provider, the articles it found now: each hit followed through any merge.

        Read-time, because a process that predates these tables merges articles
        without moving their hits.
        """
        live = self._merges().live

        found: Dict[str, set] = {}
        for row in self._conn.execute(
            "SELECT provider, article_id FROM search_hits WHERE search_id=?", (search_id,)
        ):
            found.setdefault(row["provider"], set()).add(live(row["article_id"]))
        return found

    def searches(self) -> List[Dict[str, Any]]:
        """Every search run, oldest first, with each provider's query, records and articles."""
        runs = []
        for row in self._conn.execute("SELECT * FROM searches ORDER BY id").fetchall():
            providers = json.loads(row["providers"])
            found = self._search_hits(row["id"])
            for name, detail in providers.items():
                detail["articles"] = len(found.get(name, ()))
            runs.append(
                {
                    "id": row["id"],
                    "query": row["query"],
                    "years": row["years"],
                    "searched_at": row["searched_at"],
                    "providers": providers,
                }
            )
        return runs

    def search_articles(
        self,
        search_id: int,
        *,
        found_by: Sequence[str] = (),
        only: Optional[str] = None,
    ) -> List[str]:
        """The articles of a search run, oldest first.

        With `found_by`, those any provider named there found; with `only`,
        those that provider found and no other provider of the run did. Raises
        KeyError for a run or provider the catalog does not hold.
        """
        row = self._conn.execute(
            "SELECT providers FROM searches WHERE id=?", (search_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"no search {search_id}")
        searched = json.loads(row["providers"])
        for name in [*found_by, *([only] if only else [])]:
            if name not in searched:
                raise KeyError(f"search {search_id} did not search {name}")
        found = self._search_hits(search_id)
        if only:
            others = set().union(*(a for p, a in found.items() if p != only))
            chosen = found.get(only, set()) - others
        elif found_by:
            chosen = set().union(*(found.get(p, set()) for p in found_by))
        else:
            chosen = set().union(*found.values())
        ordered: List[Tuple[str, str]] = []
        for chunk in _chunks(sorted(chosen), 900):
            ordered += [
                (r["created_at"], r["id"])
                for r in self._conn.execute(
                    "SELECT id, created_at FROM articles "
                    f"WHERE id IN ({','.join('?' * len(chunk))})",
                    tuple(chunk),
                )
            ]
        return [article_id for _, article_id in sorted(ordered)]

    def exclude(self, rows: Sequence[Tuple[str, str, str, str]]) -> int:
        """Record (article_id, table_id, reason, note) verdicts. Idempotent:
        marking a table again replaces its reason and note. A retired article id is
        recorded on the article it was merged into, so no row lands where a merge
        has already passed."""
        now = utcnow()
        with self._write() as conn:
            conn.executemany(
                "INSERT INTO exclusions(article_id, table_id, reason, note, created_at) "
                "VALUES(?, ?, ?, ?, ?) ON CONFLICT(article_id, table_id) DO UPDATE SET "
                "reason=excluded.reason, note=excluded.note",
                [(self._follow_merge(a), str(t), r, n or "", now) for a, t, r, n in rows],
            )
        return len(rows)

    def unexclude(self, article_id: str, table_id: Optional[str] = None) -> int:
        """Withdraw a verdict, or every verdict on the article when no table is given.

        A retired id withdraws the verdicts of the article it became, and of any
        article merged into it.
        """
        merges = self._merges()
        target = merges.live(article_id)
        owners = [target] + merges.retired_into({target})
        marks = ",".join("?" * len(owners))
        with self._write() as conn:
            if table_id is None:
                cur = conn.execute(
                    f"DELETE FROM exclusions WHERE article_id IN ({marks})", owners
                )
            else:
                cur = conn.execute(
                    f"DELETE FROM exclusions WHERE table_id=? AND article_id IN ({marks})",
                    (str(table_id), *owners),
                )
        return cur.rowcount

    def exclusions(
        self, article_ids: Optional[Sequence[str]] = None
    ) -> Dict[str, Dict[str, Dict[str, str]]]:
        """article_id -> table_id -> {reason, note, created_at}; every article when none are given.

        Read through `articles.merged_into`, like search hits: a process that
        predates the move in `_merge` leaves its rows on the retired id. A table
        both sides excluded shows the survivor's own row.
        """
        merges = self._merges()
        live = merges.live
        if article_ids is None:
            rows = self._conn.execute(
                "SELECT article_id, table_id, reason, note, created_at FROM exclusions"
            ).fetchall()
        else:
            wanted = set(article_ids)
            sources = list(wanted) + [
                retired for retired in merges.retired_into(wanted) if retired not in wanted
            ]
            rows = []
            for chunk in _chunks(sources, 500):
                rows += self._conn.execute(
                    "SELECT article_id, table_id, reason, note, created_at FROM exclusions "
                    "WHERE article_id IN (%s)" % ",".join("?" * len(chunk)),
                    chunk,
                ).fetchall()
        out: Dict[str, Dict[str, Dict[str, str]]] = {}
        for row in sorted(rows, key=lambda r: r["article_id"] != live(r["article_id"])):
            out.setdefault(live(row["article_id"]), {}).setdefault(row["table_id"], {
                "reason": row["reason"], "note": row["note"], "created_at": row["created_at"],
            })
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
                WHERE NOT (artifacts.status = 'ok' AND excluded.status <> 'ok'
                           AND artifacts.stage IN ({keep}))
                """.format(keep=",".join("'%s'" % s for s in KEEPS_LAST_OK)),
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

    def empty_fingerprints(self) -> Dict[str, int]:
        """OK artifacts with no fingerprint, by stage, on articles no merge retired."""
        return {
            row["stage"]: int(row["n"])
            for row in self._conn.execute(
                """
                SELECT a.stage, COUNT(*) AS n FROM artifacts a
                JOIN articles ar ON ar.id = a.article_id
                WHERE ar.merged_into IS NULL AND a.status = 'ok' AND a.fingerprint = ''
                GROUP BY a.stage
                """
            )
        }

    def store_fingerprints(
        self, rewrites: Sequence[Tuple[Artifact, Artifact, Optional[Any]]]
    ) -> int:
        """Give each artifact the fingerprint and summary of its rewritten copy, and the
        payload given with it, if any, and nothing else: no attempt is recorded and
        `updated_at` stands. A row written since it was read is left alone. Returns how
        many rows were rewritten.

        One transaction, so the rewrites are stored together or not at all. A row left
        alone because a run wrote it meanwhile still has what was carried from it
        stored, chained on a fingerprint it never got: those run again."""
        rows = [
            (new.fingerprint, json.dumps(new.summary, separators=(",", ":")),
             self.blobs.put(payload) if payload is not None else new.blob,
             old.article_id, old.stage, old.source, old.fingerprint, old.blob, old.updated_at)
            for old, new, payload in rewrites
        ]
        with self._write() as conn:
            return conn.executemany(
                "UPDATE artifacts SET fingerprint=?, summary=?, blob=? WHERE article_id=? "
                "AND stage=? AND source=? AND fingerprint=? AND blob IS ? AND updated_at=?",
                rows,
            ).rowcount

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

    def stranded_exclusions(self) -> int:
        """Exclusions held by an article that a merge retired."""
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS n FROM exclusions e
            JOIN articles ar ON ar.id = e.article_id
            WHERE ar.merged_into IS NOT NULL
            """
        ).fetchone()
        return int(row["n"])

    def repair_merges(self) -> Repair:
        """Re-run the hand-over for merges that stranded artifacts or exclusions.

        What is stranded is read inside the write lock, so a merge committed
        meanwhile is seen.
        """
        moved_artifacts = set()
        moved_exclusions = 0
        dropped: List[Tuple[str, str, str, str]] = []
        with self._write() as conn:
            for row in conn.execute(
                """
                SELECT DISTINCT a.article_id AS loser, ar.merged_into AS survivor
                FROM artifacts a JOIN articles ar ON ar.id = a.article_id
                WHERE ar.merged_into IS NOT NULL
                """
            ).fetchall():
                self._move_artifacts(conn, row["loser"], self._follow_merge(row["survivor"]))
                moved_artifacts.add(row["loser"])
            for row in conn.execute(
                """
                SELECT DISTINCT e.article_id AS loser, ar.merged_into AS survivor
                FROM exclusions e JOIN articles ar ON ar.id = e.article_id
                WHERE ar.merged_into IS NOT NULL
                """
            ).fetchall():
                held = conn.execute(
                    "SELECT COUNT(*) FROM exclusions WHERE article_id=?", (row["loser"],)
                ).fetchone()[0]
                lost = self._move_exclusions(conn, row["loser"], self._follow_merge(row["survivor"]))
                moved_exclusions += held - len(lost)
                dropped += lost
        return Repair(len(moved_artifacts), moved_exclusions, dropped)

    def vacuum(self) -> None:
        self._conn.execute("VACUUM")


class Repair(NamedTuple):
    """What `Catalog.repair_merges` handed over."""

    artifact_articles: int
    exclusions_moved: int
    exclusions_dropped: List[Tuple[str, str, str, str]]


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
