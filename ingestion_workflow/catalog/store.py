"""The catalog: article identity, stage artifacts, and attempt history.

One sqlite database, one connection per process. Every "is this already done?"
question in the pipeline is answered here and nowhere else.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.preprints import preprint_server

from .backup import backup
from .blobs import BlobStore
from .evidence import Evidence, Unreadable
from .merges import Merges, follow
from .models import (
    ALIAS_KINDS,
    NO_SOURCE,
    RENAMED_KINDS,
    SECOND_KINDS,
    ArticleRef,
    Artifact,
    Outcome,
    Status,
    field_of,
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

#: The kinds a registered record anchors on, strongest first: the version of record's ids,
#: then the preprint's, then the base study, then any alternative.
ANCHORS = ("pmid", "pmcid", "doi", "preprint_pmid", "preprint_pmcid", "preprint_doi",
           "neurostore", "alternative_pmid", "alternative_pmcid", "alternative_doi")

#: The kinds that name a paper. An article left with none of them cannot be
#: asked about by any stage, whatever else it holds.
PAPER_KINDS = ("pmcid", "pmid", "doi", "preprint_doi")


class IdentityError(ValueError):
    """An identity edit whose precondition does not hold; nothing was written."""


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

    Each bucket is presented whole. Of two ids of a kind of the version of record's, which
    the invariants report, the lowest is presented, and the other is held but not presented:
    only a repair says whether it is the paper's alternative or another paper's.
    """
    held: Dict[str, List[str]] = {}
    for kind, value in aliases:
        held.setdefault(kind, []).append(value)
    for values in held.values():
        values.sort(key=lambda v: (len(v), v))
    return Identifier(**{
        field_of(kind): held.get(kind, []) if kind in SECOND_KINDS else (held.get(kind) or [None])[0]
        for kind in ALIAS_KINDS})


def _alias_pairs(identifier: Identifier) -> List[Tuple[str, str]]:
    pairs = []
    for kind in ALIAS_KINDS:
        held = getattr(identifier, field_of(kind), None)
        for value in ([held] if isinstance(held, str) else held or ()):
            pairs.append((kind, str(value)))
    return pairs


class Catalog:
    """Read/write access to one catalog database."""

    def __init__(self, connection: sqlite3.Connection, root: Path) -> None:
        self._conn = connection
        self.root = Path(root)
        self.blobs = BlobStore(self.root / "blobs")
        #: Each identifier registration refused since the catalog was opened, as
        #: (kind, value, article id or None, why), for the caller to report.
        self.refused: List[Tuple[str, str, Optional[str], str]] = []

    # -- lifecycle -----------------------------------------------------------

    @classmethod
    def open(cls, root: Path | str) -> "Catalog":
        """Open the catalog at `root`, creating it if need be."""
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
            if version < 3:
                self._canonical_doi_aliases(conn)
            if version < 4:
                self._bucket_aliases(conn)
            conn.execute(
                "UPDATE meta SET value=? WHERE key='schema_version'", (str(SCHEMA_VERSION),)
            )

    def _bucket_aliases(self, conn: sqlite3.Connection) -> Dict[str, int]:
        """Hold each id in the bucket it belongs to: the version of record's, a preprint's
        or an alternative.

        A DOI alias of a preprint server was a `doi` beside the journal's, and the one a
        later row presented; it is a `preprint_doi` now, and so is one held as the paper's other
        DOI, which a preprint's is not. The kinds that held a paper's other
        DOI and its duplicate PubMed and PMC records are the alternative kinds. No merge is
        needed: the new kinds start empty, so no (kind, value) is taken, and no article is
        given or loses an id, only a kind of it.
        """
        counts = {new: conn.execute("UPDATE aliases SET kind=? WHERE kind=?",
                                    (new, old)).rowcount
                  for old, new in RENAMED_KINDS.items()}
        servers: Dict[str, int] = {}
        for row in conn.execute("SELECT kind, value FROM aliases "
                                "WHERE kind IN ('doi', 'alternative_doi')").fetchall():
            server = preprint_server(row["value"])
            if server:
                conn.execute("UPDATE aliases SET kind='preprint_doi' WHERE kind=? AND value=?",
                             (row["kind"], row["value"]))
                servers[server] = servers.get(server, 0) + 1
        counts["preprint_doi"] = sum(servers.values())
        logger.warning("catalog: aliases held by bucket: %s; preprint DOIs by server: %s",
                       ", ".join(f"{n:,} as {kind}" for kind, n in counts.items()),
                       ", ".join(f"{server} {n:,}" for server, n in sorted(servers.items())))
        return counts

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
        """One write, all or nothing. Inside `transaction` it is a savepoint, so a
        write that fails leaves the writes before it in place."""
        if self._conn.in_transaction:
            self._conn.execute("SAVEPOINT write")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK TO write")
                self._conn.execute("RELEASE write")
                raise
            self._conn.execute("RELEASE write")
            return
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Hold the write lock for several writes, committed together or not at all.

        An exception out of the block rolls every write in it back. One caught
        inside the block undoes only the write it came from.
        """
        with self._write():
            yield

    # -- identity ------------------------------------------------------------

    def resolve(self, identifier: Identifier) -> Optional[ArticleRef]:
        """Find an existing article by any of its identifiers."""
        for kind, value in _alias_pairs(identifier):
            row = self._holder_row(self._conn, kind, value)
            if row:
                return self.ref(self._follow_merge(row["article_id"]))
        return None

    def holder(self, kind: str, value: str) -> Optional[str]:
        """The live article holding `value` as `kind`, or as a kind found by it."""
        return self._holder(self._conn, kind, value)

    def _holder(self, conn: sqlite3.Connection, kind: str, value: str) -> Optional[str]:
        row = self._holder_row(conn, kind, value)
        return self._follow_merge(row["article_id"]) if row else None

    @staticmethod
    def _holder_row(conn: sqlite3.Connection, kind: str, value: str) -> Optional[sqlite3.Row]:
        """The alias row holding `value` as `kind`, or as any other kind of the same id: a
        PMID held as a preprint's or an alternative beside the article's own is that
        article's either way."""
        base = SECOND_KINDS.get(kind, kind)
        kinds = (base, *(k for k, b in SECOND_KINDS.items() if b == base))
        marks = ",".join("?" * len(kinds))
        return conn.execute(
            f"SELECT article_id, kind FROM aliases WHERE kind IN ({marks}) AND value=?",
            (*kinds, value)).fetchone()

    #: Kinds an article may hold only one of. Two PMIDs on one article means two
    #: papers were fused; the same for two PMCIDs. DOIs are not on the list: a
    #: paper has a preprint's DOI beside its journal's. It has one journal DOI,
    #: though, which `_journal_dois` guards.
    SINGULAR = ("pmid", "pmcid")
    #: Kinds two articles may not both hold different values of and be merged: each is one
    #: paper's, and two neurostore ids are two base studies. One article may hold several
    #: preprint PMIDs and PMCIDs, one for each server that indexes the paper, but two
    #: articles holding different ones are two papers, whatever else joins them.
    ONE_EACH = SINGULAR + ("preprint_pmid", "preprint_pmcid", "neurostore")

    def register_many(
        self, identifiers: Sequence[Identifier], evidence: Evidence
    ) -> List[ArticleRef]:
        """Register articles, adding aliases to existing ones. Idempotent.

        `evidence` is what the ids are checked against: PubMed's record of a PMID and the
        registries' record of a DOI.

        Returns one ref per identifier that carries any id, in order.

        A known id is evidence that a record is an existing article, not proof.
        It used to be treated as proof: every article any of a record's ids
        pointed to was merged into one, and the record's other ids were moved
        onto it. One cache had stamped the same PMCID on about 1,300 different
        papers, and migrating it fused all of them into a single article that
        held 1,322 PMIDs. So now:

        * the record anchors on the article holding its PMID, else its PMCID,
          else a DOI, else the preprint's PMID, PMCID or DOI, else its neurostore id,
          else an alternative id (`ANCHORS`);
        * an anchor that holds a different PMID is not this paper: a new
          article is made instead;
        * other matched articles are merged in only if the result would still
          hold at most one of each of `ONE_EACH` and one journal DOI;
        * an id another article owns is never taken from it;
        * a second journal DOI is added only as an alternative DOI, when the
          registries give both DOIs one work (`evidence.same_work`);
        * a PMID that arrives with other ids is not added when its PubMed record
          gives a DOI another article holds, one the record's other ids do not
          name, and the registries give that DOI the record's paper, or cannot say.

        Each refused identifier is written to the `refusals` table, logged, and
        kept on `self.refused`.
        """
        if not identifiers:
            return []
        refs: List[str] = []
        now = utcnow()
        # Asked before the write lock is taken, so no writer waits on NCBI or a registry.
        pubmed = self._pubmed_evidence(identifiers, evidence)
        second_dois: List[Tuple[str, str, List[str]]] = []
        with self._write() as conn:
            for identifier in identifiers:
                pairs = _alias_pairs(identifier)
                if not pairs:
                    continue
                owners: Dict[Tuple[str, str], str] = {}
                held_second = set()
                for kind, value in pairs:
                    row = self._holder_row(conn, kind, value)
                    if row:
                        owners[(kind, value)] = self._follow_merge(row["article_id"])
                        if row["kind"] in SECOND_KINDS:
                            held_second.add((kind, value))
                pubmed_doi, why = pubmed.get(str(identifier.pmid), (None, None))
                if pubmed_doi:
                    holder = self._holder(conn, "doi", pubmed_doi)
                    if holder is not None and holder not in owners.values():
                        self.refuse("pmid", str(identifier.pmid), None, why, conn)
                        pairs = [p for p in pairs if p[0] != "pmid"]
                        # the PubMed record was asked for another record with this PMID
                        if not pairs:
                            continue
                # A preprint's PMID or PMCID is not the one its holder is judged by.
                incoming = {k: v for k, v in pairs
                            if k in self.SINGULAR and (k, v) not in held_second}

                anchor = None
                for kind in ANCHORS:
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
                    anchor = self._new_article(conn, seed, f"{now}:{len(refs)}", now)

                group = [anchor]
                for other in dict.fromkeys(owners.values()):
                    if other in group:
                        continue
                    if self._compatible(conn, group + [other], incoming):
                        group.append(other)
                    else:
                        self._conflict(identifier, f"not merging {other} into {anchor}: they "
                                       "would hold two of a PMID, PMCID, journal DOI or "
                                       "base study")
                article_id = self._merge(conn, group) if len(group) > 1 else anchor

                for kind, value in pairs:
                    owner = owners.get((kind, value))
                    owner = self._follow_merge(owner) if owner else None
                    if owner == article_id:
                        continue
                    if owner is not None:
                        self._conflict(identifier, f"{kind} {value} belongs to {owner}; left there")
                        continue
                    if kind == "doi":
                        held = sorted(self._journal_dois(conn, article_id)
                                      - {normalize_doi(value)})
                        if held:
                            second_dois.append((article_id, value, held))
                            continue
                    conn.execute(
                        "INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?) "
                        "ON CONFLICT(kind, value) DO NOTHING",
                        (kind, value, article_id),
                    )
                refs.append(article_id)
        if second_dois:
            self._settle_second_dois(second_dois, evidence)
        return [self.ref(article_id) for article_id in refs]

    #: Refusals since this catalog was opened, of an identifier or of a merge.
    conflicts: int = 0

    def _conflict(self, identifier: Identifier, why: str) -> None:
        self.conflicts += 1
        logger.warning("catalog: %s -- %s", identifier.slug, why)

    def refuse(self, kind: str, value: str, article_id: Optional[str], why: str,
               conn: Optional[sqlite3.Connection] = None) -> None:
        """Record an identifier left off an article, and why, in the `refusals` table, in the
        log and on `refused`; `conn` is the caller's open write transaction, if it has one."""
        if conn is None:
            with self._write() as own:
                self.refuse(kind, value, article_id, why, own)
            return
        self.conflicts += 1
        if (kind, value, article_id, why) not in self.refused:
            self.refused.append((kind, value, article_id, why))
        # The reason names the article, so a refusal made again differs only in its time.
        conn.execute("INSERT INTO refusals(kind, value, article_id, reason, refused_at) "
                     "VALUES(?, ?, ?, ?, ?) ON CONFLICT(kind, value, reason) DO UPDATE SET "
                     "refused_at=excluded.refused_at",
                     (kind, value, article_id, why, utcnow()))
        logger.warning("catalog: %s %s not added -- %s", kind, value, why)

    def _settle_second_dois(
        self, rows: Sequence[Tuple[str, str, List[str]]], evidence: Evidence
    ) -> None:
        """A journal DOI for an article holding another: an alternative DOI when the
        registries give both one work (a publisher's move, an alias spelling), else
        refused, with why."""
        decided = []
        for article_id, doi, held in rows:
            try:
                same = any(evidence.same_dois(doi, h) for h in held)
                why = "which the registries do not give the same work"
            except Unreadable as exc:
                same = False
                why = f"and the registry cannot say whether it is the same work: {exc}"
            decided.append((article_id, doi, held, same, why))
        with self._write() as conn:
            for article_id, doi, held, same, why in decided:
                holder = self._holder(conn, "doi", doi)
                if holder is not None:
                    if holder != article_id:
                        self.refuse("doi", doi, article_id,
                                    f"{article_id} holds the journal DOI {', '.join(held)}, "
                                    f"and {holder} took {doi} in the same batch", conn)
                    continue
                if same:
                    conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                                 ("alternative_doi", doi, article_id))
                else:
                    self.refuse("doi", doi, article_id,
                                f"{article_id} holds the journal DOI {', '.join(held)}, {why}",
                                conn)

    def _pubmed_evidence(
        self, identifiers: Sequence[Identifier], evidence: Evidence
    ) -> Dict[str, Tuple[str, str]]:
        """For each PMID a record would add beside its other ids, (the DOI, why) it is
        refused unless the record also names that DOI's article: the
        DOI its PubMed record gives is held by another article and the registries give
        it that record's paper, or the registry cannot say whether they do.

        A PMID already held is not added, and a PMID alone cannot attach to another
        paper's article, so neither is asked about.
        """
        asked = sorted({
            str(i.pmid) for i in identifiers
            if i.pmid and len(_alias_pairs(i)) > 1
            and not self._holder_row(self._conn, "pmid", str(i.pmid))
        })
        found = {}
        for pmid, summary in evidence.summaries(asked).items():
            doi = normalize_doi(summary.doi)
            holder = self._holder(self._conn, "doi", doi) if doi else None
            if holder is None:
                continue
            try:
                if evidence.same_paper(doi, summary):
                    found[pmid] = (doi, f"PubMed gives it {doi}, which {holder} holds")
            except Unreadable as exc:
                found[pmid] = (doi, f"PubMed gives it {doi}, which {holder} holds, and the "
                               f"registry cannot say whether that is its paper: {exc}")
        return found

    def _journal_dois(self, conn: sqlite3.Connection, article_id: str) -> Set[str]:
        """The article's version-of-record DOIs, canonical."""
        return {normalize_doi(d) for d in self._aliases(conn, article_id, "doi")}

    @staticmethod
    def _aliases(conn: sqlite3.Connection, article_id: str, kind: str) -> List[str]:
        # `+kind` keeps sqlite off the (kind, value) key, which it otherwise picks and then
        # scans every alias of the kind: 0.9 s a call on the production catalog.
        return [row[0] for row in conn.execute(
            "SELECT value FROM aliases WHERE article_id=? AND +kind=? ORDER BY value",
            (article_id, kind))]

    def _compatible(self, conn, article_ids: Sequence[str], incoming: Mapping[str, str]) -> bool:
        """Whether fusing these articles with the record would bring two papers together.

        Judged pairwise, by what each side holds: two sides clash on a kind when
        both hold values of it and neither holds all of the other's. The kinds
        are `ONE_EACH` and journal DOI; `incoming` holds the record's PMID and
        PMCID. An article that is already wrong -- one holding many PMIDs from
        before this guard -- is not made worse by a record that brings nothing
        new, so it still resolves; otherwise every lookup of it would make an
        empty new article.
        """
        kinds = self.ONE_EACH + ("doi",)
        sides = [{**{kind: set(self._aliases(conn, a, kind)) for kind in self.ONE_EACH},
                  "doi": self._journal_dois(conn, a)}
                 for a in article_ids]
        sides.append({kind: ({incoming[kind]} if incoming.get(kind) else set()) for kind in kinds})
        for kind in kinds:
            seen: set = set()
            for side in sides:
                values = side[kind]
                if values and seen and not (values <= seen or seen <= values):
                    return False
                seen |= values
        return True

    def register(self, identifier: Identifier, evidence: Evidence) -> ArticleRef:
        refs = self.register_many([identifier], evidence)
        if not refs:
            raise ValueError("Identifier carries no pmid, pmcid, doi or neurostore id")
        return refs[0]

    def add_aliases(self, pairs: Sequence[Tuple[str, str, str]]) -> None:
        """Attach identifiers, as (article, kind, value), to articles that exist.

        Used for ids that are only learned later -- a Neurostore base_study_id is
        not known until the upload stage has run -- and to put back what
        `remove_aliases` took. A second kind's value is held as given. A row
        without an article or a value, or of a kind the catalog does not hold,
        is skipped.
        """
        rows = []
        for article_id, kind, value in pairs:
            if not article_id or not value:
                continue
            if kind in SECOND_KINDS:
                rows.append((kind, str(value), article_id))
            elif kind in ALIAS_KINDS:
                rows += [(k, v, article_id) for k, v in _alias_pairs(Identifier(**{kind: value}))]
        if not rows:
            return
        with self._write() as conn:
            for kind, value, article_id in rows:
                row = self._holder_row(conn, SECOND_KINDS.get(kind, kind), value)
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

    def _new_article(
        self, conn: sqlite3.Connection, seed: Tuple[str, str], salt: str, now: str
    ) -> str:
        """Insert an article named from `seed`, its (kind, value) when first seen."""
        article_id = _article_id(f"{seed[0]}:{seed[1]}")
        if self._follow_merge(article_id) != article_id or conn.execute(
            "SELECT 1 FROM aliases WHERE article_id=? LIMIT 1", (article_id,)
        ).fetchone():
            # The seed's natural id is taken by another paper.
            article_id = _article_id(f"{seed[0]}:{seed[1]}:{salt}")
        conn.execute(
            "INSERT INTO articles(id, created_at) VALUES(?, ?) ON CONFLICT(id) DO NOTHING",
            (article_id, now),
        )
        return article_id

    # -- identity repair -----------------------------------------------------
    # Each edit first checks that the catalog is as its caller says, and raises
    # IdentityError, writing nothing, when it is not. Alias values are matched as
    # stored, not canonicalised: a repair names rows that exist, in whatever
    # spelling they were written.

    def live(self, article_id: str) -> bool:
        """Whether `article_id` is an article that has not been merged into another."""
        row = self._conn.execute(
            "SELECT merged_into FROM articles WHERE id=?", (article_id,)).fetchone()
        return row is not None and row["merged_into"] is None

    def _require_live(self, *article_ids: str) -> None:
        for article_id in article_ids:
            if not self.live(article_id):
                raise IdentityError(f"{article_id} is not a live article")

    @staticmethod
    def _require_held(conn: sqlite3.Connection, kind: str, value: str, article_id: str) -> None:
        row = conn.execute(
            "SELECT article_id FROM aliases WHERE kind=? AND value=?", (kind, value)).fetchone()
        if row is None or row["article_id"] != article_id:
            other = row or Catalog._holder_row(conn, kind, value)
            holder = (f"{other['article_id']}" + (f" as {other['kind']}" if not row else "")
                      if other else "no article")
            raise IdentityError(f"{kind} {value} is held by {holder}, not {article_id}")

    @staticmethod
    def _require_named(conn: sqlite3.Connection, article_id: str) -> None:
        marks = ",".join("?" * len(PAPER_KINDS))
        if not conn.execute(
            f"SELECT 1 FROM aliases WHERE article_id=? AND kind IN ({marks}) LIMIT 1",
            (article_id, *PAPER_KINDS),
        ).fetchone():
            raise IdentityError(f"{article_id} would be left with no {', '.join(PAPER_KINDS)}")

    @staticmethod
    def _require_artifacts(
        conn: sqlite3.Connection, article_id: str, keys: Sequence[Tuple[str, str]]
    ) -> None:
        if len(set(keys)) != len(keys):
            raise IdentityError(f"an artifact is named twice in {list(keys)}")
        for stage, source in keys:
            if not conn.execute(
                "SELECT 1 FROM artifacts WHERE article_id=? AND stage=? AND source=?",
                (article_id, stage, source),
            ).fetchone():
                raise IdentityError(f"{article_id} holds no {stage}/{source} artifact")

    @staticmethod
    def _hand_over(
        conn: sqlite3.Connection, src: str, dst: str, keys: Sequence[Tuple[str, str]]
    ) -> None:
        for stage, source in keys:
            for table in ("artifacts", "attempts"):
                conn.execute(
                    f"UPDATE {table} SET article_id=? WHERE article_id=? AND stage=? AND source=?",
                    (dst, src, stage, source),
                )

    def reassign_aliases(self, moves: Sequence[Tuple[str, str, str, str]]) -> None:
        """Move each alias, given as (kind, value, from, to), to another article.

        All moves are one write and each is checked against the catalog before
        any is made, so a chain or a swap of PMIDs is one call. Refused when an
        alias is not held by its `from`, is named twice, an article is not live,
        a `from` would be left with no pmid, pmcid or doi, a `to` with two of
        a PMID, PMCID, preprint PMID or preprint PMCID, or either with a journal
        DOI only as an alternate.
        """
        named = [(kind, value) for kind, value, _, _ in moves]
        if len(set(named)) != len(named):
            raise IdentityError("an alias is moved twice")
        with self._write() as conn:
            for kind, value, src, dst in moves:
                if src == dst:
                    raise IdentityError(f"{kind} {value}: from and to are both {src}")
                self._require_live(src, dst)
                self._require_held(conn, kind, value, src)
            for kind, value, src, dst in moves:
                conn.execute("UPDATE aliases SET article_id=? WHERE kind=? AND value=?",
                             (dst, kind, value))
                logger.warning("catalog: %s %s moved from %s to %s", kind, value, src, dst)
            for src in dict.fromkeys(src for _, _, src, _ in moves):
                self._require_named(conn, src)
            for article_id in dict.fromkeys(a for _, _, src, dst in moves for a in (src, dst)):
                self._require_journal_doi(conn, article_id)
            for dst in dict.fromkeys(dst for _, _, _, dst in moves):
                for kind in self.SINGULAR:
                    if len(self._aliases(conn, dst, kind)) > 1:
                        raise IdentityError(f"{dst} would hold two {kind}s: "
                                            f"{', '.join(self._aliases(conn, dst, kind))}")

    def remove_aliases(self, rows: Sequence[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
        """Detach each alias, given as (kind, value, article), from its article.

        Returns each removed alias as (article, kind, value), the shape
        `add_aliases` takes to put it back. Refused when an
        alias is not held by that article, is named twice, the article is not
        live, or it would be left with no pmid, pmcid or doi, or with a journal DOI
        only as an alternate.
        """
        named = [(kind, value) for kind, value, _ in rows]
        if len(set(named)) != len(named):
            raise IdentityError("an alias is removed twice")
        with self._write() as conn:
            for kind, value, article_id in rows:
                self._require_live(article_id)
                self._require_held(conn, kind, value, article_id)
            for kind, value, article_id in rows:
                conn.execute("DELETE FROM aliases WHERE kind=? AND value=?", (kind, value))
                logger.warning("catalog: %s %s removed from %s", kind, value, article_id)
            for article_id in dict.fromkeys(article_id for _, _, article_id in rows):
                self._require_named(conn, article_id)
                self._require_journal_doi(conn, article_id)
        return [(article_id, kind, value) for kind, value, article_id in rows]

    #: The kinds a kind may be held as instead: a Preprint-typed PubMed record's PMID and
    #: PMC's PMCID of it beside the version of record's, a DOI as an alternate DOI of the
    #: paper or as a preprint's.
    REKIND_TO = {"pmid": ("preprint_pmid",), "pmcid": ("preprint_pmcid",),
                 "doi": ("alternative_doi", "preprint_doi")}
    #: The kind a merge holds a duplicate record's id as.
    DROP = {"pmid": "alternative_pmid", "pmcid": "alternative_pmcid"}

    def rekind_aliases(self, rows: Sequence[Tuple[str, str, str, str]]) -> None:
        """Hold each alias, given as (kind, value, article, as_kind), as `as_kind`, one of
        `REKIND_TO[kind]`.

        Refused when an alias is not the article's, the kind is not held as `as_kind`, the
        article is not live, or it would be left with no pmid, pmcid or doi, with
        a journal DOI only as an alternate:
        the version of record's DOI is the one an article presents.
        """
        with self._write() as conn:
            for row in rows:
                self._rekind(conn, *row)
            for article_id in dict.fromkeys(row[2] for row in rows):
                self._require_named(conn, article_id)
                self._require_journal_doi(conn, article_id)

    def _rekind(self, conn: sqlite3.Connection, kind: str, value: str, article_id: str,
                as_kind: str) -> None:
        if as_kind not in self.REKIND_TO.get(kind, ()):
            raise IdentityError(f"a {kind} is not held as a {as_kind}")
        self._require_live(article_id)
        self._require_held(conn, kind, value, article_id)
        conn.execute("UPDATE aliases SET kind=? WHERE kind=? AND value=?", (as_kind, kind, value))
        logger.warning("catalog: %s %s of %s held as %s", kind, value, article_id, as_kind)

    def _require_journal_doi(self, conn: sqlite3.Connection, article_id: str) -> None:
        alternates = self._aliases(conn, article_id, "alternative_doi")
        if alternates and not self._journal_dois(conn, article_id):
            raise IdentityError(f"{article_id} would hold its journal DOI {', '.join(alternates)} "
                                "only as an alternate")

    def split(
        self,
        article_id: str,
        aliases: Sequence[Tuple[str, str]],
        artifact_keys: Sequence[Tuple[str, str]] = (),
    ) -> str:
        """Make a new article of another paper that `article_id` holds; return its id.

        The new article takes `aliases`, as (kind, value), and the artifacts
        named by (stage, source), with their attempts. Everything else stays.
        Refused when an alias or artifact is not the article's, the new article
        would hold no pmid, pmcid or doi, the old one would be left with none, or
        either would hold a journal DOI only as an alternate.
        """
        if len(set(aliases)) != len(aliases):
            raise IdentityError("an alias is named twice")
        if not any(kind in PAPER_KINDS for kind, _ in aliases):
            raise IdentityError(f"the new article would hold no {', '.join(PAPER_KINDS)}")
        now = utcnow()
        with self._write() as conn:
            self._require_live(article_id)
            for kind, value in aliases:
                self._require_held(conn, kind, value, article_id)
            self._require_artifacts(conn, article_id, artifact_keys)
            seed = min(aliases, key=lambda pair: ALIAS_KINDS.index(pair[0]))
            new_id = self._new_article(conn, seed, article_id, now)
            for kind, value in aliases:
                conn.execute("UPDATE aliases SET article_id=? WHERE kind=? AND value=?",
                             (new_id, kind, value))
            self._hand_over(conn, article_id, new_id, artifact_keys)
            # An article once registered by a split-off id and merged into this one
            # is that id's paper: it follows the id.
            for kind, value in aliases:
                seeded = _article_id(f"{kind}:{value}")
                if seeded not in (article_id, new_id) and self._follow_merge(seeded) == article_id:
                    conn.execute("UPDATE articles SET merged_into=? WHERE id=?", (new_id, seeded))
            self._require_named(conn, article_id)
            for held in (article_id, new_id):
                self._require_journal_doi(conn, held)
        logger.warning("catalog: %s split from %s with %s and artifacts %s",
                       new_id, article_id, list(aliases), list(artifact_keys))
        return new_id

    def move_artifacts(self, src: str, dst: str, keys: Sequence[Tuple[str, str]]) -> None:
        """Hand the artifacts named by (stage, source), and their attempts, to `dst`.

        Refused when `src` lacks one or `dst` already holds one: which of the two
        to keep is the caller's to say, by forgetting the other first.
        """
        if src == dst:
            raise IdentityError(f"from and to are both {src}")
        with self._write() as conn:
            self._require_live(src, dst)
            self._require_artifacts(conn, src, keys)
            for stage, source in keys:
                if conn.execute(
                    "SELECT 1 FROM artifacts WHERE article_id=? AND stage=? AND source=?",
                    (dst, stage, source),
                ).fetchone():
                    raise IdentityError(f"{dst} already holds a {stage}/{source} artifact")
            self._hand_over(conn, src, dst, keys)
        logger.warning("catalog: artifacts %s moved from %s to %s", list(keys), src, dst)

    def swap_artifacts(self, first: str, second: str, keys: Sequence[Tuple[str, str]]) -> None:
        """Exchange the artifacts named by (stage, source), and their attempts, between
        two articles: each holds the other's copy, as two ACE downloads fetched for each
        other's PMIDs. Refused when either lacks one."""
        if first == second:
            raise IdentityError(f"both articles are {first}")
        with self._write() as conn:
            self._require_live(first, second)
            self._require_artifacts(conn, first, keys)
            self._require_artifacts(conn, second, keys)
            held = f"swap:{first}"
            self._hand_over(conn, first, held, keys)
            self._hand_over(conn, second, first, keys)
            self._hand_over(conn, held, second, keys)
        logger.warning("catalog: artifacts %s swapped between %s and %s",
                       list(keys), first, second)

    def forget_artifacts(self, article_id: str, keys: Sequence[Tuple[str, str]]) -> List[Artifact]:
        """Delete the artifacts named by (stage, source), and their attempts.

        Their stages then plan them as never run. Blobs are kept, as blobs are
        shared by content. Returns the rows deleted. Refused when the article
        lacks one.
        """
        with self._write() as conn:
            self._require_live(article_id)
            self._require_artifacts(conn, article_id, keys)
            forgotten = []
            for stage, source in keys:
                forgotten.append(_artifact(conn.execute(
                    "SELECT * FROM artifacts WHERE article_id=? AND stage=? AND source=?",
                    (article_id, stage, source)).fetchone()))
                for table in ("artifacts", "attempts"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE article_id=? AND stage=? AND source=?",
                        (article_id, stage, source))
        logger.warning("catalog: artifacts %s of %s forgotten", list(keys), article_id)
        return forgotten

    def merge(
        self,
        article_ids: Sequence[str],
        drop: Sequence[Tuple[str, str]] = (),
        rekind: Sequence[Tuple[str, str, str]] = (),
    ) -> str:
        """Merge articles that are one paper into the oldest; return its id.

        Before the merge, each PMID or PMCID in `drop`, as (kind, value), is held as
        a duplicate record's, and each alias in `rekind`, as (kind, value, as_kind), as `as_kind`;
        each must be held by one of the articles. `drop` is for a second PubMed or
        PMC record of the paper, or of its preprint beside the one kept: it still
        finds the paper, so registering it again does not make another article.
        `rekind` is for what it holds as a preprint's: an article that is only a
        preprint's PubMed record. Refused when an article is not live, fewer than two
        are named, an alias is not theirs, or the result would hold two of a PMID,
        PMCID, preprint PMID, preprint PMCID, base study or journal DOI, or a journal
        DOI only as an alternate.
        """
        article_ids = list(dict.fromkeys(article_ids))
        if len(article_ids) < 2:
            raise IdentityError("a merge needs two articles")
        with self._write() as conn:
            self._require_live(*article_ids)
            for kind, value, *_ in list(drop) + list(rekind):
                row = conn.execute("SELECT article_id FROM aliases WHERE kind=? AND value=?",
                                   (kind, value)).fetchone()
                if row is None or row["article_id"] not in article_ids:
                    holder = row["article_id"] if row else "no article"
                    raise IdentityError(f"{kind} {value} is held by {holder}, not by the merged")
            for kind, value in drop:
                if kind not in self.DROP:
                    raise IdentityError(f"a {kind} is no duplicate record's")
                conn.execute("UPDATE aliases SET kind=? WHERE kind=? AND value=?",
                             (self.DROP[kind], kind, value))
                logger.warning("catalog: %s %s held as %s in merging %s", kind, value,
                               self.DROP[kind], article_ids)
            for kind, value, as_kind in rekind:
                holder = conn.execute("SELECT article_id FROM aliases WHERE kind=? AND value=?",
                                      (kind, value)).fetchone()["article_id"]
                self._rekind(conn, kind, value, holder, as_kind)
            if not self._compatible(conn, article_ids, {}):
                raise IdentityError(f"{', '.join(article_ids)} would hold two of a PMID, PMCID, "
                                    "preprint PMID or PMCID, base study or journal DOI")
            survivor = self._merge(conn, article_ids)
            self._require_journal_doi(conn, survivor)
        logger.warning("catalog: %s merged into %s", article_ids, survivor)
        return survivor

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
        return self._aliases(self._conn, article_id, kind)

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

    #: The stage under which `attempts` holds that a provider was asked for an article's ids.
    LOOKUP_STAGE = "id_lookup"

    def record_lookups(self, article_ids: Sequence[str], provider: str) -> None:
        """Note that `provider` was asked for each article's ids. It is an attempt, not an
        artifact: no stage is waiting on it, so no stage count shows it, and a merge or a
        split carries it with the article."""
        now = utcnow()
        with self._write() as conn:
            conn.executemany(
                "INSERT INTO attempts(article_id, stage, source, attempted_at, ok, error) "
                "VALUES (?, ?, ?, ?, 1, NULL)",
                [(article_id, self.LOOKUP_STAGE, provider, now) for article_id in article_ids])

    def was_looked_up(self, article_id: str, provider: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM attempts WHERE article_id=? AND stage=? AND source=? AND ok=1 LIMIT 1",
            (article_id, self.LOOKUP_STAGE, provider)).fetchone() is not None

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
