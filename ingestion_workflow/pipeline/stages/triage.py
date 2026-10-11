"""Decide which tables are worth an LLM call, before any are made.

`extract` keeps every table a paper has, which is the right thing for a stage
whose job is to not lose data: ACE used to yield only the tables its own parser
recognised, so a table it missed was gone for good and the article's
coordinates with it. The cost is that most of what `extract` now produces holds
no coordinates at all, and `analyses` would spend a model call on each one.

This stage is where that is sorted out, using `nspond_tables`:

* the serialiser turns each table's stored source -- html, CALS xml or csv --
  into one text form, so a pdf table and an elsevier table are judged the same
  way;
* the reader tries to read coordinates straight out of it, and succeeds on
  most tables that hold them;
* two gates decide the rest. Which one sees a table depends on whether the
  reader found anything, which needs no label and so works at inference. Where
  it found a triple, the question is whether the triple is real -- an odds
  ratio beside its interval reads as one -- and that gate runs for precision.
  Where it found nothing, the question is whether coordinates are there anyway,
  and that gate runs for recall, because a table dropped here is never looked
  at again.

The verdict is recorded per table rather than per article, so `analyses` can
send the tables that passed and skip the rest, and so a decision can be
inspected afterwards: the record says which gate made it and on what score.
"""

from __future__ import annotations

import hashlib

import logging
import multiprocessing
import re
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import ExtractedContent

from ..plan import StagePlan, Work
from ..stage import Context
from .extract import current_extractions

logger = logging.getLogger(__name__)

#: Bump when a change could give a different verdict for the same table: a new
#: reader, a refitted gate, a different threshold. Only triage goes stale.
TRIAGE_VERSION = 2

#: A meta-analysis reports coordinates taken from other papers. They are real
#: coordinates and its tables are judged like any other; the article is only
#: marked, so a later stage can decide what to do about foci that are already
#: in the corpus under the paper that found them.
#:
#: Marked, not dropped. The evidence is good but not certain: of 7,662 articles
#: this catches, 415 are caught by MeSH alone and at least one of those reads
#: like a PubMed indexing slip, while 6,516 are caught by the title alone.
#: Neither is a reason to discard an article's tables outright.
#:
#: MeSH first: PubMed indexes the type, it is on 99.9% of articles that have a
#: PubMed record, and it catches papers whose title never says so. The title
#: catches the 58% of articles with no publication type at all.
META_ANALYSIS_TYPES = frozenset({"meta-analysis", "systematic review"})
META_ANALYSIS_TITLE = re.compile(
    r"meta[-\s]?analy[sz]|\bALE\b|activation\s+likelihood\s+estimation", re.I)


def publication_types(metadata: Dict) -> List[str]:
    """MeSH publication types, from wherever PubMed's raw record put them.

    `xmltodict` gives a single type as a dict and several as a list, and each
    is `{"#text": ..., "@UI": ...}`, so both shapes have to be unwrapped.
    """
    article = (((metadata.get("raw_metadata") or {}).get("pubmed") or {})
               .get("MedlineCitation") or {}).get("Article") or {}
    listed = (article.get("PublicationTypeList") or {}).get("PublicationType")
    if listed is None:
        return []
    if not isinstance(listed, list):
        listed = [listed]
    out = []
    for item in listed:
        text = item.get("#text") if isinstance(item, dict) else item
        if isinstance(text, str):
            out.append(text)
    return out


def is_a_meta_analysis(title: str, types: Sequence[str]) -> bool:
    """Whether this article collects other papers' coordinates."""
    if any(t.strip().lower() in META_ANALYSIS_TYPES for t in types):
        return True
    return bool(META_ANALYSIS_TITLE.search(title or ""))


class TriageStage:
    name = "triage"
    #: metadata, so the abstract is available: a paper often names its
    #: coordinate space in the abstract and nowhere in the table, and
    #: `read.extract` reads it from there when the table and caption are
    #: silent. Attempted is the bar, not succeeded -- see `plan`.
    requires = "metadata"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._gate = None
        self._gate_id = None

    def gate(self):
        """The fitted pair, loaded once.

        Imported here rather than at module scope so the rest of the pipeline
        still imports when the package is absent, which matters while it is
        installed from a git URL.
        """
        if self._gate is None:
            from nspond_tables.classify import RoutedGate

            path = getattr(self.settings, "coordinate_gate_path", None)
            if not path:
                raise RuntimeError(
                    "triage needs coordinate_gate_path, the fitted RoutedGate")
            self._gate = RoutedGate.load(path)
        return self._gate

    def gate_id(self) -> str:
        """What identifies the gate this stage is judging with.

        The digest of the fitted file. A refitted gate is a different gate
        even at the same path, and the path alone would not say so.
        """
        if self._gate_id is None:
            path = getattr(self.settings, "coordinate_gate_path", None)
            if not path:
                raise RuntimeError(
                    "triage needs coordinate_gate_path, the fitted RoutedGate")
            digest = hashlib.blake2b(digest_size=16)
            with open(path, "rb") as fh:
                for block in iter(lambda: fh.read(1 << 20), b""):
                    digest.update(block)
            self._gate_id = digest.hexdigest()
        return self._gate_id

    def fingerprint_for(self, metadata: Artifact, extraction: Artifact) -> str:
        """Both parents, because `requires` only names one -- and the gate.

        `metadata`'s own fingerprint does not run through `extract` -- it is
        the provider list and a version, nothing more -- so fingerprinting from
        it alone would leave triage looking fresh after a re-extraction, with
        verdicts about tables that no longer exist. The extraction's
        fingerprint therefore goes in as a part.

        **So does the gate.** It is the stage's other input: change it and
        every verdict changes, while both parents sit still. Without it,
        pointing the config at a refitted gate leaves the whole corpus looking
        fresh and the run does nothing, silently -- which is exactly the
        failure `analyses` already avoids by hanging off `triage` rather than
        off `extract`.
        """
        return fingerprint("triage", TRIAGE_VERSION, extraction.fingerprint,
                           self.gate_id(), upstream=metadata.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        extractions = ctx.catalog.artifacts(ids, "extract")
        downloads = ctx.catalog.artifacts(ids, "download")
        for ref in refs:
            # Attempted is the bar, not succeeded. Plenty of articles have no
            # metadata to find, and blocking those would strand them here
            # forever; the abstract is a help when it exists, not a condition.
            meta = upstream.get(ref.id, {}).get("")
            found = extractions.get(ref.id, {})
            extraction = judged_extraction(ctx, found, downloads.get(ref.id, {}))
            if meta is None or extraction is None:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(meta, extraction)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(
                Work(ref=ref, source="", fingerprint=fp, upstream=extraction))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs, meta_of, tables_of, skipped_of = [], {}, {}, {}
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "extraction payload missing",
                    fingerprint=work.fingerprint)
                continue
            content = ExtractedContent.from_dict(payload)
            abstract, title, types = self._context(ctx, work.article_id)
            meta_of[work.article_id] = (is_a_meta_analysis(title, types), types)
            tables_of[work.article_id] = len(content.tables)
            found = ctx.catalog.artifacts([work.article_id], "extract").get(work.article_id, {})
            downloads = ctx.catalog.artifacts([work.article_id], "download").get(work.article_id, {})
            skipped_of[work.article_id] = skipped_for_text(
                current_extractions(ctx, found, downloads) or found, work.upstream)
            jobs.append((work.article_id, abstract,
                         [_as_dict(t) for t in content.tables]))
        if not jobs:
            return

        by_article = dict(self._judge_all(jobs))
        for work in works:
            verdicts = by_article.get(work.article_id)
            if verdicts is None:
                continue
            meta, types = meta_of[work.article_id]
            kept = [v for v in verdicts if v["passes"]]
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": work.upstream.source,
                         "is_meta_analysis": meta,
                         "publication_types": types,
                         "tables": verdicts},
                summary={
                    "source": work.upstream.source,
                    "tables": len(verdicts),
                    "passed": len(kept),
                    "read_outright": sum(1 for v in verdicts if v["points"] >= 3),
                    "is_meta_analysis": meta,
                    **({"skipped_no_text": skipped_of[work.article_id]}
                       if skipped_of.get(work.article_id) else {}),
                },
            )

    def _context(self, ctx: Context, article_id: str) -> Tuple[str, str, List[str]]:
        """Abstract, title and publication types, or empty when there is none.

        The abstract is for `read.extract`: a paper often names its coordinate
        space there and nowhere in the table. The title and the types are for
        deciding whether the article is a meta-analysis.
        """
        found = ctx.catalog.artifacts([article_id], "metadata").get(article_id, {})
        payload = ctx.payload(found.get("")) or {}
        return (payload.get("abstract") or "", payload.get("title") or "",
                publication_types(payload))

    def _judge_all(self, jobs):
        """Every article's tables, across a pool when the batch earns one.

        The work is parsing and a forest, so it is processor-bound and threads
        would queue behind the interpreter lock. The gate loads once per worker
        rather than travelling with each task: it is 400 trees and over a
        megabyte, which costs more to pickle than the judging costs to do.

        A batch of one or two does not earn a pool -- starting the workers and
        loading a gate in each costs more than judging them here.
        """
        workers = max(1, getattr(self.settings, "max_workers", 1) or 1)
        path = getattr(self.settings, "coordinate_gate_path", None)
        if workers == 1 or len(jobs) < 4 or not path:
            gate = self.gate()
            for article_id, abstract, tables in jobs:
                yield article_id, [judge_table(gate, t, abstract) for t in tables]
            return
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_load_gate,
            initargs=(str(path),),
        ) as pool:
            for article_id, verdicts in pool.map(_judge_article, jobs, chunksize=8):
                yield article_id, verdicts

    def judge(self, table, abstract: str = "") -> Dict:
        """One table's verdict, for a caller holding an `ExtractedTable`."""
        return judge_table(self.gate(), _as_dict(table), abstract)


#: One gate per worker process, loaded once. A fitted forest is 400 trees and
#: over a megabyte; pickling it with every task would cost more than the work.
_GATE = None


def _load_gate(path: str) -> None:
    global _GATE
    from nspond_tables.classify import RoutedGate

    _GATE = RoutedGate.load(path)


def _judge_article(job: Tuple[str, str, List[Dict]]) -> Tuple[str, List[Dict]]:
    """One article's tables, in a worker.

    Failures are caught per table rather than per batch: one unreadable table
    must not take the other few thousand articles with it.
    """
    article_id, abstract, tables = job
    out = []
    for table in tables:
        try:
            out.append(judge_table(_GATE, table, abstract))
        except Exception as exc:  # noqa: BLE001 - one table must not lose the rest
            out.append({"table_id": table.get("table_id"), "passes": False,
                        "points": 0, "route": "error", "score": 0.0,
                        "reason": "%s: %s" % (type(exc).__name__, exc)})
    return article_id, out


def judge_table(gate, table: Dict, abstract: str = "") -> Dict:
    """One table's verdict, and enough of the reasoning to audit it.

    Takes a plain dict rather than an `ExtractedTable`, so the same call works
    in a worker process without the model layer having to be picklable.
    """
    text = _serialised(table.get("raw_content_path"))
    caption = table.get("caption") or ""
    footer = table.get("footer") or ""
    if not text.strip():
        return {"table_id": table.get("table_id"), "passes": False, "points": 0,
                "route": "unreadable", "score": 0.0,
                "reason": "the table did not serialise"}
    from nspond_tables import read

    got = read.extract(text, caption=caption, footer=footer, abstract=abstract)
    decided = gate.decide(text, caption, footer)
    return {
        "table_id": table.get("table_id"),
        "passes": bool(decided["passes"]),
        "points": len(got.points),
        "route": decided["route"],
        "score": round(float(decided["score"]), 4),
        "located_by": got.located_by,
        "space": got.space,
    }


def _as_dict(table) -> Dict:
    return {"table_id": getattr(table, "table_id", None),
            "raw_content_path": getattr(table, "raw_content_path", None),
            "caption": getattr(table, "caption", "") or "",
            "footer": getattr(table, "footer", "") or ""}

def judged_extraction(ctx, found: Dict[str, Artifact], downloads: Dict[str, Artifact]) -> Optional[Artifact]:
    """The extraction triage judges, whose text sync writes and passages reads."""
    return _most_tables(current_extractions(ctx, found, downloads) or found)


def skipped_for_text(candidates: Dict[str, Artifact], chosen: Artifact) -> List[Dict]:
    """The no-text extractions with tables that `_most_tables` passed over, by source.

    Their tables are not judged, so triage records which source it left and how
    many tables it held. Nothing is skipped when the chosen extraction has no text
    either, or when the no-text ones have no tables.
    """
    if not chosen.summary.get("has_text", True):
        return []
    return [
        {"source": a.source, "tables": a.summary.get("tables", 0)}
        for a in candidates.values()
        if a.status is Status.OK
        and a is not chosen
        and not a.summary.get("has_text", True)
        and a.summary.get("tables", 0)
    ]


def _most_tables(candidates: Dict[str, Artifact]) -> Optional[Artifact]:
    """The source that produced the most tables, coordinates or not.

    `analyses` picks the source with the most tables *with coordinates*, which
    is the old filter wearing a different hat: it prefers whichever extractor
    guessed most eagerly. Triage has not judged anything yet, so it takes the
    source that kept the most to judge.
    """
    usable = [a for a in candidates.values() if a.status is Status.OK]
    if not usable:
        return None
    # An extraction with no text is chosen only when none has any: the article's
    # one text is what sync writes and passages index.
    usable = [a for a in usable if a.summary.get("has_text", True)] or usable
    # ACE only when nothing else found a table. Its extra tables are copies
    # and uncaptioned fragments: of 111 articles also extracted from a shared
    # PDF, ACE had more tables in 31, and each one checked was a duplicate.
    # On 2026-10-06 no article had ACE beside another source with tables, so
    # this changed no verdict already made.
    preferred = [a for a in usable if a.source != "ace" and a.summary.get("tables", 0)]
    return min(preferred or usable, key=lambda a: (-a.summary.get("tables", 0), _rank(a.source), a.source))


#: A tie on tables goes to the first of these (a source not listed comes after them, by
#: name): publisher XML before ACE's page, a PDF's conversion last.
TIE_ORDER = ("pubget", "pmc", "europepmc", "elsevier", "ace", "pdf")


def _rank(source: str) -> int:
    return TIE_ORDER.index(source) if source in TIE_ORDER else len(TIE_ORDER)


def _serialised(path) -> str:
    """The table as one text form, whatever the publisher stored."""
    if not path:
        return ""
    from nspond_tables import serialize

    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return serialize.serialize(handle.read())
    except OSError:
        return ""
