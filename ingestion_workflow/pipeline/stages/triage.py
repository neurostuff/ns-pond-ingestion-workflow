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

import logging
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import ExtractedContent

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when a change could give a different verdict for the same table: a new
#: reader, a refitted gate, a different threshold. Only triage goes stale.
TRIAGE_VERSION = 1


class TriageStage:
    name = "triage"
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._gate = None

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

    def fingerprint_for(self, upstream: Artifact) -> str:
        return fingerprint("triage", TRIAGE_VERSION, upstream=upstream.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            extraction = _most_tables(upstream.get(ref.id, {}))
            if extraction is None:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(extraction)
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
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "extraction payload missing",
                    fingerprint=work.fingerprint)
                continue
            content = ExtractedContent.from_dict(payload)
            verdicts = [self.judge(table) for table in content.tables]
            kept = [v for v in verdicts if v["passes"]]
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"tables": verdicts},
                summary={
                    "tables": len(verdicts),
                    "passed": len(kept),
                    "read_outright": sum(1 for v in verdicts if v["points"] >= 3),
                },
            )

    def judge(self, table) -> Dict:
        """One table's verdict, and enough of the reasoning to audit it."""
        text = _serialised(table)
        caption = table.caption or ""
        footer = table.footer or ""
        if not text.strip():
            return {"table_id": table.table_id, "passes": False, "points": 0,
                    "route": "unreadable", "score": 0.0,
                    "reason": "the table did not serialise"}
        from nspond_tables import read

        got = read.extract(text, caption=caption, footer=footer)
        decided = self.gate().decide(text, caption, footer)
        return {
            "table_id": table.table_id,
            "passes": bool(decided["passes"]),
            "points": len(got.points),
            "route": decided["route"],
            "score": round(float(decided["score"]), 4),
            "located_by": got.located_by,
            "space": got.space,
        }


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
    return max(usable, key=lambda a: a.summary.get("tables", 0))


def _serialised(table) -> str:
    """The table as one text form, whatever the publisher stored."""
    path = getattr(table, "raw_content_path", None)
    if not path:
        return ""
    from nspond_tables import serialize

    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return serialize.serialize(handle.read())
    except OSError:
        return ""
