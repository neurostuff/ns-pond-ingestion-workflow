"""Fill in the coordinate space the extractor could not name, from the article."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import CoordinateSpace
from ingestion_workflow.services.coordinate_space import read_space

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when `services.coordinate_space` reads differently.
SPACE_VERSION = 1

_UNKNOWN = (None, "", CoordinateSpace.OTHER.value)


class SpaceStage:
    """Rewrite `OTHER` tables with the space their article states.

    Its payload is the analyses payload, with only those tables changed, so
    upload and sync read it as they read `analyses`. An article with nothing
    to fill writes the same bytes, which the blob store keeps once.
    """

    name = "space"
    requires = "analyses"
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings
        self.requires, self.requires_flag = self.upstream_for(settings)

    @classmethod
    def upstream_for(cls, settings):
        """Read `resolve` when prose is on: it holds the tables' analyses and the prose's."""
        if getattr(settings, "prose_model", None):
            return "resolve", "tables"
        return cls.requires, cls.requires_flag

    def fingerprint_for(self, upstream: Artifact) -> str:
        # Where resolve added nothing from the prose, it names the analyses
        # artifact it passed through, so an article already read from its
        # tables -- and uploaded -- stays fresh when prose is switched on.
        basis = (upstream.summary or {}).get("basis") if upstream.stage == "resolve" else None
        return fingerprint("space", SPACE_VERSION, upstream=basis or upstream.fingerprint)

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
            analyses = upstream.get(ref.id, {}).get("")
            if analyses is None or analyses.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(analyses)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=analyses))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        ids = [work.article_id for work in works]
        triaged = ctx.catalog.artifacts(ids, "triage")
        extractions = ctx.catalog.artifacts(ids, "extract")
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", f"the {self.requires} payload is gone",
                    fingerprint=work.fingerprint,
                )
                continue
            try:
                text = (
                    _article_text(
                        triaged.get(work.article_id, {}).get(""),
                        extractions.get(work.article_id, {}),
                        ctx,
                    )
                    if _needs_filling(payload)
                    else None
                )
                filled, summary = fill_spaces(payload, text)
            except Exception as exc:
                logger.warning("space failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload=filled,
                summary=summary,
            )


def _needs_filling(payload: Dict[str, dict]) -> bool:
    return any(
        collection.get("coordinate_space") in _UNKNOWN and collection.get("analyses")
        for collection in payload.values()
    )


def _article_text(
    triage: Optional[Artifact], extractions: Dict[str, Artifact], ctx: Context
) -> Optional[str]:
    """The text of the extraction the analyses were read from.

    That is the one triage judged. Without it there is no text, and only the
    tables' own captions are read.
    """
    source = (triage.summary or {}).get("source") if triage else None
    extraction = extractions.get(source) if source is not None else None
    if extraction is None or extraction.status is not Status.OK:
        return None
    path = (ctx.payload(extraction) or {}).get("full_text_path")
    if not path or not Path(path).is_file():
        return None
    return Path(path).read_text(encoding="utf-8", errors="replace")


def fill_spaces(payload: Dict[str, dict], text: Optional[str]):
    """The payload with each unknown space read from the article, and a summary.

    A table with a space keeps it. A point keeps a space of its own, which the
    extractor read off the row.
    """
    filled: Dict[str, dict] = {}
    read: Dict[str, dict] = {}
    unknown = 0
    for table_id, collection in payload.items():
        if collection.get("coordinate_space") not in _UNKNOWN or not collection.get("analyses"):
            filled[table_id] = collection
            continue
        first = collection["analyses"][0]
        reading = read_space(
            text,
            caption=first.get("table_caption") or "",
            footer=first.get("table_footer") or "",
        )
        if reading is None:
            unknown += 1
            filled[table_id] = collection
            continue
        space = reading.space.value
        filled[table_id] = {
            **collection,
            "coordinate_space": space,
            "analyses": [
                {
                    **analysis,
                    "coordinates": [
                        {**point, "space": space} if point.get("space") in _UNKNOWN else point
                        for point in analysis.get("coordinates", [])
                    ],
                }
                for analysis in collection["analyses"]
            ],
        }
        read[table_id] = {
            "space": space,
            "where": reading.where,
            "rule": reading.rule,
            "evidence": reading.evidence,
        }
    summary = {
        "tables": sum(1 for c in payload.values() if c.get("analyses")),
        "read": read,
        "unknown": unknown,
    }
    return filled, summary
