"""Parse coordinate tables into analyses, via the LLM when heuristics can't."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Dict, Iterator, List, Optional, Sequence, Set

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.extractors.table_heuristics import looks_like_coordinate_table
from ingestion_workflow.models import ArticleExtractionBundle, ExtractedContent
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.prompts.coordinate_parsing import COORDINATE_PARSING_PROMPT_VERSION

#: What reaches the model besides the prompt and the model itself: how the
#: table is serialised, and the document built around it. Neither is named
#: by the prompt version or the model, so without this a corpus extracted
#: before a serialiser fix looks fresh forever and keeps its old reading.
#: That is how 1,679 articles held a table the pipeline had dropped, and
#: 13,000 more kept cells that had been fused together.
#:
#: Bump it when the text sent to the model changes for reasons the model
#: and the prompt do not describe.
EXTRACTION_VERSION = "2026-10-01.serialised+minus+dedupe+thinspace+selfclosing"

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)


class AnalysesStage:
    name = "analyses"
    #: triage, not extract. Which tables are worth a call is triage's answer,
    #: so a refitted gate or a new reader has to make these analyses stale --
    #: and it only does if the fingerprint runs through triage.
    requires = "triage"

    #: Triage records how many tables it passed. An article it passed none for
    #: has no work here whatever else is true, so the selection drops it rather
    #: than planning it and writing an artifact that says nothing -- 477,625 of
    #: them in the first corpus run, duplicating what `triage` already recorded.
    requires_flag = "passed"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._shared_service = None
        self._service_lock = threading.Lock()

    def _service(self):
        """One service for the whole run, built on first use.

        The connection pool is the reason it is shared. Constructing the
        service builds an `OpenAI` client, and every client builds its own
        httpx pool, so one per article reuses no connection and pays a TLS
        handshake per article. The service holds no per-article state, so
        nothing is lost by sharing it.

        Locked and double-checked because the batch is run on a thread pool
        and the first calls arrive together: without it the first N workers
        each build a service and all but one is discarded, on the one batch
        where that cost is largest.
        """
        if self._shared_service is None:
            with self._service_lock:
                if self._shared_service is None:
                    from ingestion_workflow.services.create_analyses import (
                        CreateAnalysesService,
                    )

                    self._shared_service = CreateAnalysesService(self.settings)
        return self._shared_service

    def fingerprint_for(self, upstream: Artifact) -> str:
        """`upstream` is the triage artifact, whose own fingerprint runs
        through the extraction it judged. So the chain is extract -> triage ->
        analyses, and a change anywhere along it lands here."""
        return fingerprint(
            "analyses",
            COORDINATE_PARSING_PROMPT_VERSION,
            EXTRACTION_VERSION,
            self.settings.llm_model,
            # The prompt shape is an input too. Flipping this swaps a four
            # thousand token rule prompt for a fifty token schema, which is a
            # bigger change than most model swaps -- and without it here, a
            # deployment that flipped the flag while keeping the model name
            # would leave the whole corpus looking fresh.
            str(bool(getattr(self.settings, "llm_native_schema", False))),
            upstream=upstream.fingerprint,
        )

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
        # Waiting for metadata to have been attempted moved into `triage`,
        # which now requires it and is required in turn. The reason is
        # unchanged: the prompt carries the title and abstract, and an article
        # parsed without them caches that result.
        for ref in refs:
            triaged = upstream.get(ref.id, {}).get("")
            if triaged is None or triaged.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(triaged)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=triaged))
        return plan

    @staticmethod
    def _extraction_for(ctx: Context, article_id: str, source: str) -> Artifact | None:
        """The extraction triage judged, named in its payload.

        Not the one with the most coordinates, which is what this used to pick.
        Table ids are unique only within an extraction, so reading a different
        one would apply triage's verdicts to different tables.
        """
        found = ctx.catalog.artifacts([article_id], "extract").get(article_id, {})
        artifact = found.get(source)
        return artifact if artifact and artifact.status is Status.OK else None

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs = []
        for work in works:
            verdicts = ctx.payload(work.upstream) or {}
            passed = {v["table_id"] for v in verdicts.get("tables", []) if v.get("passes")}
            extraction = self._extraction_for(
                ctx, work.article_id, verdicts.get("source", ""))
            payload = ctx.payload(extraction)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "",
                    "the extraction triage judged is gone",
                    fingerprint=work.fingerprint,
                )
                continue
            content = ExtractedContent.from_dict(payload)
            content.identifier = work.ref.identifier
            content.slug = work.ref.identifier.slug
            tables = [t for t in content.tables if _worth_parsing(t, passed)]
            if not tables:
                # Triage named a table this extraction no longer has.
                yield Outcome(
                    article_id=work.article_id,
                    stage=self.name,
                    source="",
                    status=Status.SKIPPED,
                    fingerprint=work.fingerprint,
                    summary={"tables": 0, "reason": "no coordinate tables"},
                )
                continue
            jobs.append((work, replace(content, tables=tables)))

        if not jobs:
            return

        workers = max(1, self.settings.n_llm_workers)
        metadata = self._metadata_for(ctx, [work for work, _ in jobs])
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(self._run_one, work, content, metadata.get(work.article_id))
                for work, content in jobs
            ]
            for future in futures:
                yield future.result()

    def _metadata_for(self, ctx: Context, works: Sequence[Work]) -> Dict[str, ArticleMetadata]:
        found: Dict[str, ArticleMetadata] = {}
        artifacts = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        for work in works:
            payload = ctx.payload(artifacts.get(work.article_id, {}).get(""))
            if payload:
                found[work.article_id] = ArticleMetadata.from_dict(payload)
        return found

    def _run_one(self, work: Work, content: ExtractedContent, metadata) -> Outcome:
        bundle = ArticleExtractionBundle(
            article_data=content,
            article_metadata=metadata or ArticleMetadata(title=content.slug),
        )
        try:
            collections = self._service().run(bundle)
        except Exception as exc:
            logger.warning("analyses failed for %s: %s", work.article_id, exc)
            return Outcome.failure(
                work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                fingerprint=work.fingerprint,
            )
        coordinates = sum(
            len(analysis.coordinates)
            for collection in collections.values()
            for analysis in collection.analyses
        )
        return Outcome(
            article_id=work.article_id,
            stage=self.name,
            source="",
            status=Status.OK,
            fingerprint=work.fingerprint,
            payload={
                table_id: collection.to_dict() for table_id, collection in collections.items()
            },
            summary={"tables": len(collections), "coordinates": coordinates},
        )


def _worth_parsing(table, passed: Optional[Set[str]] = None) -> bool:
    """Whether this table earns a model call.

    `triage` decides it when it has run: it serialises the table, reads what it
    can, and puts the rest to a gate fitted on hand-adjudicated tables. That is
    a better answer than anything here, and it is recorded per table, so the
    set of ids it passed is all this needs.

    The fallback below is what ran before triage existed, kept for a catalog
    that has no triage artifact yet. It is a word search over the caption and
    the first forty lines, and it cannot tell an odds ratio beside its interval
    from a coordinate, which is most of what it lets through.
    """
    if passed is not None:
        return table.table_id in passed
    if table.contains_coordinates or table.coordinates:
        return True
    return looks_like_coordinate_table(table)
