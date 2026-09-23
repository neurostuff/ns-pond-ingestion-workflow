"""Parse coordinate tables into analyses, via the LLM when heuristics can't."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.extractors.table_heuristics import looks_like_coordinate_table
from ingestion_workflow.models import ArticleExtractionBundle, ExtractedContent
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.prompts.coordinate_parsing import COORDINATE_PARSING_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)


class AnalysesStage:
    name = "analyses"
    requires = "extract"

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
        return fingerprint(
            "analyses",
            COORDINATE_PARSING_PROMPT_VERSION,
            self.settings.llm_model,
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
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            extraction = self._best_extraction(upstream.get(ref.id, {}))
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
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=extraction))
        return plan

    @staticmethod
    def _best_extraction(candidates: Dict[str, Artifact]) -> Artifact | None:
        """Prefer the source that actually found coordinate tables."""
        usable = [a for a in candidates.values() if a.status is Status.OK]
        if not usable:
            return None
        return max(usable, key=lambda a: a.summary.get("tables_with_coordinates", 0))

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs = []
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "extraction payload missing",
                    fingerprint=work.fingerprint,
                )
                continue
            content = ExtractedContent.from_dict(payload)
            content.identifier = work.ref.identifier
            content.slug = work.ref.identifier.slug
            tables = [t for t in content.tables if _worth_parsing(t)]
            if not tables:
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


def _worth_parsing(table) -> bool:
    """Deterministic parse found coordinates, or the table still looks like results."""
    if table.contains_coordinates or table.coordinates:
        return True
    return looks_like_coordinate_table(table)
