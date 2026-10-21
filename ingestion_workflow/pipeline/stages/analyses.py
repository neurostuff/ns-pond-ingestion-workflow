"""Parse coordinate tables into analyses, via the LLM when heuristics can't."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
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
EXTRACTION_VERSION = "2026-10-01.serialised+minus+dedupe+thinspace+selfclosing+bysign"

#: This repo's own rules in the prompted path's prompt, which the vendored
#: COORDINATE_PARSING_PROMPT_VERSION does not cover. Only that path sends them,
#: so only its fingerprint carries this: bumping it must not make the
#: fine-tune's corpus stale, which never saw the prompt.
PROMPT_RULES_VERSION = "2026-10-09.no-placeholder+no-deactivation-seed"

from ..plan import StagePlan, Work
from ..stage import NO_DIGESTS, Context, metadata_fingerprint
from ..stage import metadata_read as read_fields

#: What this stage reads of the article's metadata: the title and abstract it sends to
#: the model. It reads the metadata only through `metadata_read`, and its fingerprint
#: holds these fields' digests, so a field it does not read changes nothing here.
METADATA_READ = ("title", "abstract")

#: Why a plan blocks an article whose triage records no `passed_tables`.
NO_VERDICT = "triage records no passed tables: run scripts/rekey_analyses_reads.py"


def metadata_read(payload: Optional[Dict]) -> Dict:
    return read_fields(payload, METADATA_READ)


logger = logging.getLogger(__name__)


class AnalysesStage:
    name = "analyses"
    #: triage, not extract. Which tables are worth a call is triage's answer,
    #: so a refitted gate or a new reader that passes other tables has to make
    #: these analyses stale -- and it only does if the fingerprint holds the verdict.
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

    def fingerprint_for(self, triage: Artifact, extraction: Optional[Artifact],
                        metadata: str) -> str:
        """What this stage reads: triage's verdict (the extraction it judged and the
        tables it passed, from its summary), that extraction, and the title and abstract
        (`metadata`, their digests' fingerprint). Not triage's whole fingerprint, which
        holds the publication types this stage never reads."""
        native = bool(getattr(self.settings, "llm_native_schema", False))
        summary = triage.summary or {}
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
            str(native),
            *(() if native else (PROMPT_RULES_VERSION,)),
            summary.get("source", ""),
            tuple(summary["passed_tables"]),
            extraction.fingerprint if extraction is not None else None,
            upstream=metadata,
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
        extractions = ctx.catalog.artifacts(ids, "extract")
        metadata = ctx.catalog.artifacts(ids, "metadata")
        # Waiting for metadata to have been attempted moved into `triage`,
        # which now requires it and is required in turn. The reason is
        # unchanged: the prompt carries the title and abstract, and an article
        # parsed without them caches that result.
        for ref in refs:
            triaged = upstream.get(ref.id, {}).get("")
            if triaged is None or triaged.status is not Status.OK:
                plan.blocked += 1
                continue
            if "passed_tables" not in (triaged.summary or {}):
                plan.block(NO_VERDICT)
                continue
            read = metadata_fingerprint(metadata.get(ref.id, {}).get(""), METADATA_READ)
            if read is None:
                plan.block(NO_DIGESTS)
                continue
            extraction = self._extraction_for(extractions.get(ref.id, {}), triaged)
            fp = self.fingerprint_for(triaged, extraction, read)
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
    def _extraction_for(found: Dict[str, Artifact], triage: Artifact) -> Artifact | None:
        """The extraction triage judged, named in its summary.

        Not the one with the most coordinates, which is what this used to pick.
        Table ids are unique only within an extraction, so reading a different
        one would apply triage's verdicts to different tables.
        """
        artifact = found.get((triage.summary or {}).get("source", ""))
        return artifact if artifact and artifact.status is Status.OK else None

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs = []
        extractions = ctx.catalog.artifacts([work.article_id for work in works], "extract")
        for work in works:
            # From triage's summary, as the fingerprint reads it, not its payload.
            passed = set(work.upstream.summary["passed_tables"])
            extraction = self._extraction_for(extractions.get(work.article_id, {}),
                                              work.upstream)
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
            tables = [t for t in content.tables if t.table_id in passed]
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
        """The title and abstract, read through `metadata_read`, whose fields the
        fingerprint holds."""
        found: Dict[str, ArticleMetadata] = {}
        artifacts = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        for work in works:
            payload = ctx.payload(artifacts.get(work.article_id, {}).get(""))
            if payload:
                read = metadata_read(payload)
                found[work.article_id] = ArticleMetadata(title=str(read["title"]),
                                                         abstract=read["abstract"])
        return found

    def _run_one(self, work: Work, content: ExtractedContent, metadata) -> Outcome:
        bundle = ArticleExtractionBundle(
            article_data=content,
            article_metadata=metadata or ArticleMetadata(title=content.slug),
        )
        readings: Dict[str, str] = {}
        unread: Dict[str, str] = {}
        try:
            collections = self._service().run(bundle, readings=readings, unread=unread)
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
            # `readings` covers every table read, the ones with no collection
            # included: a table with no coordinates is recorded here, as
            # `no_coordinates`, and never as an analysis with no points.
            summary={
                "tables": len(collections),
                "coordinates": coordinates,
                "readings": readings,
                **({"unread": unread} if unread else {}),
            },
        )
