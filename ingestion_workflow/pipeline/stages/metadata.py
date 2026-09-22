"""Article metadata, fetched once per article rather than once per run.

Split out of `extract` because it is a per-article fact, not a per-source one:
re-extracting a PDF should not refetch the abstract, and a cached extraction
should not refetch anything at all.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import ExtractedContent
from ingestion_workflow.models.metadata import is_sufficient

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

METADATA_VERSION = 1

#: Kept in the catalog row for display and for the upload stage.
SUMMARY_FIELDS = ("title", "journal", "publication_year", "license")

class MetadataStage:
    name = "metadata"
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._service = None

    @property
    def service(self):
        if self._service is None:
            from ingestion_workflow.services.metadata import MetadataService

            self._service = MetadataService(self.settings)
        return self._service

    def fingerprint_for(self) -> str:
        return fingerprint("metadata", METADATA_VERSION, tuple(self.settings.metadata_providers))

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        fp = self.fingerprint_for()
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            extractions = [a for a in upstream.get(ref.id, {}).values() if a.status is Status.OK]
            if not extractions:
                plan.blocked += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(
                Work(ref=ref, source="", fingerprint=fp, upstream=extractions[0])
            )
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        contents: List[ExtractedContent] = []
        by_slug: Dict[str, Work] = {}
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
            contents.append(content)
            by_slug[content.slug] = work

        if not contents:
            return

        try:
            found = self.service.enrich_metadata(contents)
        except Exception as exc:
            logger.warning("metadata batch failed: %s", exc)
            for work in by_slug.values():
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
            return

        for slug, work in by_slug.items():
            metadata = found.get(slug)
            if metadata is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "no provider returned metadata",
                    fingerprint=work.fingerprint,
                )
                continue
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload=metadata.to_dict(),
                summary={
                    name: getattr(metadata, name, None) for name in SUMMARY_FIELDS
                } | {"authors": len(metadata.authors), "sufficient": is_sufficient(metadata)},
            )
