"""Fetch article content, trying each configured source until one succeeds."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifiers

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when a change to extractor behaviour should invalidate stored downloads.
DOWNLOAD_VERSION = 1


def build_extractor(source: DownloadSource, settings) -> BaseExtractor:
    from ingestion_workflow.extractors import (
        ACEExtractor,
        ElsevierExtractor,
        PdfExtractor,
        PubgetExtractor,
    )

    factories = {
        DownloadSource.PUBGET: PubgetExtractor,
        DownloadSource.ELSEVIER: ElsevierExtractor,
        DownloadSource.ACE: ACEExtractor,
        DownloadSource.PDF: PdfExtractor,
    }
    return factories[source](settings=settings)


def supports(extractor: BaseExtractor, ref: ArticleRef) -> bool:
    """Whether this source can address the article with the ids we hold."""
    fields = getattr(extractor, "_SUPPORTED_IDS", None)
    if not fields:
        return True
    return any(getattr(ref.identifier, str(field), None) for field in fields)


class DownloadStage:
    name = "download"
    requires = None

    def __init__(self, settings) -> None:
        self.settings = settings
        self._order = [DownloadSource(name) for name in settings.download_sources]
        self._extractors: Dict[DownloadSource, BaseExtractor] = {}

    def extractor(self, source: DownloadSource) -> BaseExtractor:
        if source not in self._extractors:
            self._extractors[source] = build_extractor(source, self.settings)
        return self._extractors[source]

    def fingerprint_for(self, source: DownloadSource) -> str:
        return fingerprint("download", source.value, DOWNLOAD_VERSION)

    # -- planning ------------------------------------------------------------

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = {
            source.value: ctx.catalog.attempt_counts(
                [ref.id for ref in refs], self.name, source.value
            )
            for source in self._order
        }
        for ref in refs:
            existing = artifacts.get(ref.id, {})
            if any(
                ctx.is_fresh(existing.get(source.value), self.fingerprint_for(source))
                for source in self._order
            ):
                plan.fresh += 1
                continue
            candidate = self._next_source(ctx, ref, existing, attempts)
            if candidate is None:
                if any(a.status is Status.PERMANENT for a in existing.values()):
                    plan.permanent += 1
                else:
                    plan.skipped += 1
                continue
            plan.pending.append(
                Work(ref=ref, source=candidate.value, fingerprint=self.fingerprint_for(candidate))
            )
        return plan

    def _next_source(
        self,
        ctx: Context,
        ref: ArticleRef,
        existing: Dict[str, Artifact],
        attempts: Dict[str, Dict[str, tuple]],
    ) -> Optional[DownloadSource]:
        """First source that can address this article and has tries left."""
        for source in self._order:
            if not supports(self.extractor(source), ref):
                continue
            artifact = existing.get(source.value)
            count, last = attempts[source.value].get(ref.id, (0, None))
            if ctx.should_attempt(artifact, count, last, self.name):
                return source
        return None

    # -- execution -----------------------------------------------------------

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Run the waterfall: each article falls through to the next source on failure."""
        remaining = {work.article_id: work.ref for work in works}
        preferred: Dict[str, str] = {work.article_id: work.source for work in works}

        for source in self._order:
            if not remaining:
                break
            batch = [
                ref
                for article_id, ref in remaining.items()
                if self._eligible(source, article_id, preferred, ref)
            ]
            if not batch:
                continue
            for outcome in self._download(ctx, source, batch):
                yield outcome
                if outcome.status is Status.OK:
                    remaining.pop(outcome.article_id, None)

    def _eligible(
        self, source: DownloadSource, article_id: str, preferred: Dict[str, str], ref: ArticleRef
    ) -> bool:
        """Only try sources at or after the one planning chose, and only if addressable."""
        order = [s.value for s in self._order]
        if order.index(source.value) < order.index(preferred[article_id]):
            return False
        return supports(self.extractor(source), ref)

    def _download(
        self, ctx: Context, source: DownloadSource, refs: List[ArticleRef]
    ) -> Iterator[Outcome]:
        extractor = self.extractor(source)
        by_id = {ref.identifier.slug: ref for ref in refs}
        fp = self.fingerprint_for(source)
        try:
            results = extractor.download(Identifiers([ref.identifier for ref in refs]))
        except Exception as exc:  # a source outage must not lose the batch
            logger.warning("download[%s] batch failed: %s", source.value, exc)
            for ref in refs:
                yield Outcome.failure(
                    ref.id, self.name, source.value, f"{type(exc).__name__}: {exc}", fingerprint=fp
                )
            return

        seen = set()
        for result in results:
            ref = by_id.get(result.identifier.slug)
            if ref is None:
                continue
            seen.add(ref.id)
            yield self._outcome(ref, source, result, fp)

        for ref in refs:
            if ref.id not in seen:
                yield Outcome.failure(
                    ref.id, self.name, source.value, "source returned no result", fingerprint=fp
                )

    def _outcome(
        self, ref: ArticleRef, source: DownloadSource, result: DownloadResult, fp: str
    ) -> Outcome:
        if not result.success:
            return Outcome.failure(
                ref.id,
                self.name,
                source.value,
                result.error_message or "download failed",
                permanent=_is_permanent(result.error_message),
                fingerprint=fp,
            )
        return Outcome(
            article_id=ref.id,
            stage=self.name,
            source=source.value,
            status=Status.OK,
            fingerprint=fp,
            payload=result.to_dict(),
            summary={
                "files": len(result.files),
                "types": sorted({f.file_type.value for f in result.files}),
            },
        )


#: Errors that will not resolve on a retry, so the article is marked permanent.
_PERMANENT_MARKERS = (
    "no pmcid",
    "not open access",
    "no full text",
    "404",
    "unsupported identifier",
    "no pdf url",
)


def _is_permanent(message: Optional[str]) -> bool:
    if not message:
        return False
    lowered = message.lower()
    return any(marker in lowered for marker in _PERMANENT_MARKERS)
