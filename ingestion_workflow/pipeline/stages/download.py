"""Fetch article content, trying each configured source until one succeeds."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.extractors.base import BaseExtractor, SourceExhausted, SourceMisconfigured
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifiers
from ingestion_workflow.services import download_check

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when a change to extractor behaviour should invalidate stored downloads.
DOWNLOAD_VERSION = 1


def build_extractor(source: DownloadSource, settings) -> BaseExtractor:
    from ingestion_workflow.extractors import (
        ACEExtractor,
        ElsevierExtractor,
        EuropePmcExtractor,
        PdfExtractor,
        PmcExtractor,
        PubgetExtractor,
        SpringerExtractor,
        WileyExtractor,
    )

    factories = {
        DownloadSource.PUBGET: PubgetExtractor,
        DownloadSource.ELSEVIER: ElsevierExtractor,
        DownloadSource.ACE: ACEExtractor,
        DownloadSource.PDF: PdfExtractor,
        DownloadSource.PMC: PmcExtractor,
        DownloadSource.EUROPEPMC: EuropePmcExtractor,
        DownloadSource.WILEY: WileyExtractor,
        DownloadSource.SPRINGER: SpringerExtractor,
    }
    return factories[source](settings=settings)


def supports(extractor: BaseExtractor, ref: ArticleRef) -> bool:
    """Whether this source can address the article with the ids we hold."""
    return extractor.can_address(ref.identifier)


class DownloadStage:
    name = "download"
    requires = None

    def __init__(self, settings) -> None:
        self.settings = settings
        self._order = [DownloadSource(name) for name in settings.download_sources]
        self._extractors: Dict[DownloadSource, BaseExtractor] = {}
        #: Why the stage stopped for the rest of this run.
        self._stopped: Optional[str] = None
        #: Each spent source's reason and the articles it left pending, for the run report.
        self._exhausted: Dict[str, List[object]] = {}

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
            candidate = self.next_source(ctx, ref, existing, attempts)
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

    def next_source(
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
            if ctx.should_attempt(artifact, count, last, self.name, source.value):
                return source
        return None

    # -- execution -----------------------------------------------------------

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Run the waterfall: each article falls through to the next source on failure."""
        if self._stopped:
            logger.error("download stopped: %s; %d article(s) left pending",
                         self._stopped, len(works))
            return
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
            if self._stopped:
                return

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
        from ingestion_workflow.extractors.pdf_extractor import (
            remembered_dead_urls,
            with_dead_urls_kept,
        )

        extractor = self.extractor(source)
        by_id = {ref.identifier.slug: ref for ref in refs}
        fp = self.fingerprint_for(source)
        previous = self._pdf_attempts(ctx, refs) if source is DownloadSource.PDF else {}
        try:
            identifiers = Identifiers([ref.identifier for ref in refs])
            if source is DownloadSource.PDF:
                dead = {
                    ref.identifier.slug: remembered_dead_urls(previous.get(ref.id, []))
                    for ref in refs
                }
                results = extractor.download(identifiers, dead_urls=dead)
            else:
                results = extractor.download(identifiers)
        except SourceExhausted as exc:
            # The articles it did not answer are not its failures: they stay
            # pending for it, and go on to the next source. The source itself
            # remembers that it is spent, and asks nothing more this run.
            results = exc.results
            answered = {result.identifier.slug for result in results}
            refs = [ref for ref in refs if ref.identifier.slug in answered]
            left = len(by_id) - len(refs)
            logger.error("download[%s] stopped for this run: %s; %d article(s) left pending",
                         source.value, exc, left)
            self._exhausted.setdefault(source.value, [str(exc), 0])[1] += left
        except SourceMisconfigured as exc:
            # Not these articles' failure: recording one would spend every remaining
            # article's tries. They stay pending, and so does every later batch.
            self._stopped = f"{source.value}: {exc}"
            logger.error("download stopped: %s; %d article(s) left pending",
                         self._stopped, len(refs))
            return
        except Exception as exc:  # a source outage must not lose the batch
            logger.warning("download[%s] batch failed: %s", source.value, exc)
            for ref in refs:
                yield Outcome.failure(
                    ref.id, self.name, source.value, f"{type(exc).__name__}: {exc}",
                    fingerprint=fp,
                    summary=_url_summary(with_dead_urls_kept(previous.get(ref.id, []), [])),
                )
            return

        seen = set()
        bad_pages: Dict[str, str] = {}
        for result in results:
            ref = by_id.get(result.identifier.slug)
            if ref is None:
                continue
            seen.add(ref.id)
            result.url_attempts = with_dead_urls_kept(
                previous.get(ref.id, []), result.url_attempts
            )
            outcome = self._outcome(ref, source, result, fp)
            yield outcome
            if outcome.summary.get("bad_page"):
                bad_pages[ref.id] = outcome.summary["bad_page"]
        yield from _fail_extractions(ctx, source.value, bad_pages)

        for ref in refs:
            if ref.id not in seen:
                yield Outcome.failure(
                    ref.id, self.name, source.value, "source returned no result",
                    fingerprint=fp,
                    summary=_url_summary(with_dead_urls_kept(previous.get(ref.id, []), [])),
                )

    def take_notes(self) -> List[str]:
        """What the run report says of the sources spent since it last asked."""
        notes = [f"{source} EXHAUSTED: {reason}; {left:,} article(s) left pending for it"
                 for source, (reason, left) in self._exhausted.items()]
        self._exhausted.clear()
        return notes

    def _pdf_attempts(
        self, ctx: Context, refs: List[ArticleRef]
    ) -> Dict[str, List[Dict[str, object]]]:
        """Each article's URL attempts in its latest PDF download, by article id.

        Every PDF outcome carries forward the URLs found dead before it, so
        these hold every dead URL the article ever had. A dead URL is never
        fetched again, `--refresh` included.
        """
        found = ctx.catalog.artifacts([ref.id for ref in refs], self.name)
        out: Dict[str, List[Dict[str, object]]] = {}
        for ref in refs:
            artifact = found.get(ref.id, {}).get(DownloadSource.PDF.value)
            attempts = (artifact.summary or {}).get("url_attempts") if artifact else None
            if attempts:
                out[ref.id] = list(attempts)
        return out

    def _outcome(
        self, ref: ArticleRef, source: DownloadSource, result: DownloadResult, fp: str
    ) -> Outcome:
        bad = download_check.check_download(result) if result.success else None
        if not result.success or bad is not None:
            message = bad.message if bad else (result.error_message or "download failed")
            reason = download_check.reason_of(message)
            # A PDF failure is never final: its dead URLs are remembered instead,
            # and a provider may list a new copy. A page that is not the article
            # may be a passing outage or a challenge, and its message can quote
            # "404" from the page itself.
            permanent = (
                source is not DownloadSource.PDF and reason is None and _is_permanent(message)
            )
            return Outcome.failure(
                ref.id,
                self.name,
                source.value,
                message,
                permanent=permanent,
                fingerprint=fp,
                summary={**({"bad_page": reason} if reason else {}),
                         **({"moved_aside": result.moved_aside} if result.moved_aside else {}),
                         **_url_summary(result.url_attempts)},
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
                **_url_summary(result.url_attempts),
                **({"article_version": result.article_version} if result.article_version else {}),
            },
        )


def _url_summary(url_attempts: List[Dict[str, object]]) -> Dict[str, object]:
    """Each URL tried or passed over, and a count per outcome for the funnel."""
    if not url_attempts:
        return {}
    outcomes: Dict[str, int] = {}
    for attempt in url_attempts:
        outcome = str(attempt.get("outcome"))
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    return {"url_attempts": url_attempts, "url_outcomes": outcomes}


def _fail_extractions(ctx: Context, source: str, bad_pages: Dict[str, str]) -> Iterator[Outcome]:
    """Fail the OK extraction each bad page was once extracted into, as revalidation does.

    Its fingerprint still matches (a download's names only its source), so left OK it
    would read as the article's text to anything not checking the download."""
    if not bad_pages:
        return
    from .extract import failed_validation, made_from

    found = ctx.catalog.artifacts(list(bad_pages), "extract")
    for article_id, reason in bad_pages.items():
        extraction = made_from(found.get(article_id, {}), source)
        if extraction is not None and extraction.status is Status.OK:
            yield failed_validation(extraction, reason)


#: Errors that will not resolve on a retry, so the article is marked permanent.
_PERMANENT_MARKERS = (
    "no pmcid",
    "not open access",
    "no full text",
    "404",
    "unsupported identifier",
    "does not allow full-text download",
    "query refused",
)


def _is_permanent(message: Optional[str]) -> bool:
    if not message:
        return False
    lowered = message.lower()
    return any(marker in lowered for marker in _PERMANENT_MARKERS)
