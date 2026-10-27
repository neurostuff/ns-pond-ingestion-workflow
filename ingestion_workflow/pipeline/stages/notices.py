"""The retraction, erratum and other notices PubMed links to each paper.

OpenAlex's `is_retracted` is read as well, by DOI, for a paper with a DOI:
the only source for one with no PMID. A retraction from either sets
`retracted`, and each correction records its `source`.

Kept apart from `metadata` so that looking them up again touches nothing but
`upload`: triage hashes the metadata fingerprint, and through it every
analysis, space and sync row. One efetch per 200 PMIDs, never from a cache.
A notice can be issued years after the paper, so an answer older than
`notices_max_age_days` is asked again.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models.notices import PartialAnswer, openalex_retraction, retraction_of
from ingestion_workflow.utils.isolation import error_text

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when what is read from CommentsCorrections, or how it is named, changes.
NOTICES_VERSION = 2


def _age(artifact: Artifact) -> Optional[timedelta]:
    try:
        stamp = datetime.fromisoformat(artifact.updated_at)
    except (TypeError, ValueError):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - stamp


class NoticesStage:
    name = "notices"
    #: An article metadata reached; the PMID is all that is read.
    requires = "metadata"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._client = None
        self._openalex = None

    @property
    def client(self):
        if self._client is None:
            from ingestion_workflow.clients.pubmed import PubMedClient

            self._client = PubMedClient(
                email=self.settings.pubmed_email or "", api_key=self.settings.pubmed_api_key
            )
        return self._client

    @property
    def openalex(self):
        if self._openalex is None:
            from ingestion_workflow.clients.openalex import OpenAlexClient

            self._openalex = OpenAlexClient.from_settings(self.settings) or False
        return self._openalex or None

    def fingerprint_for(self, pmids: Optional[str], doi: Optional[str] = None) -> str:
        """`pmids`: every PMID of the paper, comma-joined."""
        return fingerprint("notices", NOTICES_VERSION, str(pmids or ""), doi or "")

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        max_age = timedelta(days=self.settings.notices_max_age_days)
        for ref in refs:
            pmids, doi = ref.identifier.pmids, ref.identifier.asking_doi
            found = upstream.get(ref.id, {}).get("")
            if not (pmids or doi) or found is None or found.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(",".join(pmids), doi)
            existing = artifacts.get(ref.id, {}).get("")
            age = _age(existing) if existing is not None else None
            if ctx.is_fresh(existing, fp) and age is not None and age < max_age:
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=None))
        return plan

    def _pubmed(self, works: List[Work]) -> Tuple[Dict, Optional[Exception]]:
        # A notice may be linked to any of the paper's PubMed records.
        pmids = [pmid for work in works for pmid in work.ref.identifier.pmids]
        if not pmids:
            return {}, None
        try:
            return self.client.get_notices(pmids), None
        except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
            logger.warning("notices batch failed: %s", exc)
            return getattr(exc, "found", {}), exc

    def _openalex_retracted(self, works: List[Work]) -> Tuple[Dict[str, bool], Optional[Exception]]:
        dois = [work.ref.identifier.asking_doi for work in works if work.ref.identifier.asking_doi]
        if not dois or self.openalex is None:
            return {}, None
        try:
            return self.openalex.get_retractions(dois), None
        except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
            logger.warning("openalex retraction batch failed: %s", exc)
            return getattr(exc, "found", {}), exc

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        pubmed, pubmed_error = self._pubmed(works)
        retracted, openalex_error = self._openalex_retracted(works)
        for work in works:
            ident = work.ref.identifier
            pmid = str(ident.pmid) if ident.pmid else None
            answered = [p for p in ident.pmids if p in pubmed]
            doi = ident.asking_doi or ""
            # Each source answers what it can: a failure is the paper's only
            # when the other source has nothing for it.
            from_pubmed = bool(answered)
            from_openalex = bool(doi) and doi in retracted
            if not (from_pubmed or from_openalex):
                errors = [
                    f"{name}: {why}"
                    for name, exc, asked in (
                        ("pubmed", pubmed_error, ident.pmids),
                        ("openalex", openalex_error, [doi] if doi else []),
                    )
                    if (why := _failed_for(exc, asked))
                ]
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    "; ".join(errors) or "no source returned a record",
                    fingerprint=work.fingerprint,
                )
                continue
            corrections, is_notice = [], False
            for record in answered:
                found, notice = pubmed[record]
                corrections += [c for c in found if c not in corrections]
                is_notice = is_notice or notice
            if retracted.get(doi) and retraction_of(corrections) is None:
                corrections.append(openalex_retraction())
            retraction = retraction_of(corrections)
            # OK only when every source asked has answered; otherwise the
            # answers received are kept but the paper is asked again next run.
            unanswered = [
                f"{name}: {why}"
                for name, exc, asked, got in (
                    ("pubmed", pubmed_error, ident.pmids, len(answered) == len(ident.pmids)),
                    ("openalex", openalex_error, [doi] if doi else [], from_openalex),
                )
                if not got and (why := _failed_for(exc, asked))
            ]
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.FAILED if unanswered else Status.OK,
                error="; ".join(unanswered)[:2000] or None,
                fingerprint=work.fingerprint,
                payload={
                    "pmid": pmid,
                    "doi": doi or None,
                    "corrections": corrections,
                    "retraction_notice": is_notice,
                },
                # Upload reads these two without the payload; `retraction` is
                # what neurostore keeps as `base_studies.retraction_notice`.
                summary={
                    "retracted": retraction is not None,
                    "retraction": retraction,
                    "retraction_notice": is_notice,
                    "corrections": len(corrections),
                },
            )


def _failed_for(exc: Optional[Exception], asked: Sequence[str]) -> Optional[str]:
    """Why a source's failure leaves the article asked by `asked`, or None when it does not:
    a request that failed leaves every article it did not answer, a record that could not
    be read only its own."""
    if exc is None or not asked:
        return None
    if isinstance(exc, PartialAnswer) and not exc.causes:
        return next((exc.unreadable[key] for key in asked if key in exc.unreadable), None)
    return error_text(exc)
