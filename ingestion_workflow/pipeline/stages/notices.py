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
from ingestion_workflow.models.notices import openalex_retraction, retraction_of
from ingestion_workflow.utils.doi import normalize_doi

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

    def fingerprint_for(self, pmid: Optional[str], doi: Optional[str] = None) -> str:
        doi = (normalize_doi(doi) or "").lower()
        return fingerprint("notices", NOTICES_VERSION, str(pmid or ""), doi)

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
            pmid, doi = ref.identifier.pmid, ref.identifier.doi
            found = upstream.get(ref.id, {}).get("")
            if not (pmid or doi) or found is None or found.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(pmid, doi)
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
        pmids = [work.ref.identifier.pmid for work in works if work.ref.identifier.pmid]
        if not pmids:
            return {}, None
        try:
            return self.client.get_notices(pmids), None
        except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
            logger.warning("notices batch failed: %s", exc)
            return getattr(exc, "found", {}), exc

    def _openalex_retracted(self, works: List[Work]) -> Tuple[Dict[str, bool], Optional[Exception]]:
        dois = [work.ref.identifier.doi for work in works if work.ref.identifier.doi]
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
            doi = (normalize_doi(ident.doi) or "").lower()
            # Each source answers what it can: a failure is the paper's only
            # when the other source has nothing for it.
            from_pubmed = pmid in pubmed if pmid else False
            from_openalex = bool(doi) and doi in retracted
            if not (from_pubmed or from_openalex):
                errors = [
                    f"{name}: {type(exc).__name__}: {exc}"
                    for name, exc, asked in (
                        ("pubmed", pubmed_error, pmid),
                        ("openalex", openalex_error, doi),
                    )
                    if exc is not None and asked
                ]
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    "; ".join(errors) or "no source returned a record",
                    fingerprint=work.fingerprint,
                )
                continue
            corrections, is_notice = pubmed[pmid] if from_pubmed else ([], False)
            corrections = list(corrections)
            if retracted.get(doi) and retraction_of(corrections) is None:
                corrections.append(openalex_retraction())
            retraction = retraction_of(corrections)
            # OK only when every source asked has answered; otherwise the
            # answers received are kept but the paper is asked again next run.
            unanswered = [
                f"{name}: {type(exc).__name__}: {exc}"
                for name, exc, asked, got in (
                    ("pubmed", pubmed_error, pmid, from_pubmed),
                    ("openalex", openalex_error, doi, from_openalex),
                )
                if exc is not None and asked and not got
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
