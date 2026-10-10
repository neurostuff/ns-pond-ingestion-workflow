"""The retraction, erratum and other notices PubMed links to each paper.

Kept apart from `metadata` so that looking them up again touches nothing but
`upload`: triage hashes the metadata fingerprint, and through it every
analysis, space and sync row. One efetch per 200 PMIDs, never from a cache.
A notice can be issued years after the paper, so an answer older than
`notices_max_age_days` is asked again.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models.notices import retraction_of

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when what is read from CommentsCorrections, or how it is named, changes.
NOTICES_VERSION = 1


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

    @property
    def client(self):
        if self._client is None:
            from ingestion_workflow.clients.pubmed import PubMedClient

            self._client = PubMedClient(
                email=self.settings.pubmed_email or "", api_key=self.settings.pubmed_api_key
            )
        return self._client

    def fingerprint_for(self, pmid: str) -> str:
        return fingerprint("notices", NOTICES_VERSION, str(pmid))

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
            pmid = ref.identifier.pmid
            found = upstream.get(ref.id, {}).get("")
            if not pmid or found is None or found.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(pmid)
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

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        try:
            found = self.client.get_notices([work.ref.identifier.pmid for work in works])
        except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
            logger.warning("notices batch failed: %s", exc)
            for work in works:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    f"pubmed: {type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
            return
        for work in works:
            pmid = str(work.ref.identifier.pmid)
            if pmid not in found:
                yield Outcome.failure(
                    work.article_id,
                    self.name,
                    "",
                    "pubmed returned no record",
                    fingerprint=work.fingerprint,
                )
                continue
            corrections, is_notice = found[pmid]
            retraction = retraction_of(corrections)
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"pmid": pmid, "corrections": corrections, "retraction_notice": is_notice},
                # Upload reads these two without the payload; `retraction` is
                # what neurostore keeps as `base_studies.retraction_notice`.
                summary={
                    "retracted": retraction is not None,
                    "retraction": retraction,
                    "retraction_notice": is_notice,
                    "corrections": len(corrections),
                },
            )
