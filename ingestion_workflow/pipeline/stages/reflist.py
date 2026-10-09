"""Each paper's reference list as its publisher deposited it with Crossref.

The list `references` falls back on when the download marks none (a PDF), and where the
source's own entries borrow their DOIs and PMIDs from (an Elsevier list has DOIs on 16%
of entries). One request per paper by its DOI; OpenAlex then names the entries that are
a bare DOI.
"""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when what is kept from Crossref, or how it is named, changes.
REFLIST_VERSION = 1


class ReflistStage:
    name = "reflist"
    #: Only for articles something was downloaded for: the list serves their text.
    requires = "download"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._crossref = None
        self._openalex = None

    def clients(self):
        from ingestion_workflow.clients.crossref import CrossrefClient
        from ingestion_workflow.clients.openalex import OpenAlexClient

        if self._crossref is None:
            self._crossref = CrossrefClient(self.settings.crossref_email)
            self._openalex = OpenAlexClient.from_settings(self.settings)
        return self._crossref, self._openalex

    def fingerprint_for(self, doi: str) -> str:
        return fingerprint("reflist", REFLIST_VERSION, doi.lower())

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
            doi = ref.identifier.doi
            downloaded = any(a.status is Status.OK for a in upstream.get(ref.id, {}).values())
            if not doi or not downloaded:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(doi)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=None))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.reference_lists import enrich, from_crossref

        crossref, openalex = self.clients()
        for work in works:
            doi = work.ref.identifier.doi
            try:
                message = crossref.work(doi)
            except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
                yield Outcome.failure(work.article_id, self.name, "", f"crossref: {type(exc).__name__}: {exc}",
                                      fingerprint=work.fingerprint)
                continue
            references = from_crossref(message)
            named = 0
            if references and openalex is not None:
                try:
                    named = enrich(references, openalex)
                except Exception as exc:  # noqa: BLE001 - the list stands without the names
                    logger.warning("reflist: OpenAlex names failed for %s: %s", doi, exc)
            # No record, or a record with no list (its publisher deposits none), is a
            # fact about the paper, not a failure.
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"doi": doi, "provider": "crossref", "found": message is not None,
                         "publisher": (message or {}).get("publisher"), "references": references},
                summary={"found": message is not None, "references": len(references),
                         "with_doi": sum(bool(r["doi"]) for r in references), "named_by_openalex": named},
            )
