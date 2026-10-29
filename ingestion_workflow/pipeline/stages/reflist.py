"""Each paper's reference list as its publisher deposited it with Crossref.

The list `references` falls back on when the download marks none (a PDF), and where the
source's own entries borrow their DOIs and PMIDs from (an Elsevier list has DOIs on 16%
of entries). One request per paper by its DOI; OpenAlex then names the entries that are
a bare DOI.
"""

from __future__ import annotations

import copy
import logging
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Notes, Outcome, Status, fingerprint

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
        #: OpenAlex's answer for each cited DOI asked this run, None for one it does not
        #: know: papers cite many of the same works (each cited DOI 3.1 times across the
        #: first 18,500 lists, and more as lists are added), and every request is a credit.
        self._cited: Dict[str, Optional[Dict]] = {}

    def clients(self):
        from ingestion_workflow.clients.crossref import CrossrefClient
        from ingestion_workflow.clients.openalex import OpenAlexClient

        if self._crossref is None:
            self._crossref = CrossrefClient(self.settings.crossref_email)
            self._openalex = OpenAlexClient.from_settings(self.settings)
        return self._crossref, self._openalex

    def fingerprint_for(self, doi: str) -> str:
        return fingerprint("reflist", REFLIST_VERSION, doi)

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
            doi = ref.identifier.asking_doi
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
        from ingestion_workflow.services.reference_lists import cited, enrich, from_crossref
        from ingestion_workflow.utils.http import transient

        crossref, openalex = self.clients()
        asked = {work.article_id: work.ref.identifier.asking_doi for work in works}
        messages, failed = _records(crossref, list(dict.fromkeys(asked.values())))
        lists = {doi: from_crossref(message) for doi, message in messages.items()}
        # The batch's cited DOIs in one pool, less those asked earlier in the run.
        wanted = sorted({doi for refs in lists.values() for doi in cited(refs)
                         if doi not in self._cited})
        notes = Notes()
        if wanted and openalex is not None:
            try:
                found = openalex.get_cited_works(wanted)
            except Exception as exc:  # noqa: BLE001 - the lists stand without the names
                logger.warning("reflist: OpenAlex names failed for %d works: %s", len(wanted), exc)
                notes.error("names", f"openalex: {type(exc).__name__}: {exc}",
                            transient=transient(exc))
            else:
                self._cited.update({doi: found.get(doi) for doi in wanted})
        for work in works:
            doi = asked[work.article_id]
            if doi in failed:
                yield Outcome.failure(work.article_id, self.name, "", failed[doi],
                                      fingerprint=work.fingerprint)
                continue
            message = messages[doi]
            references = copy.deepcopy(lists[doi])
            named = enrich(references, self._cited)
            # No record, or a record with no list (its publisher deposits none), is a
            # fact about the paper, not a failure.
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"doi": doi, "provider": "crossref", "found": message is not None,
                         "publisher": (message or {}).get("publisher"), "references": references},
                summary={"found": message is not None, "references": len(references),
                         "with_doi": sum(bool(r["doi"]) for r in references),
                         "named_by_openalex": named}
                | (notes.summary() if any(d not in self._cited for d in cited(references))
                   else {}),
            )


#: What a list query keeps of each record: all `execute` reads.
LIST_FIELDS = "DOI,reference,publisher"


def _records(crossref, dois: List[str]) -> Tuple[Dict[str, Optional[Dict]], Dict[str, str]]:
    """Each DOI's Crossref record (None when Crossref does not know it), and why each DOI
    whose request failed has none. Many are asked by list queries; a DOI a filter cannot
    hold, or one the queries did not return, is asked for singly."""
    from ingestion_workflow.clients.crossref import filterable, packed

    found: Dict[str, Optional[Dict]] = {}
    failed: Dict[str, str] = {}
    for batch in packed("doi", [doi for doi in dois if filterable(doi)]):
        try:
            items = crossref.works(batch, LIST_FIELDS)
        except Exception as exc:  # noqa: BLE001 - the batch's articles are retried
            failed.update(dict.fromkeys(batch, f"crossref: {type(exc).__name__}: {exc}"))
            continue
        found.update({doi: items[doi.lower()] for doi in batch if doi.lower() in items})
    for doi in dois:
        if doi in found or doi in failed:
            continue
        try:
            found[doi] = crossref.work(doi)
        except Exception as exc:  # noqa: BLE001 - retried by the scheduler's backoff
            failed[doi] = f"crossref: {type(exc).__name__}: {exc}"
    return found, failed
