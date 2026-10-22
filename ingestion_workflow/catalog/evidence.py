"""What NCBI and the DOI registries say of an article's identifiers.

The catalog's identity checks (registration, the invariants, a repair plan) read it
and nothing else. Each answer is asked once and kept for the life of the object.

A DOI is evidence of which paper a PubMed record is only when the registering agency
gives it that paper's title: PubMed sometimes gives a record another paper's DOI
(10.14670/HH-25-013 is a parotid tumour study, and PubMed gives it to an ocular
lymphoma review too), or a DOI with a typo that no agency has.
"""

from __future__ import annotations

import difflib
import html
import re
from typing import Dict, Iterable, List, Optional, Sequence

from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PmcRecord, PubMedClient, PubMedSummary
from ingestion_workflow.utils.doi import normalize_doi

#: How alike two titles must be to name one paper. Publishers and PubMed differ in
#: case, punctuation and the odd word, not in more.
TITLE_LIKENESS = 0.8

#: Crossref relations joining a preprint and its version of record.
_VERSION_OF = ("is-preprint-of", "has-preprint")
#: The version number a publisher appends to a version's DOI.
_VERSIONED = re.compile(r"\.\d{1,2}$")


def _words(title: Optional[str]) -> str:
    text = re.sub(r"<[^>]+>", " ", html.unescape(title or "")).lower()
    return " ".join(re.findall(r"[^\W_]+", text))


def same_title(a: Optional[str], b: Optional[str]) -> bool:
    """Whether two titles name one paper: alike, or one the other with a subtitle."""
    a, b = _words(a), _words(b)
    if not a or not b:
        return False
    return (a.startswith(b) or b.startswith(a)
            or difflib.SequenceMatcher(None, a, b).ratio() >= TITLE_LIKENESS)


class Evidence:
    def __init__(self, pubmed: PubMedClient, registry: DoiRegistry) -> None:
        self._pubmed = pubmed
        self._registry = registry
        self._summaries: Dict[str, Optional[PubMedSummary]] = {}
        self._pmc: Dict[str, Optional[PmcRecord]] = {}
        self._titles: Dict[str, Optional[str]] = {}
        self._versions: Dict[str, List[str]] = {}
        self._same_titles: Dict[str, List[str]] = {}

    def summaries(self, pmids: Iterable[str]) -> Dict[str, PubMedSummary]:
        """PubMed's summary of each PMID PubMed has a record of."""
        pmids = list(dict.fromkeys(pmids))
        asked = [p for p in pmids if p not in self._summaries]
        if asked:
            found = self._pubmed.summaries(asked)
            self._summaries.update({p: found.get(p) for p in asked})
        return {p: s for p in pmids if (s := self._summaries[p]) is not None}

    def pmc(self, pmcids: Iterable[str]) -> Dict[str, PmcRecord]:
        """The ID converter's record of each PMCID it knows."""
        pmcids = list(dict.fromkeys(pmcids))
        asked = [c for c in pmcids if c not in self._pmc]
        if asked:
            found = self._pubmed.pmc_records(asked)
            self._pmc.update({c: found.get(c) for c in asked})
        return {c: r for c in pmcids if (r := self._pmc[c]) is not None}

    def pmids_for_dois(self, dois: Sequence[str]) -> List[str]:
        """Every PMID whose PubMed record gives one of `dois`."""
        return self._pubmed.pmids_for_dois(list(dois))

    def same_title_pmids(self, pmid: str) -> List[str]:
        """The other PMIDs whose PubMed records have this one's title: a preprint's record
        and its paper's."""
        if pmid not in self._same_titles:
            summary = self.summaries([pmid]).get(pmid)
            found = self._pubmed.pmids_for_title(summary.title) if summary and summary.title else []
            self._same_titles[pmid] = [p for p in found if p != pmid]
        return self._same_titles[pmid]

    def title(self, doi: str) -> Optional[str]:
        """The registering agency's title of the DOI; None when no agency has it."""
        if doi not in self._titles:
            self._titles[doi] = self._registry.title(doi)
        return self._titles[doi]

    def same_paper(self, doi: Optional[str], title: Optional[str]) -> bool:
        """Whether the registering agency gives `doi` to the paper titled `title`."""
        return bool(doi) and same_title(self.title(doi), title)

    def versions(self, doi: str) -> List[str]:
        """The DOIs Crossref records as this DOI's preprint or version of record, with
        each one's DOI without its version number when the registry gives both one title
        (eLife's reviewed version 10.7554/eLife.92563.3 is 10.7554/eLife.92563, which
        Crossref does not relate to the preprint)."""
        if doi not in self._versions:
            related = [r for kind in _VERSION_OF for r in self._registry.relations(doi).get(kind, ())]
            concepts = [base for r in related if (base := _VERSIONED.sub("", r)) != r
                        and same_title(self.title(base), self.title(r))]
            self._versions[doi] = list(dict.fromkeys(
                d for d in (normalize_doi(v) for v in related + concepts) if d))
        return self._versions[doi]
