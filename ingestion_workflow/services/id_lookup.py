"""Ask external providers for the identifiers an article is missing.

Results are remembered by the catalog's alias table: an article that already
carries a pmid, a doi and a pmcid has nothing left for Semantic Scholar, OpenAlex or
PubMed to look up, so they are not queried for it again. Crossref, which files an
article's preprint and other ids, remembers that it has been asked in the catalog.
"""

from __future__ import annotations

import json
import time
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog.evidence import Evidence, Unreadable, unversioned
from ingestion_workflow.catalog.store import Catalog, IdentityError
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient, PubMedSummary, summary_of_record
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.models.ids import BUCKETS
from ingestion_workflow.services import logging
from ingestion_workflow.services.metadata import PROVIDER_CACHE_DIRS
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.preprints import is_preprint_doi

LookupOrder = Sequence[str]


def _clone_identifier(identifier: Identifier) -> Identifier:
    data = dict(identifier.__dict__)
    other_ids = data.get("other_ids")
    if other_ids is not None:
        data["other_ids"] = dict(other_ids)
    return Identifier(**data)  # type: ignore[arg-type]


class IDLookupService:
    """Base class that handles caching and logging for ID lookups."""

    extractor_name: str = "id_lookup"
    lookup_order: LookupOrder = ("pmid", "doi", "pmcid")
    #: Articles looked up and saved together; None looks them all up before saving any.
    chunk_size: Optional[int] = None

    def __init__(self, settings: Settings, catalog: Catalog, evidence: Evidence) -> None:
        self.settings = settings
        self.catalog = catalog
        self.evidence = evidence
        self.logger = logging.get_logger(self.__class__.__name__)

    # -- Public API -----------------------------------------------------
    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        if not identifiers.identifiers:
            return identifiers

        if not self.can_run():
            self._log_missing_credentials()
            return identifiers

        identifiers.set_index("pmid", "doi", "pmcid")
        pending = self._hydrate_from_cache(identifiers)

        if not pending:
            return identifiers

        # A chunk is saved before the next is asked, so a stopped run keeps what it found.
        size = self.chunk_size or len(pending)
        started = time.monotonic()
        for start in range(0, len(pending), size):
            chunk = pending[start : start + size]
            for id_type in self.lookup_order:
                subset = [identifier for identifier in chunk if identifier.asking(id_type)]
                if not subset:
                    continue

                subset_identifiers = Identifiers(list(subset))
                subset_identifiers.set_index(id_type)

                try:
                    self._lookup_by_type(id_type, subset_identifiers)
                except Exception as exc:  # noqa: BLE001 - log and continue
                    self._log_lookup_failure(id_type, subset, exc)

            self._persist_cache_entries(chunk)
            identifiers.set_index("pmid", "doi", "pmcid")
            if self.chunk_size:
                done, took = start + len(chunk), time.monotonic() - started
                self.logger.info("%s: %d of %d articles asked and saved (%.1f a second)",
                                 self.extractor_name, done, len(pending), done / max(took, 1e-9))
        return identifiers

    # -- Hooks for subclasses -------------------------------------------
    def can_run(self) -> bool:
        return True

    def _missing_credentials_reason(self) -> str:
        return "missing required configuration"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        raise NotImplementedError

    # -- Internal helpers ------------------------------------------------
    def _hydrate_from_cache(self, identifiers: Identifiers) -> List[Identifier]:
        """Fill in what the catalog already knows; return what still needs asking."""
        pending: List[Identifier] = []
        filled = False
        for identifier in identifiers.identifiers:
            if self._is_complete(identifier):
                continue
            known = self.catalog.resolve(identifier)
            if known is not None:
                self._merge_identifier(identifier, known.identifier)
                if self._is_complete(identifier):
                    filled = True
                    continue
            pending.append(identifier)

        if filled:
            identifiers.set_index("pmid", "doi", "pmcid")
        return pending

    def _is_complete(self, identifier: Identifier) -> bool:
        return bool(identifier.pmid and identifier.doi and identifier.pmcid)

    def _merge_cached_entry(self, identifier: Identifier, entry) -> None:
        for cached_identifier in entry.identifiers.identifiers:
            self._merge_identifier(identifier, cached_identifier)

    def _merge_identifier(
        self,
        target: Identifier,
        source: Identifier,
    ) -> None:
        updated = False

        if source.pmid and not target.pmid:
            target.pmid = source.pmid
            updated = True
        if source.doi and not target.doi:
            target.doi = source.doi
            updated = True
        if source.pmcid and not target.pmcid:
            target.pmcid = source.pmcid
            updated = True
        if source.neurostore and not target.neurostore:
            target.neurostore = source.neurostore
            updated = True
        for name in BUCKETS:
            held = getattr(target, name)
            setattr(target, name, (*held, *getattr(source, name)))
            updated = updated or getattr(target, name) != held

        if source.other_ids:
            if target.other_ids is None:
                target.other_ids = {}
            for key, value in source.other_ids.items():
                target.other_ids.setdefault(key, value)
                updated = True

        if updated:
            target.normalize()

    def _persist_cache_entries(self, identifiers: Sequence[Identifier]) -> None:
        """Newly discovered ids become aliases, so nobody looks them up twice."""
        if not identifiers:
            return
        self.catalog.register_many([_clone_identifier(i) for i in identifiers], self.evidence)

    def _log_missing_credentials(self) -> None:
        self.logger.warning(
            "Skipping %s lookup: %s",
            self.extractor_name,
            self._missing_credentials_reason(),
        )

    def _log_lookup_failure(
        self,
        id_type: str,
        identifiers: Sequence[Identifier],
        error: Exception,
    ) -> None:
        values = [
            str(identifier.asking(id_type))
            for identifier in identifiers
            if identifier.asking(id_type)
        ]
        self.logger.warning(
            "Failed %s lookup for %s (provider=%s): %s",
            id_type,
            values or [identifier.slug for identifier in identifiers],
            self.extractor_name,
            error,
        )


class SemanticScholarIDLookupService(IDLookupService):
    extractor_name = "semantic_scholar"
    lookup_order: LookupOrder = ("pmid", "doi")

    def __init__(self, settings: Settings, catalog: Catalog, evidence: Evidence) -> None:
        super().__init__(settings, catalog, evidence)
        self._api_key = settings.semantic_scholar_api_key
        self._client: Optional[SemanticScholarClient]
        if self._api_key:
            self._client = SemanticScholarClient(self._api_key)
        else:
            self._client = None

    def can_run(self) -> bool:  # pragma: no cover - trivial
        return self._client is not None

    def _missing_credentials_reason(self) -> str:
        return "semantic_scholar_api_key is not configured"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        if self._client is None:
            return
        if id_type not in {"pmid", "doi"}:
            return
        self._client.get_ids(id_type, identifiers)


class OpenAlexIDLookupService(IDLookupService):
    extractor_name = "openalex"
    lookup_order: LookupOrder = ("pmid", "doi")

    def __init__(self, settings: Settings, catalog: Catalog, evidence: Evidence) -> None:
        super().__init__(settings, catalog, evidence)
        self._client: Optional[OpenAlexClient] = OpenAlexClient.from_settings(settings)

    def can_run(self) -> bool:  # pragma: no cover - trivial
        return self._client is not None

    def _missing_credentials_reason(self) -> str:
        return "neither openalex_email nor openalex_api_key is configured"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        if self._client is None:
            return
        if id_type not in {"pmid", "doi"}:
            return
        self._client.get_ids(id_type, identifiers)


class PubMedIDLookupService(IDLookupService):
    extractor_name = "pubmed"
    lookup_order: LookupOrder = ("pmid", "doi", "pmcid")

    def __init__(self, settings: Settings, catalog: Catalog, evidence: Evidence) -> None:
        super().__init__(settings, catalog, evidence)
        self._email = settings.pubmed_email
        self._api_key = settings.pubmed_api_key
        self._tool = settings.pubmed_tool or "ingestion-workflow"
        self._client: Optional[PubMedClient]
        if self._email:
            self._client = PubMedClient(
                email=self._email,
                api_key=self._api_key or None,
                tool=self._tool,
            )
        else:
            self._client = None

    def can_run(self) -> bool:  # pragma: no cover - trivial
        return self._client is not None

    def _missing_credentials_reason(self) -> str:
        return "pubmed_email is not configured"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        if self._client is None:
            return
        self._client.get_ids(id_type, identifiers)


#: Crossmark update types whose updating work is a later version of the updated one, not a
#: notice about it (a correction, a retraction) or a new edition of a book: the updated DOI
#: is another id of the same work.
_NEW_VERSIONS = {"new_version", "major_revision", "minor_revision"}
#: Where each field's ids are held by the catalog, for finding whom an id already belongs to.
_KIND = {"doi": "doi", "preprint_dois": "doi", "alternative_dois": "doi",
         "preprint_pmids": "pmid", "preprint_pmcids": "pmcid"}


class CrossrefIDLookupService(IDLookupService):
    """Files each article's other ids into the preprint and alternative buckets.

    Runs last in the chain, so it files what the other providers found too. PubMed's
    record of the article's PMID says whether that PMID is a preprint's (a
    `Preprint`-typed record) and which DOI is the paper's; Crossref says what each of the
    article's DOIs is (a `posted-content` preprint, whatever its prefix) and which DOIs it
    is related to, from either side. Each id is checked against the catalog, the rest of
    the batch and the registries before it is filed, with the evidence the catalog's repair
    uses, so registration has nothing left to refuse: an id another article holds, a second
    DOI the registries do not give the same work, a DOI PubMed gives another paper. What is
    not filed is written to the catalog's `refusals`; what was a collision and was filed is
    in `resolved`. The catalog is told each article was asked of, and does not ask again.
    """

    extractor_name = "crossref"
    #: PubMed first: its spellings of a DOI are what Crossref's other side is asked by.
    lookup_order: LookupOrder = ("pmid", "doi")
    #: Saved every 500 articles: a pass over a search's articles takes hours.
    chunk_size = 500

    def __init__(self, settings: Settings, catalog: Catalog, evidence: Evidence) -> None:
        super().__init__(settings, catalog, evidence)
        #: (kind, value, why) of each id that would have collided where the record had it,
        #: and was filed where the evidence puts it.
        self.resolved: List[Tuple[str, str, str]] = []
        self._batch: List[Identifier] = []
        #: The batch's identifiers holding each id, by (kind, value).
        self._claims: Dict[Tuple[str, str], List[Identifier]] = {}
        self._failed: Set[int] = set()

    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        self._batch = identifiers.identifiers
        self._claims = {}
        for identifier in self._batch:
            for claim in _ids(identifier):
                self._claims.setdefault(claim, []).append(identifier)
        self._failed = set()
        return super().find_identifiers(identifiers)

    def _is_complete(self, identifier: Identifier) -> bool:
        # Having all three ids says nothing of the article's preprints and other ids; it is
        # done once the catalog has a record of Crossref being asked.
        owner = self.catalog.resolve(identifier)
        return owner is not None and self.catalog.was_looked_up(owner.id, self.extractor_name)

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        # Asked of all at once; an article the batch could not answer is asked singly below.
        try:
            if id_type == "pmid":
                self._ask_pubmed(identifiers.identifiers)
            else:
                self.evidence.ask_crossref(
                    d for i in identifiers.identifiers for d in (i.doi, *i.preprint_dois))
        except Exception as exc:  # noqa: BLE001 - each article is asked on its own instead
            self._log_lookup_failure(id_type, identifiers.identifiers, exc)
        for identifier in identifiers.identifiers:
            owner = self.catalog.resolve(identifier)
            article = owner.id if owner is not None else None
            try:
                if id_type == "pmid":
                    self._file_pubmed(identifier, article)
                else:
                    self._file_crossref_dois(identifier, article)
            except Exception as exc:  # noqa: BLE001 - this article fails, and the pass goes on
                self._failed.add(id(identifier))
                self._log_lookup_failure(id_type, [identifier], exc)
            identifier.normalize()

    def _persist_cache_entries(self, identifiers: Sequence[Identifier]) -> None:
        """Register the articles, hold in the catalog as the preprint's each id filed as
        one that the catalog holds as the paper's, and record that Crossref was asked."""
        super()._persist_cache_entries(identifiers)
        asked = []
        for identifier in identifiers:
            owner = self.catalog.resolve(identifier)
            if owner is None:
                continue
            self._rekind(owner.id, identifier)
            if id(identifier) not in self._failed:
                asked.append(owner.id)
        self.catalog.record_lookups(asked, self.extractor_name)

    def _rekind(self, article: str, identifier: Identifier) -> None:
        held = [(kind, value, article, as_kind)
                for kind, values, as_kind in (("pmid", identifier.preprint_pmids, "preprint_pmid"),
                                              ("pmcid", identifier.preprint_pmcids,
                                               "preprint_pmcid"),
                                              ("doi", identifier.preprint_dois, "preprint_doi"))
                for value in values if value in self.catalog.aliases(article, kind)]
        try:
            self.catalog.rekind_aliases(held)
        except IdentityError as exc:
            for kind, value, *_ in held:
                self.catalog.refuse(kind, value, article, f"not held as the preprint's: {exc}")

    # -- Filing ----------------------------------------------------------
    def _file_pubmed(self, identifier: Identifier, article: Optional[str]) -> None:
        pmid = identifier.pmid
        summary = self._summary(identifier, pmid) if pmid else None
        if summary is None:
            return
        doi = normalize_doi(summary.doi) if summary.doi else None
        held = {identifier.doi, *identifier.preprint_dois, *identifier.alternative_dois}
        # An id the article holds needs no evidence: nothing is filed by it.
        paper = False
        if doi is not None and doi not in held:
            try:
                paper = self.evidence.same_paper(doi, summary)
                why = "which the registry does not give that paper"
            except Unreadable as exc:
                why = f"and the registry cannot say whether that is its paper: {exc}"
            if not paper:
                self._omit("doi", doi, article, f"PubMed's record of {pmid} gives it, {why}")
        if summary.preprint:
            identifier.pmid = None
            if self._add(identifier, "preprint_pmids", pmid, article):
                self.resolved.append(("pmid", pmid, "PubMed types the record Preprint"))
            if summary.pmcid and summary.pmcid == identifier.pmcid:
                identifier.pmcid = None
                self._add(identifier, "preprint_pmcids", summary.pmcid, article)
            if paper:
                self._add(identifier, "preprint_dois", doi, article)
            return
        if not paper:
            return
        holder = self.catalog.holder("doi", doi)
        if holder not in (None, article):
            # The paper's DOI is another article's, so the PMID is that article's.
            identifier.pmid = None
            self._omit("pmid", pmid, article, f"PubMed gives it {doi}, which {holder} holds")
        elif identifier.doi is None:
            self._add(identifier, "doi", doi, article)
        else:
            self._omit("doi", doi, article, f"PubMed gives pmid {pmid} this DOI, not the "
                       f"article's {identifier.doi}; the repair reports it as pmid-doi")

    def _file_crossref_dois(self, identifier: Identifier, article: Optional[str]) -> None:
        """Ask Crossref of the article's DOI and its preprints', and of each preprint DOI
        that asking finds. A preprint's relation names the paper, which is checked against
        the DOI the article holds before that DOI's own relations file it."""
        asked: Set[str] = set()
        while True:
            todo = [d for d in dict.fromkeys([*identifier.preprint_dois, identifier.doi])
                    if d and d not in asked]
            if not todo:
                return
            for doi in todo:
                asked.add(doi)
                self._file_crossref(identifier, doi, article)

    def _file_crossref(self, identifier: Identifier, doi: str, article: Optional[str]) -> None:
        evidence = self.evidence
        record = evidence.record(doi)
        if record is None:
            return
        if identifier.doi == doi and _is_preprint(record):
            identifier.doi = None
            self._add(identifier, "preprint_dois", doi, article)
        for updated, kind in record.updates:
            if updated == doi:
                continue
            if kind in _NEW_VERSIONS:
                self._add_other_id(identifier, updated, article, version=True)
            else:
                self._omit("doi", updated, article, f"{doi} is a {kind or 'notice'} of it, "
                           "which is another work")
        related = evidence.related(doi)
        for preprint in related.preprints:
            self._add(identifier, "preprint_dois", preprint, article)
        for paper in related.papers:
            self._add_paper(identifier, paper, article)
        for other in related.versions:
            self._add_other_id(identifier, other, article, version=True)
        for other in related.aliases:
            self._add_other_id(identifier, other, article, version=False)

    def _add_other_id(self, identifier: Identifier, doi: str, article: Optional[str],
                      version: bool) -> None:
        """Another DOI of the work, which Crossref relates as one of its versions (`version`) or
        as the same work.

        A work with a version of record has its own versions (eLife's .1, .2, .3) as
        alternatives, though Crossref types the reviewed preprints among them posted-content:
        the work is the one the article holds, Crossref relates the DOI to it as a version,
        and it is that DOI's own version. Otherwise a DOI on a preprint server's pattern, or
        that Crossref types a preprint, is a preprint's, and any other an alternative.
        """
        if (version and identifier.doi is not None
                and unversioned(doi) == unversioned(identifier.doi)):
            self._add(identifier, "alternative_dois", doi, article)
            return
        record = None if is_preprint_doi(doi) else self.evidence.record(doi)
        if is_preprint_doi(doi) or (record is not None and _is_preprint(record)):
            self._add(identifier, "preprint_dois", doi, article)
        else:
            self._add_alternative(identifier, doi, article)

    def _add_paper(self, identifier: Identifier, doi: str, article: Optional[str]) -> None:
        """The DOI of the paper a preprint is of: the version of record's, or another id of
        the work when the article has one already."""
        if identifier.doi is None:
            self._add(identifier, "doi", doi, article)
        else:
            self._add_alternative(identifier, doi, article)

    def _add_alternative(self, identifier: Identifier, doi: str,
                         article: Optional[str]) -> None:
        """Another DOI of the article's version of record, if the registries give it and the
        one the article holds one work."""
        held = identifier.doi
        if held is None or doi == held:
            self._add(identifier, "alternative_dois", doi, article)
            return
        try:
            same = self.evidence.same_dois(held, doi)
            why = f"the registries do not give it and the article's {held} one work"
        except Unreadable as exc:
            same, why = False, f"the registry cannot say whether it is {held}'s work: {exc}"
        if not same:
            self._omit("doi", doi, article, why)
        elif self._add(identifier, "alternative_dois", doi, article):
            self.resolved.append(("doi", doi, f"a second DOI of {held}'s work"))

    # -- Helpers ---------------------------------------------------------
    def _add(self, identifier: Identifier, field: str, value: str,
             article: Optional[str]) -> bool:
        """File `value` in `field` unless the article holds it already, or another article
        does: one in the catalog, or one of the batch."""
        kind = _KIND[field]
        if kind == "doi":
            value = normalize_doi(value)
        held = {identifier.doi, *identifier.preprint_dois, *identifier.alternative_dois,
                *identifier.preprint_pmids, *identifier.preprint_pmcids}
        if not value or value in held:
            return False
        holder = self.catalog.holder(kind, value)
        if holder not in (None, article):
            self._omit(kind, value, article, f"{holder} holds it")
            return False
        for other in self._claims.get((kind, value), ()):
            owner = self.catalog.resolve(other)
            if other is not identifier and (owner is None or owner.id != article):
                self._omit(kind, value, article, f"{other.slug}, in this batch, holds it")
                return False
        if field == "doi":
            identifier.doi = value
        else:
            setattr(identifier, field, (*getattr(identifier, field), value))
        self._claims.setdefault((kind, value), []).append(identifier)
        return True

    def _omit(self, kind: str, value: str, article: Optional[str], why: str) -> None:
        self.catalog.refuse(kind, value, article, why)

    def _ask_pubmed(self, identifiers: Sequence[Identifier]) -> None:
        """PubMed's summary of each PMID, in as few requests as NCBI takes (the metadata
        stage's stored record of an article when it holds one, else NCBI's), and the
        registry's record of each DOI they give."""
        for identifier in identifiers:
            pmid = identifier.pmid
            path = (self.settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS["pubmed"]
                    / f"{identifier.slug}.json")
            if not pmid or not path.exists():
                continue
            try:
                record = json.loads(path.read_text(encoding="utf-8"))["raw_metadata"]["pubmed"]
                stored = summary_of_record(record)
            except (OSError, ValueError, KeyError) as exc:
                self.logger.warning("Unreadable stored PubMed record %s: %s", path, exc)
            else:
                if stored is not None and PubMedClient._extract_pmid(record) == pmid:
                    self.evidence.hold({pmid: stored})
        summaries = self.evidence.summaries(i.pmid for i in identifiers if i.pmid)
        # `_file_pubmed` asks the registry whether each PubMed DOI is the record's paper
        self.evidence.ask_registry(normalize_doi(s.doi) for s in summaries.values() if s.doi)

    def _summary(self, identifier: Identifier, pmid: str) -> Optional[PubMedSummary]:
        """PubMed's summary of the PMID, as `_ask_pubmed` found it (NCBI's, when the batch
        could not be asked)."""
        return self.evidence.summaries([pmid]).get(pmid)


def _is_preprint(record) -> bool:
    """Crossref types a preprint `posted-content`, subtype `preprint`; its other
    posted-content (a working paper, a report, a dissertation) is not one."""
    return record.type == "posted-content" and record.subtype == "preprint"


def _ids(identifier: Identifier):
    """Each (kind, value) the identifier holds, in whatever bucket."""
    yield from (("doi", d) for d in
                {identifier.doi, *identifier.preprint_dois, *identifier.alternative_dois} - {None})
    yield from (("pmid", p) for p in
                {identifier.pmid, *identifier.preprint_pmids, *identifier.alternative_pmids}
                - {None})
    yield from (("pmcid", c) for c in
                {identifier.pmcid, *identifier.preprint_pmcids, *identifier.alternative_pmcids}
                - {None})
