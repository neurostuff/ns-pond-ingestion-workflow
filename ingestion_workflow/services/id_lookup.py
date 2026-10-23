"""Ask external providers for the identifiers an article is missing.

Results are remembered by the catalog's alias table: an article that already
carries a pmid, a doi and a pmcid has nothing left to look up, so no provider
is queried for it again.
"""

from __future__ import annotations

import json
from typing import List, Optional, Sequence, Tuple

import httpx
import requests

from ingestion_workflow.catalog.evidence import Unreadable
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

    def __init__(self, settings: Settings, catalog=None) -> None:
        self.settings = settings
        self.catalog = catalog
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

        any_success = False
        for id_type in self.lookup_order:
            subset = [identifier for identifier in pending if identifier.asking(id_type)]
            if not subset:
                continue

            subset_identifiers = Identifiers(list(subset))
            subset_identifiers.set_index(id_type)

            try:
                self._lookup_by_type(id_type, subset_identifiers)
                any_success = True
            except Exception as exc:  # noqa: BLE001 - log and continue
                self._log_lookup_failure(id_type, subset, exc)

        if any_success:
            identifiers.set_index("pmid", "doi", "pmcid")

        self._persist_cache_entries(pending)
        identifiers.set_index("pmid", "doi", "pmcid")
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
            known = self.catalog.resolve(identifier) if self.catalog is not None else None
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
        if not identifiers or self.catalog is None:
            return
        self.catalog.register_many([_clone_identifier(i) for i in identifiers])

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

    def __init__(self, settings: Settings, catalog=None) -> None:
        super().__init__(settings, catalog)
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

    def __init__(self, settings: Settings, catalog=None) -> None:
        super().__init__(settings, catalog)
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

    def __init__(self, settings: Settings, catalog=None) -> None:
        super().__init__(settings, catalog)
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
#: Failures of a registry or PubMed that leave one article's ids unfiled, not the batch.
_UNREADABLE = (Unreadable, requests.RequestException, httpx.HTTPError)


class CrossrefIDLookupService(IDLookupService):
    """Files each article's other ids into the preprint and alternative buckets.

    Runs after the other providers, so it files what they found too. PubMed's record of
    the article's PMID says whether that PMID is a preprint's (a `Preprint`-typed record)
    and which DOI is the paper's; Crossref says what each of the article's DOIs is (a
    preprint is `posted-content`, whatever its prefix) and which DOIs it is related to,
    from either side. Each id is checked against the catalog and the registries before it
    is filed, with the evidence the catalog's repair uses, so that registration is not left
    to refuse it: an id another article holds, a second journal DOI the registries do not
    give the same work, a DOI PubMed gives another paper. What is not filed is in
    `omitted`, with why; what was a collision and was filed is in `resolved`.
    """

    extractor_name = "crossref"
    #: PubMed first: its spellings of a DOI are what Crossref's other side is asked by.
    lookup_order: LookupOrder = ("pmid", "doi")

    def __init__(self, settings: Settings, catalog=None) -> None:
        super().__init__(settings, catalog)
        #: (kind, value, article id or None, why) of each id left unfiled since creation.
        self.omitted: List[Tuple[str, str, Optional[str], str]] = []
        #: (kind, value, why) of each id that would have collided where the record had it,
        #: and was filed where the evidence puts it.
        self.resolved: List[Tuple[str, str, str]] = []

    def can_run(self) -> bool:
        return self.catalog is not None and self.catalog.evidence is not None

    def _missing_credentials_reason(self) -> str:
        return "the catalog was opened without PubMed and registry evidence"

    def _is_complete(self, identifier: Identifier) -> bool:
        # Having all three ids says nothing of the article's preprints and other ids.
        return False

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        for identifier in identifiers.identifiers:
            owner = self.catalog.resolve(identifier)
            article = owner.id if owner is not None else None
            try:
                if id_type == "pmid":
                    self._file_pubmed(identifier, article)
                else:
                    # A preprint's relation names the paper, which is checked against the DOI
                    # the article holds before that DOI's own relations file it unchecked.
                    for doi in dict.fromkeys([*identifier.preprint_dois, identifier.doi]):
                        if doi:
                            self._file_crossref(identifier, doi, article)
            except _UNREADABLE as exc:
                self._log_lookup_failure(id_type, [identifier], exc)
            identifier.normalize()

    # -- Filing ----------------------------------------------------------
    def _file_pubmed(self, identifier: Identifier, article: Optional[str]) -> None:
        pmid = identifier.pmid
        summary = self._summary(identifier, pmid) if pmid else None
        if summary is None:
            return
        doi = normalize_doi(summary.doi) if summary.doi else None
        paper = False
        if doi is not None:
            try:
                paper = self.catalog.evidence.same_paper(doi, summary)
                why = "which the registry does not give that paper"
            except Unreadable as exc:
                why = f"and the registry cannot say whether that is its paper: {exc}"
            held = {identifier.doi, *identifier.preprint_dois, *identifier.alternative_dois}
            if not paper and doi not in held:
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
        if not paper or doi == identifier.doi:
            return
        holder = self.catalog.holder("doi", doi)
        if holder not in (None, article):
            # The paper's DOI is another article's, so the PMID is that article's.
            identifier.pmid = None
            self._omit("pmid", pmid, article, f"PubMed gives it {doi}, which {holder} holds")
        elif identifier.doi is None:
            self._add(identifier, "doi", doi, article)
        else:
            self._omit("doi", doi, article,
                       f"PubMed gives pmid {pmid} this DOI, not the article's {identifier.doi}")

    def _file_crossref(self, identifier: Identifier, doi: str, article: Optional[str]) -> None:
        evidence = self.catalog.evidence
        record = evidence.record(doi)
        if record is None:
            return
        if record.type == "posted-content" and identifier.doi == doi:
            identifier.doi = None
            self._add(identifier, "preprint_dois", doi, article)
        for updated, kind in record.updates:
            if updated == doi:
                continue
            if kind in _NEW_VERSIONS:
                self._add(identifier, "alternative_dois", updated, article)
            else:
                self._omit("doi", updated, article, f"{doi} is a {kind or 'notice'} of it, "
                           "which is another work")
        related = evidence.related(doi)
        for preprint in related.preprints:
            self._add(identifier, "preprint_dois", preprint, article)
        for paper in related.papers:
            self._add_paper(identifier, paper, article)
        for version in related.versions:
            self._add(identifier, "preprint_dois" if is_preprint_doi(version)
                      else "alternative_dois", version, article)

    def _add_paper(self, identifier: Identifier, doi: str, article: Optional[str]) -> None:
        """The DOI of the paper a preprint is of: the version of record's, or when the
        article has one already, another id of it only if the registries give both DOIs one
        work."""
        held = identifier.doi
        if held is None or held == doi:
            self._add(identifier, "doi", doi, article)
            return
        try:
            same = self.catalog.evidence.same_dois(held, doi)
            why = f"the registries do not give it and the article's {held} one work"
        except Unreadable as exc:
            same, why = False, f"the registry cannot say whether it is {held}'s work: {exc}"
        if same:
            if self._add(identifier, "alternative_dois", doi, article):
                self.resolved.append(("doi", doi, f"a second journal DOI of {held}'s work"))
        else:
            self._omit("doi", doi, article, why)

    # -- Helpers ---------------------------------------------------------
    def _add(self, identifier: Identifier, field: str, value: str,
             article: Optional[str]) -> bool:
        """File `value` in `field` unless the article holds it already or another does."""
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
        if field == "doi":
            identifier.doi = value
        else:
            setattr(identifier, field, (*getattr(identifier, field), value))
        return True

    def _omit(self, kind: str, value: str, article: Optional[str], why: str) -> None:
        self.omitted.append((kind, value, article, why))
        self.logger.warning("%s %s not filed -- %s", kind, value, why)

    def _summary(self, identifier: Identifier, pmid: str) -> Optional[PubMedSummary]:
        """PubMed's summary of the PMID: the metadata stage's stored record of the article
        when it holds one, else NCBI's."""
        path = (self.settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS["pubmed"]
                / f"{identifier.slug}.json")
        if path.exists():
            try:
                record = json.loads(path.read_text(encoding="utf-8"))["raw_metadata"]["pubmed"]
                stored = summary_of_record(record)
            except (OSError, ValueError, KeyError) as exc:
                self.logger.warning("Unreadable stored PubMed record %s: %s", path, exc)
            else:
                if stored is not None and PubMedClient._extract_pmid(record) == pmid:
                    self.catalog.evidence.hold({pmid: stored})
        return self.catalog.evidence.summaries([pmid]).get(pmid)
