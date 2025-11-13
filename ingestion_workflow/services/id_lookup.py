"""Identifier lookup services with cache integration."""

from __future__ import annotations

from typing import List, Optional, Sequence

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Identifier,
    IdentifierCacheEntry,
    IdentifierExpansion,
    Identifiers,
)
from ingestion_workflow.services import cache, logging


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

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
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
            subset = [identifier for identifier in pending if getattr(identifier, id_type)]
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
        pending: List[Identifier] = []
        for identifier in identifiers.identifiers:
            if self._is_complete(identifier):
                continue
            entry = cache.get_identifier_cache_entry(
                self.settings,
                self.extractor_name,
                identifier,
            )
            if entry is None:
                pending.append(identifier)
                continue
            self._merge_cached_entry(identifier, entry)

        if len(pending) != len(identifiers.identifiers):
            identifiers.set_index("pmid", "doi", "pmcid")

        return pending

    def _is_complete(self, identifier: Identifier) -> bool:
        return bool(identifier.pmid and identifier.doi and identifier.pmcid)

    def _merge_cached_entry(self, identifier: Identifier, entry: IdentifierCacheEntry) -> None:
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

        if source.other_ids:
            if target.other_ids is None:
                target.other_ids = {}
            for key, value in source.other_ids.items():
                target.other_ids.setdefault(key, value)
                updated = True

        if updated:
            target.normalize()

    def _persist_cache_entries(
        self,
        identifiers: Sequence[Identifier],
    ) -> None:
        if not identifiers:
            return

        entries: List[IdentifierCacheEntry] = []
        for identifier in identifiers:
            clone = _clone_identifier(identifier)
            expansion = IdentifierExpansion(
                seed_identifier=_clone_identifier(identifier),
                identifiers=Identifiers([clone]),
                sources=[self.extractor_name],
            )
            entries.append(IdentifierCacheEntry.from_expansion(expansion))

        if entries:
            cache.cache_identifier_entries(
                self.settings,
                self.extractor_name,
                entries,
            )

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
            str(getattr(identifier, id_type))
            for identifier in identifiers
            if getattr(identifier, id_type)
        ]
        self.logger.warning(
            "Failed %s lookup for %s (provider=%s): %s",
            id_type,
            values or [identifier.hash_id for identifier in identifiers],
            self.extractor_name,
            error,
        )


class SemanticScholarIDLookupService(IDLookupService):
    extractor_name = "semantic_scholar"
    lookup_order: LookupOrder = ("pmid", "doi")

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
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

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._email = settings.openalex_email
        self._client: Optional[OpenAlexClient]
        if self._email:
            self._client = OpenAlexClient(self._email)
        else:
            self._client = None

    def can_run(self) -> bool:  # pragma: no cover - trivial
        return self._client is not None

    def _missing_credentials_reason(self) -> str:
        return "openalex_email is not configured"

    def _lookup_by_type(self, id_type: str, identifiers: Identifiers) -> None:
        if self._client is None:
            return
        if id_type not in {"pmid", "doi"}:
            return
        self._client.get_ids(id_type, identifiers)


class PubMedIDLookupService(IDLookupService):
    extractor_name = "pubmed"
    lookup_order: LookupOrder = ("pmid", "doi", "pmcid")

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
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
