"""Helpers for interacting with the Semantic Scholar Graph API."""

from __future__ import annotations

import time
from typing import Dict, Iterable, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models import Identifiers
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author

SEMANTIC_SCHOLAR_BATCH_SIZE = 500
SEMANTIC_SCHOLAR_REQUEST_LIMIT = 1  # requests per second with API key
_MIN_REQUEST_INTERVAL = 1 / SEMANTIC_SCHOLAR_REQUEST_LIMIT


class SemanticScholarClient:
    """Client for Semantic Scholar paper identifier enrichment."""

    BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
    _SUPPORTED_ID_TYPES = {"doi", "pmid"}

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._session = requests.Session()
        self._session.headers.update({"x-api-key": api_key})
        self._last_request = 0.0

    def get_ids(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """Fetch additional identifiers for the provided collection."""
        self.validate_ids(id_type, identifiers)
        return self.get_ids_by_type(id_type, identifiers)

    def validate_ids(self, id_type: str, identifiers: Identifiers) -> None:
        """Ensure the identifiers support the requested lookup type."""
        if id_type not in self._SUPPORTED_ID_TYPES:
            raise ValueError("Semantic Scholar lookups support 'doi' or 'pmid'.")

        if any(getattr(identifier, id_type) is None for identifier in identifiers):
            raise ValueError(f"All identifiers must provide a {id_type.upper()} for lookup.")

    def get_ids_by_type(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """Query Semantic Scholar for the chosen identifier type."""
        values = self._collect_values(id_type, identifiers)
        if not values:
            return identifiers

        batches = [
            values[index : index + SEMANTIC_SCHOLAR_BATCH_SIZE]
            for index in range(0, len(values), SEMANTIC_SCHOLAR_BATCH_SIZE)
        ]

        for batch in batches:
            request_ids = [self._format_for_request(id_type, value) for value in batch]
            payload = self._request_semantic_scholar(request_ids)
            self._apply_records(id_type, identifiers, batch, payload)

        return identifiers

    def _collect_values(self, id_type: str, identifiers: Identifiers) -> List[str]:
        return [
            str(getattr(identifier, id_type)).strip()
            for identifier in identifiers
            if getattr(identifier, id_type)
        ]

    @staticmethod
    def _format_for_request(id_type: str, value: str) -> str:
        prefix = "DOI" if id_type == "doi" else "PMID"
        return f"{prefix}:{value}"

    def _apply_records(
        self,
        id_type: str,
        identifiers: Identifiers,
        batch: Iterable[str],
        records: Iterable[Dict[str, object]],
    ) -> None:
        for original_value, record in zip(batch, records or []):
            if not record or "error" in record:
                continue

            identifier = identifiers.lookup(original_value, key=id_type)
            if identifier is None:
                continue

            external_ids = record.get("externalIds") or {}

            updated = False
            doi = external_ids.get("DOI")
            if doi and not identifier.doi:
                identifier.doi = str(doi)
                updated = True

            pmid = external_ids.get("PubMed")
            if pmid and not identifier.pmid:
                identifier.pmid = str(pmid)
                updated = True

            pmcid = external_ids.get("PubMedCentral")
            if pmcid and not identifier.pmcid:
                identifier.pmcid = str(pmcid)
                updated = True

            self._store_additional_ids(identifier, record, external_ids)

            if updated:
                identifier.normalize()

    @staticmethod
    def _store_additional_ids(
        identifier,
        record: Dict[str, object],
        external_ids: Dict[str, object],
    ) -> None:
        other_ids = identifier.other_ids or {}

        mappings = {
            "semantic_scholar_paper_id": record.get("paperId"),
            "semantic_scholar_corpus_id": external_ids.get("CorpusId"),
            "mag_id": external_ids.get("MAG"),
            "arxiv_id": external_ids.get("ArXiv"),
        }

        for key, value in mappings.items():
            if value is None:
                continue
            other_ids.setdefault(key, str(value))

        # Persist remaining external identifiers that are not handled above.
        for ext_key, ext_value in external_ids.items():
            if ext_value is None:
                continue
            storage_key = ext_key.lower()
            other_ids.setdefault(storage_key, str(ext_value))

        if other_ids:
            identifier.other_ids = other_ids

    def _rate_limit_sleep(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request = time.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
    )
    def _request_semantic_scholar(self, request_ids: Iterable[str]) -> Iterable[Dict[str, object]]:
        self._rate_limit_sleep()
        response = self._session.post(
            self.BASE_URL,
            params={"fields": "externalIds"},
            json={"ids": list(request_ids)},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """
        Fetch article metadata for batch of identifiers.

        Parameters
        ----------
        identifiers : list of Identifier
            Identifiers to fetch metadata for

        Returns
        -------
        dict
            Mapping from slug to ArticleMetadata
        """
        if not identifiers:
            return {}

        # Build request IDs (DOI or PMID format)
        request_map: Dict[str, Identifier] = {}
        request_ids: List[str] = []

        for identifier in identifiers:
            if identifier.doi:
                req_id = f"DOI:{identifier.doi}"
                request_map[req_id] = identifier
                request_ids.append(req_id)
            elif identifier.pmid:
                req_id = f"PMID:{identifier.pmid}"
                request_map[req_id] = identifier
                request_ids.append(req_id)

        if not request_ids:
            return {}

        # Batch requests
        results: Dict[str, ArticleMetadata] = {}
        batches = [
            request_ids[i : i + SEMANTIC_SCHOLAR_BATCH_SIZE]
            for i in range(0, len(request_ids), SEMANTIC_SCHOLAR_BATCH_SIZE)
        ]

        for batch in batches:
            try:
                response_data = self._request_metadata_batch(batch)
                self._process_metadata_response(batch, response_data, request_map, results)
            except Exception:
                # Continue with other batches on failure
                continue

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
    )
    def _request_metadata_batch(self, request_ids: List[str]) -> List[Dict[str, object]]:
        """Request metadata batch from Semantic Scholar API."""
        self._rate_limit_sleep()
        fields = [
            "title",
            "authors",
            "abstract",
            "year",
            "venue",
            "publicationDate",
            "isOpenAccess",
        ]
        response = self._session.post(
            self.BASE_URL,
            params={"fields": ",".join(fields)},
            json={"ids": request_ids},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _process_metadata_response(
        self,
        batch: List[str],
        response_data: List[Dict[str, object]],
        request_map: Dict[str, Identifier],
        results: Dict[str, ArticleMetadata],
    ) -> None:
        """Process metadata response and build ArticleMetadata objects."""
        for req_id, record in zip(batch, response_data or []):
            if not record or "error" in record:
                continue

            identifier = request_map.get(req_id)
            if not identifier:
                continue

            # Parse authors
            authors_data = record.get("authors") or []
            authors = [
                Author(name=str(author.get("name", "")))
                for author in authors_data
                if author.get("name")
            ]

            # Build metadata
            metadata = ArticleMetadata(
                title=str(record.get("title", "")),
                authors=authors,
                abstract=record.get("abstract"),
                journal=record.get("venue"),
                publication_year=record.get("year"),
                open_access=record.get("isOpenAccess"),
                source="semantic_scholar",
                raw_metadata={"semantic_scholar": record},
            )

            results[identifier.slug] = metadata
