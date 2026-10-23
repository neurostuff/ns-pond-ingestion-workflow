"""Helpers for interacting with the Semantic Scholar Graph API."""

from __future__ import annotations

import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from study_schema.models.paper_parse import MetadataSource
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion_workflow.models import Identifiers
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.utils.http import transient

SEMANTIC_SCHOLAR_BATCH_SIZE = 500
SEMANTIC_SCHOLAR_REQUEST_LIMIT = 1  # requests per second with API key
_MIN_REQUEST_INTERVAL = 1 / SEMANTIC_SCHOLAR_REQUEST_LIMIT
#: What the batch endpoint answers, with 400, when it knows none of the ids it was asked.
_NONE_KNOWN = "No valid paper ids given"


logger = logging.getLogger(__name__)


def _paper_identifier(external_ids: Dict, paper_id: Optional[str]) -> Optional[Identifier]:
    """A paper's DOI, PMID and PMCID, or None when any of them is malformed.

    Such a paper is logged and skipped whole, its other ids too; nothing is kept for
    it, so its article is asked about again on the next run.
    """
    pmid, pmcid = external_ids.get("PubMed"), external_ids.get("PubMedCentral")
    try:
        return Identifier(
            doi=external_ids.get("DOI"),
            pmid=str(pmid) if pmid else None,
            pmcid=str(pmcid) if pmcid else None,
        )
    except ValueError as exc:
        logger.warning("Semantic Scholar paper %s: %s; its ids not used", paper_id, exc)
        return None


def request_id(identifier: Identifier) -> Optional[str]:
    """The id a batch request names the paper by: its DOI (a preprint's when it has no other),
    else its PMID."""
    if identifier.asking_doi:
        return f"DOI:{identifier.asking_doi}"
    return f"PMID:{identifier.pmid}" if identifier.pmid else None


class SemanticScholarClient:
    """Client for Semantic Scholar paper identifier enrichment."""

    BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
    MATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search/match"
    BULK_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
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

        if any(identifier.asking(id_type) is None for identifier in identifiers):
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
            str(identifier.asking(id_type)).strip()
            for identifier in identifiers
            if identifier.asking(id_type)
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
            found = _paper_identifier(external_ids, record.get("paperId"))
            if found is None:
                continue

            updated = False
            for kind in ("doi", "pmid", "pmcid"):
                if found.asking(kind) and not getattr(identifier, kind):
                    setattr(identifier, kind, found.asking(kind))
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
        retry=retry_if_exception(transient),
        reraise=True,
    )
    def _request_semantic_scholar(self, request_ids: Iterable[str]) -> Iterable[Dict[str, object]]:
        return self._post_batch({"fields": "externalIds"}, list(request_ids))

    def _post_batch(self, params: Dict[str, str], ids: List[str]) -> List[Optional[Dict]]:
        """The batch endpoint's record of each id, None for one it does not know. Asked
        only for ids it knows none of, it answers 400 "No valid paper ids given"."""
        self._rate_limit_sleep()
        response = self._session.post(self.BASE_URL, params=params, json={"ids": ids}, timeout=30)
        if response.status_code == 400 and _NONE_KNOWN in response.text:
            return [None] * len(ids)
        response.raise_for_status()
        return response.json()

    def search_bulk(self, params: Dict[str, str]) -> Identifiers:
        """Every paper the bulk search finds for `params` (`query`, optionally `year`).

        Pages hold up to 1,000 papers; each carries the token for the next one.
        A paper with no DOI, PMID or PMCID is kept with its Semantic Scholar id.
        """
        found: List[Identifier] = []
        token = None
        while True:
            payload = self._request_bulk_search(
                {**params, "fields": "externalIds", **({"token": token} if token else {})}
            )
            for paper in payload.get("data") or []:
                identifier = _paper_identifier(paper.get("externalIds") or {}, paper["paperId"])
                if identifier is not None:
                    identifier["semantic_scholar"] = str(paper["paperId"])
                    found.append(identifier)
            token = payload.get("token")
            if not token:
                return Identifiers(found)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
        retry=retry_if_exception(transient),
        reraise=True,
    )
    def _request_bulk_search(self, params: Dict[str, str]) -> Dict[str, object]:
        self._rate_limit_sleep()
        response = self._session.get(self.BULK_SEARCH_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def search_title(self, title: str) -> List[Tuple[Identifier, ArticleMetadata]]:
        """The one paper Semantic Scholar matches to `title`, with its authors.

        The match endpoint answers 404 when nothing is close enough.
        """
        self._rate_limit_sleep()
        response = self._session.get(
            self.MATCH_URL,
            params={"query": title, "fields": "title,authors,year,externalIds"},
            timeout=30,
        )
        if response.status_code == 404:
            return []
        response.raise_for_status()
        found = []
        for record in response.json().get("data", []) or []:
            identifier = _paper_identifier(record.get("externalIds") or {}, record.get("paperId"))
            if identifier is None:
                continue
            metadata = ArticleMetadata(
                title=str(record.get("title") or ""),
                authors=[
                    Author(name=str(author["name"]))
                    for author in record.get("authors") or []
                    if author.get("name")
                ],
                publication_year=record.get("year"),
                source="semantic_scholar",
            )
            found.append((identifier, metadata))
        return found

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
            req_id = request_id(identifier)
            if req_id:
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

        failures: List[BaseException] = []
        for batch in batches:
            try:
                response_data = self._request_metadata_batch(batch)
                self._process_metadata_response(batch, response_data, request_map, results)
            except Exception as exc:  # noqa: BLE001
                # Continue with other batches, and say so: an unanswered article is not
                # one Semantic Scholar has nothing for.
                failures.append(exc)

        if failures:
            first = failures[0]
            raise PartialAnswer(f"Semantic Scholar: {type(first).__name__}: {first}", results,
                                failures)
        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
        retry=retry_if_exception(transient),
        reraise=True,
    )
    def _request_metadata_batch(self, request_ids: List[str]) -> List[Dict[str, object]]:
        """Request metadata batch from Semantic Scholar API."""
        fields = [
            "title",
            "authors",
            "abstract",
            "year",
            "venue",
            "publicationDate",
            "isOpenAccess",
            "openAccessPdf",
        ]
        return self._post_batch({"fields": ",".join(fields)}, request_ids)

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

            open_access_pdf = record.get("openAccessPdf") or {}
            pdf_url = open_access_pdf.get("url") if isinstance(open_access_pdf, dict) else None

            # Build metadata
            metadata = ArticleMetadata(
                title=str(record.get("title", "")),
                authors=authors,
                abstract=record.get("abstract"),
                journal=record.get("venue"),
                publication_year=record.get("year"),
                pdf_url=pdf_url,
                source=MetadataSource.semantic_scholar.value,
                raw_metadata={"semantic_scholar": record},
            )

            results[identifier.slug] = metadata
