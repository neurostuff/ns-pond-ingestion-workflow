"""Elsevier's ScienceDirect Search API (v2)."""

from __future__ import annotations

from typing import Dict, List, Tuple

import requests

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.http import Pacer, retry_transient

_MIN_REQUEST_INTERVAL = 1.0  # bursts at 2 a second were refused with 429


def identifier_of(result: Dict) -> Identifier:
    """A search result's DOI, and its PII so a result without a DOI can still be counted."""
    return Identifier(
        doi=result.get("doi"),
        other_ids={"pii": str(result["pii"])} if result.get("pii") else None,
    )


def key_of(result: Dict) -> str:
    """What tells one result from another: its DOI, else its PII, else its title and date."""
    if result.get("doi"):
        return normalize_doi(result["doi"])
    if result.get("pii"):
        return str(result["pii"])
    return f"{result.get('title')}|{result.get('publicationDate')}"


class ScienceDirectClient:
    """Pages of ScienceDirect search results, as identifiers."""

    URL = "https://api.elsevier.com/content/search/sciencedirect"
    PAGE_SIZE = 100  # the largest `show` the API takes
    MAX_RESULTS = 6000  # `offset` goes no further, so a search can be paged this far

    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-ELS-APIKey": api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._pace = Pacer()

    def count(self, body: Dict[str, str]) -> int:
        """`resultsFound` for the search `body`, asking for the smallest page."""
        payload = self._request({**body, "display": {"offset": 0, "show": 10}})
        return int(payload.get("resultsFound") or 0)

    def page(self, body: Dict[str, str], offset: int) -> List[Tuple[str, Identifier]]:
        """The page of results of the search `body` from `offset`, each with its key."""
        payload = self._request({**body, "display": {"offset": offset, "show": self.PAGE_SIZE}})
        return [(key_of(result), identifier_of(result)) for result in payload.get("results") or []]

    @retry_transient(3, 8)
    def _request(self, body: Dict) -> Dict:
        self._pace.wait(_MIN_REQUEST_INTERVAL)
        response = self._session.put(self.URL, json=body, timeout=30)
        response.raise_for_status()
        return response.json()
