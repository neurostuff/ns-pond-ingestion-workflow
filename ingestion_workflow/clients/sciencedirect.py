"""Elsevier's ScienceDirect Search API (v2)."""

from __future__ import annotations

import time
from typing import Dict, List, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models.ids import Identifier

_MIN_REQUEST_INTERVAL = 1.0  # bursts at 2 a second were refused with 429


def identifier_of(result: Dict) -> Identifier:
    """A search result's DOI, and its PII so a result without a DOI can still be counted."""
    return Identifier(
        doi=result["doi"].lower() if result.get("doi") else None,
        other_ids={"pii": str(result["pii"])} if result.get("pii") else None,
    )


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
        self._last_request = 0.0

    def page(self, body: Dict[str, str], offset: int) -> Tuple[int, List[Identifier]]:
        """`resultsFound` for the search `body`, and the page of results from `offset`."""
        payload = self._request({**body, "display": {"offset": offset, "show": self.PAGE_SIZE}})
        found = [identifier_of(result) for result in payload.get("results") or []]
        return int(payload.get("resultsFound") or 0), found

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=8))
    def _request(self, body: Dict) -> Dict:
        elapsed = time.monotonic() - self._last_request
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request = time.monotonic()
        response = self._session.put(self.URL, json=body, timeout=30)
        response.raise_for_status()
        return response.json()
