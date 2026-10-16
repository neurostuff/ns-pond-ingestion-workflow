"""Elsevier's ScienceDirect Search API (v2)."""

from __future__ import annotations

import time
from typing import Dict, List, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models.ids import Identifier

SCIENCEDIRECT_PAGE_SIZE = 100  # the largest `show` the API takes
SCIENCEDIRECT_MAX_RESULTS = 6000  # `offset` goes no further, so a search can be paged this far
_MIN_REQUEST_INTERVAL = 1.0  # bursts at 2 a second were refused with 429


class ScienceDirectClient:
    """Pages of ScienceDirect search results, as identifiers."""

    URL = "https://api.elsevier.com/content/search/sciencedirect"

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

    def page(
        self, body: Dict[str, str], offset: int, show: int = SCIENCEDIRECT_PAGE_SIZE
    ) -> Tuple[int, List[Identifier]]:
        """`resultsFound` for the search `body`, and the results from `offset` on.

        A result without a DOI is kept with its PII, so the caller can count it.
        """
        payload = self._request({**body, "display": {"offset": offset, "show": show}})
        found = [
            Identifier(
                doi=result["doi"].lower() if result.get("doi") else None,
                other_ids={"pii": str(result["pii"])} if result.get("pii") else None,
            )
            for result in payload.get("results") or []
        ]
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
