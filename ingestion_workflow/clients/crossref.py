"""Crossref's REST API: one work's record, which holds its deposited reference list."""

from __future__ import annotations

import time
from typing import Dict, Optional
from urllib.parse import quote

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

#: The polite pool's single-record limit. Sending an email puts a client in it; there is no key.
CROSSREF_REQUEST_LIMIT = 10
_MIN_REQUEST_INTERVAL = 1 / CROSSREF_REQUEST_LIMIT


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class CrossrefClient:
    BASE_URL = "https://api.crossref.org/works/"

    def __init__(self, email: Optional[str] = None) -> None:
        self.email = email
        self._session = requests.Session()
        agent = "ns-pond-ingestion-workflow (https://github.com/neurostuff/ns-pond-ingestion-workflow"
        self._session.headers["User-Agent"] = agent + (f"; mailto:{email})" if email else ")")
        self._last_request = 0.0

    def _rate_limit_sleep(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request = time.monotonic()

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, max=16),
           retry=retry_if_exception(_retryable), reraise=True)
    def work(self, doi: str) -> Optional[Dict]:
        """The work's `message`, or None when Crossref does not know the DOI."""
        self._rate_limit_sleep()
        params = {"mailto": self.email} if self.email else {}
        response = self._session.get(self.BASE_URL + quote(doi.strip(), safe="/"), params=params, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get("message")
