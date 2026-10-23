"""ORCID's public API: the email addresses a researcher has made public on their record."""

from __future__ import annotations

import time
from typing import List

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion_workflow.utils.http import transient

#: Below the public API's 24 requests a second (https://info.orcid.org/ufaqs/what-are-the-api-limits/).
ORCID_REQUEST_LIMIT = 12


class OrcidClient:
    BASE_URL = "https://pub.orcid.org/v3.0/"

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "ns-pond-ingestion-workflow "
                          "(https://github.com/neurostuff/ns-pond-ingestion-workflow)"})
        self._last_request = 0.0

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, max=16),
           retry=retry_if_exception(transient), reraise=True)
    def emails(self, orcid: str) -> List[str]:
        """The record's public, verified addresses, primary first; none for an unknown iD."""
        elapsed = time.monotonic() - self._last_request
        if elapsed < 1 / ORCID_REQUEST_LIMIT:
            time.sleep(1 / ORCID_REQUEST_LIMIT - elapsed)
        self._last_request = time.monotonic()
        response = self._session.get(f"{self.BASE_URL}{orcid}/email", timeout=30)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        listed = [e for e in response.json().get("email") or []
                  if e.get("email") and e.get("verified")]
        return [e["email"] for e in sorted(listed, key=lambda e: not e.get("primary"))]
