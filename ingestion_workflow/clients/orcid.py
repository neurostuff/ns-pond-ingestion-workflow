"""ORCID's public API: the email addresses a researcher has made public on their record."""

from __future__ import annotations

from typing import List

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient

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
        self._pace = Pacer()

    @retry_transient(4, 16)
    def emails(self, orcid: str) -> List[str]:
        """The record's public, verified addresses, primary first; none for an unknown iD."""
        self._pace.wait(1 / ORCID_REQUEST_LIMIT)
        response = self._session.get(f"{self.BASE_URL}{orcid}/email", timeout=30)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        listed = [e for e in response.json().get("email") or []
                  if e.get("email") and e.get("verified")]
        return [e["email"] for e in sorted(listed, key=lambda e: not e.get("primary"))]
