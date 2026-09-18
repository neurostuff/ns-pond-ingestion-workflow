"""OpenAlex client helpers."""

from __future__ import annotations

import time
from typing import Dict, Optional
from urllib.parse import urljoin

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models import Identifiers

OPENALEX_BATCH_LOOKUP_SIZE = 100
OPENALEX_REQUEST_LIMIT = 10  # polite pool: 10 req / second
_MIN_REQUEST_INTERVAL = 1 / OPENALEX_REQUEST_LIMIT


class OpenAlexClient:
    BASE_URL = "https://api.openalex.org"
    LOOKUP_ENDPOINT = "/works"
    ID_QUERY_ARGS = "select=ids,doi"

    def __init__(self, email: str) -> None:
        self.email = email
        self._session = requests.Session()
        self._last_request = 0.0

    def get_ids(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """
        Fetch OpenAlex IDs for a list of identifiers.

        Parameters
        ----------
        identifiers : Identifiers
            Identifiers containing DOIs or PMIDs

        Returns
        -------
        Identifiers
            Identifiers with OpenAlex IDs populated
        """
        self.validate_ids(id_type, identifiers)
        return self.get_ids_by_type(id_type, identifiers)

    def validate_ids(self, id_type: str, identifiers: Identifiers) -> None:
        """Ensure we have values available for the requested id_type."""
        if id_type == "doi":
            if any(identifier.doi is None for identifier in identifiers):
                raise ValueError("All identifiers must have a DOI for doi lookup.")
        elif id_type == "pmid":
            if any(identifier.pmid is None for identifier in identifiers):
                raise ValueError("All identifiers must have a PMID for pmid lookup.")
        else:
            raise ValueError(f"Unsupported id_type: {id_type}")

    def get_ids_by_type(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """Query OpenAlex using the provided id type and enrich identifiers."""
        values = [
            str(getattr(identifier, id_type)).strip()
            for identifier in identifiers
            if getattr(identifier, id_type)
        ]

        batches = [
            values[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
            for index in range(0, len(values), OPENALEX_BATCH_LOOKUP_SIZE)
        ]
        for batch in batches:
            params = {
                "filter": f"{id_type}:{'|'.join(batch)}",
                "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                "mailto": self.email,
                "select": "ids",
            }
            payload = self._request_openalex(params)

            for work in payload.get("results", []) or []:
                ids_data = work.get("ids", {})
                key = ids_data.get(id_type)
                if not key:
                    continue

                identifier = identifiers.lookup(key, key=id_type)

                if identifier is None:
                    continue

                openalex_id = ids_data.get("openalex")
                if openalex_id:
                    if identifier.other_ids is None:
                        identifier.other_ids = {}
                    identifier.other_ids["openalex"] = openalex_id

        return identifiers

    def get_pdf_urls(self, identifiers: Identifiers) -> Dict[str, str]:
        """Return a mapping of identifier slug to an open-access PDF URL.

        Prefers `best_oa_location.pdf_url` (a direct PDF link) over
        `open_access.oa_url`, which may point at a landing page.
        """
        pdf_urls: Dict[str, str] = {}

        for id_type in ("doi", "pmid"):
            values = [
                str(getattr(identifier, id_type)).strip()
                for identifier in identifiers
                if getattr(identifier, id_type) and identifier.slug not in pdf_urls
            ]
            if not values:
                continue

            batches = [
                values[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
                for index in range(0, len(values), OPENALEX_BATCH_LOOKUP_SIZE)
            ]
            for batch in batches:
                params = {
                    "filter": f"{id_type}:{'|'.join(batch)}",
                    "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                    "mailto": self.email,
                    "select": "ids,best_oa_location,open_access",
                }
                try:
                    payload = self._request_openalex(params)
                except Exception:
                    continue

                for work in payload.get("results", []) or []:
                    ids_data = work.get("ids", {}) or {}
                    key = ids_data.get(id_type)
                    if not key:
                        continue
                    identifier = identifiers.lookup(key, key=id_type)
                    if identifier is None:
                        continue

                    url = self._pdf_url_from_work(work)
                    if url:
                        pdf_urls[identifier.slug] = url

        return pdf_urls

    @staticmethod
    def _pdf_url_from_work(work: Dict) -> Optional[str]:
        best_location = work.get("best_oa_location") or {}
        if isinstance(best_location, dict):
            url = best_location.get("pdf_url")
            if url:
                return str(url)

        open_access = work.get("open_access") or {}
        if isinstance(open_access, dict):
            url = open_access.get("oa_url")
            if url:
                return str(url)

        return None

    def _rate_limit_sleep(self) -> None:
        """Ensure we respect the 10 requests/sec polite pool limit."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request = time.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
    )
    def _request_openalex(self, params: Dict[str, str]) -> Dict:
        """Issue a GET request to the OpenAlex Works endpoint."""
        self._rate_limit_sleep()
        url = urljoin(self.BASE_URL, self.LOOKUP_ENDPOINT)
        response = self._session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
