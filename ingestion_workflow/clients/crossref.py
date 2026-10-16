"""Crossref's REST API: one work's record, which holds its deposited reference list,
and the licences publishers deposit for many works at once."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, License, LicenseSource
from ingestion_workflow.utils.doi import normalize_doi

#: The polite pool's single-record limit. Sending an email puts a client in it; there is no key.
CROSSREF_REQUEST_LIMIT = 10
#: Its limit on list queries (`x-rate-limit-limit: 3` on /works?filter=, 2026-10-16).
CROSSREF_LIST_REQUEST_LIMIT = 3
#: DOIs per `filter=doi:...` list query: one page, and a URL well under any server's limit.
CROSSREF_DOI_BATCH_SIZE = 50
_LICENSE_VERSIONS = ("vor", "am")


def license_from(
    entries: Optional[List[Dict]], now: Optional[datetime] = None
) -> Optional[License]:
    """The licence a work's `license` deposits state for it now.

    The version of record's wins, then the accepted manuscript's, then any other
    (`tdm`, `unspecified`). A deposit that starts in the future (an embargo) is
    not yet in force; of several in force for one version, the latest started.
    """
    now = now or datetime.now(timezone.utc)
    in_force = []
    for entry in entries or []:
        start = ((entry.get("start") or {}).get("date-time")) or ""
        try:
            started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        except ValueError:
            started = None
        if entry.get("URL") and (started is None or started <= now):
            in_force.append((started or datetime.min.replace(tzinfo=timezone.utc), entry))
    for version in _LICENSE_VERSIONS:
        matching = [pair for pair in in_force if pair[1].get("content-version") == version]
        if matching:
            entry = max(matching, key=lambda pair: pair[0])[1]
            return License.stated(LicenseSource.CROSSREF, entry["URL"], version=version)
    if in_force:
        return License.stated(LicenseSource.CROSSREF, in_force[0][1]["URL"])
    return None


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

    def _rate_limit_sleep(self, limit: int = CROSSREF_REQUEST_LIMIT) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < 1 / limit:
            time.sleep(1 / limit - elapsed)
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

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, max=16),
           retry=retry_if_exception(_retryable), reraise=True)
    def _licenses(self, dois: List[str]) -> List[Dict]:
        """The `DOI` and `license` of each work Crossref knows among `dois`."""
        self._rate_limit_sleep(CROSSREF_LIST_REQUEST_LIMIT)
        params = {"filter": ",".join(f"doi:{doi}" for doi in dois), "select": "DOI,license",
                  "rows": str(len(dois))}
        if self.email:
            params["mailto"] = self.email
        response = self._session.get(self.BASE_URL.rstrip("/"), params=params, timeout=60)
        response.raise_for_status()
        return response.json()["message"]["items"]

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """Each known work's licence, as a record holding only that, keyed by identifier slug.

        A work Crossref knows without a licence deposit gets a record too, so that the
        answer is cached; a DOI it does not know is absent. A filter value cannot hold
        a comma, so a DOI with one is asked for singly.
        """
        wanted: Dict[str, Identifier] = {}
        for identifier in identifiers:
            doi = normalize_doi(identifier.doi) if identifier.doi else None
            if doi:
                wanted.setdefault(doi.lower(), identifier)
        batched = [doi for doi in wanted if "," not in doi]
        works: List[Dict] = []
        for index in range(0, len(batched), CROSSREF_DOI_BATCH_SIZE):
            works += self._licenses(batched[index : index + CROSSREF_DOI_BATCH_SIZE])
        for doi in wanted:
            if "," in doi:
                work = self.work(doi)
                works += [work] if work else []
        results: Dict[str, ArticleMetadata] = {}
        for work in works:
            identifier = wanted.get(str(work.get("DOI") or "").lower())
            if identifier is not None:
                results[identifier.slug] = ArticleMetadata(
                    title="", license=license_from(work.get("license")), source="crossref"
                )
        return results
