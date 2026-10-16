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
from ingestion_workflow.models.metadata import (
    WEAK_LICENSES,
    ArticleMetadata,
    License,
    LicenseSource,
    normalise_license,
)
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.utils.doi import normalize_doi

#: The polite pool's single-record limit. Sending an email puts a client in it; there is no key.
CROSSREF_REQUEST_LIMIT = 10
#: Its limit on list queries (`x-rate-limit-limit: 3` on /works?filter=, 2026-10-16).
CROSSREF_LIST_REQUEST_LIMIT = 3
#: DOIs per `filter=doi:...` list query: one page, and a URL well under any server's limit.
CROSSREF_DOI_BATCH_SIZE = 50
#: Which deposit speaks for the article, best first. `tdm` entries are a mining grant,
#: used only when nothing else is deposited. `stm-asf` entries are the STM Article
#: Sharing Framework's policies on where a manuscript may be shared, not a licence,
#: and are left out.
_VERSION_ORDER = {"vor": 0, "am": 1, "unspecified": 2, "tdm": 3}
_EARLIEST = datetime.min.replace(tzinfo=timezone.utc)
#: The longest a Retry-After is waited out before the request counts as failed.
_MAX_RETRY_AFTER = 120


def license_from(
    entries: Optional[List[Dict]], now: Optional[datetime] = None
) -> Optional[License]:
    """The licence a work's `license` deposits state for it now.

    Among the deposits in force (an embargoed one, starting in the future, is not),
    the one chosen is the best by, in turn: content-version (vor, am, unspecified,
    tdm); a licence over a weak statement (a TDM grant or terms page); the latest
    start; the URL, so the choice never depends on deposit order.
    """
    now = now or datetime.now(timezone.utc)
    candidates = []
    for entry in entries or []:
        version, url = entry.get("content-version"), entry.get("URL")
        if not url or version not in _VERSION_ORDER:
            continue
        start = ((entry.get("start") or {}).get("date-time")) or ""
        try:
            started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        except ValueError:
            started = _EARLIEST
        if started <= now:
            candidates.append((version, started, str(url)))
    if not candidates:
        return None
    version, _, url = min(candidates, key=lambda c: (
        _VERSION_ORDER[c[0]], normalise_license(c[2]) in WEAK_LICENSES, -c[1].timestamp(), c[2]))
    return License.stated(LicenseSource.CROSSREF, url,
                          version=version if version in ("vor", "am") else None)


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _wait(retry_state) -> float:
    """Crossref's Retry-After when it sends one, else an exponential backoff."""
    response = getattr(retry_state.outcome.exception(), "response", None)
    after = response.headers.get("Retry-After", "") if response is not None else ""
    if after.strip().isdigit():
        return min(float(after), _MAX_RETRY_AFTER)
    return wait_exponential(multiplier=1, max=16)(retry_state)


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

    @retry(stop=stop_after_attempt(4), wait=_wait,
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

    @retry(stop=stop_after_attempt(4), wait=_wait,
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
        """Each asked work's licence, as a record holding only that, keyed by identifier slug.

        Every DOI asked gets a record, so that the answer is cached: a work with no
        licence deposit, or one Crossref does not know, has none. A filter value cannot
        hold a comma, so a DOI with one is asked for singly. A failed request raises
        `PartialAnswer` with the records of the requests that did not fail.
        """
        wanted: Dict[str, List[Identifier]] = {}
        for identifier in identifiers:
            doi = normalize_doi(identifier.doi) if identifier.doi else None
            if doi:
                wanted.setdefault(doi.lower(), []).append(identifier)
        answered: Dict[str, Optional[License]] = {}
        failures: List[str] = []
        batched = [doi for doi in wanted if "," not in doi]
        for index in range(0, len(batched), CROSSREF_DOI_BATCH_SIZE):
            batch = batched[index : index + CROSSREF_DOI_BATCH_SIZE]
            try:
                works = self._licenses(batch)
            except Exception as exc:  # noqa: BLE001 - the batch's articles are retried
                failures.append(f"{type(exc).__name__}: {exc}")
                continue
            answered.update(dict.fromkeys(batch))
            for work in works:
                doi = str(work.get("DOI") or "").lower()
                if doi in answered:
                    answered[doi] = license_from(work.get("license"))
        for doi in wanted:
            if "," in doi:
                try:
                    work = self.work(doi)
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{type(exc).__name__}: {exc}")
                    continue
                answered[doi] = license_from((work or {}).get("license"))
        results = {
            identifier.slug: ArticleMetadata(title="", license=found)
            for doi, found in answered.items() for identifier in wanted[doi]
        }
        if failures:
            raise PartialAnswer(f"Crossref: {failures[0]}", results)
        return results
