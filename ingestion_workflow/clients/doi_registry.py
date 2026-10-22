"""What the DOI registration agencies say of a DOI.

doi.org's citation (CSL) record is answered by whichever agency registered the DOI:
Crossref, DataCite or mEDRA. Crossref's own record carries the relations between a
preprint and its version of record.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional
from urllib.parse import quote

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .crossref import CROSSREF_REQUEST_LIMIT, CrossrefClient

logger = logging.getLogger(__name__)

CSL = "application/vnd.citationstyles.csl+json"


def _bad_certificate(exc: BaseException) -> bool:
    """An agency serving a certificate for another host: it will not be read, however often asked."""
    return isinstance(exc, httpx.ConnectError) and "CERTIFICATE_VERIFY_FAILED" in str(exc)


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError) and not _bad_certificate(exc)


class DoiRegistry:
    def __init__(self, crossref: CrossrefClient) -> None:
        self._crossref = crossref
        agent = "ns-pond-ingestion-workflow" + (f" (mailto:{crossref.email})" if crossref.email else "")
        self._client = httpx.Client(timeout=30, follow_redirects=True,
                                    headers={"Accept": CSL, "User-Agent": agent})
        self._lock = threading.Lock()
        self._last = 0.0

    def _pace(self) -> None:
        # doi.org hands a Crossref DOI's record to Crossref, which limits a caller's rate.
        with self._lock:
            wait = self._last + 1 / CROSSREF_REQUEST_LIMIT - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    def close(self) -> None:
        self._client.close()

    def title(self, doi: str) -> Optional[str]:
        """The title the registering agency gives the DOI; None when no agency has it, or
        the agency's site cannot be read for its certificate."""
        try:
            return self._title(doi)
        except httpx.ConnectError as exc:
            if not _bad_certificate(exc):
                raise
            logger.warning("doi registry: %s: the agency's site cannot be read: %s", doi, exc)
            return None

    @retry(retry=retry_if_exception(_retryable), stop=stop_after_attempt(6),
           wait=wait_exponential(multiplier=1, max=30), reraise=True)
    def _title(self, doi: str) -> Optional[str]:
        self._pace()
        response = self._client.get("https://doi.org/" + quote(doi, safe="/"))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        title = response.json().get("title")
        if isinstance(title, list):
            title = title[0] if title else None
        return title or ""

    def relations(self, doi: str) -> Dict[str, List[str]]:
        """The DOIs Crossref relates this one to, by relation type; {} for a DOI it lacks."""
        work = self._crossref.work(doi) or {}
        return {kind: [r["id"].lower() for r in related if r.get("id-type") == "doi" and r.get("id")]
                for kind, related in (work.get("relation") or {}).items()}
