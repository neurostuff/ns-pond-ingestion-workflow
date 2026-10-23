"""What the DOI registration agencies say of a DOI.

doi.org's citation (CSL) record is answered by whichever agency registered the DOI:
Crossref, DataCite or mEDRA. Crossref's own record carries the relations between a
preprint and its version of record.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .crossref import CROSSREF_LIST_REQUEST_LIMIT, CROSSREF_REQUEST_LIMIT, CrossrefClient
from .pubmed import in_supplement

logger = logging.getLogger(__name__)

CSL = "application/vnd.citationstyles.csl+json"


def _bad_certificate(exc: BaseException) -> bool:
    """An agency serving a certificate for another host: it will not be read, however
    often asked."""
    return isinstance(exc, httpx.ConnectError) and "CERTIFICATE_VERIFY_FAILED" in str(exc)


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError) and not _bad_certificate(exc)


class Unreadable(Exception):
    """The registry cannot say whether it has the DOI."""


@dataclass(frozen=True)
class RegisteredWork:
    """The agency's citation record of a DOI, as far as it tells one work from another."""

    title: str
    #: The first author's family name, or a collective author's name.
    first_author: Optional[str]
    year: Optional[int]
    #: The CSL type: article-journal, chapter, posted-content, ...
    type: Optional[str]
    #: The journal's ISSNs, and its name.
    issns: Tuple[str, ...] = ()
    journal: Optional[str] = None
    #: Published in a supplement issue: a meeting's abstracts.
    supplement: bool = False


def _first(value) -> Optional[str]:
    if isinstance(value, list):
        value = value[0] if value else None
    return value or None


def _registered(csl: Dict) -> RegisteredWork:
    title = csl.get("title")
    if isinstance(title, list):
        title = title[0] if title else None
    authors = csl.get("author") or []
    first = next((a.get("family") or a.get("literal") or a.get("name") for a in authors
                  if a.get("family") or a.get("literal") or a.get("name")), None)
    year = None
    for key in ("issued", "published-print", "published-online", "created"):
        parts = (csl.get(key) or {}).get("date-parts") or [[None]]
        if parts and parts[0] and isinstance(parts[0][0], int):
            year = parts[0][0]
            break
    issns = csl.get("ISSN") or ()
    return RegisteredWork(title=title or "", first_author=first, year=year, type=csl.get("type"),
                          issns=tuple([issns] if isinstance(issns, str) else issns),
                          journal=_first(csl.get("container-title")),
                          supplement=in_supplement(csl.get("issue"), csl.get("volume"),
                                                   csl.get("page")))


#: The most redirects doi.org and an agency may send one request through.
MAX_REDIRECTS = 5
_CROSSREF_WORKS = "https://api.crossref.org/works"


class DoiRegistry:
    def __init__(self, crossref: CrossrefClient) -> None:
        self._crossref = crossref
        # No contact email: doi.org sends a request on to whichever agency registered the
        # DOI, some of them over plain HTTP.
        self._client = httpx.Client(timeout=30, follow_redirects=False,
                                    headers={"User-Agent": "ns-pond-ingestion-workflow"})
        self._lock = threading.Lock()
        self._last = 0.0

    def _pace(self, limit: int = CROSSREF_REQUEST_LIMIT) -> None:
        # doi.org hands a Crossref DOI's record to Crossref, which limits a caller's rate,
        # and its list queries' more.
        with self._lock:
            wait = self._last + 1 / limit - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    def close(self) -> None:
        self._client.close()

    def work(self, doi: str) -> Optional[RegisteredWork]:
        """What the registering agency says of the DOI's work; None when no agency has
        the DOI. Raises Unreadable when the agency cannot be read for it."""
        try:
            return self._work(doi)
        except httpx.ConnectError as exc:
            if not _bad_certificate(exc):
                raise
            raise Unreadable(f"{doi}: the agency's site serves a bad certificate") from exc

    @retry(retry=retry_if_exception(_retryable), stop=stop_after_attempt(6),
           wait=wait_exponential(multiplier=1, max=30), reraise=True)
    def _work(self, doi: str) -> Optional[RegisteredWork]:
        self._pace()
        url = httpx.URL("https://doi.org/" + quote(doi, safe="/"))
        for hop in range(MAX_REDIRECTS + 1):
            response = self._client.get(url, headers={"Accept": CSL})
            if not response.is_redirect:
                break
            url = url.join(response.headers["location"])
            if url.scheme != "https":
                raise Unreadable(f"{doi}: doi.org sends it to {url.host} over plain HTTP")
        else:
            raise Unreadable(f"{doi}: doi.org redirects it more than {MAX_REDIRECTS} times")
        if response.status_code == 404:
            # doi.org answers for a DOI no agency registered; a 404 from the agency it
            # sent us to says nothing.
            if hop:
                raise Unreadable(f"{doi}: doi.org sends it to {url.host}, which has no record")
            return None
        response.raise_for_status()
        return _registered(response.json())

    @retry(retry=retry_if_exception(_retryable), stop=stop_after_attempt(6),
           wait=wait_exponential(multiplier=1, max=30), reraise=True)
    def relating(self, doi: str) -> Dict[str, List[str]]:
        """The DOIs whose Crossref records relate them to this one, by relation type: the
        other side of a relation the DOI's own record may not give back (a preprint's
        is-preprint-of its paper without the paper's has-preprint)."""
        self._pace(CROSSREF_LIST_REQUEST_LIMIT)
        response = self._client.get(_CROSSREF_WORKS, params={
            "filter": f"relation.object:{doi}", "select": "DOI,relation", "rows": "100"})
        response.raise_for_status()
        found: Dict[str, List[str]] = {}
        for item in response.json()["message"]["items"]:
            for kind, related in (item.get("relation") or {}).items():
                if any((r.get("id") or "").lower() == doi.lower() for r in related):
                    found.setdefault(kind, []).append(item["DOI"].lower())
        return found

    def relations(self, doi: str) -> Dict[str, List[str]]:
        """The DOIs Crossref relates this one to, by relation type; {} for a DOI it lacks."""
        work = self._crossref.work(doi) or {}
        return {kind: [r["id"].lower() for r in related
                       if r.get("id-type") == "doi" and r.get("id")]
                for kind, related in (work.get("relation") or {}).items()}
