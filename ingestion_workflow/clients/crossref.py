"""Crossref's REST API: one work's record, which holds its deposited reference list,
and the licences publishers deposit, for many works at once."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests
from study_schema.models.paper_parse import MetadataSource
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.http import transient

#: The polite pool's single-record limit. Sending an email puts a client in it; there is no key.
CROSSREF_REQUEST_LIMIT = 10
#: Its limit on list queries (`x-rate-limit-limit: 3` on /works?filter=, 2026-10-16).
CROSSREF_LIST_REQUEST_LIMIT = 3
#: DOIs per `filter=doi:...` list query: one page, and a URL well under any server's limit.
CROSSREF_DOI_BATCH_SIZE = 50
#: Records per page of a cursor-paged list query.
CROSSREF_PAGE_ROWS = 1000
#: The longest a Retry-After is waited out before the request counts as failed.
_MAX_RETRY_AFTER = 120


def _wait(retry_state) -> float:
    """Crossref's Retry-After when it sends one, else an exponential backoff."""
    response = getattr(retry_state.outcome.exception(), "response", None)
    after = response.headers.get("Retry-After", "") if response is not None else ""
    if after.strip().isdigit():
        return min(float(after), _MAX_RETRY_AFTER)
    return wait_exponential(multiplier=1, max=16)(retry_state)


@dataclass(frozen=True)
class CrossrefWork:
    """What a DOI's own Crossref record says of what it is and what it is related to."""

    doi: str
    #: The DOI as its publisher spells it (`alternative-id`), when that differs only in case:
    #: Crossref matches the DOI a relation names by case.
    spellings: Tuple[str, ...]
    #: Crossref's type: journal-article, posted-content, proceedings-article, ...
    type: Optional[str]
    #: A posted-content work's subtype: `preprint`, or working_paper, report, dissertation, ...
    subtype: Optional[str]
    #: The DOIs this one is related to, by relation type (is-preprint-of, has-version, ...).
    relations: Dict[str, Tuple[str, ...]]
    #: The works this one is a Crossmark update to, as (DOI, update type): the work a
    #: correction or a retraction is of, or the earlier edition a new version replaces.
    updates: Tuple[Tuple[str, str], ...]


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

    @retry(stop=stop_after_attempt(4), wait=_wait, retry=retry_if_exception(transient),
           reraise=True)
    def work(self, doi: str) -> Optional[Dict]:
        """The work's `message`, or None when Crossref does not know the DOI."""
        self._rate_limit_sleep()
        params = {"mailto": self.email} if self.email else {}
        response = self._session.get(self.BASE_URL + quote(doi.strip(), safe="/"), params=params, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get("message")

    def record(self, doi: str) -> Optional[CrossrefWork]:
        """The work's type, relations and updates; None when Crossref does not know the DOI."""
        message = self.work(doi)
        if message is None:
            return None
        return CrossrefWork(
            doi=message["DOI"].lower(),
            spellings=tuple(a for a in message.get("alternative-id") or ()
                            if a.lower() == message["DOI"].lower()),
            type=message.get("type"), subtype=message.get("subtype"),
            relations={kind: tuple(r["id"].lower() for r in related
                                   if r.get("id-type") == "doi" and r.get("id"))
                       for kind, related in (message.get("relation") or {}).items()},
            updates=tuple((u["DOI"].lower(), u.get("type") or "")
                          for u in message.get("update-to") or () if u.get("DOI")))

    @retry(stop=stop_after_attempt(4), wait=_wait, retry=retry_if_exception(transient),
           reraise=True)
    def _licenses(self, dois: List[str]) -> List[Dict]:
        """The `DOI` and `license` of each work Crossref knows among `dois`."""
        self._rate_limit_sleep(CROSSREF_LIST_REQUEST_LIMIT)
        params = {"filter": ",".join(f"doi:{doi}" for doi in dois), "select": "DOI,license",
                  "rows": str(len(dois))}
        response = self._session.get(self.BASE_URL.rstrip("/"), params=params, timeout=60)
        response.raise_for_status()
        return response.json()["message"]["items"]

    @retry(stop=stop_after_attempt(4), wait=_wait, retry=retry_if_exception(transient),
           reraise=True)
    def _page(self, params: Dict) -> Dict:
        self._rate_limit_sleep(CROSSREF_LIST_REQUEST_LIMIT)
        response = self._session.get(self.BASE_URL.rstrip("/"), params=params, timeout=60)
        response.raise_for_status()
        return response.json()["message"]

    def relating(self, doi: str, kinds: Sequence[str]) -> Dict[str, List[str]]:
        """The DOIs whose records relate them to this one by one of `kinds`, by kind: the
        other side of a relation the DOI's own record may not give back (a preprint's
        is-preprint-of its paper without the paper's has-preprint)."""
        params = {"filter": ",".join([f"relation.object:{doi}",
                                      *(f"relation.type:{kind}" for kind in kinds)]),
                  "select": "DOI,relation", "rows": str(CROSSREF_PAGE_ROWS), "cursor": "*"}
        if self.email:
            params["mailto"] = self.email
        found: Dict[str, List[str]] = {}
        while True:
            message = self._page(params)
            for item in message["items"]:
                for kind, related in (item.get("relation") or {}).items():
                    if kind in kinds and any((r.get("id") or "").lower() == doi.lower()
                                             for r in related):
                        found.setdefault(kind, []).append(item["DOI"].lower())
            if len(message["items"]) < CROSSREF_PAGE_ROWS:
                return found
            params["cursor"] = message["next-cursor"]

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """Each asked work's licence deposits, verbatim in `raw_metadata["crossref"]`,
        keyed by identifier slug.

        Every DOI asked gets a record, so that the answer is cached: a work with no
        licence deposit, or one Crossref does not know, has an empty list. A filter value
        cannot hold a comma, so a DOI with one is asked for singly, as is a DOI a filter
        query did not return. A failed request raises `PartialAnswer` with the records of
        the requests that did not fail.
        """
        wanted: Dict[str, List[Identifier]] = {}
        for identifier in identifiers:
            doi = normalize_doi(identifier.asking_doi)
            if doi:
                wanted.setdefault(doi, []).append(identifier)
        answered: Dict[str, List[Dict]] = {}
        failures: List[BaseException] = []
        failed: set = set()
        batched = [doi for doi in wanted if "," not in doi]
        for index in range(0, len(batched), CROSSREF_DOI_BATCH_SIZE):
            batch = batched[index : index + CROSSREF_DOI_BATCH_SIZE]
            try:
                works = self._licenses(batch)
            except Exception as exc:  # noqa: BLE001 - the batch's articles are retried
                failures.append(exc)
                failed.update(batch)
                continue
            for work in works:
                doi = normalize_doi(work.get("DOI"))
                if doi in batch:
                    answered[doi] = list(work.get("license") or [])
        # A DOI a filter query leaves out is asked for singly: Crossref may know it under
        # a spelling the filter does not match.
        for doi in wanted:
            if doi not in answered and doi not in failed:
                try:
                    work = self.work(doi)
                except Exception as exc:  # noqa: BLE001
                    failures.append(exc)
                    continue
                answered[doi] = list((work or {}).get("license") or [])
        results = {
            identifier.slug: ArticleMetadata(
                title="", source=MetadataSource.crossref.value,
                raw_metadata={"crossref": {"license": entries}})
            for doi, entries in answered.items() for identifier in wanted[doi]
        }
        if failures:
            first = failures[0]
            raise PartialAnswer(f"Crossref: {type(first).__name__}: {first}", results, failures)
        return results
