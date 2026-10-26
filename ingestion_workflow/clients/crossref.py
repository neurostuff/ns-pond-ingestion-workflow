"""Crossref's REST API: one work's record, which holds its deposited reference list,
and, for many works at once, their records, the works relating to them, and the
licences publishers deposit."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests
from study_schema.models.paper_parse import MetadataSource
from tenacity import wait_exponential

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.http import Pacer, retry_transient

#: The polite pool's single-record limit. Sending an email puts a client in it; there is no key.
CROSSREF_REQUEST_LIMIT = 10
#: Its limit on list queries (`x-rate-limit-limit: 3` on /works?filter=, 2026-10-16).
CROSSREF_LIST_REQUEST_LIMIT = 3
#: Bytes of `filter` values one list query carries. Crossref refuses a request line past
#: about 8,000 bytes (414, or 431 with the headers; 183 `doi:` values of 40 characters
#: passed and 184 did not, 2026-10-26), so values are packed by size, not counted.
CROSSREF_FILTER_BYTES = 6000
#: Records per page of a cursor-paged list query.
CROSSREF_PAGE_ROWS = 1000
#: The longest a Retry-After is waited out before the request counts as failed.
_MAX_RETRY_AFTER = 120


#: What a `doi:` filter takes: Crossref refuses the whole query (400 doi-not-valid) for one
#: value whose prefix has fewer than 4 digits; a comma would end the filter.
_FILTERABLE_DOI = re.compile(r"^10\.\d{4,}/[^,]+$")


def filterable(doi: str) -> bool:
    """Whether the DOI can go in a `doi:` filter; one that cannot is asked for singly."""
    return bool(_FILTERABLE_DOI.match(doi))


def packed(key: str, values: Sequence[str], spare: int = 0) -> Iterator[List[str]]:
    """`values` in order, in runs whose `key:value` filters fill at most
    `CROSSREF_FILTER_BYTES - spare` bytes of URL. A value cannot hold a comma, which
    separates filters; the caller asks for such a value singly."""
    budget = CROSSREF_FILTER_BYTES - spare
    run: List[str] = []
    size = 0
    for value in values:
        cost = len(quote(f"{key}:{value},", safe=""))
        if run and size + cost > budget:
            yield run
            run, size = [], 0
        run.append(value)
        size += cost
    if run:
        yield run


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
        self._pace = Pacer()

    @retry_transient(4, 16, wait=_wait)
    def work(self, doi: str) -> Optional[Dict]:
        """The work's `message`, or None when Crossref does not know the DOI."""
        self._pace.wait(1 / CROSSREF_REQUEST_LIMIT)
        params = {"mailto": self.email} if self.email else {}
        response = self._session.get(self.BASE_URL + quote(doi.strip(), safe="/"), params=params, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json().get("message")

    def record(self, doi: str) -> Optional[CrossrefWork]:
        """The work's type, relations and updates; None when Crossref does not know the DOI."""
        message = self.work(doi)
        return None if message is None else _work_of(message)

    def records(self, dois: Sequence[str]) -> Dict[str, CrossrefWork]:
        """The records of many DOIs by list queries, keyed by the DOI in lower case.

        A list query cannot return `subtype`, which tells a posted-content preprint from a
        working paper, so a posted-content work is left out, as is a DOI a filter cannot
        hold and one Crossref does not know: `record` asks for each of those singly.
        """
        found = self.works(dois, "DOI,type,relation,update-to,alternative-id")
        return {doi: _work_of(item) for doi, item in found.items()
                if item.get("type") != "posted-content"}

    def works(self, dois: Sequence[str], select: str) -> Dict[str, Dict]:
        """The `select`ed fields of each of many DOIs' records, by list queries, keyed by
        the DOI in lower case. A DOI a filter cannot hold, or one Crossref does not know,
        is left out."""
        found: Dict[str, Dict] = {}
        for batch in packed("doi", [d for d in dict.fromkeys(dois) if filterable(d)]):
            for item in self._list({"filter": ",".join(f"doi:{d}" for d in batch),
                                    "select": select, "rows": str(len(batch))}):
                found[item["DOI"].lower()] = item
        return found

    @retry_transient(4, 16, wait=_wait)
    def _page(self, params: Dict) -> Dict:
        self._pace.wait(1 / CROSSREF_LIST_REQUEST_LIMIT)
        response = self._session.get(self.BASE_URL.rstrip("/"), params=params, timeout=60)
        response.raise_for_status()
        return response.json()["message"]

    def _list(self, params: Dict) -> List[Dict]:
        """The items of a list query that fits one page."""
        return self._page(params)["items"]

    def relating(self, dois: Sequence[str],
                 kinds: Sequence[str]) -> Dict[str, Dict[str, List[str]]]:
        """For each of `dois`, the DOIs whose records relate them to it by one of `kinds`, by
        kind: the other side of a relation a DOI's own record may not give back (a
        preprint's is-preprint-of its paper without the paper's has-preprint).

        Crossref matches a related DOI by case, so each spelling is asked as given and
        answered under it. Many are asked in one query, the `relation.object` filters
        being alternatives; a DOI with a comma cannot be put in a filter and has no answer.
        """
        found: Dict[str, Dict[str, List[str]]] = {d: {} for d in dois}
        types = [f"relation.type:{kind}" for kind in kinds]
        spare = len(quote(",".join(types), safe=""))
        for batch in packed("relation.object", [d for d in found if "," not in d], spare):
            asked: Dict[str, List[str]] = {}
            for doi in batch:
                asked.setdefault(doi.lower(), []).append(doi)
            params = {"filter": ",".join([*(f"relation.object:{d}" for d in batch), *types]),
                      "select": "DOI,relation", "rows": str(CROSSREF_PAGE_ROWS), "cursor": "*"}
            if self.email:
                params["mailto"] = self.email
            while True:
                message = self._page(params)
                for item in message["items"]:
                    for kind, related in (item.get("relation") or {}).items():
                        if kind not in kinds:
                            continue
                        # a record may name one DOI twice under a kind; it is related once
                        named = {doi for r in related
                                 for doi in asked.get((r.get("id") or "").lower(), ())}
                        for doi in sorted(named):
                            found[doi].setdefault(kind, []).append(item["DOI"].lower())
                if len(message["items"]) < CROSSREF_PAGE_ROWS:
                    break
                params["cursor"] = message["next-cursor"]
        return found

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """Each asked work's licence deposits, verbatim in `raw_metadata["crossref"]`,
        keyed by identifier slug.

        Every DOI asked gets a record, so that the answer is cached: a work with no
        licence deposit, or one Crossref does not know, has an empty list. A DOI a filter
        cannot hold (`filterable`) is asked for singly, as is a DOI a filter query did not
        return. A failed request raises `PartialAnswer` with the records of
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
        for batch in packed("doi", [doi for doi in wanted if filterable(doi)]):
            try:
                works = self._list({"filter": ",".join(f"doi:{doi}" for doi in batch),
                                    "select": "DOI,license", "rows": str(len(batch))})
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


def _work_of(message: Dict) -> CrossrefWork:
    """A work's record, from its `message` (a list query's item has no `subtype`)."""
    doi = message["DOI"].lower()
    return CrossrefWork(
        doi=doi,
        spellings=tuple(a for a in message.get("alternative-id") or () if a.lower() == doi),
        type=message.get("type"), subtype=message.get("subtype"),
        relations={kind: tuple(r["id"].lower() for r in related
                               if r.get("id-type") == "doi" and r.get("id"))
                   for kind, related in (message.get("relation") or {}).items()},
        updates=tuple((u["DOI"].lower(), u.get("type") or "")
                      for u in message.get("update-to") or () if u.get("DOI")))
