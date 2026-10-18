"""Full-text JATS from Springer Nature's Open Access API.

The API's ``openaccess/jats`` endpoint serves the JATS of the articles Springer
Nature publishes open access under a licence (gold, and hybrid with a licence),
and nothing else: a free-to-read article without one is not there. The JATS is
laid out as pubget lays out a PMC article and extracted by pubget's code, under
this source's name.
"""

from __future__ import annotations

import json
import time
from typing import Callable, Dict, List, Optional, Sequence, Union

import httpx
from lxml import etree
from pubget._articles import _extract_tables

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import SourceExhausted, SourceMisconfigured
from ingestion_workflow.extractors.pdf_extractor import DOWNLOAD_TIMEOUT, _HostGate, _retry_after
from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor
from ingestion_workflow.extractors.utils import doi_prefix, safe_hash_stem
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifier, Identifiers
from ingestion_workflow.utils.progress import emit_progress

API_URL = "https://api.springernature.com/openaccess/jats"
HOST = "api.springernature.com"

#: The most DOIs one query may name: 20 are answered, 25 are refused with a 400.
#: It is also the page size: a key like ours is refused a page of 50 (403, "a
#: premium feature").
BATCH_SIZE = 20

#: An Open Access API key may make 100 requests a minute (and 500 a day).
REQUEST_INTERVAL_SECONDS = 60 / 100

#: The DOI prefixes Crossref lists for Springer Nature, member 297
#: (https://api.crossref.org/members/297, 2026-10-15): Springer's, Nature's,
#: BioMed Central's, Kluwer's, Humana's, and those of the societies and imprints
#: whose DOIs Springer Nature deposits.
SPRINGER_DOI_PREFIXES = frozenset({
    "10.1007", "10.1013", "10.1023", "10.1038", "10.1057", "10.1065", "10.1114",
    "10.1140", "10.1186", "10.1208", "10.1245", "10.1251", "10.1361", "10.1365",
    "10.1379", "10.1381", "10.1385", "10.1557", "10.1617", "10.2165", "10.3758",
    "10.3858", "10.4056", "10.4076", "10.4333", "10.5052", "10.5819", "10.7123",
    "10.7603", "10.7759", "10.17269", "10.17504", "10.19150", "10.21203", "10.26777",
    "10.26778", "10.33283",
})


#: The API's answer to a key it refuses.
KEY_REFUSED = 401

#: What a 404 says when the query matched no open-access article. Any other 404
#: (a gateway's, a moved path's) says nothing about the articles.
NO_DATA = "No data was found"

#: An article not returned is not open access there: final, as for PMC.
NOT_OPEN_ACCESS = "Springer Nature: not open access (no full text)"

#: An article served without its body is its metadata alone: not a full text,
#: and not known to be final, so it is retried on the usual schedule.
NO_BODY = "Springer Nature served the article's metadata without its body"

#: A DOI the API refuses to be queried by (400) even alone: final.
QUERY_REFUSED = "Springer Nature: query refused for this DOI (HTTP 400)"

#: The API refusing every DOI of a query even alone is the query's fault, not the
#: DOIs': the source is left for the run, with nothing charged.
QUERIES_REFUSED = "refused every DOI of a query, each asked alone (400)"

#: Tries for a request answered 5xx or not answered, before its batch fails.
SERVER_ERROR_TRIES = 3

#: Every Springer extractor in the process takes its turn here, so the rate holds
#: across the download stage's batches. The quota spent (a 403, or a 429 longer
#: than a run waits) leaves the API for the rest of the run.
GATE = _HostGate(time.monotonic, time.sleep, min_interval=REQUEST_INTERVAL_SECONDS)

#: Dashes a manifest may hold in place of a DOI's hyphen (10.1007/s10339‐004‐0024‐0).
_DASHES = dict.fromkeys(map(ord, "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"), "-")


class _BatchFailed(Exception):
    """The API gave no usable answer for this batch: its articles fail, and are retried."""


class _QueryRefused(Exception):
    """The API refused the query (400): one of its DOIs cannot be asked."""


def query_doi(doi: str) -> str:
    """The DOI as the query names it: hyphens for look-alike dashes, and no
    backslash (a Markdown escape, 10.1007/978-981-33-6044-0\\_18) or quote."""
    return doi.strip().translate(_DASHES).replace("\\", "").replace('"', "")


def doi_key(doi: str) -> str:
    """What a DOI asked for and a DOI answered are matched by: DOIs ignore case."""
    return query_doi(doi).lower()


def doi_query(dois: Sequence[str]) -> str:
    """The query naming each DOI. Quoted, since an unquoted ':' (10.1023/A:...) is
    read as a field and fails the whole query."""
    return " OR ".join(f'doi:"{doi}"' for doi in dois)


class SpringerExtractor(PubgetExtractor):
    """Download Springer Nature's open-access articles' JATS by DOI."""

    SOURCE = DownloadSource.SPRINGER

    def __init__(self, settings: Settings | None = None) -> None:
        super().__init__(settings)
        self._client = httpx.Client(timeout=DOWNLOAD_TIMEOUT)

    def can_address(self, identifier: Identifier) -> bool:
        return doi_prefix(identifier.doi) in SPRINGER_DOI_PREFIXES

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        """Each article's JATS, `BATCH_SIZE` DOIs to a request. A batch that fails
        fails its own articles only."""
        key = self.settings.springer_api_key
        if not key:
            raise SourceMisconfigured(
                "no Springer Nature API key: set SPRINGER_API_KEY, "
                "or remove springer from download_sources"
            )
        by_key: Dict[str, List[Identifier]] = {}
        for identifier in identifiers:
            by_key.setdefault(doi_key(identifier.doi), []).append(identifier)
        keys = list(by_key)
        results: List[DownloadResult] = []
        for start in range(0, len(keys), BATCH_SIZE):
            batch = keys[start:start + BATCH_SIZE]
            try:
                answers = self._ask([query_doi(by_key[k][0].doi) for k in batch], key)
                if len(batch) > 1 and all(a == QUERY_REFUSED for a in answers.values()):
                    raise SourceExhausted(
                        f"Springer Nature {GATE.defer(HOST, QUERIES_REFUSED)}", []
                    )
            except SourceExhausted as exc:
                raise SourceExhausted(str(exc), results) from None
            for k in batch:
                answer = answers[k]
                for identifier in by_key[k]:
                    if isinstance(answer, str):
                        results.append(self._build_failure(identifier, answer))
                    else:
                        results.append(self._stored(identifier, answer))
                    emit_progress(progress_hook)
        return results

    def _ask(self, dois: List[str], key: str) -> Dict[str, Union[etree._Element, str]]:
        """Each DOI's article, or why it has none, by `doi_key`. A refused query is
        halved until the DOI it cannot name is alone, so the others are still asked."""
        try:
            articles = self._fetch(dois, key)
        except _QueryRefused:
            if len(dois) == 1:
                return {doi_key(dois[0]): QUERY_REFUSED}
            half = len(dois) // 2
            return {**self._ask(dois[:half], key), **self._ask(dois[half:], key)}
        except _BatchFailed as exc:
            return dict.fromkeys(map(doi_key, dois), str(exc))
        answers: Dict[str, Union[etree._Element, str]] = {}
        for doi in dois:
            article = articles.get(doi_key(doi))
            if article is None:
                answers[doi_key(doi)] = NOT_OPEN_ACCESS
            elif article.find("body") is None:
                answers[doi_key(doi)] = NO_BODY
            else:
                answers[doi_key(doi)] = article
        return answers

    def _fetch(self, dois: List[str], key: str) -> Dict[str, etree._Element]:
        """The open-access articles among `dois`, by `doi_key`, every page of them.

        Of two records for one DOI the first with a body is kept, else the first."""
        articles: Dict[str, etree._Element] = {}
        start = 1
        while True:
            root = self._page(dois, key, start)
            if root is None:
                return articles
            records = root.findall("records/article")
            for article in records:
                doi = article.findtext("front/article-meta/article-id[@pub-id-type='doi']")
                if not doi:
                    continue
                kept = articles.get(doi_key(doi))
                has_body = article.find("body") is not None
                if kept is None or (kept.find("body") is None and has_body):
                    articles[doi_key(doi)] = article
            start += len(records)
            try:
                total = int(root.findtext("result/total") or 0)
            except ValueError:
                raise _BatchFailed("Springer Nature sent an answer without its count") from None
            if not records or start > total:
                return articles

    def _page(self, dois: List[str], key: str, start: int) -> Optional[etree._Element]:
        """One page of the answer to the query naming `dois`; None when nothing matched.

        Failure messages name a status or an error kind only: httpx's quote the URL,
        key and all."""
        params = {"q": doi_query(dois), "p": len(dois), "api_key": key}
        if start > 1:
            params["s"] = start
        # Server errors and dropped connections only: a 429 is the gate's to count.
        failures = 0
        while True:
            deferred = GATE.deferred(HOST)
            if deferred is not None:
                raise SourceExhausted(f"Springer Nature {deferred}", [])
            GATE.wait(HOST)
            try:
                response = self._client.get(API_URL, params=params)
            except httpx.HTTPError as exc:
                failures += 1
                if failures < SERVER_ERROR_TRIES:
                    time.sleep(2.0**failures)
                    continue
                raise _BatchFailed(
                    f"Springer Nature request failed: {type(exc).__name__}"
                ) from None
            status = response.status_code
            if status == KEY_REFUSED:
                raise SourceMisconfigured(
                    "Springer Nature refused the API key in SPRINGER_API_KEY (401)"
                )
            if status == 429:
                GATE.rate_limited(HOST, _retry_after(response))
                continue
            if status == 403:
                # A key the API knows, refused: an account's answer, not an article's.
                # Taken as the day's quota, which no documentation says how it is told.
                reason = GATE.defer(
                    HOST, f"refused this key (403): {_api_message(response, key)}"
                )
                raise SourceExhausted(f"Springer Nature {reason}", [])
            if status >= 500:
                failures += 1
                if failures < SERVER_ERROR_TRIES:
                    time.sleep(2.0**failures)
                    continue
            GATE.answered(HOST)
            if status == 400:
                raise _QueryRefused()
            if status == 404:
                if NO_DATA in response.text:
                    return None
                # Worded without the status: a "404" in a message reads as final.
                raise _BatchFailed(
                    "Springer Nature request failed: not found, without saying it found no data"
                )
            if status != 200:
                raise _BatchFailed(f"Springer Nature request failed: HTTP status {status}")
            try:
                return etree.fromstring(
                    response.content, etree.XMLParser(huge_tree=True, resolve_entities=False)
                )
            except etree.XMLSyntaxError:
                raise _BatchFailed("Springer Nature sent an answer that is not XML") from None

    def _stored(self, identifier: Identifier, article: etree._Element) -> DownloadResult:
        """Write the article as pubget's layout of it: article.xml, and its tables."""
        article_dir = self._resolve_data_dir() / safe_hash_stem(identifier.slug)
        article_dir.mkdir(parents=True, exist_ok=True)
        article_dir.joinpath("article.xml").write_bytes(
            etree.tostring(article, encoding="UTF-8", xml_declaration=True)
        )
        _extract_tables(article_dir)
        return self._build_success(identifier, article_dir, None)


def _api_message(response: httpx.Response, key: str) -> str:
    """The API's own words for an answer, short, and without the key."""
    try:
        message = str(json.loads(response.text).get("message") or "")
    except (ValueError, AttributeError):
        message = response.text
    return (message.replace(key, "***")[:200] or f"HTTP {response.status_code}").strip()
