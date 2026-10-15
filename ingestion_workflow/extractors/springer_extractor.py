"""Full-text JATS from Springer Nature's Open Access API.

The API's ``openaccess/jats`` endpoint serves the JATS of the articles Springer
Nature publishes open access under a licence (gold, and hybrid with a licence),
and nothing else: a free-to-read article without one is not there. The JATS is
laid out as pubget lays out a PMC article and extracted by pubget's code, under
this source's name.
"""

from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Sequence

import httpx
from lxml import etree
from pubget._articles import _extract_tables

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import SourceExhausted, SourceMisconfigured
from ingestion_workflow.extractors.pdf_extractor import DOWNLOAD_TIMEOUT, _HostGate, _retry_after
from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifier, Identifiers
from ingestion_workflow.utils.progress import emit_progress

API_URL = "https://api.springernature.com/openaccess/jats"
HOST = "api.springernature.com"

#: The most DOIs one query may name: 20 are answered, 25 are refused with a 400.
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

#: The API serves the version of record.
PUBLISHED_VERSION = "publishedVersion"

#: The API's answer to a key it refuses.
KEY_REFUSED = 401

#: A query matching no open-access article is answered 404, and an article absent
#: from a batch's answer is not open access either: both are final.
NOT_OPEN_ACCESS = "Springer Nature: not open access (no full text)"

#: An article served without its body is its metadata alone: not a full text,
#: and not known to be final, so it is retried on the usual schedule.
NO_BODY = "Springer Nature served the article's metadata without its body"

#: Tries for a request answered 5xx or not answered, before its batch fails.
SERVER_ERROR_TRIES = 3

#: Every Springer extractor in the process takes its turn here, so the rate holds
#: across the download stage's batches; a 429 longer than a run waits (the daily
#: quota) leaves the API for the rest of the run.
GATE = _HostGate(time.monotonic, time.sleep, min_interval=REQUEST_INTERVAL_SECONDS)


def is_springer_doi(doi: Optional[str]) -> bool:
    return bool(doi) and doi.split("/", 1)[0].strip().lower() in SPRINGER_DOI_PREFIXES


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
        return is_springer_doi(identifier.doi)

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        """Each article's JATS, `BATCH_SIZE` DOIs to a request."""
        key = self.settings.springer_api_key
        if not key:
            raise SourceMisconfigured(
                "no Springer Nature API key: set SPRINGER_API_KEY, "
                "or remove springer from download_sources"
            )
        # A DOI is matched without regard to case, and asked for as written.
        by_doi: Dict[str, List[Identifier]] = {}
        for identifier in identifiers:
            by_doi.setdefault(identifier.doi.strip().lower(), []).append(identifier)
        dois = list(by_doi)
        results: List[DownloadResult] = []
        for start in range(0, len(dois), BATCH_SIZE):
            batch = dois[start:start + BATCH_SIZE]
            try:
                articles: Dict[str, etree._Element] = self._fetch(
                    [by_doi[doi][0].doi.strip() for doi in batch], key
                )
                failure = NOT_OPEN_ACCESS
            # Named by status or kind alone: httpx's message quotes the URL, key and all.
            except httpx.HTTPStatusError as exc:
                articles = {}
                failure = f"Springer Nature request failed: HTTP {exc.response.status_code}"
            except httpx.TransportError as exc:
                articles, failure = {}, f"Springer Nature request failed: {type(exc).__name__}"
            except SourceExhausted as exc:
                raise SourceExhausted(str(exc), results) from None
            for doi in batch:
                for identifier in by_doi[doi]:
                    article = articles.get(doi)
                    if article is None:
                        results.append(self._build_failure(identifier, failure))
                    elif article.find("body") is None:
                        results.append(self._build_failure(identifier, NO_BODY))
                    else:
                        results.append(self._stored(identifier, article))
                    emit_progress(progress_hook)
        return results

    def _fetch(self, dois: List[str], key: str) -> Dict[str, etree._Element]:
        """The open-access articles among `dois`, by lower-cased DOI."""
        params = {"q": doi_query(dois), "p": len(dois), "api_key": key}
        tries = 0
        while True:
            deferred = GATE.deferred(HOST)
            if deferred is not None:
                raise SourceExhausted(f"Springer Nature {deferred}", [])
            GATE.wait(HOST)
            tries += 1
            try:
                response = self._client.get(API_URL, params=params)
            except httpx.TransportError:
                if tries >= SERVER_ERROR_TRIES:
                    raise
                time.sleep(2.0**tries)
                continue
            status = response.status_code
            if status == KEY_REFUSED:
                raise SourceMisconfigured(
                    "Springer Nature refused the API key in SPRINGER_API_KEY (401)"
                )
            if status == 429:
                GATE.rate_limited(HOST, _retry_after(response))
                continue
            if status >= 500 and tries < SERVER_ERROR_TRIES:
                time.sleep(2.0**tries)
                continue
            GATE.answered(HOST)
            if status == 404:
                return {}
            response.raise_for_status()
            root = etree.fromstring(
                response.content, etree.XMLParser(huge_tree=True, resolve_entities=False)
            )
            articles = {}
            for article in root.iterfind("records/article"):
                doi = article.findtext("front/article-meta/article-id[@pub-id-type='doi']")
                if doi:
                    articles[doi.strip().lower()] = article
            return articles

    def _stored(self, identifier: Identifier, article: etree._Element) -> DownloadResult:
        """Write the article as pubget's layout of it: article.xml, and its tables."""
        article_dir = self._resolve_data_dir() / safe_hash_stem(identifier.slug)
        article_dir.mkdir(parents=True, exist_ok=True)
        article_dir.joinpath("article.xml").write_bytes(
            etree.tostring(article, encoding="UTF-8", xml_declaration=True)
        )
        _extract_tables(article_dir)
        result = self._build_success(identifier, article_dir, None)
        result.article_version = PUBLISHED_VERSION
        return result
