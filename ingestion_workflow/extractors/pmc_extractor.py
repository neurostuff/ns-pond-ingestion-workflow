"""PMC full text from NCBI and from Europe PMC.

Pubget asks PMC for its Open Access subset only, so an article PMC would hand
over anyway -- an author manuscript deposited under a funder mandate -- never
reaches the pipeline. These sources fetch the XML directly, keep the articles
that came with a body, and hand them to pubget's own article and table
splitting, so extraction is pubget's, unchanged, under their own source name.
"""

from __future__ import annotations

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import requests
from lxml import etree
from pubget._articles import extract_articles

from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifiers

logger = logging.getLogger(__name__)

#: A fetched article without a body is front matter only: PMC answers that way
#: when the publisher does not allow the full text to be downloaded.
RESTRICTED = "publisher does not allow full-text download"


def has_body(article: etree._Element) -> bool:
    return article.find(".//body") is not None


class _PmcXmlExtractor(PubgetExtractor):
    """Fetch PMC-format articles, then lay them out and extract them as pubget does."""

    #: Articles per articleset file and per request where a source takes many.
    batch_size = 100

    def _fetch(self, pmcids: List[str]) -> Tuple[Dict[str, etree._Element], Dict[str, str]]:
        """Articles with a body by PMCID, and a reason for every PMCID without one."""
        raise NotImplementedError

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        if not identifiers:
            return []
        pmcid_map: Dict[str, List[int]] = {}
        for index, identifier in enumerate(identifiers.identifiers):
            normalized = self._normalize_pmcid(identifier.pmcid)
            if normalized is not None:
                pmcid_map.setdefault(normalized, []).append(index)

        results: Dict[int, DownloadResult] = {}
        try:
            articles, reasons = self._fetch(sorted(pmcid_map))
        except Exception as exc:  # a source outage must not lose the batch
            articles, reasons = {}, dict.fromkeys(pmcid_map, f"{self.SOURCE.value} request failed: {exc}")

        article_index: Dict[str, Path] = {}
        if articles:
            articlesets_dir = self._write_articlesets(articles)
            articles_dir, _ = extract_articles(articlesets_dir, n_jobs=max(1, self.settings.max_workers))
            article_index = self._index_articles(articles_dir)

        for pmcid, indices in pmcid_map.items():
            article_dir = article_index.get(pmcid)
            for idx in indices:
                identifier = identifiers.identifiers[idx]
                if article_dir is not None:
                    results[idx] = self._build_success(identifier, article_dir, None)
                else:
                    results[idx] = self._build_failure(
                        identifier, reasons.get(pmcid, f"{self.SOURCE.value} returned no article")
                    )
        return self._ordered_results(
            identifiers,
            results,
            lambda identifier: self._build_failure(identifier, "no PMCID to request"),
            progress_hook=progress_hook,
        )

    def _write_articlesets(self, articles: Dict[str, etree._Element]) -> Path:
        """Write the articles as pubget's bulk-download files, in a directory named by the set."""
        pmcids = sorted(articles, key=int)
        digest = hashlib.md5(",".join(pmcids).encode()).hexdigest()
        articlesets_dir = self._resolve_data_dir() / f"pmcidList_{digest}" / "articlesets"
        articlesets_dir.mkdir(parents=True, exist_ok=True)
        for n, start in enumerate(range(0, len(pmcids), self.batch_size)):
            batch = etree.Element("pmc-articleset")
            for pmcid in pmcids[start:start + self.batch_size]:
                batch.append(articles[pmcid])
            articlesets_dir.joinpath(f"articleset_{n:05d}.xml").write_bytes(
                etree.tostring(batch, encoding="UTF-8", xml_declaration=True)
            )
        # pubget skips a step whose input it believes is incomplete
        articlesets_dir.joinpath("info.json").write_text('{"is_complete": true, "name": "download"}')
        return articlesets_dir


class PmcExtractor(_PmcXmlExtractor):
    """NCBI's PMC efetch: the Open Access subset and author manuscripts."""

    SOURCE = DownloadSource.PMC
    EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    def _fetch(self, pmcids: List[str]) -> Tuple[Dict[str, etree._Element], Dict[str, str]]:
        articles: Dict[str, etree._Element] = {}
        reasons: Dict[str, str] = {}
        # E-utilities allow 3 requests a second, 10 with an API key
        interval = 0.11 if self.settings.pubmed_api_key else 0.34
        session = requests.Session()
        for start in range(0, len(pmcids), self.batch_size):
            chunk = pmcids[start:start + self.batch_size]
            data = {"db": "pmc", "id": ",".join(chunk), "retmode": "xml"}
            if self.settings.pubmed_api_key:
                data["api_key"] = self.settings.pubmed_api_key
            if self.settings.pubmed_email:
                data["email"] = self.settings.pubmed_email
            response = _with_retries(lambda: session.post(self.EFETCH, data=data, timeout=120))
            time.sleep(interval)
            root = etree.fromstring(response.content, etree.XMLParser(recover=True, huge_tree=True))
            for article in root.iterfind("article"):
                pmcid = _pmcid_of(article)
                if pmcid is None:
                    continue
                if has_body(article):
                    _set_pmc_article_id(article, pmcid)
                    articles[pmcid] = article
                else:
                    reasons[pmcid] = RESTRICTED
            for pmcid in chunk:
                if pmcid not in articles:
                    reasons.setdefault(pmcid, "PMC returned no article for this PMCID")
        return articles, reasons


class EuropePmcExtractor(_PmcXmlExtractor):
    """Europe PMC's full-text service: its Open Access subset and Europe PMC funders' manuscripts."""

    SOURCE = DownloadSource.EUROPEPMC
    FULL_TEXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC{}/fullTextXML"
    #: Parallel requests; Europe PMC asks for reasonable use and has no key.
    concurrency = 4

    def _fetch(self, pmcids: List[str]) -> Tuple[Dict[str, etree._Element], Dict[str, str]]:
        session = requests.Session()

        def one(pmcid: str) -> Tuple[str, Optional[etree._Element], Optional[str]]:
            try:
                response = _with_retries(
                    lambda: session.get(self.FULL_TEXT.format(pmcid), timeout=120), ok=(200, 404, 500)
                )
            except Exception as exc:  # noqa: BLE001 - reported per article
                return pmcid, None, f"Europe PMC request failed: {exc}"
            if response.status_code != 200:
                # 500 is how the service says "not open access", which is final
                return pmcid, None, "Europe PMC: not open access (no full text)"
            article = etree.fromstring(response.content, etree.XMLParser(recover=True, huge_tree=True))
            if article is None or article.tag != "article":
                return pmcid, None, "Europe PMC returned no article"
            if not has_body(article):
                return pmcid, None, RESTRICTED
            _set_pmc_article_id(article, pmcid)
            return pmcid, article, None

        articles: Dict[str, etree._Element] = {}
        reasons: Dict[str, str] = {}
        with ThreadPoolExecutor(self.concurrency) as pool:
            for pmcid, article, reason in pool.map(one, pmcids):
                if article is not None:
                    articles[pmcid] = article
                else:
                    reasons[pmcid] = reason or "Europe PMC returned no article"
        return articles, reasons


def _pmcid_of(article: etree._Element) -> Optional[str]:
    for tag in article.iter("article-id"):
        kind = (tag.get("pub-id-type") or "").lower()
        value = (tag.text or "").strip().upper().removeprefix("PMC")
        if kind in ("pmc", "pmcid") and value.isdigit():
            return str(int(value))
    return None


def _set_pmc_article_id(article: etree._Element, pmcid: str) -> None:
    """pubget files an article by `article-id[@pub-id-type='pmc']`, digits only."""
    meta = article.find("front/article-meta")
    if meta is None:
        return
    pmc = meta.find("article-id[@pub-id-type='pmc']")
    if pmc is None:
        pmc = etree.Element("article-id", {"pub-id-type": "pmc"})
        meta.insert(0, pmc)
    pmc.text = pmcid


def _with_retries(call, ok=(200,), attempts: int = 5):
    """Retry transient failures (429, 5xx outside `ok`, connection errors) with backoff."""
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = call()
        except requests.RequestException as exc:
            last = exc
        else:
            if response.status_code in ok:
                return response
            last = requests.HTTPError(f"HTTP {response.status_code}")
            if response.status_code not in (429, 500, 502, 503, 504):
                break
        time.sleep(2**attempt)
    raise last  # type: ignore[misc]
