"""Full-text PDFs from Wiley's Text and Data Mining (TDM) API.

Wiley serves the PDF of an article it publishes to a registered TDM token, at
``/onlinelibrary/tdm/v1/articles/<DOI>``. The extract stage reads the PDF as
it reads every downloaded PDF, into the `pdf` text.
"""

from __future__ import annotations

import time
from typing import Callable, List, Optional
from urllib.parse import quote

import httpx

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor, SourceMisconfigured
from ingestion_workflow.extractors.pdf_extractor import (
    DOWNLOAD_TIMEOUT,
    _HostGate,
    fetch_pdf,
    store_pdf,
)
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifier, Identifiers
from ingestion_workflow.utils.progress import emit_progress

TDM_URL = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
TOKEN_HEADER = "Wiley-TDM-Client-Token"

#: Wiley's TDM terms allow up to 3 articles a second and 60 requests per 10
#: minutes; the second is the one a long run meets.
REQUEST_INTERVAL_SECONDS = 600 / 60

#: The DOI prefixes Crossref lists for Wiley, member 311
#: (https://api.crossref.org/members/311, 2026-10-13): Wiley's own and those of
#: the societies and imprints whose DOIs Wiley deposits.
WILEY_DOI_PREFIXES = frozenset({
    "10.1002", "10.1034", "10.1046", "10.1096", "10.1100", "10.1111", "10.1112",
    "10.1113", "10.1118", "10.1155", "10.1196", "10.1256", "10.1301", "10.1348",
    "10.1359", "10.1506", "10.1516", "10.1526", "10.1576", "10.1581", "10.1592",
    "10.1634", "10.1889", "10.1890", "10.1892", "10.1897", "10.1901", "10.2134",
    "10.2135", "10.2136", "10.2164", "10.2746", "10.2755", "10.2903", "10.2966",
    "10.3162", "10.3170", "10.3322", "10.3401", "10.3405", "10.3814", "10.4004",
    "10.4061", "10.4195", "10.4218", "10.4284", "10.4319", "10.5054", "10.5402",
    "10.5694", "10.6064", "10.7167", "10.7217", "10.7863", "10.14814", "10.15200",
    "10.18934", "10.21815", "10.22541", "10.64286",
})

#: Wiley serves the version of record.
PUBLISHED_VERSION = "publishedVersion"

#: Wiley's answer to a token it refuses. A 403 is not that: it denies this address
#: the article (so Wiley's own client reports it), and a token Wiley never issued
#: is still served from an entitled address.
TOKEN_REFUSED = 401

#: Every Wiley extractor in the process takes its turn here, so the rate holds
#: across the download stage's batches and every extractor built in the run.
GATE = _HostGate(time.monotonic, time.sleep, min_interval=REQUEST_INTERVAL_SECONDS)


def is_wiley_doi(doi: Optional[str]) -> bool:
    return bool(doi) and doi.split("/", 1)[0].strip().lower() in WILEY_DOI_PREFIXES


class WileyExtractor(BaseExtractor):
    """Download Wiley articles' PDFs by DOI."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        self._client = httpx.Client(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True)

    def can_address(self, identifier: Identifier) -> bool:
        return is_wiley_doi(identifier.doi)

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        """Each article's PDF, one request at a time: the published rate
        leaves nothing for a second worker to do."""
        token = self.settings.wiley_api_key
        if not token:
            raise SourceMisconfigured(
                "no Wiley TDM token: set WILEY_API_KEY, or remove wiley from download_sources"
            )
        base_dir = self.settings.get_cache_dir(DownloadSource.WILEY.value)
        results = []
        for identifier in identifiers:
            results.append(self._download_single(identifier, base_dir, token))
            emit_progress(progress_hook)
        return results

    def _download_single(self, identifier: Identifier, base_dir, token: str) -> DownloadResult:
        url = TDM_URL + quote(identifier.doi, safe="")
        attempt = {"url": url, "provider": "wiley"}
        payload = fetch_pdf(
            lambda target: self._client.get(target, headers={TOKEN_HEADER: token}),
            url,
            attempt,
            GATE,
        )
        if attempt.get("status") == TOKEN_REFUSED:
            raise SourceMisconfigured("Wiley refused the TDM token in WILEY_API_KEY (401)")
        if payload is None:
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.WILEY,
                success=False,
                error_message=str(attempt["reason"]),
                url_attempts=[attempt],
            )
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.WILEY,
            success=True,
            files=[store_pdf(base_dir, identifier, payload, DownloadSource.WILEY)],
            url_attempts=[attempt],
            article_version=PUBLISHED_VERSION,
        )
