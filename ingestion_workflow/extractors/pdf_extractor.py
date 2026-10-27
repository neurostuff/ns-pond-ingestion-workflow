"""Download and extract article content from PDFs.

Fallback source for articles that neither PubMed Central (via pubget) nor
Elsevier serve as XML. Candidate PDF URLs come from every open-access
location OpenAlex lists and from Semantic Scholar's ``openAccessPdf``; each is
tried in turn until one is a PDF. A location with only a landing page is
followed through the PDF the page names in ``citation_pdf_url``. Parsing is
Docling's, and it reads every downloaded PDF, whichever source fetched it.
`fetch_pdf` and `store_pdf` are the fetching and storing the sources whose
download is a PDF share.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlsplit

import requests

from ingestion_workflow.clients.openalex import (
    LocationLookupFailed,
    OaLocation,
    OpenAlexClient,
)
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.extractors.docling_convert import usable_cuda_devices
from ingestion_workflow.extractors.utils import safe_hash_stem, sanitize_table_id
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    ExtractedTable,
    ExtractionResult,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.utils.preprints import is_preprint_host
from ingestion_workflow.utils.progress import emit_progress

logger = logging.getLogger(__name__)

PDF_MAGIC = b"%PDF-"
DOWNLOAD_TIMEOUT = 60

# Docling renders some glyphs it cannot map as GLYPH<n>; a decimal point read
# as a colon is the other artifact seen in practice. Cells matching this are
# flagged rather than rewritten -- "1 : 2" is a legitimate ratio, so guessing
# would silently corrupt real values.
_SUSPECT_DECIMAL = re.compile(r"^\s*\d+\s*:\s*\d+\s*$")


#: The longest Retry-After waited out. A host asking for longer is left for the
#: rest of the run, and so is one that answers 429 more often than this in a row.
_MAX_BACKOFF_SECONDS = 60.0
_RATE_LIMIT_STRIKES = 3
#: A host that has refused this many URLs in a run without serving one PDF is
#: refusing scripted downloads; its remaining URLs wait for a later run.
_HOST_REFUSAL_LIMIT = 20

_TYPE_RANK = {"repository": 0, "publisher": 1, "unknown": 2}
_PUBLISHER_SOURCE_TYPES = {"journal", "conference", "book series", "ebook platform"}

#: A copy on a preprint server with no version label is taken as a submitted version,
#: so it is tried after any published copy.
PREPRINT_VERSION = "submittedVersion"

#: Statuses that may answer differently later; any other 4xx is the URL's answer.
#: A 429 is not here because `fetch_pdf` handles it before any other status.
_TRANSIENT_CLIENT_STATUSES = {408, 425}
#: The URL names nothing on the host. That is the URL's answer, so it never
#: counts toward leaving the host for the run, as a 403 or a page in place of a
#: PDF does: an API asked about an article it does not have says 404.
_GONE_STATUSES = {404, 410}
#: Europe PMC serves its PMC articles under their PMCID; its preprints (PPR ids)
#: have no other source.
_PMCID = re.compile(r"pmc\d", re.IGNORECASE)

SKIPPED_PMC = "skipped: handled by the PMC/Europe PMC sources"
#: Outcomes of a fetch that stopped before asking for the PDF: a host left for
#: the run, or a PDF a landing page names that is not fetched from there.
_NOT_ASKED = {"deferred", "known_dead", "listed_elsewhere", "skipped_pmc"}


@dataclass(frozen=True)
class PdfCandidate:
    """One URL that may hold the article's PDF, and where it was found."""

    url: str
    provider: str
    #: "repository", "publisher" or "unknown"; repository copies are tried first.
    location_type: str
    source: Optional[str] = None
    #: OpenAlex's label: "publishedVersion", "acceptedVersion" or "submittedVersion".
    version: Optional[str] = None
    #: An open-access landing page with no PDF link. It is fetched once for the
    #: PDF it names in its `citation_pdf_url` meta tag.
    landing_page: bool = False
    #: A location OpenAlex lists as closed. It is recorded, not fetched.
    open_access: bool = True
    #: Other providers that listed the same URL.
    also_listed_by: Tuple[str, ...] = ()
    #: The licence the listing states for the copy at the URL, as the provider gave it,
    #: and the provider that gave it.
    license: Optional[str] = None
    license_by: Optional[str] = None

    def describe(self) -> Dict[str, object]:
        out: Dict[str, object] = {
            "url": self.url,
            "provider": self.provider,
            "location_type": self.location_type,
        }
        if self.also_listed_by:
            out["also_listed_by"] = list(self.also_listed_by)
        if self.source:
            out["source"] = self.source
        if self.version:
            out["version"] = self.version
        if self.license:
            out["license"] = self.license
            out["license_by"] = self.license_by
        return out


def _host(url: str) -> str:
    return urlsplit(url).netloc.lower()


def _host_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith("." + domain)


def _is_pmc_copy(url: str) -> bool:
    """A copy of a PMC article on the PMC or Europe PMC website.

    The pmc and europepmc sources run before this one and own these articles,
    through the services PMC's terms point automated retrieval to; the websites
    themselves refused every PDF fetched from them.
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if host == "pmc.ncbi.nlm.nih.gov":
        return True
    if _host_matches(host, "europepmc.org"):
        return bool(_PMCID.search(f"{parts.path}?{parts.query}"))
    return host in ("www.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov") and (
        parts.path == "/pmc" or parts.path.startswith("/pmc/")
    )


def _version_of(url: str, version: Optional[str]) -> Optional[str]:
    host = (urlsplit(url).hostname or "").lower()
    if version is None and is_preprint_host(host):
        return PREPRINT_VERSION
    return version


def _type_of(source_type: Optional[str]) -> str:
    if source_type == "repository":
        return "repository"
    if source_type in _PUBLISHER_SOURCE_TYPES:
        return "publisher"
    return "unknown"


def site_type(url: str, listed: Sequence[PdfCandidate]) -> str:
    """The kind of site `url` is on, as the work's typed locations on its host list it:
    "repository" or "publisher" when they agree, else "unknown"."""
    host = _host(url)
    types = {c.location_type for c in listed if _host(c.url) == host} - {"unknown"}
    return types.pop() if len(types) == 1 else "unknown"


def _openalex_candidates(
    locations: Dict[str, List[OaLocation]],
) -> Dict[str, List[PdfCandidate]]:
    """Every slug OpenAlex answered for, with its locations as candidates."""
    out: Dict[str, List[PdfCandidate]] = {slug: [] for slug in locations}
    for slug, listed in locations.items():
        for loc in listed:
            url = loc.pdf_url or loc.landing_page_url
            if not url:
                continue
            out[slug].append(
                PdfCandidate(
                    url=url,
                    provider="openalex",
                    location_type=_type_of(loc.source_type),
                    source=loc.source_name,
                    version=loc.version,
                    landing_page=not loc.pdf_url,
                    open_access=loc.is_oa,
                    license=loc.license,
                    license_by="openalex" if loc.license else None,
                )
            )
    return out


def _s2_license(meta) -> Optional[str]:
    """The licence Semantic Scholar states for its open-access PDF."""
    pdf = meta.raw_metadata["semantic_scholar"].get("openAccessPdf")
    return (pdf.get("license") or None) if isinstance(pdf, dict) else None


def _url_key(url: str) -> str:
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower()
    return f"{host}{parts.path.rstrip('/')}?{parts.query}"


def _merge(first: PdfCandidate, other: PdfCandidate) -> PdfCandidate:
    """One URL listed twice: keep every label either listing carries.

    A listing that says what kind of copy it is describes it, so it leads and
    the other provider is recorded in `also_listed_by`.
    """
    if first.location_type == "unknown" and other.location_type != "unknown":
        first, other = other, first
    lender = first if first.license else other
    listed_by = first.also_listed_by + tuple(
        p for p in (other.provider, *other.also_listed_by)
        if p != first.provider and p not in first.also_listed_by
    )
    return replace(
        first,
        source=first.source or other.source,
        version=first.version or other.version,
        landing_page=first.landing_page and other.landing_page,
        open_access=first.open_access or other.open_access,
        also_listed_by=listed_by,
        license=lender.license,
        license_by=lender.license_by,
    )


def _try_order(candidates: Sequence[PdfCandidate]) -> List[PdfCandidate]:
    """Each URL once, with its listings' labels merged, in the order to try them.

    Repository copies, then publisher copies, then untyped ones. A submitted
    version is a preprint whose text and tables can differ from the published
    article, so it goes after every other copy. Within a rank the providers'
    order holds. Landing pages, which cost a second request, come after every
    PDF link, and closed locations, which are only recorded, come last.
    """
    merged: Dict[str, PdfCandidate] = {}
    for candidate in candidates:
        key = _url_key(candidate.url)
        merged[key] = _merge(merged[key], candidate) if key in merged else candidate
    labelled = [replace(c, version=_version_of(c.url, c.version)) for c in merged.values()]
    return [
        candidate
        for _, candidate in sorted(
            enumerate(labelled),
            key=lambda item: (
                not item[1].open_access,
                item[1].landing_page,
                item[1].version == PREPRINT_VERSION,
                _TYPE_RANK.get(item[1].location_type, len(_TYPE_RANK)),
                item[0],
            ),
        )
    ]


def remembered_dead_urls(url_attempts: Sequence[Dict[str, object]]) -> Dict[str, str]:
    """URLs an earlier run found dead (a final 4xx, or not a PDF), with why."""
    return {
        _url_key(str(a["url"])): str(a.get("reason") or "")
        for a in url_attempts
        if a.get("url") and a.get("dead")
    }


def with_dead_urls_kept(
    previous: Sequence[Dict[str, object]], attempts: Sequence[Dict[str, object]]
) -> List[Dict[str, object]]:
    """`attempts`, then a `known_dead` entry for each URL `previous` found dead
    that `attempts` does not.

    The catalog keeps only an article's latest PDF attempt, so a dead URL this
    run did not list (a failed lookup, a source outage) would otherwise be
    forgotten and fetched again once it is listed.
    """
    kept = remembered_dead_urls(attempts)
    carried = [
        {"url": a["url"], "outcome": "known_dead", "dead": True, "reason": a.get("reason") or ""}
        for a in previous
        if a.get("url") and a.get("dead") and _url_key(str(a["url"])) not in kept
    ]
    return [*attempts, *carried]


def _is_pdf(payload: bytes) -> bool:
    return payload.startswith(PDF_MAGIC)


def _retry_after(response: Any) -> Optional[float]:
    """The host's Retry-After in seconds, given as seconds or as an HTTP date.

    None when there is none or it cannot be read; a date already past is 0.
    """
    header = response.headers.get("Retry-After")
    if header is None:
        return None
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(header)
        if when.tzinfo is None:  # the asctime form and "-0000" carry no zone; HTTP dates are UTC
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


class _HostGate:
    """What this run has learnt about each host, shared by the download workers.

    A host that asked to be left alone is not asked again before the time it
    gave, by any worker, and one that asked for longer than the run should wait
    is left for the rest of the run. A host that publishes a request rate is
    asked at most once per `min_interval` seconds.
    """

    def __init__(
        self,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        min_interval: float = 0.0,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._not_before: Dict[str, float] = {}
        self._next_slot: Dict[str, float] = {}
        self._strikes: Counter = Counter()
        self._refusals: Counter = Counter()
        self._served: Counter = Counter()
        self._deferred: Dict[str, str] = {}

    def deferred(self, host: str) -> Optional[str]:
        with self._lock:
            return self._deferred.get(host)

    def wait(self, host: str) -> None:
        """Sleep until `host` may be asked, and take that slot."""
        with self._lock:
            now = self._clock()
            start = max(now, self._not_before.get(host, 0.0), self._next_slot.get(host, 0.0))
            # Reserved under the lock, so two workers never take the same slot.
            self._next_slot[host] = start + self._min_interval
        if start > now:
            self._sleep(start - now)

    def rate_limited(self, host: str, retry_after: Optional[float]) -> Optional[str]:
        """Note a 429. Returns why the host is now left for the run, or None to retry."""
        with self._lock:
            self._strikes[host] += 1
            if retry_after is not None and retry_after > _MAX_BACKOFF_SECONDS:
                reason = f"deferred: {host} asked to wait {retry_after:.0f}s"
            elif self._strikes[host] > _RATE_LIMIT_STRIKES:
                reason = f"deferred: {host} kept answering 429"
            else:
                delay = retry_after if retry_after is not None else 2.0 ** self._strikes[host]
                self._not_before[host] = max(
                    self._not_before.get(host, 0.0), self._clock() + delay
                )
                return None
            return self._deferred.setdefault(host, reason)

    def defer(self, host: str, reason: str) -> str:
        """Leave `host` for the rest of the run; the reason it was first left for."""
        with self._lock:
            return self._deferred.setdefault(host, reason)

    def answered(self, host: str, *, served: bool = False, refused: bool = False) -> None:
        with self._lock:
            self._strikes[host] = 0
            if served:
                self._served[host] += 1
            elif refused:
                self._refusals[host] += 1
                if self._refusals[host] >= _HOST_REFUSAL_LIMIT and not self._served[host]:
                    self._deferred.setdefault(
                        host,
                        f"deferred: {host} refused {self._refusals[host]} URLs this run "
                        "and served no PDF",
                    )


class _CitationPdfUrl(HTMLParser):
    """The first `<meta name="citation_pdf_url">` of a page, the Highwire / Google
    Scholar tag publishers and repositories use to name an article's PDF."""

    def __init__(self) -> None:
        super().__init__()
        self.url: Optional[str] = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if self.url is None and tag == "meta":
            found = dict(attrs)
            if (found.get("name") or "").lower() == "citation_pdf_url" and found.get("content"):
                self.url = found["content"].strip()


def citation_pdf_url(page: bytes, page_url: str) -> Optional[str]:
    """The absolute PDF URL `page` (fetched from `page_url`) names, if it names one."""
    parser = _CitationPdfUrl()
    parser.feed(page.decode("utf-8", "replace"))
    if not parser.url:
        return None
    url = urljoin(page_url, parser.url)
    return url if urlsplit(url).scheme in ("http", "https") else None


def _answer(
    get: Callable[[str], Any], url: str, attempt: Dict[str, object], hosts: _HostGate, what: str
) -> Optional[Tuple[bytes, str]]:
    """The body of a 2xx answer for `url` and the URL it came from after redirects,
    or None with the outcome written into `attempt`.

    A refusal is counted against the host that answered, after redirects: a
    resolver such as doi.org fronts many hosts, and one of them refusing must
    not leave the others for the run.

    A 429 is waited out for as long as the host asks, by every worker,
    unless it asks for longer than the run should wait; see `_HostGate`.
    The host is checked before every request, so a URL on a host that this
    or another worker has left for the run is recorded `deferred`, not asked.
    """
    host = _host(url)
    while True:
        deferred = hosts.deferred(host)
        if deferred is not None:
            attempt.update(outcome="deferred", reason=deferred)
            return None
        hosts.wait(host)
        try:
            response = get(url)
        except Exception as exc:  # noqa: BLE001 - recorded on the attempt
            attempt.update(outcome="fetch_error", reason=f"{what} fetch failed: {exc}")
            return None
        status = response.status_code
        attempt["status"] = status
        if status == 429:
            retry_after = _retry_after(response)
            response.close()
            deferred = hosts.rate_limited(host, retry_after)
            if deferred is not None:
                attempt.update(
                    outcome="rate_limited",
                    reason=f"{what} fetch failed: 429 from {host}; {deferred}",
                )
                return None
            continue
        answered_from = str(response.url)
        try:
            response.raise_for_status()
            return response.content, answered_from
        except Exception as exc:  # noqa: BLE001 - recorded on the attempt
            dead = 400 <= status < 500 and status not in _TRANSIENT_CLIENT_STATUSES
            attempt.update(outcome="http_error", reason=f"{what} fetch failed: {exc}")
            if dead:
                attempt["dead"] = True
            hosts.answered(_host(answered_from), refused=dead and status not in _GONE_STATUSES)
            return None
        finally:
            response.close()


def _as_pdf(
    answer: Tuple[bytes, str], url: str, attempt: Dict[str, object], hosts: _HostGate
) -> Optional[bytes]:
    """`answer`'s body if it is a PDF; either way, credited to the host that sent it."""
    payload, answered_from = answer
    if not _is_pdf(payload):
        # Landing pages and paywall interstitials return HTML with a 200.
        attempt.update(outcome="not_pdf", dead=True, reason=f"Response from {url} is not a PDF.")
        hosts.answered(_host(answered_from), refused=True)
        return None
    attempt.update(outcome="pdf", reason="PDF downloaded")
    hosts.answered(_host(answered_from), served=True)
    return payload


def fetch_pdf(
    get: Callable[[str], Any], url: str, attempt: Dict[str, object], hosts: _HostGate
) -> Optional[bytes]:
    """The PDF at `url`, or None with the outcome written into `attempt`.

    `get` sends the request (a requests or an httpx GET) and returns its response.
    Rate limits and hosts left for the run are handled as `_answer` says.
    """
    answer = _answer(get, url, attempt, hosts, "PDF")
    return None if answer is None else _as_pdf(answer, url, attempt, hosts)


def store_pdf(
    base_dir: Path, identifier: Identifier, payload: bytes, source: DownloadSource
) -> DownloadedFile:
    """Write a fetched PDF to `<base_dir>/<article>/article.pdf`."""
    article_dir = base_dir / safe_hash_stem(identifier.slug)
    article_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = article_dir / "article.pdf"
    pdf_path.write_bytes(payload)
    return DownloadedFile(
        file_path=pdf_path,
        file_type=FileType.PDF,
        content_type="application/pdf",
        source=source,
    )


class PdfExtractor(BaseExtractor):
    """Extractor that downloads open-access PDFs and parses them with Docling."""

    _SUPPORTED_IDS = {"doi", "pmid"}

    #: CUDA cannot be re-initialised in a forked child ("Cannot re-initialize
    #: CUDA in forked subprocess"), so this extractor alone pays for spawn.
    mp_start_method = "spawn"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self._session = session or requests.Session()
        self._sleep = time.sleep
        self._clock = time.monotonic
        # One per extractor, which the download stage keeps for the whole run.
        self._hosts = _HostGate(lambda: self._clock(), lambda delay: self._sleep(delay))

    # ----------------------------------------------------------------- download

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
        *,
        dead_urls: Mapping[str, Mapping[str, str]] | None = None,
    ) -> List[DownloadResult]:
        """Download each article's PDF.

        `dead_urls` holds, per slug, the URLs an earlier run found dead
        (`remembered_dead_urls`); they are recorded, never fetched again. A
        later run fetches only URLs that are new, or that failed for a passing
        reason.
        """
        if not identifiers:
            return []

        identifier_list = list(identifiers)
        candidates, lookup_failures = self._resolve_pdf_candidates(identifiers)
        dead_urls = dead_urls or {}

        base_dir = Path(self.settings.pdf_cache_root or self.settings.get_cache_dir("pdf"))
        base_dir.mkdir(parents=True, exist_ok=True)

        worker_count = max(1, self.settings.max_workers)
        ordered: List[Optional[DownloadResult]] = [None] * len(identifier_list)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    self._download_single,
                    identifier,
                    candidates.get(identifier.slug, []),
                    base_dir,
                    lookup_failures.get(identifier.slug, []),
                    dead_urls.get(identifier.slug, {}),
                ): index
                for index, identifier in enumerate(identifier_list)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                identifier = identifier_list[index]
                try:
                    ordered[index] = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.exception("PDF download raised for %s", identifier.slug)
                    ordered[index] = self._failure(identifier, f"PDF download failed: {exc}")
                emit_progress(progress_hook)

        return [
            result or self._failure(identifier, "PDF download did not return a result.")
            for identifier, result in zip(identifier_list, ordered)
        ]

    def _resolve_pdf_candidates(
        self, identifiers: Identifiers
    ) -> Tuple[Dict[str, List[PdfCandidate]], Dict[str, List[Dict[str, object]]]]:
        """Every candidate URL per article from every provider, in the order to try them.

        Both providers are asked about every article, and their order breaks
        ties between equally ranked copies: the first provider's URL is
        often a publisher copy that refuses the download while a repository
        holds the same article. Also returns, per article, each provider whose
        lookup failed for it, so that its absence is recorded rather than read
        as "no open-access copy".
        """
        found: Dict[str, List[PdfCandidate]] = {}
        failures: Dict[str, List[Dict[str, object]]] = {}

        def fail(provider: str, reasons: Mapping[str, object]) -> None:
            for slug, reason in reasons.items():
                failures.setdefault(slug, []).append(
                    {
                        "provider": provider,
                        "outcome": "lookup_failed",
                        "reason": f"{provider} lookup failed: {reason}",
                    }
                )

        for provider, ask in (
            ("semantic_scholar", self._semantic_scholar_pdf_candidates),
            ("openalex", self._openalex_pdf_candidates),
        ):
            try:
                by_slug = ask(identifiers)
            except LocationLookupFailed as exc:
                logger.warning("PDF URL lookup partly failed for provider %s: %s", provider, exc)
                by_slug = exc.found
                fail(provider, exc.failed)
            except Exception as exc:  # noqa: BLE001 - one provider down must not stop the rest
                logger.warning("PDF URL lookup failed for provider %s: %s", provider, exc)
                fail(provider, {identifier.slug: exc for identifier in identifiers})
                continue
            for slug, listed in by_slug.items():
                found.setdefault(slug, []).extend(listed)

        return {slug: _try_order(listed) for slug, listed in found.items()}, failures

    def _semantic_scholar_pdf_candidates(
        self, identifiers: Identifiers
    ) -> Dict[str, List[PdfCandidate]]:
        api_key = self.settings.semantic_scholar_api_key
        if not api_key:
            return {}
        client = SemanticScholarClient(api_key)

        def candidate(meta):
            licence = _s2_license(meta)
            return PdfCandidate(
                url=meta.pdf_url, provider="semantic_scholar", location_type="unknown",
                license=licence, license_by="semantic_scholar" if licence else None)

        def candidates(metadata):
            return {slug: [candidate(meta)] for slug, meta in metadata.items() if meta.pdf_url}

        try:
            return candidates(client.get_metadata(list(identifiers)))
        except PartialAnswer as exc:
            # A request that failed leaves every article it did not answer; a record that
            # could not be read, only its own.
            unanswered = ({i.slug: str(exc) for i in identifiers if i.slug not in exc.found}
                          if exc.causes else dict(exc.unreadable))
            raise LocationLookupFailed(str(exc), candidates(exc.found), unanswered) from exc

    def _openalex_pdf_candidates(self, identifiers: Identifiers) -> Dict[str, List[PdfCandidate]]:
        client = OpenAlexClient.from_settings(self.settings)
        if client is None:
            return {}
        try:
            return _openalex_candidates(client.get_locations(identifiers))
        except LocationLookupFailed as exc:
            raise LocationLookupFailed(
                str(exc), _openalex_candidates(exc.found), exc.failed
            ) from exc

    def _download_single(
        self,
        identifier: Identifier,
        candidates: Sequence[PdfCandidate],
        base_dir: Path,
        lookup_failures: Sequence[Dict[str, object]] = (),
        dead_urls: Mapping[str, str] | None = None,
    ) -> DownloadResult:
        """Fetch the candidates in order until one is a PDF, recording every one."""
        attempts: List[Dict[str, object]] = [dict(failure) for failure in lookup_failures]
        tried: List[Dict[str, object]] = []
        dead_urls = dead_urls or {}
        listed = {_url_key(c.url) for c in candidates}

        for candidate in candidates:
            attempt = candidate.describe()
            attempts.append(attempt)
            passed_over = self._passed_over(candidate, dead_urls)
            if passed_over is not None:
                attempt.update(passed_over)
                continue
            if candidate.landing_page:
                payload = self._fetch_through_landing_page(
                    candidate.url, attempt, dead_urls, listed
                )
            else:
                payload = self._fetch(candidate.url, attempt)
            if payload is None:
                # the error names a URL that was asked, not one passed over on the way
                if attempt["outcome"] not in _NOT_ASKED:
                    tried.append(attempt)
                continue
            if candidate.location_type == "unknown":
                attempt["site_type"] = site_type(str(attempt.get("pdf_url") or candidate.url),
                                                 candidates)
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.PDF,
                success=True,
                files=[store_pdf(base_dir, identifier, payload, DownloadSource.PDF)],
                url_attempts=attempts,
            )

        recorded = [
            a for a in attempts if a.get("outcome") not in ("lookup_failed", "not_open_access")
        ]
        if tried:
            message = str(tried[-1]["reason"])
        elif recorded:
            message = str(recorded[-1]["reason"])
        elif lookup_failures:
            message = str(lookup_failures[-1]["reason"])
        else:
            message = "No open-access PDF URL found."
        result = self._failure(identifier, message)
        result.url_attempts = attempts
        return result

    @staticmethod
    def _passed_over(
        candidate: PdfCandidate, dead_urls: Mapping[str, str]
    ) -> Optional[Dict[str, object]]:
        """What to record for a candidate that is not fetched, else None."""
        dead = dead_urls.get(_url_key(candidate.url))
        if dead is not None:
            return {"outcome": "known_dead", "dead": True, "reason": dead}
        if not candidate.open_access:
            return {"outcome": "not_open_access", "reason": "closed copy: not fetched"}
        if _is_pmc_copy(candidate.url):
            return {"outcome": "skipped_pmc", "reason": SKIPPED_PMC}
        return None

    def _get(self, url: str) -> Any:
        return self._session.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)

    def _fetch(self, url: str, attempt: Dict[str, object]) -> Optional[bytes]:
        return fetch_pdf(self._get, url, attempt, self._hosts)

    def _fetch_through_landing_page(
        self,
        url: str,
        attempt: Dict[str, object],
        dead_urls: Mapping[str, str],
        listed: set,
    ) -> Optional[bytes]:
        """The PDF the landing page at `url` names, fetched once each.

        The attempt keeps the landing page as its `url` and the PDF it names as
        `pdf_url`, so a dead PDF leaves the page remembered dead too. A page
        that answers with the PDF itself is the PDF.
        """
        answer = _answer(self._get, url, attempt, self._hosts, "landing page")
        if answer is None:
            return None
        page, page_url = answer
        if _is_pdf(page):
            return _as_pdf(answer, url, attempt, self._hosts)
        self._hosts.answered(_host(page_url))
        pdf_url = citation_pdf_url(page, page_url)
        if pdf_url is None:
            attempt.update(
                outcome="no_pdf_link",
                dead=True,
                reason=f"Landing page {url} names no citation_pdf_url.",
            )
            return None
        attempt["pdf_url"] = pdf_url
        key = _url_key(pdf_url)
        if key in dead_urls:
            attempt.update(outcome="known_dead", dead=True, reason=dead_urls[key])
            return None
        if key in listed:
            # tried, or passed over, as its own candidate
            attempt.update(
                outcome="listed_elsewhere", reason=f"{pdf_url} is listed as its own copy."
            )
            return None
        if _is_pmc_copy(pdf_url):
            attempt.update(outcome="skipped_pmc", reason=SKIPPED_PMC)
            return None
        return self._fetch(pdf_url, attempt)

    @staticmethod
    def _failure(identifier: Identifier, message: str) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PDF,
            success=False,
            error_message=message,
        )

    # ------------------------------------------------------------------ extract

    def extract(
        self,
        download_results: List[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[ExtractionResult]:
        extraction_root = Path(self.settings.data_root) / "extractions" / "pdf"
        gpus = usable_cuda_devices()
        worker_count = self._worker_count(gpus)
        logger.info(
            "PDF extraction: %d worker(s) over %s",
            worker_count,
            f"GPU(s) {gpus}" if gpus else "CPU",
        )
        return self._run_extraction_pipeline(
            download_results,
            extraction_root=extraction_root,
            worker=_extract_pdf_article,
            worker_count=worker_count,
            worker_initializer=_pin_worker_to_gpu if gpus else None,
            worker_initargs=(
                (multiprocessing.get_context("spawn").Value("i", 0), gpus) if gpus else ()
            ),
            source_name="PDF",
            failure_message="PDF extraction did not produce a result.",
            failure_builder=_build_failure_content,
            progress_hook=progress_hook,
        )

    def _worker_count(self, gpus: List[int]) -> int:
        """How many Docling processes to run at once.

        Docling holds a model per process, so this is bounded by memory rather
        than by cores: one worker per usable GPU, and one on CPU, where a second
        worker would contend for the cores the pipeline already threads across.
        """
        configured = self.settings.pdf_extract_workers
        if configured:
            return max(1, configured)
        return max(1, len(gpus))


_COORD_COLUMN = re.compile(
    r"coordinate|\bcoords?\b|talairach|\btal\b|\bmni\b|location", re.IGNORECASE
)
_TRIPLET_CELL = re.compile(
    r"^\s*[-+]?\d+(?:\.\d+)?\s*[,;\s]\s*[-+]?\d+(?:\.\d+)?\s*[,;\s]\s*[-+]?\d+(?:\.\d+)?\s*$"
)
_MIN_MERGED_TRIPLETS = 3


def _write_table_html(table, document, tables_dir: Path, table_id: str) -> Optional[Path]:
    """Docling's HTML for one table, or None if it could not be produced.

    `export_to_html` needs the document; without it Docling logs a deprecation
    and returns an empty string. Returning None on any failure lets the caller
    fall back to the CSV rather than lose the table -- a table that cannot be
    read is not counted as a miss by anything downstream.
    """
    try:
        html = table.export_to_html(doc=document, add_caption=False)
    except Exception as exc:  # pragma: no cover - depends on the docling build
        logger.warning("%s: HTML export failed, keeping CSV: %s", table_id, exc)
        return None
    if not html or "<table" not in html.lower():
        return None
    path = tables_dir / f"{table_id}.html"
    path.write_text(html, encoding="utf-8")
    return path


def _merge_split_coordinate_columns(frame: Any) -> Any:
    """Rejoin an x/y/z triplet that Docling split across coordinate columns.

    Docling sometimes cuts a single "42, 30, 45" cell down the middle, leaving
    "42," in one column and "30, 45" in the next. Downstream detection needs
    either three columns headed x/y/z or one column holding whole triplets, so a
    split like this yields nothing at all.

    Only runs of adjacent columns whose headers already say coordinate/MNI/
    Talairach are considered, and the merge is kept only if it produces whole
    triplets for at least a few rows. A demographics table cannot qualify: its
    headers do not match, and pasting its columns together does not make
    triplets.
    """
    columns = [str(c) for c in frame.columns]
    runs = []
    start = None
    for index, name in enumerate(columns + [""]):
        if index < len(columns) and _COORD_COLUMN.search(name):
            start = index if start is None else start
            continue
        if start is not None:
            if index - start >= 2:
                runs.append((start, index))
            start = None

    if not runs:
        return frame

    result = frame
    # right to left, so earlier column positions stay valid as columns collapse
    for begin, end in reversed(runs):
        block = result.iloc[:, begin:end]
        joined = block.apply(
            lambda row: " ".join(str(v).strip() for v in row if str(v).strip()), axis=1
        )
        whole = joined.apply(lambda v: bool(_TRIPLET_CELL.match(v))).sum()
        if whole < _MIN_MERGED_TRIPLETS:
            continue
        kept = [i for i in range(result.shape[1]) if not begin <= i < end]
        rebuilt = result.iloc[:, kept]
        rebuilt.insert(begin, "Coordinates", joined.values, allow_duplicates=True)
        result = rebuilt
        logger.debug(
            "merged %d coordinate columns into one, %d whole triplets",
            end - begin,
            int(whole),
        )
    return result


def _pin_worker_to_gpu(counter: Any, gpus: List[int]) -> None:
    """Give this worker process exactly one GPU, before torch is imported.

    Each worker takes the next index off a shared counter, so two workers never
    land on the same card. Setting CUDA_VISIBLE_DEVICES rather than choosing a
    device later means the worker's torch only ever sees the one it owns.
    """
    with counter.get_lock():
        ordinal = counter.value
        counter.value += 1
    device = gpus[ordinal % len(gpus)]
    os.environ["CUDA_VISIBLE_DEVICES"] = str(device)


def _build_failure_content(
    download_result: DownloadResult, message: str
) -> ExtractedContent:
    return ExtractedContent(
        slug=download_result.identifier.slug,
        source=DownloadSource.PDF,
        identifier=download_result.identifier,
        full_text_path=None,
        tables=[],
        has_coordinates=False,
        error_message=message,
    )


def _extract_pdf_article(
    download_result: DownloadResult,
    extraction_root: Path | str,
) -> ExtractedContent:
    """Convert one downloaded PDF into text plus per-table CSV files."""
    from pubget._coordinate_space import _neurosynth_guess_space
    from pubget._coordinates import _extract_coordinates_from_table

    from ingestion_workflow.extractors.docling_convert import (
        convert_pdf,
        normalize_text_tokens,
    )
    from ingestion_workflow.extractors.utils import (
        coordinate_from_row,
        coordinate_space_from_guess,
    )

    slug = download_result.identifier.slug
    pdf_file = next(
        (f for f in download_result.files if f.file_type is FileType.PDF),
        None,
    )
    if pdf_file is None:
        return _build_failure_content(download_result, "No PDF file in download result.")

    output_dir = Path(extraction_root) / safe_hash_stem(slug)
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    try:
        document = convert_pdf(pdf_file.file_path)
    except Exception as exc:
        return _build_failure_content(download_result, f"Docling conversion failed: {exc}")

    full_text, figure_captions = pdf_text_and_captions(document, normalize_text_tokens)
    full_text_path = output_dir / "article.txt"
    full_text_path.write_text(full_text, encoding="utf-8")

    misread = symbols_as_digits(full_text)
    if misread:
        # The cells are read from the same layer, so the tables lost their signs too.
        return ExtractedContent(
            slug=slug,
            source=DownloadSource.PDF,
            identifier=download_result.identifier,
            symbols_as_digits={**misread, "tables_set_aside": len(document.tables)},
        )

    article_space = coordinate_space_from_guess(_neurosynth_guess_space(full_text))

    extracted_tables: List[ExtractedTable] = []
    for index, table in enumerate(document.tables):
        caption = " ".join(_caption_texts(table, document)).strip()
        footer = " ".join(_footnote_texts(table, document)).strip()
        table_id = sanitize_table_id(None, _table_label(caption), index)

        # Normalise the cells on the document, before either export reads them.
        # `normalize_text_tokens` turns Docling's minus glyphs into ASCII, and a
        # coordinate whose sign is a typographic dash lands in the wrong
        # hemisphere. Applying it per DataFrame cell left the HTML export
        # unnormalised; doing it here keeps the two exports agreeing.
        for cell in getattr(getattr(table, "data", None), "table_cells", []) or []:
            cell.text = normalize_text_tokens(cell.text)

        frame = table.export_to_dataframe(doc=document).map(normalize_text_tokens)
        frame = _merge_split_coordinate_columns(frame)
        csv_path = tables_dir / f"{table_id}.csv"
        frame.to_csv(csv_path, index=False)

        # HTML is what the serialiser reads best: `<th>` marks the header and
        # colspan/rowspan survive, where a DataFrame has neither. Docling
        # detects that structure and `export_to_dataframe` discards it, so a pdf
        # table arrived with no header marking at all and its coordinates often
        # packed into one space-separated cell. The CSV stays, because
        # `_extract_coordinates_from_table` and `_merge_split_coordinate_columns`
        # both want the frame.
        html_path = _write_table_html(table, document, tables_dir, table_id)

        try:
            coordinates_frame = _extract_coordinates_from_table(frame)
        except Exception as exc:
            logger.warning("%s: coordinate parsing failed for %s: %s", slug, table_id, exc)
            coordinates_frame = None

        coordinates = []
        if coordinates_frame is not None and not coordinates_frame.empty:
            for _, row in coordinates_frame.iterrows():
                coord = coordinate_from_row(row, article_space)
                if coord is not None:
                    coordinates.append(coord)

        extracted_tables.append(
            ExtractedTable(
                table_id=table_id,
                raw_content_path=html_path or csv_path,
                table_number=index + 1,
                caption=caption,
                footer=footer,
                coordinates=coordinates,
                space=article_space,
                metadata={
                    "suspect_numeric_glyphs": _has_suspect_numerics(frame),
                },
            )
        )

    return ExtractedContent(
        slug=slug,
        source=DownloadSource.PDF,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=any(table.coordinates for table in extracted_tables),
        figure_captions=figure_captions,
    )


_TABLE_LABEL = re.compile(r"\b(table\s*[0-9ivx]+)", re.IGNORECASE)


def pdf_text_and_captions(document: Any, normalize: Callable[[str], str]):
    """The document's text without its pictures, the pictures' captions under "Figure
    legends" at the end, and each caption's `{"ids", "span"}` in the text.

    No export label leaves a picture's caption out and a table's in, so the text is
    exported from a copy without the pictures and their captions. A document that
    cannot be copied that way is exported whole, its captions where docling put them.
    """
    from ingestion_workflow.extractors.figure_captions import append_legends, dedupe

    pictures = list(getattr(document, "pictures", []) or [])
    if not pictures:
        return normalize(document.export_to_text()), []
    captions = dedupe((getattr(p, "self_ref", None), normalize(" ".join(_caption_texts(p, document))))
                      for p in pictures)
    try:
        bare = document.model_copy(deep=True)
        refs = [ref.resolve(doc=bare) for p in bare.pictures for ref in (getattr(p, "captions", None) or [])]
        if refs:
            bare.delete_items(node_items=refs)
        bare.delete_items(node_items=list(bare.pictures))
        body = normalize(bare.export_to_text())
    except Exception as exc:  # noqa: BLE001 - an older docling-core
        logger.warning("Could not export the text without its pictures: %s", exc)
        return normalize(document.export_to_text()), []
    return append_legends(body, captions)


def _table_label(caption: str) -> Optional[str]:
    """Pull a 'Table 3'-style label out of a caption, for use as a filename."""
    match = _TABLE_LABEL.search(caption)
    return match.group(1) if match else None


def _footnote_texts(table: Any, document: Any) -> List[str]:
    """Docling resolves footnotes exactly as it resolves captions.

    A table footnote carries the threshold, the statistic and the laterality
    key -- "L, left; R, right; BA, Brodmann area" -- which the space and
    laterality rules both read. This was passed as an empty string, so every
    pdf table arrived without one.
    """
    notes: List[str] = []
    for note_ref in getattr(table, "footnotes", []) or []:
        try:
            resolved = note_ref.resolve(doc=document)
        except Exception:
            continue
        text = getattr(resolved, "text", None) or getattr(resolved, "content", None)
        if text:
            notes.append(str(text))
    return notes or _footnotes_below(table, document)


#: A footnote sits against the table; body prose starts further down. Measured
#: on sampled PDFs, legends were 5-8pt below the table and the next paragraph
#: 28-40pt, so the boundary is comfortably wide.
_FOOTNOTE_GAP_PT = 14.0
#: Docling sometimes labels an item `footnote` without linking it to the table.
#: Trust that label further down the page than an unlabelled one.
_LABELLED_FOOTNOTE_GAP_PT = 60.0


def _footnotes_below(table: Any, document: Any) -> List[str]:
    """Text sitting immediately under the table, when nothing was linked.

    Docling classifies a table's footnote as ordinary body text and leaves
    `TableItem.footnotes` empty: across sampled PDFs every table had zero linked
    footnotes while the legend sat 5-8pt beneath it, and some items were even
    labelled `footnote` without being attached. Only 3.9% of tables carried a
    footnote before this fallback.
    """
    prov = (getattr(table, "prov", None) or [None])[0]
    page, box = getattr(prov, "page_no", None), getattr(prov, "bbox", None)
    if page is None or box is None:
        return []

    found = []
    for item in getattr(document, "texts", []) or []:
        ip = (getattr(item, "prov", None) or [None])[0]
        if ip is None or getattr(ip, "page_no", None) != page:
            continue
        ib = getattr(ip, "bbox", None)
        text = (getattr(item, "text", "") or "").strip()
        if ib is None or not text:
            continue
        # Docling's y axis runs bottom-up, so an item below the table has its
        # top edge under the table's bottom edge.
        gap = box.b - ib.t
        label = str(getattr(item, "label", "") or "")
        limit = _LABELLED_FOOTNOTE_GAP_PT if "footnote" in label else _FOOTNOTE_GAP_PT
        if 0 <= gap <= limit:
            found.append((gap, text))
    found.sort()
    return [text for _, text in found]


def _caption_texts(table: Any, document: Any) -> List[str]:
    captions: List[str] = []
    for caption_ref in getattr(table, "captions", []) or []:
        try:
            resolved = caption_ref.resolve(doc=document)
        except Exception:
            continue
        text = getattr(resolved, "text", None) or getattr(resolved, "content", None)
        if text:
            captions.append(str(text))
    return captions


def _has_suspect_numerics(frame: Any) -> bool:
    """True when any cell looks like a decimal point Docling read as a colon."""
    return bool(
        frame.map(lambda cell: bool(_SUSPECT_DECIMAL.match(str(cell)))).to_numpy().any()
    )


#: What a text layer that maps `=` to 5 makes of things papers type often: `P 5 0.05`,
#: `p 5 .001`, `x5222, y5` (its minus a 2, or a minus `normalize_text_tokens` restored),
#: `size5 12`. And the p-values typed with `=`, which a sound layer has instead; fonts
#: that lose `=` keep `<`, so `<` does not count.
_EQUALS_AS_5 = re.compile(
    r"\b[Pp]\s?5\s?0?\.\d|\b[xyz]\s?5\s?-?\d{1,3}\s*,\s*[xyz]\s?5|size\s?5\s?\d{2}"
)
_EQUALS_TYPED = re.compile(r"\b[Pp]\s?=\s?0?\.\d")
#: Fewer than this are what coincidence gives ("cluster size 5 12").
_MIN_SYMBOLS_AS_DIGITS = 3


def symbols_as_digits(text: str) -> Optional[Dict[str, int]]:
    """How often `text` reads as `=` mapped to 5, or None when it does not.

    The text is taken as such when the misread forms are at least three and outnumber
    the typed ones. Such a layer maps the minus to 2 as well, which no rule can tell
    from a digit, so the text cannot be repaired."""
    misread = len(_EQUALS_AS_5.findall(text))
    typed = len(_EQUALS_TYPED.findall(text))
    if misread < _MIN_SYMBOLS_AS_DIGITS or misread <= typed:
        return None
    return {"misread": misread, "typed": typed}


__all__ = [
    "PdfCandidate",
    "PdfExtractor",
    "citation_pdf_url",
    "remembered_dead_urls",
    "symbols_as_digits",
]
