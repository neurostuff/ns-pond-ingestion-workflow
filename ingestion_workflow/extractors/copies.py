"""Where each held copy came from, which version it is, and how we obtained it.

Every rule reads only what the download holds (its files and its URL attempts), so a
stored download yields the same facts as a fresh one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Dict, Optional
from urllib.parse import urlsplit

from lxml import etree

from ingestion_workflow.models import (
    CopyAccess,
    CopyRoute,
    CopyVersion,
    DownloadCopy,
    DownloadResult,
    DownloadSource,
)
from ingestion_workflow.utils.preprints import preprint_server_of_host

#: OpenAlex's version labels, which the PDF candidates carry, in study_schema's terms.
OPENALEX_VERSIONS = {
    "publishedVersion": CopyVersion.VOR,
    "acceptedVersion": CopyVersion.AM,
    "submittedVersion": CopyVersion.SUBMITTED,
}

#: The article ids PMC gives an author manuscript: `manuscript` in efetch's JATS,
#: `manuscript-id` in the Cloud Service's JATS 1.4 (on every record it marks is_manuscript).
_MANUSCRIPT_IDS = {"manuscript", "manuscript-id"}
#: PMC marks a preprint it hosts by the status of its article version.
_STATUS = ".//article-version[@article-version-type='status']"
_PREPRINT_STATUS = "preprint"
#: The licence PMC states on an author manuscript it may only offer for text mining.
_TDM_STATEMENT = "available for text mining"
#: The Cloud Service's licence code for that manuscript.
_TDM_CODE = "TDM"
_PDF_LOCATIONS = {"repository": CopyRoute.REPOSITORY, "publisher": CopyRoute.PUBLISHER}


def _file(result: DownloadResult, name: str) -> Optional[Path]:
    return next((f.file_path for f in result.files if f.file_path.name == name), None)


def _pmc_copy(result: DownloadResult) -> DownloadCopy:
    """A PMC article's JATS says whether it is a preprint or an author manuscript; the Cloud
    Service's record, else the JATS licence, says whether it is offered for text mining only."""
    # As the PMC sources read it: some of PMC's JATS is malformed past the front matter.
    parser = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
    article = etree.parse(str(_file(result, "article.xml")), parser).getroot()
    # Malformed JATS can lose its front matter, and with it what says the version.
    meta = article.find("front/article-meta")
    version = None if meta is None else _pmc_version(meta)
    cloud = _file(result, "pmc_cloud.json")
    if cloud is not None:
        tdm = json.loads(cloud.read_text(encoding="utf-8")).get("license_code") == _TDM_CODE
    else:
        permissions = meta.find("permissions") if meta is not None else None
        text = " ".join(permissions.itertext()) if permissions is not None else ""
        tdm = _TDM_STATEMENT in text.lower()
    return DownloadCopy(
        route=CopyRoute.PMC,
        version=version,
        access=CopyAccess.TDM_GRANT if tdm else CopyAccess.PUBLIC,
    )


def _pmc_version(meta: etree._Element) -> CopyVersion:
    statuses = {(v.text or "").strip().lower() for v in meta.iterfind(_STATUS)}
    if _PREPRINT_STATUS in statuses:
        return CopyVersion.SUBMITTED
    ids = {(i.get("pub-id-type") or "").lower() for i in meta.iterfind("article-id")}
    return CopyVersion.AM if ids & _MANUSCRIPT_IDS else CopyVersion.VOR


def _pdf_copy(result: DownloadResult) -> DownloadCopy:
    """The location the PDF was fetched from, as its provider listed it; a PDF with no
    fetch recorded (supplied by hand, or fetched before attempts were kept) is unrecorded."""
    fetched = [a for a in result.url_attempts if a.get("outcome") == "pdf"]
    if not fetched:
        return DownloadCopy(route=CopyRoute.UNRECORDED, access=CopyAccess.UNRECORDED)
    attempt = fetched[-1]
    url = str(attempt.get("pdf_url") or attempt["url"])
    server = preprint_server_of_host((urlsplit(url).hostname or "").lower())
    if server is not None:
        route = CopyRoute.PREPRINT_SERVER
    else:
        # Semantic Scholar does not say what kind of site a PDF is on.
        route = _PDF_LOCATIONS.get(str(attempt.get("location_type")), CopyRoute.UNRECORDED)
    return DownloadCopy(
        route=route,
        version=OPENALEX_VERSIONS.get(str(attempt.get("version"))),
        access=CopyAccess.PUBLIC,
        url=url,
        preprint_server=server,
    )


def _wiley_copy(result: DownloadResult) -> DownloadCopy:
    return DownloadCopy(
        route=CopyRoute.PUBLISHER,
        version=CopyVersion.VOR,
        access=CopyAccess.TDM_GRANT,
        url=str(result.url_attempts[-1]["url"]),
    )


def _fixed(route: CopyRoute, access: CopyAccess) -> Callable[[DownloadResult], DownloadCopy]:
    copy = DownloadCopy(route=route, version=CopyVersion.VOR, access=access)
    return lambda result: copy


#: One rule per download source.
DOWNLOAD_COPY: Dict[DownloadSource, Callable[[DownloadResult], DownloadCopy]] = {
    DownloadSource.PUBGET: _pmc_copy,
    DownloadSource.PMC: _pmc_copy,
    DownloadSource.EUROPEPMC: _pmc_copy,
    # Through our API key, whatever the article's licence.
    DownloadSource.ELSEVIER: _fixed(CopyRoute.PUBLISHER, CopyAccess.TDM_GRANT),
    DownloadSource.WILEY: _wiley_copy,
    # The open-access API serves only openly licensed articles.
    DownloadSource.SPRINGER: _fixed(CopyRoute.PUBLISHER, CopyAccess.PUBLIC),
    # The publisher's page, scraped as a reader sees it.
    DownloadSource.ACE: _fixed(CopyRoute.PUBLISHER, CopyAccess.PUBLIC),
    DownloadSource.PDF: _pdf_copy,
}


def copy_of(result: DownloadResult) -> DownloadCopy:
    """The copy a successful download holds."""
    return DOWNLOAD_COPY[result.source](result)


__all__ = ["DOWNLOAD_COPY", "OPENALEX_VERSIONS", "copy_of"]
