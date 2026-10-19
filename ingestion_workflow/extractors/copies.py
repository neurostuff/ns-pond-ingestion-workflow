"""Where each held copy came from, which version it is, and how we obtained it.

Every rule reads only what the download holds (its files and its URL attempts), so a
stored download yields the same facts as a fresh one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from lxml import etree
from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

from ingestion_workflow.extractors.pdf_extractor import _is_pmc_copy
from ingestion_workflow.models import (
    DownloadCopy,
    DownloadResult,
    DownloadSource,
)
from ingestion_workflow.utils.preprints import preprint_server_of_url

#: OpenAlex's version labels, which the PDF candidates carry, in study_schema's terms.
OPENALEX_VERSIONS = {
    "publishedVersion": CopyVersion.vor,
    "acceptedVersion": CopyVersion.am,
    "submittedVersion": CopyVersion.submitted,
}

#: The article ids PMC gives an author manuscript: `manuscript` in efetch's JATS,
#: `manuscript-id` in the Cloud Service's JATS 1.4 (on every record it marks is_manuscript)
#: and in Europe PMC's newer records, `nihmsid` in Europe PMC's older ones.
_MANUSCRIPT_IDS = {"manuscript", "manuscript-id", "nihmsid"}
#: PMC marks a preprint it hosts by the status of its article version.
_STATUS = ".//article-version[@article-version-type='status']"
_PREPRINT_STATUS = "preprint"
#: The Cloud Service's licence code for an author manuscript it offers for text mining only.
_TDM_CODE = "TDM"
_CREATIVE_COMMONS = b"creativecommons.org"
_PDF_LOCATIONS = {"repository": CopyRoute.repository, "publisher": CopyRoute.publisher}


def _file(result: DownloadResult, name: str) -> Optional[Path]:
    return next((f.file_path for f in result.files if f.file_path.name == name), None)


def _pmc_copy(result: DownloadResult) -> DownloadCopy:
    """A PMC article's JATS says whether it is a preprint or an author manuscript."""
    # As the PMC sources read it: some of PMC's JATS is malformed past the front matter.
    parser = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
    article = etree.parse(str(_file(result, "article.xml")), parser).getroot()
    # Malformed JATS can lose its front matter, and with it what says the version.
    meta = article.find("front/article-meta")
    version = None if meta is None else _pmc_version(meta)
    return DownloadCopy(route=CopyRoute.pmc, version=version,
                        access=_pmc_access(result, meta, version))


def _pmc_access(result: DownloadResult, meta: Optional[etree._Element],
                version: Optional[CopyVersion]) -> CopyAccess:
    """The Cloud Service's record says whether PMC offers the copy for text mining only.
    Without one, an author manuscript is offered so unless its own licence is Creative
    Commons, as the Cloud Service codes them."""
    cloud = _file(result, "pmc_cloud.json")
    if cloud is not None:
        code = json.loads(cloud.read_text(encoding="utf-8")).get("license_code")
        return CopyAccess.tdm_grant if code == _TDM_CODE else CopyAccess.public
    if meta is None:
        return CopyAccess.unrecorded
    if version is not CopyVersion.am:
        return CopyAccess.public
    licences = meta.iterfind("permissions/license")
    return (CopyAccess.public if any(_CREATIVE_COMMONS in etree.tostring(lic) for lic in licences)
            else CopyAccess.tdm_grant)


def _pmc_version(meta: etree._Element) -> CopyVersion:
    statuses = {v.text for v in meta.iterfind(_STATUS)}
    if _PREPRINT_STATUS in statuses:
        return CopyVersion.submitted
    ids = {(i.get("pub-id-type") or "").lower() for i in meta.iterfind("article-id")}
    return CopyVersion.am if ids & _MANUSCRIPT_IDS else CopyVersion.vor


def located(
    url: str, doi: Optional[str], version: Optional[CopyVersion], location_type: Optional[str]
) -> Tuple[CopyRoute, Optional[CopyVersion], Optional[str]]:
    """The route, version and preprint server of a copy at `url`, which a location of the
    work (`location_type`, as `pdf_extractor` types them, and `version`) lists."""
    server = preprint_server_of_url(url, doi)
    if _is_pmc_copy(url):
        return CopyRoute.pmc, version, None
    if server is not None:
        return (CopyRoute.preprint_server,
                CopyVersion.submitted if server.preprints_only else version, server.name)
    return _PDF_LOCATIONS.get(str(location_type), CopyRoute.unrecorded), version, None


def _pdf_copy(result: DownloadResult) -> DownloadCopy:
    """The kind of site the PDF was fetched from; a PDF with no fetch recorded (supplied by
    hand, or fetched before attempts were kept) is unrecorded."""
    fetched = [a for a in result.url_attempts if a.get("outcome") == "pdf"]
    if not fetched:
        return DownloadCopy(route=CopyRoute.unrecorded, access=CopyAccess.unrecorded)
    attempt = fetched[-1]
    url = str(attempt.get("pdf_url") or attempt["url"])
    # Semantic Scholar does not say what kind of site a PDF is on; the work's other
    # locations on the same site may (`pdf_extractor.site_type`).
    location_type = (attempt.get("location_type") if attempt.get("location_type") in _PDF_LOCATIONS
                     else attempt.get("site_type"))
    route, version, server = located(url, result.identifier.doi,
                                     OPENALEX_VERSIONS.get(str(attempt.get("version"))),
                                     location_type)
    return DownloadCopy(route=route, version=version, access=CopyAccess.public, url=url,
                        preprint_server=server)


def _wiley_copy(result: DownloadResult) -> DownloadCopy:
    return DownloadCopy(
        route=CopyRoute.publisher,
        version=CopyVersion.vor,
        access=CopyAccess.tdm_grant,
        url=str(result.url_attempts[-1]["url"]),
    )


def _fixed(route: CopyRoute, access: CopyAccess) -> Callable[[DownloadResult], DownloadCopy]:
    copy = DownloadCopy(route=route, version=CopyVersion.vor, access=access)
    return lambda result: copy


#: One rule per download source.
DOWNLOAD_COPY: Dict[DownloadSource, Callable[[DownloadResult], DownloadCopy]] = {
    DownloadSource.PUBGET: _pmc_copy,
    DownloadSource.PMC: _pmc_copy,
    DownloadSource.EUROPEPMC: _pmc_copy,
    # Through our API key, whatever the article's licence.
    DownloadSource.ELSEVIER: _fixed(CopyRoute.publisher, CopyAccess.tdm_grant),
    DownloadSource.WILEY: _wiley_copy,
    # The open-access API serves only openly licensed articles.
    DownloadSource.SPRINGER: _fixed(CopyRoute.publisher, CopyAccess.public),
    # The publisher's page, scraped as our network sees it, often through the
    # university's subscription: not what any reader could fetch.
    DownloadSource.ACE: _fixed(CopyRoute.publisher, CopyAccess.unrecorded),
    DownloadSource.PDF: _pdf_copy,
}


def copy_of(result: DownloadResult) -> DownloadCopy:
    """The copy a successful download holds."""
    return DOWNLOAD_COPY[result.source](result)


__all__ = ["DOWNLOAD_COPY", "OPENALEX_VERSIONS", "copy_of", "located"]
