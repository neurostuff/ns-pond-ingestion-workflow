"""Each download source's rule for the copy it holds, on real downloads.

The PMC-family fixtures are excerpts of real downloads: each article.xml keeps its root
element and its article-meta's article ids, versions, related articles and permissions;
the Cloud Service records are whole. The two malformed ones are a whole file and a
file's head, as stored.
"""

from pathlib import Path

import pytest
from ingestion_workflow.extractors.copies import DOWNLOAD_COPY, copy_of
from ingestion_workflow.models import (
    DownloadCopy,
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
    Identifier,
)
from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

COPIES = Path(__file__).resolve().parents[1] / "data" / "copies"


def _held(source: DownloadSource, fixture: str) -> DownloadResult:
    files = [
        DownloadedFile(path, FileType.JSON if path.suffix == ".json" else FileType.XML,
                       "application/octet-stream", source)
        for path in sorted((COPIES / fixture).iterdir())
    ]
    return DownloadResult(identifier=Identifier(pmcid="PMC1"), source=source, success=True,
                          files=files)


def test_every_download_source_has_a_rule():
    assert set(DOWNLOAD_COPY) == set(DownloadSource)


@pytest.mark.parametrize("source, fixture, version, access", [
    # PMC10518222: an NIH manuscript whose cloud record says TDM; its VoR is CC BY-NC.
    (DownloadSource.PMC, "pmc_tdm_manuscript", CopyVersion.am, CopyAccess.tdm_grant),
    (DownloadSource.PMC, "pmc_open_access", CopyVersion.vor, CopyAccess.public),
    # PMC12036453: a Research Square preprint, by its article version's status.
    (DownloadSource.PMC, "pmc_preprint", CopyVersion.submitted, CopyAccess.public),
    # PMC12823415: a Nature VoR whose related-article names its bioRxiv preprint.
    (DownloadSource.PMC, "pmc_links_its_preprint", CopyVersion.vor, CopyAccess.public),
    # PMC4001721, from efetch, with no cloud record: the JATS licence says text mining only.
    (DownloadSource.PMC, "pmc_efetch_tdm_manuscript", CopyVersion.am, CopyAccess.tdm_grant),
    # PMC4180771: efetch's JATS names an HHMI manuscript id, under Nature's terms, which
    # PMC's cloud record codes TDM.
    (DownloadSource.PUBGET, "pubget_manuscript", CopyVersion.am, CopyAccess.tdm_grant),
    # PMC10635735: an NIH manuscript under CC BY.
    (DownloadSource.PUBGET, "pubget_cc_manuscript", CopyVersion.am, CopyAccess.public),
    # PMC3713899: a VoR under an open licence that is not Creative Commons.
    (DownloadSource.PUBGET, "pubget_open_licence", CopyVersion.vor, CopyAccess.public),
    (DownloadSource.EUROPEPMC, "europepmc", CopyVersion.vor, CopyAccess.public),
    # PMC4180771 again, as Europe PMC's JATS names it: by its NIHMS id.
    (DownloadSource.EUROPEPMC, "europepmc_manuscript", CopyVersion.am, CopyAccess.tdm_grant),
    # PMC2778627, whole: a corrupt tag in its references, past the front matter.
    (DownloadSource.PUBGET, "pubget_malformed", CopyVersion.vor, CopyAccess.public),
    # PMC4684219, to the end of its front matter: a corrupt namespace in its root
    # element, which loses the front matter.
    (DownloadSource.PUBGET, "pubget_lost_front", None, CopyAccess.unrecorded),
])
def test_a_pmc_copy_is_read_from_its_jats_and_cloud_record(source, fixture, version, access):
    assert copy_of(_held(source, fixture)) == DownloadCopy(
        route=CopyRoute.pmc, version=version, access=access)


def test_a_copy_whose_front_matter_is_lost_records_no_version():
    copy = copy_of(_held(DownloadSource.PUBGET, "pubget_lost_front"))

    assert copy.to_dict() == {"route": "pmc", "access": "unrecorded"}


def test_the_cloud_record_decides_access_over_the_jats_licence(tmp_path):
    """PMC10518222's JATS says text mining only; a record saying CC BY is what counts."""
    held = _held(DownloadSource.PMC, "pmc_tdm_manuscript")
    record = tmp_path / "pmc_cloud.json"
    record.write_text('{"license_code": "CC BY", "is_manuscript": true}')
    held.files = [f for f in held.files if f.file_path.name != "pmc_cloud.json"]
    held.files.append(DownloadedFile(record, FileType.JSON, "application/json",
                                     DownloadSource.PMC))

    assert copy_of(held).access is CopyAccess.public


@pytest.mark.parametrize("source, access", [
    (DownloadSource.ELSEVIER, CopyAccess.tdm_grant),
    (DownloadSource.SPRINGER, CopyAccess.public),
    # Scraped through the university's network, which may hold a subscription.
    (DownloadSource.ACE, CopyAccess.unrecorded),
])
def test_a_publisher_copy_is_its_version_of_record(source, access):
    held = DownloadResult(identifier=Identifier(doi="10.1/x"), source=source, success=True)

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.publisher, version=CopyVersion.vor, access=access)


def _fetched_pdf(**attempt) -> DownloadResult:
    """A PDF download as the pdf source records it: what it passed over, then the fetch."""
    return DownloadResult(
        identifier=Identifier(doi="10.1/x"), source=DownloadSource.PDF, success=True,
        url_attempts=[
            {"url": "https://example.org/dead.pdf", "provider": "openalex",
             "location_type": "publisher", "version": "publishedVersion",
             "outcome": "http_error", "status": 403},
            {"provider": "openalex", "status": 200, "outcome": "pdf",
             "reason": "PDF downloaded", **attempt},
        ],
    )


@pytest.mark.parametrize("location_type, route", [
    ("repository", CopyRoute.repository),
    ("publisher", CopyRoute.publisher),
])
def test_a_pdf_copy_is_the_location_it_was_fetched_from(location_type, route):
    held = _fetched_pdf(url="https://site.example.edu/a.pdf", location_type=location_type,
                        version="acceptedVersion")

    assert copy_of(held) == DownloadCopy(
        route=route, version=CopyVersion.am, access=CopyAccess.public,
        url="https://site.example.edu/a.pdf")


def test_a_pdf_on_pmc_is_pmc_s_copy():
    url = "https://europepmc.org/articles/PMC4180771?pdf=render"
    held = _fetched_pdf(url=url, location_type="repository", version="acceptedVersion")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.pmc, version=CopyVersion.am, access=CopyAccess.public, url=url)


def test_a_pdf_on_a_preprint_server_names_the_server():
    """OpenAlex lists bioRxiv as a repository; the host says it is a preprint server. A
    landing page's copy is the PDF it named."""
    held = _fetched_pdf(url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1",
                        pdf_url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1.full.pdf",
                        location_type="repository", version="submittedVersion")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.preprint_server, version=CopyVersion.submitted,
        access=CopyAccess.public, preprint_server="biorxiv",
        url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1.full.pdf")


@pytest.mark.parametrize("url, version, expected", [
    # 2ecgjgulmuoc: Research Square holds only preprints, whatever OpenAlex labels it.
    ("https://www.researchsquare.com/article/rs-2725537/latest.pdf", "acceptedVersion",
     CopyVersion.submitted),
    ("https://www.medrxiv.org/content/10.1101/2021.01.01.21249999v1.full.pdf",
     "publishedVersion", CopyVersion.submitted),
    # arXiv also holds authors' accepted manuscripts.
    ("https://arxiv.org/pdf/2111.08743", "acceptedVersion", CopyVersion.am),
])
def test_a_server_holding_only_preprints_holds_the_submitted_version(url, version, expected):
    held = _fetched_pdf(url=url, location_type="repository", version=version)

    assert copy_of(held).version is expected


@pytest.mark.parametrize("url, doi, server", [
    ("https://osf.io/preprints/psyarxiv/x2vhs/download", "10.1093/schbul/sbad113", "psyarxiv"),
    ("https://osf.io/preprints/lawarchive/7bt2v_v1", None, "lawarxiv"),
    ("https://osf.io/x2vhs/download", "10.31235/osf.io/x2vhs", "socarxiv"),
    ("https://psyarxiv.com/x2vhs/download", None, "psyarxiv"),
    # production's osf.io PDFs: the URL and a journal's DOI name no provider
    ("https://osf.io/x2vhs/download", "10.1093/schbul/sbad113", "osf_preprints"),
    ("https://osf.io/preprints/eartharxiv/x2vhs", None, "osf_preprints"),
])
def test_an_osf_pdf_names_its_provider_when_the_url_or_the_doi_says(url, doi, server):
    held = _fetched_pdf(url=url, location_type="repository")
    held.identifier = Identifier(doi=doi)

    copy = copy_of(held)

    assert (copy.route, copy.preprint_server) == (CopyRoute.preprint_server, server)


def test_a_semantic_scholar_pdf_is_the_kind_of_site_that_served_it():
    held = _fetched_pdf(url="https://discovery.ucl.ac.uk/10062208/1/a.pdf",
                        provider="semantic_scholar", location_type="unknown",
                        site_type="repository")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.repository, access=CopyAccess.public,
        url="https://discovery.ucl.ac.uk/10062208/1/a.pdf")


def test_a_semantic_scholar_pdf_on_a_site_nothing_types_has_no_recorded_route():
    held = _fetched_pdf(url="https://example.org/a.pdf", provider="semantic_scholar",
                        location_type="unknown", site_type="unknown")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.unrecorded, access=CopyAccess.public, url="https://example.org/a.pdf")


def test_a_pdf_with_no_fetch_recorded_is_unrecorded():
    held = DownloadResult(identifier=Identifier(doi="10.1/x"), source=DownloadSource.PDF,
                          success=True)

    assert copy_of(held) == DownloadCopy(route=CopyRoute.unrecorded,
                                         access=CopyAccess.unrecorded)


def test_a_copy_round_trips_through_its_summary():
    copy = DownloadCopy(route=CopyRoute.preprint_server, version=CopyVersion.submitted,
                        access=CopyAccess.public, url="https://arxiv.org/pdf/1",
                        preprint_server="arxiv")

    assert copy.to_dict() == {"route": "preprint_server", "version": "submitted",
                              "access": "public", "url": "https://arxiv.org/pdf/1",
                              "preprint_server": "arxiv"}
    assert DownloadCopy.from_dict(copy.to_dict()) == copy
    unrecorded = DownloadCopy(route=CopyRoute.unrecorded, access=CopyAccess.unrecorded)
    assert unrecorded.to_dict() == {"route": "unrecorded", "access": "unrecorded"}
    assert DownloadCopy.from_dict(unrecorded.to_dict()) == unrecorded
