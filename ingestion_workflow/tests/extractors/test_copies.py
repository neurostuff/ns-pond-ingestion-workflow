"""Each download source's rule for the copy it holds, on real downloads.

The PMC-family fixtures are excerpts of real downloads: each article.xml keeps its root
element and its article-meta's article ids, permissions and (for the preprint) version;
the Cloud Service records are whole.
"""

from pathlib import Path

import pytest
from ingestion_workflow.extractors.copies import DOWNLOAD_COPY, copy_of
from ingestion_workflow.models import (
    CopyAccess,
    CopyRoute,
    CopyVersion,
    DownloadCopy,
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
    Identifier,
)

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
    (DownloadSource.PMC, "pmc_tdm_manuscript", CopyVersion.AM, CopyAccess.TDM_GRANT),
    (DownloadSource.PMC, "pmc_open_access", CopyVersion.VOR, CopyAccess.PUBLIC),
    # PMC12036453: a Research Square preprint, by its article version's status.
    (DownloadSource.PMC, "pmc_preprint", CopyVersion.SUBMITTED, CopyAccess.PUBLIC),
    # PMC12823415: a Nature VoR whose related-article names its bioRxiv preprint.
    (DownloadSource.PMC, "pmc_links_its_preprint", CopyVersion.VOR, CopyAccess.PUBLIC),
    # PMC4001721, from efetch, with no cloud record: the JATS licence says text mining only.
    (DownloadSource.PMC, "pmc_efetch_tdm_manuscript", CopyVersion.AM, CopyAccess.TDM_GRANT),
    # PMC4180771: efetch's JATS names an HHMI manuscript id.
    (DownloadSource.PUBGET, "pubget_manuscript", CopyVersion.AM, CopyAccess.PUBLIC),
    (DownloadSource.EUROPEPMC, "europepmc", CopyVersion.VOR, CopyAccess.PUBLIC),
])
def test_a_pmc_copy_is_read_from_its_jats_and_cloud_record(source, fixture, version, access):
    assert copy_of(_held(source, fixture)) == DownloadCopy(
        route=CopyRoute.PMC, version=version, access=access)


def test_the_cloud_record_decides_access_over_the_jats_licence(tmp_path):
    """PMC10518222's JATS says text mining only; a record saying CC BY is what counts."""
    held = _held(DownloadSource.PMC, "pmc_tdm_manuscript")
    record = tmp_path / "pmc_cloud.json"
    record.write_text('{"license_code": "CC BY", "is_manuscript": true}')
    held.files = [f for f in held.files if f.file_path.name != "pmc_cloud.json"]
    held.files.append(DownloadedFile(record, FileType.JSON, "application/json",
                                     DownloadSource.PMC))

    assert copy_of(held).access is CopyAccess.PUBLIC


@pytest.mark.parametrize("source, access", [
    (DownloadSource.ELSEVIER, CopyAccess.TDM_GRANT),
    (DownloadSource.SPRINGER, CopyAccess.PUBLIC),
    (DownloadSource.ACE, CopyAccess.PUBLIC),
])
def test_a_publisher_copy_is_its_version_of_record(source, access):
    held = DownloadResult(identifier=Identifier(doi="10.1/x"), source=source, success=True)

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.PUBLISHER, version=CopyVersion.VOR, access=access)


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


def test_a_pdf_copy_is_the_location_it_was_fetched_from():
    held = _fetched_pdf(url="https://repo.example.edu/a.pdf", location_type="repository",
                        version="acceptedVersion")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.REPOSITORY, version=CopyVersion.AM, access=CopyAccess.PUBLIC,
        url="https://repo.example.edu/a.pdf")


def test_a_pdf_on_a_preprint_server_names_the_server():
    """OpenAlex lists bioRxiv as a repository; the host says it is a preprint server. A
    landing page's copy is the PDF it named."""
    held = _fetched_pdf(url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1",
                        pdf_url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1.full.pdf",
                        location_type="repository", version="submittedVersion")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.PREPRINT_SERVER, version=CopyVersion.SUBMITTED,
        access=CopyAccess.PUBLIC, preprint_server="biorxiv",
        url="https://www.biorxiv.org/content/10.1101/2020.01.01.123456v1.full.pdf")


def test_a_semantic_scholar_pdf_has_no_recorded_route():
    held = _fetched_pdf(url="https://example.org/a.pdf", provider="semantic_scholar",
                        location_type="unknown")

    assert copy_of(held) == DownloadCopy(
        route=CopyRoute.UNRECORDED, access=CopyAccess.PUBLIC, url="https://example.org/a.pdf")


def test_a_pdf_with_no_fetch_recorded_is_unrecorded():
    held = DownloadResult(identifier=Identifier(doi="10.1/x"), source=DownloadSource.PDF,
                          success=True)

    assert copy_of(held) == DownloadCopy(route=CopyRoute.UNRECORDED,
                                         access=CopyAccess.UNRECORDED)


def test_a_copy_round_trips_through_its_summary():
    copy = DownloadCopy(route=CopyRoute.PREPRINT_SERVER, version=CopyVersion.SUBMITTED,
                        access=CopyAccess.PUBLIC, url="https://arxiv.org/pdf/1",
                        preprint_server="arxiv")

    assert copy.to_dict() == {"route": "preprint_server", "version": "submitted",
                              "access": "public", "url": "https://arxiv.org/pdf/1",
                              "preprint_server": "arxiv"}
    assert DownloadCopy.from_dict(copy.to_dict()) == copy
    unrecorded = DownloadCopy(route=CopyRoute.UNRECORDED, access=CopyAccess.UNRECORDED)
    assert unrecorded.to_dict() == {"route": "unrecorded", "access": "unrecorded"}
    assert DownloadCopy.from_dict(unrecorded.to_dict()) == unrecorded
