"""A downloaded page that is not the article is a failed download, and the next source is tried."""

import gzip
from pathlib import Path

import pytest
from ingestion_workflow.catalog import ArticleRef, Catalog, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import DownloadStage

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"


class FakeSource:
    def __init__(self, source, page=None):
        self.source, self.page, self.calls = source, page, 0

    def download(self, identifiers):
        self.calls += 1
        results = []
        for identifier in identifiers:
            if self.page is None:
                results.append(
                    DownloadResult(identifier, self.source, False, error_message="no full text")
                )
                continue
            results.append(DownloadResult(identifier, self.source, True, files=[
                DownloadedFile(self.page, FileType.HTML, "text/html", self.source)]))
        return results


def page(tmp_path, article_id):
    path = tmp_path / f"{article_id}.html"
    path.write_bytes(gzip.decompress((PAGES / f"{article_id}.html.gz").read_bytes()))
    return path


@pytest.fixture()
def stage(tmp_path):
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k",
        download_sources=["ace", "pdf"],
    )
    stage = DownloadStage(settings)
    ctx = Context(settings, Catalog.open(settings.catalog_root))
    return stage, ctx


def run(stage, ctx, ace, pdf):
    stage._extractors = {DownloadSource.ACE: ace, DownloadSource.PDF: pdf}
    ref = ArticleRef(id="a1", identifier=Identifier(pmid="123", doi="10.1/x"))
    work = Work(ref=ref, source="ace", fingerprint=stage.fingerprint_for(DownloadSource.ACE))
    return list(stage.execute(ctx, [work]))


def test_a_paywall_page_fails_with_its_reason_and_the_next_source_is_tried(tmp_path, stage):
    ace = FakeSource(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d"))
    pdf = FakeSource(DownloadSource.PDF, page(tmp_path, "ytpicngb4emb"))
    first, second = run(*stage, ace, pdf)
    assert (first.source, first.status) == ("ace", Status.FAILED)
    assert first.summary == {"bad_page": "paywall"}
    assert "abstract/extract view" in first.error
    assert (second.source, second.status) == ("pdf", Status.OK)


def test_with_no_source_left_the_article_stays_failed(tmp_path, stage):
    ace = FakeSource(DownloadSource.ACE, page(tmp_path, "a6mheoqpeocv"))
    outcomes = run(*stage, ace, FakeSource(DownloadSource.PDF))
    assert [o.status for o in outcomes] == [Status.FAILED, Status.PERMANENT]
    assert outcomes[0].summary == {"bad_page": "error_page"}


def test_an_error_page_quoting_404_is_retried_later_not_given_up_on(tmp_path, stage):
    html = tmp_path / "404.html"
    html.write_text("<html><head><title>404 Not Found</title></head><body><h1>404 Not Found</h1>"
                    + "<p>The page you requested could not be found on this server.</p>" * 5
                    + "</body></html>")
    outcomes = run(*stage, FakeSource(DownloadSource.ACE, html), FakeSource(DownloadSource.PDF))
    assert outcomes[0].status is Status.FAILED and "404" in outcomes[0].error


def test_a_full_article_is_kept(tmp_path, stage):
    ace = FakeSource(DownloadSource.ACE, page(tmp_path, "onv6piwppvwo"))
    pdf = FakeSource(DownloadSource.PDF, page(tmp_path, "ytpicngb4emb"))
    outcomes = run(*stage, ace, pdf)
    assert [(o.source, o.status) for o in outcomes] == [("ace", Status.OK)]
    assert pdf.calls == 0
