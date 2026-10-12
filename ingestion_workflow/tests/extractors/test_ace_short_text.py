"""ACE text that readability cut short of the article, built again from stored pages.

Each fixture is a stored page from the ns-pond catalog, named by its article id. The
extraction stored for each had a small fraction of the article: the Karger page gave 55
words (readability kept the citation dialog), the Pediatrics page 473 (the table
placeholders led readability to a small block). Needs node >= 20.19 for readabilipy.
"""

import gzip
from pathlib import Path

import pytest
from ingestion_workflow.extractors import ace_extractor as ace
from ingestion_workflow.extractors.figure_captions import HEADING
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
)
from ingestion_workflow.patches.ace_patch import set_skip_remote_tables

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"


def real(article_id: str) -> str:
    return gzip.decompress((PAGES / f"{article_id}.html.gz").read_bytes()).decode("utf-8")


@pytest.fixture(autouse=True)
def _local_tables_only():
    set_skip_remote_tables(True)
    yield
    set_skip_remote_tables(False)


def built(article_id, pmid, tmp_path):
    return ace.article_text(real(article_id), pmid, tmp_path)


def test_a_page_whose_readability_text_is_a_dialog_is_read_without_readability(tmp_path):
    out = built("af3skhwhkgkm", "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    assert out.text_source["prose_words"][ace.READABILITY] == 0
    assert "readability kept 0 of the page's" in out.text_source["reason"]
    assert len(out.text.split()) > 5000
    for heading in ("Introduction", "Results", "Discussion"):
        assert f"\n{heading}\n" in f"\n{out.text}\n"


def test_table_placeholders_that_mislead_readability_are_left_out(tmp_path):
    page = real("gdsqtwkjfl24")
    first = ace.SourceManager(table_dir=str(tmp_path / "first"))
    source = first.identify_source(page) or first.default_source
    tables = source.parse_article(page, pmid="12456913", metadata_dir=None,
                                  skip_metadata=True, keep_tables=True).tables
    out = built("gdsqtwkjfl24", "12456913", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_TABLE_PLACEHOLDERS
    assert out.text_source["prose_words"][ace.READABILITY] == 0
    assert len(out.text.split()) > 8000
    # The tables are the first parse's, and every table on the page is in the text.
    assert len(out.article.tables) == len(tables) == 1
    soup = ace.BeautifulSoup(source.decode_html_entities(page), "lxml")
    rendered = [ace.table_text(t) for t in soup.find_all("table") if not t.find("table")]
    assert len({r for r in rendered if r}) == 3
    assert all(r in out.text for r in rendered if r)


def test_a_letter_keeps_readability_s_text_unrebuilt(tmp_path):
    out = built("ipxutfjxf2p7", "41196626", tmp_path)
    assert out.text_source is None
    assert out.text == ace.article_text_and_captions(real("ipxutfjxf2p7"), "41196626",
                                                     tmp_path)[1]


def test_a_page_without_body_sections_is_not_rebuilt(tmp_path):
    """Epilepsia: an abstract of 35 words, which readability keeps, beside a password
    dialog and a copyright notice, which the page's other 119 words of prose are."""
    out = built("fi4fhyepaqsc", "9579947", tmp_path)
    assert out.text_source is None


def test_caption_spans_are_those_of_the_rebuilt_text(tmp_path):
    caption = "Figure 1. Insula activity during feedback in the trained group."
    page = real("af3skhwhkgkm").replace(
        "</body>", f'<figure id="F1"><figcaption>{caption}</figcaption></figure></body>', 1)
    out = ace.article_text(page, "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    [found] = out.figure_captions
    start, end = found["span"]
    assert out.text.index(HEADING) < start
    assert out.text[start:end] == caption


def test_the_extraction_records_how_its_text_was_made(tmp_path):
    page = tmp_path / "page.html"
    page.write_text(real("af3skhwhkgkm"), encoding="utf-8")
    download = DownloadResult(
        identifier=Identifier(pmid="30699438"),
        source=DownloadSource.ACE,
        success=True,
        files=[DownloadedFile(file_path=page, file_type=FileType.HTML,
                              content_type="text/html", source=DownloadSource.ACE)],
    )
    content = ace._extract_ace_article(download, tmp_path / "out")
    assert content.full_text_path.read_text(encoding="utf-8").count(" ") > 5000
    assert content.text_source["method"] == ace.WITHOUT_READABILITY
    again = ExtractedContent.from_dict(content.to_dict())
    assert again.text_source == content.text_source


def test_the_extract_summary_says_which_way_and_why(tmp_path):
    from ingestion_workflow.catalog import ArticleRef
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages import ExtractStage

    text = tmp_path / "article.txt"
    text.write_text("text", encoding="utf-8")
    source = {"method": ace.WITHOUT_READABILITY, "reason": "readability kept 0 of 4,460",
              "page_prose_words": 4460, "prose_words": {}}
    content = ExtractedContent(slug="s", source=DownloadSource.ACE, full_text_path=text,
                               text_source=source)
    stage = ExtractStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))
    work = Work(ref=ArticleRef(id="a1", identifier=Identifier(pmid="1")), source="ace",
                fingerprint="fp")
    summary = stage._outcome(work, "ace", content).summary
    assert summary["text_from"] == "without_readability (readability kept 0 of 4,460)"
    plain = ExtractedContent(slug="s", source=DownloadSource.ACE, full_text_path=text)
    assert "text_from" not in stage._outcome(work, "ace", plain).summary
    assert "text_source" not in plain.to_dict()
