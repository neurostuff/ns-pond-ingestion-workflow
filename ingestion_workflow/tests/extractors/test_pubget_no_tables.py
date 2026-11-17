"""A Pubget article whose tables cannot be read still has its figure legends in the text."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from ingestion_workflow.extractors.pubget_extractor import (
    UnreadableJats,
    _extract_pubget_article,
    _run_pubget_extraction_task,
    article_text_and_captions,
    read_article,
)
from ingestion_workflow.extractors.utils import build_downloaded_file
from ingestion_workflow.models import DownloadResult, DownloadSource, FileType, Identifier
import pytest
from lxml import etree

ARTICLE = Path(__file__).parent.parent / "data" / "test_pubget" / "articles" / "1a4" / "pmcid_9056519"


def test_a_pubget_article_whose_tables_cannot_be_read_keeps_its_figure_caption_spans(tmp_path):
    article_dir = tmp_path / "article"
    shutil.copytree(ARTICLE, article_dir)
    for info in (article_dir / "tables").glob("table_*_info.json"):
        data = json.loads(info.read_text(encoding="utf-8"))
        del data["table_data_file"]
        info.write_text(json.dumps(data), encoding="utf-8")
    source = DownloadSource.PUBGET
    download = DownloadResult(
        identifier=Identifier(pmcid="PMC9056519"),
        source=source,
        success=True,
        files=[
            build_downloaded_file(article_dir / "article.xml", FileType.XML, source=source),
            build_downloaded_file(article_dir / "tables" / "tables.xml", FileType.XML, source=source),
        ],
    )

    extraction = _extract_pubget_article(download, tmp_path / "extracted")

    assert extraction.error_message.startswith("Pubget failed to extract any tables")
    text, captions = article_text_and_captions(etree.parse(str(article_dir / "article.xml")), article_dir)
    assert captions and extraction.figure_captions == captions
    assert extraction.full_text_path.read_text(encoding="utf-8") == text


def _download(article_dir):
    source = DownloadSource.PUBGET
    return DownloadResult(
        identifier=Identifier(pmcid="PMC9056519"),
        source=source,
        success=True,
        files=[
            build_downloaded_file(article_dir / "article.xml", FileType.XML, source=source),
            build_downloaded_file(article_dir / "tables" / "tables.xml", FileType.XML, source=source),
        ],
    )


def _broken(tmp_path, edit):
    """A copy of the fixture article whose article.xml `edit` has changed."""
    broken = tmp_path / "article"
    shutil.copytree(ARTICLE, broken)
    xml = (broken / "article.xml").read_text(encoding="utf-8")
    (broken / "article.xml").write_text(edit(xml), encoding="utf-8")
    return broken


def _break_in_body(xml):
    """One closing tag broken the way pubget's copy of PMC4542504 is (`<bold>SD</bold=`),
    before the reference list."""
    at = xml.index("<back>")
    return xml[:at].replace("</italic>", "</italic=", 1) + xml[at:]


def test_malformed_jats_is_read_as_the_pmc_sources_read_it(tmp_path):
    """PMC's JATS is sometimes malformed past the front matter. A strict parse failed the
    whole extraction (267 articles); recovery keeps all of the text but what broke, its
    `&lt;` and `&amp;` included, and says what it got past."""
    intact = _extract_pubget_article(_download(ARTICLE), tmp_path / "intact")
    want = intact.full_text_path.read_text(encoding="utf-8")
    broken = _broken(tmp_path, _break_in_body)
    with pytest.raises(etree.XMLSyntaxError):
        etree.parse(str(broken / "article.xml"))

    extraction = _extract_pubget_article(_download(broken), tmp_path / "extracted")

    text = extraction.full_text_path.read_text(encoding="utf-8")
    at = next(i for i, (a, b) in enumerate(zip(want, text)) if a != b)
    # the `=` of `</italic=` is kept as text; every `&lt;` after it too
    assert text[:at] + text[at + 1:] == want
    assert len(extraction.tables) == len(intact.tables) > 0
    assert intact.error_message is None
    assert extraction.error_message.startswith("article.xml recovered past 1 XML error(s)")


def test_references_and_links_read_malformed_jats_as_the_extraction_did(tmp_path):
    """References and links place spans in the text the extraction wrote, so they read the
    JATS as it did; a strict parse failed them on the same files."""
    from ingestion_workflow.services.citations import read_jats
    from ingestion_workflow.services.paper_links import _jats

    intact = _extract_pubget_article(_download(ARTICLE), tmp_path / "intact")
    intact_text = intact.full_text_path.read_text(encoding="utf-8")
    broken = _broken(tmp_path, _break_in_body)
    extraction = _extract_pubget_article(_download(broken), tmp_path / "extracted")
    text = extraction.full_text_path.read_text(encoding="utf-8")
    want = read_jats(ARTICLE / "article.xml", intact_text)
    want_links = _jats(ARTICLE / "article.xml", intact_text, {})

    got = read_jats(broken / "article.xml", text)
    links = _jats(broken / "article.xml", text, {})

    def cited(result, text):
        return [(c["references"], text[c["text_span"]["start_char"]:c["text_span"]["end_char"]])
                for c in result.citations]

    def placed(occurrences, text):
        return [(o.url, None if o.start_char is None else text[o.start_char:o.end_char])
                for o in occurrences]

    assert got.references == want.references and len(want.references) == 54
    assert cited(got, text) == cited(want, intact_text) and want.citations
    assert placed(links, text) == placed(want_links, intact_text) and want_links


def test_text_refresh_rebuilds_malformed_jats_as_the_extraction_wrote_it(tmp_path):
    from ingestion_workflow.services.text_refresh import build

    broken = _broken(tmp_path, _break_in_body)
    extraction = _extract_pubget_article(_download(broken), tmp_path / "extracted")

    text, captions, _ = build("pubget", broken / "article.xml", extraction.full_text_path)

    assert text == extraction.full_text_path.read_text(encoding="utf-8")
    assert captions == extraction.figure_captions


def _xml_1_1(xml):
    """The same article declared XML 1.1, which libxml2 reads with a warning, not an error."""
    return '<?xml version="1.1" encoding="utf-8"?>\n' + xml[xml.index("<article"):]


@pytest.mark.parametrize(
    "edit",
    [_xml_1_1, lambda xml: _xml_1_1(xml[: xml.index("<body")] + "</article>\n")],
    ids=["with-body", "without-body"],
)
def test_a_warning_is_not_a_recovery(tmp_path, edit):
    broken = _broken(tmp_path, edit)
    etree.parse(str(broken / "article.xml"))  # strict accepts it

    tree, recovered = read_article(broken / "article.xml")

    assert recovered is None
    assert etree.QName(tree.getroot()).localname == "article"


@pytest.mark.parametrize(
    "edit, reason",
    [
        (lambda xml: xml[: len(xml) // 10], "article.xml is truncated"),
        (lambda xml: xml[: xml.index("<body")], "article.xml is truncated"),
        (lambda xml: "<html><head><title>503 Service Unavailable</title></head>"
                     "<body><p>Service Unavailable</p></body></html>", "is not a JATS article"),
    ],
    ids=["truncated-in-body", "truncated-before-body", "error-page"],
)
def test_jats_that_recovery_cannot_make_an_article_fails_the_extraction(tmp_path, edit, reason):
    """Recovery keeps whatever parses: of a truncated file the part before the cut, of
    an error page nothing. Such a file is no article, and its extraction fails."""
    broken = _broken(tmp_path, edit)

    extraction = _run_pubget_extraction_task(_download(broken), tmp_path / "extracted")

    assert reason in extraction.error_message
    assert extraction.tables == []
    with pytest.raises(UnreadableJats):
        read_article(broken / "article.xml")
