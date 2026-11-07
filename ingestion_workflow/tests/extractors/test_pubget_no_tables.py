"""A Pubget article whose tables cannot be read still has its figure legends in the text."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from ingestion_workflow.extractors.pubget_extractor import (
    _extract_pubget_article,
    article_text_and_captions,
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


def test_malformed_jats_is_read_as_the_pmc_sources_read_it(tmp_path):
    """PMC's JATS is sometimes malformed past the front matter; pubget's copy of PMC4542504
    holds `<bold>SD</bold=`. A strict parse failed the whole extraction (267 articles)."""
    intact = _extract_pubget_article(_download(ARTICLE), tmp_path / "intact")
    broken = tmp_path / "article"
    shutil.copytree(ARTICLE, broken)
    xml = (broken / "article.xml").read_text(encoding="utf-8")
    (broken / "article.xml").write_text(xml.replace("</italic>", "</italic=", 1), encoding="utf-8")
    with pytest.raises(etree.XMLSyntaxError):
        etree.parse(str(broken / "article.xml"))

    extraction = _extract_pubget_article(_download(broken), tmp_path / "extracted")

    assert len(extraction.tables) == len(intact.tables) > 0
    assert extraction.full_text_path.read_text(encoding="utf-8")


def test_references_and_links_read_malformed_jats_as_the_extraction_did(tmp_path):
    """References and links place spans in the text the extraction wrote, so they parse the
    JATS with the extraction's parser; a strict parse failed them on the same files."""
    from ingestion_workflow.services.citations import read_jats
    from ingestion_workflow.services.paper_links import _jats

    intact = _extract_pubget_article(_download(ARTICLE), tmp_path / "intact")
    broken = tmp_path / "article"
    shutil.copytree(ARTICLE, broken)
    xml = (broken / "article.xml").read_text(encoding="utf-8")
    at = xml.index("<back>")  # the break is in the body; the reference list is intact
    (broken / "article.xml").write_text(
        xml[:at].replace("</italic>", "</italic=", 1) + xml[at:], encoding="utf-8")
    extraction = _extract_pubget_article(_download(broken), tmp_path / "extracted")
    text = extraction.full_text_path.read_text(encoding="utf-8")
    want = read_jats(ARTICLE / "article.xml", intact.full_text_path.read_text(encoding="utf-8"))

    got = read_jats(broken / "article.xml", text)

    assert len(got.references) == len(want.references) > 0
    assert _jats(broken / "article.xml", text, {}) is not None
