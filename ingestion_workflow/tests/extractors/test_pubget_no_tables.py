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
