from pathlib import Path
from types import SimpleNamespace

import pytest
from ace.config import reset_config
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ace_extractor as ace_module
from ingestion_workflow.extractors.ace_extractor import ACEExtractor
from ingestion_workflow.models import (
    CoordinateSpace,
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
    Identifier,
    Identifiers,
)


@pytest.mark.usefixtures("manifest_identifiers")
@pytest.mark.vcr()
def test_ace_downloads_html_articles(tmp_path, manifest_identifiers):
    excluded_pmids = {"31268615", "29069521"}

    subset_identifiers = []
    for identifier in manifest_identifiers.identifiers:
        if identifier.pmid in excluded_pmids:
            continue
        subset_identifiers.append(identifier)
        if len(subset_identifiers) == 10:
            break

    subset = Identifiers(subset_identifiers)

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        ace_cache_root=tmp_path / "ace_cache",
        ace_max_workers=1,
    )

    extractor = ACEExtractor(settings=settings, download_mode="browser")

    try:
        results = extractor.download(subset)
    finally:
        reset_config("SAVE_ORIGINAL_HTML")

    assert len(results) == len(subset.identifiers)

    successes = [result for result in results if result.success]
    if not successes:
        failure_messages = [result.error_message or "" for result in results]
        pytest.fail("No ACE downloads succeeded; failure details: " + " | ".join(failure_messages))

    ace_root = settings.ace_cache_root

    for index, result in enumerate(results):
        identifier = subset.identifiers[index]
        assert result.identifier is identifier
        assert result.source is DownloadSource.ACE

        if result.success:
            assert result.files, "Successful downloads should persist files"
            downloaded_file = result.files[0]
            assert downloaded_file.file_type is FileType.HTML
            assert downloaded_file.file_path.exists()
            assert downloaded_file.file_path.suffix == ".html"
            assert downloaded_file.content_type == "text/html"
            assert ace_root in downloaded_file.file_path.parents
            assert result.error_message is None
        else:
            assert not result.files
            assert result.error_message is not None


def _build_settings(tmp_path: Path) -> Settings:
    return Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        ace_cache_root=tmp_path / "ace_cache",
        ace_max_workers=1,
        max_workers=1,
    )


def test_ace_extract_translates_tables(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    extractor = ACEExtractor(settings=settings, download_mode="browser")

    html_path = tmp_path / "article.html"
    html_path.write_text("<html><body>Example</body></html>", encoding="utf-8")

    identifier = Identifier(pmid="12345678")
    downloaded_file = DownloadedFile(
        file_path=html_path,
        file_type=FileType.HTML,
        content_type="text/html",
        source=DownloadSource.ACE,
    )
    download_result = DownloadResult(
        identifier=identifier,
        source=DownloadSource.ACE,
        success=True,
        files=[downloaded_file],
    )

    def fake_guess_space(_: str) -> str:
        return "MNI"

    monkeypatch.setattr(
        ace_module.ace_extract,
        "guess_space",
        fake_guess_space,
    )

    class DummySource:
        def __init__(self, table_dir: str) -> None:
            self.table_dir = Path(table_dir)

        def parse_article(
            self,
            html_text: str,
            pmid: str | None,
            metadata_dir,
            skip_metadata: bool = False,
        ):
            table = SimpleNamespace(
                number="1",
                input_html="<table><tr><td>X</td></tr></table>",
                caption="Activation peaks",
                notes="Sample notes",
                activations=[
                    SimpleNamespace(
                        x="1",
                        y="2",
                        z="3",
                        statistic="4.5",
                        size="10",
                    )
                ],
                label="Table 1",
                position="Main",
                n_activations=1,
                n_columns=3,
            )
            return SimpleNamespace(
                text="Full article text",
                tables=[table],
                space=None,
            )

    class DummySourceManager:
        def __init__(self, table_dir: str) -> None:
            self.table_dir = Path(table_dir)

        def identify_source(self, html_text: str):
            return DummySource(str(self.table_dir))

    monkeypatch.setattr(ace_module, "SourceManager", DummySourceManager)

    try:
        extraction_results = extractor.extract([download_result])
    finally:
        reset_config("SAVE_ORIGINAL_HTML")

    assert len(extraction_results) == 1
    result = extraction_results[0]
    assert result.error_message is None
    assert result.full_text_path is not None
    assert result.full_text_path.read_text(encoding="utf-8") == ("Full article text")
    assert result.has_coordinates is True
    assert len(result.tables) == 1

    table = result.tables[0]
    assert table.raw_content_path.exists()
    assert table.raw_content_path.read_text(encoding="utf-8") == (
        "<table><tr><td>X</td></tr></table>"
    )
    assert table.space is CoordinateSpace.MNI
    assert len(table.coordinates) == 1
    coord = table.coordinates[0]
    assert coord.x == 1.0
    assert coord.y == 2.0
    assert coord.z == 3.0
    assert coord.statistic_value == 4.5
    assert coord.cluster_size == 10


def test_ace_extract_reports_missing_html(tmp_path):
    settings = _build_settings(tmp_path)
    extractor = ACEExtractor(settings=settings, download_mode="browser")

    pdf_path = tmp_path / "article.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    identifier = Identifier(pmid="24681357")
    downloaded_file = DownloadedFile(
        file_path=pdf_path,
        file_type=FileType.PDF,
        content_type="application/pdf",
        source=DownloadSource.ACE,
    )
    download_result = DownloadResult(
        identifier=identifier,
        source=DownloadSource.ACE,
        success=True,
        files=[downloaded_file],
    )

    try:
        extraction_results = extractor.extract([download_result])
    finally:
        reset_config("SAVE_ORIGINAL_HTML")

    assert len(extraction_results) == 1
    result = extraction_results[0]
    assert result.error_message is not None
    assert "HTML" in result.error_message
    assert result.full_text_path is None
    assert result.tables == []
    assert result.has_coordinates is False


def test_tables_ace_did_not_parse_are_still_kept(tmp_path):
    """ACE returns only the tables it recognised as activation tables.

    One it missed used to be absent from the artifact entirely, so no later
    detector could look at it and the article's coordinates were lost. Keeping
    them with an empty coordinate list leaves `create_analyses` unaffected --
    it already skips a table with no coordinates -- while making the miss
    recoverable.
    """
    from ingestion_workflow.extractors.ace_extractor import (
        _table_fingerprint,
        _unparsed_html_tables,
    )
    from ingestion_workflow.models import CoordinateSpace, ExtractedTable

    activations = (
        '<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>'
        '<tr><td>L IFG</td><td>-42</td><td>18</td><td>4</td></tr></table>'
    )
    demographics = (
        '<table><tr><th>Group</th><th>Age</th></tr>'
        '<tr><td>Patients</td><td>34</td></tr></table>'
    )
    missed = (
        '<table><tr><th>Region</th><th>MNI</th></tr>'
        '<tr><td>R IFG</td><td>44 16 2</td></tr></table>'
    )
    document = "<html>%s<p>prose</p>%s%s</html>" % (activations, demographics, missed)

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    # ACE rewrites the markup it keeps, so the bytes differ while the text does not
    ace_copy = tables_dir / "table-1.html"
    ace_copy.write_text(
        '<table border="1"><tbody><tr><th>Region</th><th>x</th><th>y</th>'
        '<th>z</th></tr><tr><td>L IFG</td><td>-42</td><td>18</td>'
        '<td>4</td></tr></tbody></table>',
        encoding="utf-8",
    )
    already = ExtractedTable(
        table_id="table-1",
        raw_content_path=ace_copy,
        table_number=1,
        caption="Activations",
        footer="",
        coordinates=[],
        space=CoordinateSpace.MNI,
    )

    extra = _unparsed_html_tables(document, [already], tables_dir, CoordinateSpace.MNI)

    ids = [t.table_id for t in extra]
    assert len(extra) == 2, ids
    # the one ACE already returned is not duplicated, despite the rewritten markup
    kept = [_table_fingerprint(t.raw_content_path.read_text(encoding="utf-8"))
            for t in extra]
    assert _table_fingerprint(activations) not in kept
    assert _table_fingerprint(demographics) in kept
    assert _table_fingerprint(missed) in kept
    # they arrive with nothing claimed, so nothing downstream treats them as results
    assert all(t.coordinates == [] for t in extra)
    assert all(t.metadata.get("origin") == "html-scan" for t in extra)
    assert all(t.raw_content_path.exists() for t in extra)


def test_the_html_scan_ignores_a_document_with_no_tables(tmp_path):
    from ingestion_workflow.extractors.ace_extractor import _unparsed_html_tables
    from ingestion_workflow.models import CoordinateSpace

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    assert _unparsed_html_tables("<html><p>no tables here</p></html>", [],
                                 tables_dir, CoordinateSpace.OTHER) == []


def test_a_remote_table_is_skipped_when_asked(tmp_path, monkeypatch):
    """Many publishers serve a table on its own page, and ACE fetches each one
    while parsing. That makes extraction network-bound and fails outright when
    the publisher is unreachable."""
    from ingestion_workflow.patches import ace_patch

    calls = []
    monkeypatch.setattr(ace_patch.ace_sources.scrape, "get_url",
                        lambda url: calls.append(url) or "<table><tr><td>1</td></tr></table>")

    class _Source:
        table_dir = str(tmp_path)

        def decode_html_entities(self, html):
            return html

    ace_patch.set_skip_remote_tables(True)
    try:
        assert ace_patch._patched_download_table(_Source(), "http://x/tbl1") is None
        assert calls == []
    finally:
        ace_patch.set_skip_remote_tables(False)

    # with the skip off it fetches as before
    assert ace_patch._patched_download_table(_Source(), "http://x/tbl1") is not None
    assert calls == ["http://x/tbl1"]


def test_a_cached_table_is_used_even_when_skipping(tmp_path, monkeypatch):
    """Only the fetch is skipped. A table an earlier run downloaded is still
    returned, so the skip does not silently shrink the corpus."""
    from ingestion_workflow.patches import ace_patch

    monkeypatch.setattr(ace_patch.ace_sources.scrape, "get_url",
                        lambda url: pytest.fail("should not have been called"))
    cached = tmp_path / "http:__x_tbl1"
    cached.write_text("<table><tr><td>cached</td></tr></table>", encoding="utf-8")

    class _Source:
        table_dir = str(tmp_path)

        def decode_html_entities(self, html):
            return html

    ace_patch.set_skip_remote_tables(True)
    try:
        soup = ace_patch._patched_download_table(_Source(), "http://x/tbl1")
    finally:
        ace_patch.set_skip_remote_tables(False)
    assert soup is not None
    assert "cached" in str(soup)


def test_the_skip_travels_through_the_environment_to_a_worker():
    """A module flag does not reach a spawned worker; the environment does."""
    import os

    from ingestion_workflow.patches import ace_patch

    ace_patch.set_skip_remote_tables(True)
    try:
        assert os.environ[ace_patch.SKIP_REMOTE_ENV] == "1"
        assert ace_patch.skipping_remote_tables() is True
    finally:
        ace_patch.set_skip_remote_tables(False)
    assert ace_patch.SKIP_REMOTE_ENV not in os.environ
    assert ace_patch.skipping_remote_tables() is False


def test_a_rescued_table_keeps_its_caption_and_footnote():
    """A caption names the contrast and often the coordinate space; a footnote
    carries the threshold and the statistic. 15.7% of real tables state their
    space only in that surrounding text, so a table scanned out of the document
    without it arrives strictly poorer than one the parser returned."""
    import re

    from ingestion_workflow.extractors.ace_extractor import (
        _HTML_TABLE,
        _caption_and_footer,
    )

    doc = (
        '<html><body>'
        '<div class="tblCaption">Table 3. Regions showing activation in MNI space.</div>'
        '<table><tr><th>Region</th><th>x</th></tr>'
        '<tr><td>L IFG</td><td>-42</td></tr></table>'
        '<div class="tblFn">Threshold p&lt;0.05 FWE-corrected. L: left; R: right.</div>'
        '<p>Unrelated prose.</p>'
        '<table><tr><th>Group</th><th>Age</th></tr>'
        '<tr><td>Patients</td><td>34</td></tr></table>'
        '</body></html>'
    )
    blocks = list(_HTML_TABLE.finditer(doc))
    assert len(blocks) == 2

    caption, footer = _caption_and_footer(doc, blocks[0].start(), blocks[0].end(),
                                          blocks[0].group(0))
    assert "Regions showing activation in MNI space" in caption
    assert "FWE-corrected" in footer

    # the second table has no captioned block of its own; it must not inherit
    # the first table's footnote as a caption
    caption2, _ = _caption_and_footer(doc, blocks[1].start(), blocks[1].end(),
                                      blocks[1].group(0))
    assert "Regions showing activation" not in caption2


def test_an_inline_caption_element_is_preferred():
    from ingestion_workflow.extractors.ace_extractor import (
        _HTML_TABLE,
        _caption_and_footer,
    )

    doc = ('<div class="caption">Wrong one</div>'
           '<table><caption>Table 1. Peak activations.</caption>'
           '<tr><td>x</td></tr></table>')
    m = next(_HTML_TABLE.finditer(doc))
    caption, _ = _caption_and_footer(doc, m.start(), m.end(), m.group(0))
    assert caption == "Table 1. Peak activations."


def test_a_bare_table_label_is_used_when_no_captioned_block_exists():
    from ingestion_workflow.extractors.ace_extractor import (
        _HTML_TABLE,
        _caption_and_footer,
    )

    doc = ('<p>Table 2. Clusters surviving correction.</p>'
           '<table><tr><td>-42</td></tr></table>')
    m = next(_HTML_TABLE.finditer(doc))
    caption, _ = _caption_and_footer(doc, m.start(), m.end(), m.group(0))
    assert caption.startswith("Table 2.")
