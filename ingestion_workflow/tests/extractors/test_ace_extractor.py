from types import SimpleNamespace
from pathlib import Path

import pytest

from ace.config import reset_config

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ace_extractor as ace_module
from ingestion_workflow.extractors.ace_extractor import ACEExtractor
from ingestion_workflow.models import (
    CoordinateSpace,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
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
