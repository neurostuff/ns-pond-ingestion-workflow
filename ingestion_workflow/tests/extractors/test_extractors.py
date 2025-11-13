import json
import shutil
from pathlib import Path

import pytest

from pubget._typing import ExitCode
from pubget._utils import article_bucket_from_pmcid

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    FileType,
    Identifier,
    Identifiers,
)


@pytest.mark.usefixtures("manifest_identifiers")
@pytest.mark.vcr()
def test_elsevier_download_records_articles(tmp_path, manifest_identifiers):
    subset = Identifiers(list(manifest_identifiers.identifiers[:100]))

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )

    if not settings.elsevier_api_key:
        pytest.skip("Elsevier API key not configured for recording")

    extractor = ElsevierExtractor(settings=settings)

    results = extractor.download(subset)

    assert len(results) == len(subset.identifiers)
    successes = [result for result in results if result.success]
    assert successes, "Expected at least one successful Elsevier download"

    for index, result in enumerate(results):
        identifier = subset.identifiers[index]
        assert result.identifier is identifier
        assert result.source is DownloadSource.ELSEVIER

        if result.success:
            assert result.files, "Successful downloads should persist files"

            metadata_file = next(file for file in result.files if file.file_type is FileType.JSON)
            metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))

            expected_lookup_type = "doi" if identifier.doi else "pmid"
            expected_lookup_value = identifier.doi or identifier.pmid

            assert metadata["lookup_type"] == expected_lookup_type
            assert metadata["lookup_value"] == expected_lookup_value
            assert metadata["identifier_hash"] == identifier.hash_id
        else:
            assert not result.files


def test_pubget_download_persists_article_and_tables(tmp_path, monkeypatch):
    identifier = Identifier(pmcid="PMC12345")
    identifiers = Identifiers([identifier])

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        max_workers=3,
        pubget_cache_root=tmp_path / "pubget_cache",
    )

    extractor = PubgetExtractor(settings=settings)

    data_dir = settings.pubget_cache_root
    assert data_dir is not None
    data_dir.mkdir(parents=True, exist_ok=True)
    download_dir = data_dir / "pmcidList_demo" / "articlesets"
    download_dir.mkdir(parents=True)
    articles_dir = download_dir.with_name("articles")
    bucket = article_bucket_from_pmcid(12345)
    article_dir = articles_dir / bucket / "pmcid_12345"
    article_dir.mkdir(parents=True)
    article_dir.joinpath("article.xml").write_text(
        "<article />",
        encoding="utf-8",
    )
    tables_dir = article_dir / "tables"
    tables_dir.mkdir(parents=True)
    tables_dir.joinpath("tables.xml").write_text(
        "<tables />",
        encoding="utf-8",
    )
    tables_dir.joinpath("table_000_info.json").write_text(
        json.dumps(
            {
                "table_id": "tbl1",
                "table_label": "Table 1",
                "table_caption": "",
                "table_foot": "",
                "n_header_rows": 1,
                "table_data_file": "table_000.csv",
            }
        ),
        encoding="utf-8",
    )
    tables_dir.joinpath("table_000.csv").write_text(
        "x,y,z\n1,2,3\n",
        encoding="utf-8",
    )

    captured: dict[str, tuple] = {}

    def fake_download(
        pmcids: list[int],
        *,
        data_dir: Path,
        api_key: str | None,
        retmax: int,
    ) -> tuple[Path, ExitCode]:
        captured["download"] = (pmcids, data_dir, api_key, retmax)
        return download_dir, ExitCode.COMPLETED

    def fake_extract(articlesets_dir: Path, *, n_jobs: int) -> tuple[Path, ExitCode]:
        captured["extract"] = (articlesets_dir, n_jobs)
        return articles_dir, ExitCode.COMPLETED

    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.download_pmcids",
        fake_download,
    )
    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.extract_articles",
        fake_extract,
    )

    results = extractor.download(identifiers)

    assert captured["download"] == (
        [12345],
        data_dir,
        settings.pubmed_api_key,
        settings.pubmed_batch_size,
    )
    assert captured["extract"] == (download_dir, 3)

    assert len(results) == 1
    result = results[0]
    assert result.identifier is identifier
    assert result.success is True
    assert result.source is DownloadSource.PUBGET
    assert result.error_message is None

    paths = {file.file_path for file in result.files}
    assert article_dir.joinpath("article.xml") in paths
    assert tables_dir.joinpath("tables.xml") in paths
    assert tables_dir.joinpath("table_000_info.json") in paths
    assert tables_dir.joinpath("table_000.csv") in paths


def test_pubget_download_warns_on_incomplete_exit_codes(tmp_path, monkeypatch):
    identifier = Identifier(pmcid="PMC987")
    identifiers = Identifiers([identifier])

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        max_workers=2,
        pubget_cache_root=tmp_path / "pubget_cache",
    )

    extractor = PubgetExtractor(settings=settings)

    data_dir = settings.pubget_cache_root
    assert data_dir is not None
    data_dir.mkdir(parents=True, exist_ok=True)
    download_dir = data_dir / "pmcidList_warn" / "articlesets"
    download_dir.mkdir(parents=True)
    articles_dir = download_dir.with_name("articles")
    bucket = article_bucket_from_pmcid(987)
    article_dir = articles_dir / bucket / "pmcid_987"
    article_dir.mkdir(parents=True)
    article_dir.joinpath("article.xml").write_text(
        "<article />",
        encoding="utf-8",
    )
    tables_dir = article_dir / "tables"
    tables_dir.mkdir(parents=True)
    tables_dir.joinpath("tables.xml").write_text(
        "<tables />",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.download_pmcids",
        lambda *_, **__: (download_dir, ExitCode.INCOMPLETE),
    )
    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.extract_articles",
        lambda *_args, **_kwargs: (articles_dir, ExitCode.INCOMPLETE),
    )

    results = extractor.download(identifiers)

    assert len(results) == 1
    result = results[0]
    assert result.success is True
    assert result.error_message is not None
    assert "incomplete" in result.error_message.lower()


def test_pubget_extract_translates_tables(tmp_path):
    fixture_root = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "test_pubget"
        / "articles"
        / "1a4"
        / "pmcid_9056519"
    )
    article_dir = tmp_path / "pmcid_9056519"
    shutil.copytree(fixture_root, article_dir)

    identifier = Identifier(pmcid="PMC9056519")

    def _downloaded(
        path: Path,
        file_type: FileType,
        content: str,
    ) -> DownloadedFile:
        return DownloadedFile(
            file_path=path,
            file_type=file_type,
            content_type=content,
            source=DownloadSource.PUBGET,
        )

    tables_dir = article_dir / "tables"
    download_files = [
        _downloaded(
            article_dir / "article.xml",
            FileType.XML,
            "application/xml",
        ),
        _downloaded(
            tables_dir / "tables.xml",
            FileType.XML,
            "application/xml",
        ),
    ]

    for info_path in sorted(tables_dir.glob("table_*_info.json")):
        download_files.append(_downloaded(info_path, FileType.JSON, "application/json"))
    for csv_path in sorted(tables_dir.glob("table_*.csv")):
        download_files.append(_downloaded(csv_path, FileType.CSV, "text/csv"))

    download_result = DownloadResult(
        identifier=identifier,
        source=DownloadSource.PUBGET,
        success=True,
        files=download_files,
    )

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        pubget_cache_root=tmp_path / "pubget_cache",
        max_workers=1,
    )
    extractor = PubgetExtractor(settings=settings)

    results = extractor.extract([download_result])

    assert len(results) == 1
    content = results[0]
    assert content.error_message is None
    assert content.full_text_path is not None
    assert content.full_text_path.exists()
    assert len(content.tables) == 3
    assert content.has_coordinates is True

    tables_output_dir = content.tables[0].raw_content_path.parent
    manifest_path = tables_output_dir / "table_sources.json"
    assert manifest_path.exists()

    sources = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert set(sources) == {"tbl0005", "tbl0010", "tbl0015"}
    for table_id, payload in sources.items():
        info_path = Path(payload["info_path"])
        data_path = Path(payload["data_path"])
        coords_path = Path(payload["coordinates_path"])
        assert info_path.exists()
        assert data_path.exists()
        assert coords_path.exists()
        table_file = tables_output_dir / f"{table_id}.xml"
        assert table_file.exists()
        assert "<original-table" in table_file.read_text(encoding="utf-8")

    coordinate_tables = {table.table_id: table for table in content.tables}
    assert coordinate_tables["tbl0015"].coordinates


def test_pubget_extract_reports_missing_metadata(tmp_path):
    article_dir = tmp_path / "pmcid_1"
    tables_dir = article_dir / "tables"
    tables_dir.mkdir(parents=True)
    article_dir.joinpath("article.xml").write_text(
        "<article><front/><body/></article>",
        encoding="utf-8",
    )
    tables_dir.joinpath("tables.xml").write_text(
        (
            "<extracted-tables-set>"
            "<extracted-table>"
            "<table-id>T1</table-id>"
            "<table-label>Table 1</table-label>"
            "<original-table><table-wrap/></original-table>"
            "</extracted-table>"
            "</extracted-tables-set>"
        ),
        encoding="utf-8",
    )

    identifier = Identifier(pmcid="PMC1")
    download_files = [
        DownloadedFile(
            file_path=article_dir / "article.xml",
            file_type=FileType.XML,
            content_type="application/xml",
            source=DownloadSource.PUBGET,
        ),
        DownloadedFile(
            file_path=tables_dir / "tables.xml",
            file_type=FileType.XML,
            content_type="application/xml",
            source=DownloadSource.PUBGET,
        ),
    ]

    download_result = DownloadResult(
        identifier=identifier,
        source=DownloadSource.PUBGET,
        success=True,
        files=download_files,
    )

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        max_workers=1,
    )
    extractor = PubgetExtractor(settings=settings)

    results = extractor.extract([download_result])

    assert len(results) == 1
    content = results[0]
    assert content.tables == []
    assert content.error_message is not None
    assert "metadata not found" in content.error_message.lower()
    assert content.full_text_path is not None
    assert content.full_text_path.exists()
