from __future__ import annotations

from pathlib import Path

from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource, Identifier
from ingestion_workflow.services.cache import index_legacy_downloads


DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns",
    )


def test_index_legacy_downloads_ace(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    source = DATA_DIR / "test_html"
    index = index_legacy_downloads(settings, DownloadSource.ACE.value, source)

    html_files = sorted(source.rglob("*.html"))
    assert len(index.entries) == len(html_files)

    sample_file = html_files[0]
    entry = next(
        (entry for entry in index.entries.values() if entry.result.identifier.pmid == sample_file.stem),
        None,
    )
    assert entry is not None
    result = entry.result
    assert result.success is True
    assert result.source is DownloadSource.ACE
    assert result.files[0].file_path == sample_file


def test_index_legacy_downloads_pubget(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    source = DATA_DIR / "test_pubget"
    index = index_legacy_downloads(settings, DownloadSource.PUBGET.value, source)

    article_dirs = [path for path in source.rglob("pmcid_*") if path.is_dir()]
    assert len(index.entries) == len(article_dirs)

    entry = next(
        (entry for entry in index.entries.values() if entry.result.identifier.pmcid == "PMC9056519"),
        None,
    )
    assert entry is not None
    result = entry.result
    names = {file.file_path.name for file in result.files}
    assert "article.xml" in names
    assert "table_000.csv" in names
