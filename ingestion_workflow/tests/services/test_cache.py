from __future__ import annotations

import shutil
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
    result = index_legacy_downloads(settings, DownloadSource.ACE.value, source)
    index = result.index

    html_files = sorted(source.rglob("*.html"))
    assert index.count() == len(html_files)

    sample_file = html_files[0]
    entry = next(
        (entry for entry in index.iter_entries() if entry.result.identifier.pmid == sample_file.stem),
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
    result = index_legacy_downloads(settings, DownloadSource.PUBGET.value, source)
    index = result.index

    article_dirs = [path for path in source.rglob("pmcid_*") if path.is_dir()]
    assert index.count() == len(article_dirs)

    entry = next(
        (entry for entry in index.iter_entries() if entry.result.identifier.pmcid == "PMC9056519"),
        None,
    )
    assert entry is not None
    result = entry.result
    names = {file.file_path.name for file in result.files}
    assert "article.xml" in names
    assert "table_000.csv" in names


def test_index_legacy_downloads_pubget_nested_structure(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    source = tmp_path / "pubget_nested"
    article_dir = (
        source
        / "original"
        / "fmri_journal"
        / "query_abc123"
        / "articles"
        / "bucket_a"
        / "nested_dir"
    )
    article_dir.mkdir(parents=True)
    article_xml = article_dir / "article.xml"
    article_xml.write_text(
        """
        <article>
          <front>
            <article-meta>
              <article-id pub-id-type=\"pmcid\">9056519</article-id>
            </article-meta>
          </front>
        </article>
        """.strip()
    )
    (article_dir / "fulltext.pdf").write_bytes(b"pdf")

    result = index_legacy_downloads(settings, DownloadSource.PUBGET.value, source)
    index = result.index
    assert index.count() == 1
    entry = next(index.iter_entries())
    assert entry.result.identifier.pmcid == "PMC9056519"
    names = {file.file_path.name for file in entry.result.files}
    assert "article.xml" in names
    assert "fulltext.pdf" in names


def test_index_legacy_downloads_update_existing(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    source = tmp_path / "pubget_source"
    shutil.copytree(DATA_DIR / "test_pubget", source)

    initial = index_legacy_downloads(settings, DownloadSource.PUBGET.value, source)
    index = initial.index
    assert index.count() > 0
    entry = next(index.iter_entries())
    target_dir = entry.result.files[0].file_path.parent
    new_file = target_dir / "new_payload.bin"
    new_file.write_bytes(b"new payload")

    updated = index_legacy_downloads(
        settings,
        DownloadSource.PUBGET.value,
        source,
        update_existing=True,
    )
    updated_entry = next(updated.index.iter_entries())
    names = {file.file_path.name for file in updated_entry.result.files}

    assert updated.added == 0
    assert updated.updated >= 1
    assert "new_payload.bin" in names
