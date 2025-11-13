import json
from types import SimpleNamespace

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifier,
    Identifiers,
)


def _make_fake_article(*, doi=None, pmid=None, identifier_type="doi"):
    identifier_value = doi if identifier_type == "doi" else pmid
    metadata = {
        "identifier_lookup": {"doi": doi, "pmid": pmid},
        "identifier_type": identifier_type,
        "identifier": identifier_value,
    }
    return SimpleNamespace(
        payload=b"<xml></xml>",
        content_type="application/xml",
        format="xml",
        metadata=metadata,
    )


def test_elsevier_download_mixed_success(monkeypatch, tmp_path):
    identifiers = Identifiers(
        [
            Identifier(doi="10.1234/failed"),
            Identifier(pmid="123456"),
            Identifier(doi="10.1234/success"),
        ]
    )

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )

    extractor = ElsevierExtractor(settings=settings)

    def fake_run_download(self, records, progress_hook=None):
        assert len(records) == 3
        articles = [
            _make_fake_article(pmid="123456", identifier_type="pmid"),
            _make_fake_article(doi="10.1234/success", identifier_type="doi"),
        ]
        if progress_hook:
            for _ in articles:
                progress_hook(1)
        return articles

    monkeypatch.setattr(ElsevierExtractor, "_run_download", fake_run_download)

    results = extractor.download(identifiers)

    assert len(results) == 3

    first, second, third = results

    assert first.identifier is identifiers.identifiers[0]
    assert not first.success
    assert first.source is DownloadSource.ELSEVIER
    assert not first.files
    assert first.error_message

    assert second.identifier is identifiers.identifiers[1]
    assert second.success
    assert second.source is DownloadSource.ELSEVIER
    assert second.files
    metadata_file = next(file for file in second.files if file.file_type is FileType.JSON)
    metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))
    assert metadata["lookup_type"] == "pmid"
    assert metadata["lookup_value"] == "123456"
    assert metadata["identifier_slug"] == second.identifier.slug

    assert third.identifier is identifiers.identifiers[2]
    assert third.success
    metadata_file = next(file for file in third.files if file.file_type is FileType.JSON)
    metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))
    assert metadata["lookup_type"] == "doi"
    assert metadata["lookup_value"] == "10.1234/success"
    assert metadata["identifier_slug"] == third.identifier.slug
